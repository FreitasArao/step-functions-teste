import { Injectable, Logger } from '@nestjs/common';
import type {
  OrderWorkflowPort,
  StartSagaPayload,
  WorkflowExecutionSummary,
} from '../../modules/order/application/ports/order-workflow.outbound-port.js';
import type { DomainError } from '../../shared/core/domain-error.js';
import { Result } from '../../shared/core/result.js';
import type { MessageWrapper } from '../../shared/messaging/message-wrapper.js';
import { TemporalConnectionService } from './temporal-client.provider.js';
import { TemporalLocalEngineService } from './temporal-local-engine.service.js';
import {
  TEMPORAL_ORDER_SAGA_TASK_QUEUE,
  TemporalAclTranslator,
} from './translators/temporal-acl.translator.js';
import { orderSagaWorkflow } from './workflows/order-saga.workflow.js';

export class TemporalInfraError implements DomainError {
  readonly code = 'TEMPORAL_INFRA_ERROR';
  readonly message: string;
  readonly retryable = true;

  constructor(operation: string, cause: unknown) {
    const detail = cause instanceof Error ? cause.message : String(cause);
    this.message = `Temporal operation '${operation}' failed: ${detail}`;
  }
}

/**
 * Infrastructure Adapter implementing the agnostic OrderWorkflowPort using Temporal.io:
 * - WorkflowClient.start(orderSagaWorkflow)
 * - AsyncCompletionClient.complete(taskToken, output)
 * - AsyncCompletionClient.fail(taskToken, ApplicationFailure)
 * - WorkflowHandle.describe()
 */
@Injectable()
export class TemporalOrderWorkflowAdapter implements OrderWorkflowPort {
  private readonly logger = new Logger(TemporalOrderWorkflowAdapter.name);
  private readonly lastFailureByOrder = new Map<
    string,
    { code: string; reason: string }
  >();

  constructor(
    private readonly temporalConnection: TemporalConnectionService,
    private readonly translator: TemporalAclTranslator,
    private readonly localEngine: TemporalLocalEngineService,
  ) {}

  async startOrderWorkflow(
    wrapper: MessageWrapper<StartSagaPayload>,
  ): Promise<Result<{ workflowInstanceId: string }, DomainError>> {
    const workflowClient = this.temporalConnection.getWorkflowClient();

    if (this.temporalConnection.isTemporalServerActive() && workflowClient) {
      try {
        const startInput = this.translator.toStartWorkflowInput(
          TEMPORAL_ORDER_SAGA_TASK_QUEUE,
          wrapper,
        );
        const handle = await workflowClient.start(orderSagaWorkflow, startInput);
        const workflowInstanceId = `${handle.workflowId}:${handle.firstExecutionRunId}`;
        this.logger.log(
          `[TemporalAdapter] Workflow started -> workflowId=${handle.workflowId}, runId=${handle.firstExecutionRunId}`,
        );
        return Result.ok({ workflowInstanceId });
      } catch (err) {
        this.logger.warn(
          `[TemporalAdapter] Live Temporal start failed (${err instanceof Error ? err.message : String(err)}), falling back to local engine.`,
        );
      }
    }

    const fallbackExec = this.localEngine.startLocalWorkflowExecution(wrapper);
    this.logger.log(
      `[TemporalAdapter] Started workflow execution -> workflowInstanceId=${fallbackExec.workflowInstanceId}`,
    );
    return Result.ok(fallbackExec);
  }

  async completeWorkflowStep(params: {
    correlationToken: string;
    output: unknown;
  }): Promise<Result<void, DomainError>> {
    if (this.localEngine.isLocalTaskToken(params.correlationToken)) {
      this.localEngine.handleAsyncActivityComplete(
        params.correlationToken,
        params.output,
      );
      return Result.ok(undefined);
    }

    const asyncClient = this.temporalConnection.getAsyncCompletionClient();
    if (!asyncClient || !this.temporalConnection.isTemporalServerActive()) {
      this.localEngine.handleAsyncActivityComplete(
        params.correlationToken,
        params.output,
      );
      return Result.ok(undefined);
    }

    try {
      const { taskToken, result } =
        this.translator.toAsyncActivityCompleteInput(params);
      await asyncClient.complete(taskToken, result);
      this.logger.log(
        `[TemporalAdapter] AsyncCompletionClient.complete sent to Temporal (token=${params.correlationToken.slice(0, 16)}...)`,
      );
      return Result.ok(undefined);
    } catch (err) {
      this.logger.error(
        `[TemporalAdapter] AsyncCompletionClient.complete failed: ${err instanceof Error ? err.message : String(err)}`,
      );
      return Result.err(
        new TemporalInfraError('AsyncCompletionClient.complete', err),
      );
    }
  }

  async failWorkflowStep(params: {
    correlationToken: string;
    error: DomainError;
  }): Promise<Result<void, DomainError>> {
    if (this.localEngine.isLocalTaskToken(params.correlationToken)) {
      this.localEngine.handleAsyncActivityFailure(
        params.correlationToken,
        params.error,
      );
      return Result.ok(undefined);
    }

    const asyncClient = this.temporalConnection.getAsyncCompletionClient();
    if (!asyncClient || !this.temporalConnection.isTemporalServerActive()) {
      this.localEngine.handleAsyncActivityFailure(
        params.correlationToken,
        params.error,
      );
      return Result.ok(undefined);
    }

    try {
      const { taskToken, failure } =
        this.translator.toAsyncActivityFailureInput(params);
      await asyncClient.fail(taskToken, failure);
      this.logger.warn(
        `[TemporalAdapter] AsyncCompletionClient.fail sent to Temporal (error=${params.error.code}, token=${params.correlationToken.slice(0, 16)}...)`,
      );
      return Result.ok(undefined);
    } catch (err) {
      this.logger.error(
        `[TemporalAdapter] AsyncCompletionClient.fail failed: ${err instanceof Error ? err.message : String(err)}`,
      );
      return Result.err(
        new TemporalInfraError('AsyncCompletionClient.fail', err),
      );
    }
  }

  async getWorkflowStatus(
    orderId: string,
  ): Promise<Result<WorkflowExecutionSummary, DomainError>> {
    const localSummary = this.localEngine.getExecutionSummary(orderId);
    if (localSummary) {
      return Result.ok(localSummary);
    }

    const workflowClient = this.temporalConnection.getWorkflowClient();
    if (!workflowClient || !this.temporalConnection.isTemporalServerActive()) {
      return Result.ok({ status: 'NOT_STARTED' });
    }

    const workflowId = `order-saga-${orderId}`;
    try {
      const handle = workflowClient.getHandle(workflowId);
      const description = await handle.describe();
      const failureMeta = this.lastFailureByOrder.get(orderId) ?? {
        code: 'ORDER_SAGA_ROLLED_BACK',
        reason: 'Order saga failed and all compensations were executed.',
      };
      return Result.ok(
        this.translator.toWorkflowExecutionSummary(
          workflowId,
          description,
          description.status.name === 'FAILED' ? failureMeta : undefined,
        ),
      );
    } catch {
      return Result.ok({
        status: 'NOT_STARTED',
      });
    }
  }
}
