import { Injectable, Logger } from '@nestjs/common';
import {
  ActivityNotFoundError,
  WorkflowExecutionAlreadyStartedError,
} from '@temporalio/client';
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
 * Infrastructure Adapter implementing the agnostic OrderWorkflowPort using Temporal.io.
 * Called exclusively by the Outbox Relay, therefore every operation is an IDEMPOTENT RECEIVER:
 * - startOrderWorkflow   -> deterministic workflowId + REJECT_DUPLICATE; "already started" == success
 * - completeWorkflowStep -> AsyncCompletionClient.complete; unknown/stale token == no-op success
 * - failWorkflowStep     -> AsyncCompletionClient.fail;     unknown/stale token == no-op success
 * Any other failure returns Result.err so the Outbox row stays PENDING and is retried.
 */
@Injectable()
export class TemporalOrderWorkflowAdapter implements OrderWorkflowPort {
  private readonly logger = new Logger(TemporalOrderWorkflowAdapter.name);

  constructor(
    private readonly temporalConnection: TemporalConnectionService,
    private readonly translator: TemporalAclTranslator,
    private readonly localEngine: TemporalLocalEngineService,
  ) {}

  async startOrderWorkflow(
    wrapper: MessageWrapper<StartSagaPayload>,
  ): Promise<Result<{ workflowInstanceId: string }, DomainError>> {
    if (this.temporalConnection.isLocalEngineMode()) {
      return Result.ok(this.localEngine.startLocalWorkflowExecution(wrapper));
    }

    const workflowClient = await this.temporalConnection.getWorkflowClient();
    if (!workflowClient) {
      return Result.err(
        new TemporalInfraError('WorkflowClient.start', 'Temporal Server not connected'),
      );
    }

    const startInput = this.translator.toStartWorkflowInput(
      TEMPORAL_ORDER_SAGA_TASK_QUEUE,
      wrapper,
    );

    try {
      const handle = await workflowClient.start(orderSagaWorkflow, startInput);
      this.logger.log(
        `[TemporalAdapter] Workflow started -> workflowId=${handle.workflowId}, runId=${handle.firstExecutionRunId}`,
      );
      return Result.ok({
        workflowInstanceId: `${handle.workflowId}:${handle.firstExecutionRunId}`,
      });
    } catch (err) {
      if (err instanceof WorkflowExecutionAlreadyStartedError) {
        this.logger.warn(
          `[TemporalAdapter] Workflow '${startInput.workflowId}' already started — duplicate Outbox delivery acknowledged (idempotent).`,
        );
        return Result.ok({ workflowInstanceId: startInput.workflowId });
      }
      return Result.err(new TemporalInfraError('WorkflowClient.start', err));
    }
  }

  async completeWorkflowStep(params: {
    correlationToken: string;
    output: unknown;
  }): Promise<Result<void, DomainError>> {
    if (this.temporalConnection.isLocalEngineMode()) {
      this.localEngine.handleAsyncActivityComplete(
        params.correlationToken,
        params.output,
      );
      return Result.ok(undefined);
    }

    const asyncClient = await this.temporalConnection.getAsyncCompletionClient();
    if (!asyncClient) {
      return Result.err(
        new TemporalInfraError(
          'AsyncCompletionClient.complete',
          'Temporal Server not connected',
        ),
      );
    }

    try {
      const { taskToken, result } =
        this.translator.toAsyncActivityCompleteInput(params);
      await asyncClient.complete(taskToken, result);
      this.logger.log(
        `[TemporalAdapter] AsyncCompletionClient.complete sent (token=${params.correlationToken.slice(0, 16)}...)`,
      );
      return Result.ok(undefined);
    } catch (err) {
      return this.handleCompletionError('AsyncCompletionClient.complete', err);
    }
  }

  async failWorkflowStep(params: {
    correlationToken: string;
    error: DomainError;
  }): Promise<Result<void, DomainError>> {
    if (this.temporalConnection.isLocalEngineMode()) {
      this.localEngine.handleAsyncActivityFailure(
        params.correlationToken,
        params.error,
      );
      return Result.ok(undefined);
    }

    const asyncClient = await this.temporalConnection.getAsyncCompletionClient();
    if (!asyncClient) {
      return Result.err(
        new TemporalInfraError(
          'AsyncCompletionClient.fail',
          'Temporal Server not connected',
        ),
      );
    }

    try {
      const { taskToken, failure } =
        this.translator.toAsyncActivityFailureInput(params);
      await asyncClient.fail(taskToken, failure);
      this.logger.warn(
        `[TemporalAdapter] AsyncCompletionClient.fail sent (error=${params.error.code}, token=${params.correlationToken.slice(0, 16)}...)`,
      );
      return Result.ok(undefined);
    } catch (err) {
      return this.handleCompletionError('AsyncCompletionClient.fail', err);
    }
  }

  /**
   * ActivityNotFoundError means the token belongs to an attempt that is already closed
   * (completed by a previous delivery, or timed out and superseded by a retry whose Inbox
   * replay produced a NEW outbox message with the new token). Either way: idempotent no-op.
   */
  private handleCompletionError(
    operation: string,
    err: unknown,
  ): Result<void, DomainError> {
    if (err instanceof ActivityNotFoundError) {
      this.logger.warn(
        `[TemporalAdapter] ${operation}: activity attempt already closed (stale/duplicate token) — acknowledged as idempotent no-op.`,
      );
      return Result.ok(undefined);
    }
    this.logger.error(
      `[TemporalAdapter] ${operation} failed: ${err instanceof Error ? err.message : String(err)}`,
    );
    return Result.err(new TemporalInfraError(operation, err));
  }

  async getWorkflowStatus(
    orderId: string,
  ): Promise<Result<WorkflowExecutionSummary, DomainError>> {
    if (this.temporalConnection.isLocalEngineMode()) {
      return Result.ok(
        this.localEngine.getExecutionSummary(orderId) ?? { status: 'NOT_STARTED' },
      );
    }

    const workflowClient = await this.temporalConnection.getWorkflowClient();
    if (!workflowClient) {
      return Result.ok({ status: 'NOT_STARTED' });
    }

    const workflowId = `order-saga-${orderId}`;
    try {
      const description = await workflowClient.getHandle(workflowId).describe();
      return Result.ok(
        this.translator.toWorkflowExecutionSummary(
          workflowId,
          description,
          description.status.name === 'FAILED'
            ? {
                code: 'ORDER_SAGA_ROLLED_BACK',
                reason: 'Order saga failed and all compensations were executed.',
              }
            : undefined,
        ),
      );
    } catch {
      return Result.ok({ status: 'NOT_STARTED' });
    }
  }
}
