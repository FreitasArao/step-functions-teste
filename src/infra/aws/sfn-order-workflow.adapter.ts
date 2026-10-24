import {
  DescribeExecutionCommand,
  ListStateMachinesCommand,
  type SFNClient,
  SendTaskFailureCommand,
  SendTaskSuccessCommand,
  StartExecutionCommand,
} from '@aws-sdk/client-sfn';
import { Inject, Injectable } from '@nestjs/common';
import type {
  OrderWorkflowPort,
  StartSagaPayload,
  StepFunctionsExecutionSummary,
} from '../../modules/order/domain/order-workflow.port.js';
import type { DomainError } from '../../shared/core/domain-error.js';
import { Result } from '../../shared/core/result.js';
import type { MessageWrapper } from '../../shared/messaging/message-wrapper.js';
import { AslLocalEngineService } from './asl-local-engine.service.js';
import { SFN_CLIENT } from './aws-clients.provider.js';

export class StepFunctionsInfraError implements DomainError {
  readonly code = 'STEP_FUNCTIONS_INFRA_ERROR';
  readonly message: string;
  readonly retryable = true;

  constructor(operation: string, cause: unknown) {
    const detail = cause instanceof Error ? cause.message : String(cause);
    this.message = `Step Functions operation '${operation}' failed: ${detail}`;
  }
}

@Injectable()
export class StepFunctionsOrderWorkflowAdapter implements OrderWorkflowPort {
  private cachedStateMachineArn?: string;
  private readonly executionArnByOrder = new Map<string, string>();

  constructor(
    @Inject(SFN_CLIENT)
    private readonly sfnClient: SFNClient,
    private readonly aslLocalEngine: AslLocalEngineService,
  ) {}

  private shouldUseLocalEngine(): boolean {
    return process.env.USE_LOCAL_ASL_ENGINE === 'true';
  }

  private async resolveStateMachineArn(): Promise<
    Result<string, DomainError>
  > {
    if (process.env.SFN_STATE_MACHINE_ARN) {
      return Result.ok(process.env.SFN_STATE_MACHINE_ARN);
    }
    if (this.cachedStateMachineArn) {
      return Result.ok(this.cachedStateMachineArn);
    }

    try {
      const listResponse = await this.sfnClient.send(
        new ListStateMachinesCommand({}),
      );
      const found = listResponse.stateMachines?.find(
        (sm) => sm.name === 'order-saga-state-machine',
      );
      if (found?.stateMachineArn) {
        this.cachedStateMachineArn = found.stateMachineArn;
        return Result.ok(found.stateMachineArn);
      }
      return Result.err(
        new StepFunctionsInfraError(
          'ListStateMachines',
          'order-saga-state-machine not found on MiniStack',
        ),
      );
    } catch (err) {
      return Result.err(
        new StepFunctionsInfraError('ListStateMachines', err),
      );
    }
  }

  async startSagaExecution(
    wrapper: MessageWrapper<StartSagaPayload>,
  ): Promise<Result<{ executionArn: string }, DomainError>> {
    if (this.shouldUseLocalEngine()) {
      const localExec = this.aslLocalEngine.startExecution(wrapper);
      this.executionArnByOrder.set(
        wrapper.payload.orderId,
        localExec.executionArn,
      );
      return Result.ok(localExec);
    }

    const arnResult = await this.resolveStateMachineArn();
    if (arnResult.isOk()) {
      try {
        const response = await this.sfnClient.send(
          new StartExecutionCommand({
            stateMachineArn: arnResult.value,
            name: wrapper.payload.orderId,
            input: JSON.stringify(wrapper),
          }),
        );
        const executionArn =
          response.executionArn ??
          `${arnResult.value.replace(':stateMachine:', ':execution:')}:${wrapper.payload.orderId}`;
        this.executionArnByOrder.set(wrapper.payload.orderId, executionArn);
        return Result.ok({ executionArn });
      } catch {
        // Fallback to local ASL engine if MiniStack endpoint is unreachable
      }
    }

    const fallbackExec = this.aslLocalEngine.startExecution(wrapper);
    this.executionArnByOrder.set(
      wrapper.payload.orderId,
      fallbackExec.executionArn,
    );
    return Result.ok(fallbackExec);
  }

  async sendTaskSuccess(params: {
    taskToken: string;
    output: unknown;
  }): Promise<Result<void, DomainError>> {
    if (
      this.shouldUseLocalEngine() ||
      params.taskToken.startsWith('task_token_')
    ) {
      this.aslLocalEngine.handleSendTaskSuccess(
        params.taskToken,
        params.output,
      );
      return Result.ok(undefined);
    }

    try {
      await this.sfnClient.send(
        new SendTaskSuccessCommand({
          taskToken: params.taskToken,
          output: JSON.stringify(params.output ?? {}),
        }),
      );
      return Result.ok(undefined);
    } catch (err) {
      return Result.err(new StepFunctionsInfraError('SendTaskSuccess', err));
    }
  }

  async sendTaskFailure(params: {
    taskToken: string;
    error: DomainError;
  }): Promise<Result<void, DomainError>> {
    if (
      this.shouldUseLocalEngine() ||
      params.taskToken.startsWith('task_token_')
    ) {
      this.aslLocalEngine.handleSendTaskFailure(params.taskToken, params.error);
      return Result.ok(undefined);
    }

    try {
      await this.sfnClient.send(
        new SendTaskFailureCommand({
          taskToken: params.taskToken,
          error: params.error.code,
          cause: JSON.stringify({
            code: params.error.code,
            message: params.error.message,
            retryable: params.error.retryable ?? false,
            metadata: params.error.metadata ?? {},
          }),
        }),
      );
      return Result.ok(undefined);
    } catch (err) {
      return Result.err(new StepFunctionsInfraError('SendTaskFailure', err));
    }
  }

  async getExecutionStatus(
    orderId: string,
  ): Promise<Result<StepFunctionsExecutionSummary, DomainError>> {
    const localSummary = this.aslLocalEngine.getExecutionSummary(orderId);
    if (localSummary) {
      return Result.ok(localSummary);
    }

    const executionArn = this.executionArnByOrder.get(orderId);
    if (!executionArn) {
      return Result.ok({ status: 'NOT_STARTED' });
    }

    try {
      const desc = await this.sfnClient.send(
        new DescribeExecutionCommand({ executionArn }),
      );
      return Result.ok({
        executionArn,
        status:
          (desc.status as StepFunctionsExecutionSummary['status']) ?? 'RUNNING',
        startDate: desc.startDate?.toISOString(),
        stopDate: desc.stopDate?.toISOString(),
        ...(desc.error ? { error: desc.error } : {}),
        ...(desc.cause ? { cause: desc.cause } : {}),
      });
    } catch {
      return Result.ok({
        executionArn,
        status: 'RUNNING',
      });
    }
  }
}
