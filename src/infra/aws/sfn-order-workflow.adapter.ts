import {
  DescribeExecutionCommand,
  ListStateMachinesCommand,
  type SFNClient,
  SendTaskFailureCommand,
  SendTaskSuccessCommand,
  StartExecutionCommand,
} from '@aws-sdk/client-sfn';
import { Inject, Injectable, Logger } from '@nestjs/common';
import type {
  OrderWorkflowPort,
  StartSagaPayload,
  WorkflowExecutionSummary,
} from '../../modules/order/application/ports/order-workflow.outbound-port.js';
import type { DomainError } from '../../shared/core/domain-error.js';
import { Result } from '../../shared/core/result.js';
import type { MessageWrapper } from '../../shared/messaging/message-wrapper.js';
import { AslLocalEngineService } from './asl-local-engine.service.js';
import { SFN_CLIENT } from './aws-clients.provider.js';
import { AwsStepFunctionsTranslator } from './translators/sfn-acl.translator.js';

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
  private readonly logger = new Logger(StepFunctionsOrderWorkflowAdapter.name);
  private cachedStateMachineArn?: string;
  private readonly executionArnByOrder = new Map<string, string>();

  constructor(
    @Inject(SFN_CLIENT)
    private readonly sfnClient: SFNClient,
    private readonly translator: AwsStepFunctionsTranslator,
    private readonly aslLocalEngine: AslLocalEngineService,
  ) {}

  private shouldUseLocalEngine(): boolean {
    return (
      process.env.USE_LOCAL_ASL_ENGINE === 'true' ||
      process.env.NODE_ENV === 'test'
    );
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
          "order-saga-state-machine not found on LocalStack (run 'cd infra && terraform apply -auto-approve')",
        ),
      );
    } catch (err) {
      return Result.err(new StepFunctionsInfraError('ListStateMachines', err));
    }
  }

  async startOrderWorkflow(
    wrapper: MessageWrapper<StartSagaPayload>,
  ): Promise<Result<{ workflowInstanceId: string }, DomainError>> {
    const arnResult = await this.resolveStateMachineArn();
    if (arnResult.isOk()) {
      try {
        const commandInput = this.translator.toStartExecutionInput(
          arnResult.value,
          wrapper,
        );
        const response = await this.sfnClient.send(
          new StartExecutionCommand(commandInput),
        );
        const executionArn =
          response.executionArn ??
          `${arnResult.value.replace(':stateMachine:', ':execution:')}:${wrapper.payload.orderId}`;
        this.executionArnByOrder.set(wrapper.payload.orderId, executionArn);
        this.logger.log(
          `[StepFunctionsAdapter] StartExecution succeeded -> executionArn=${executionArn}`,
        );
        return Result.ok({ workflowInstanceId: executionArn });
      } catch (err) {
        if (!this.shouldUseLocalEngine()) {
          this.logger.error(
            `[StepFunctionsAdapter] StartExecution failed for orderId=${wrapper.payload.orderId}: ${err instanceof Error ? err.message : String(err)}`,
          );
          return Result.err(
            new StepFunctionsInfraError('StartExecution', err),
          );
        }
      }
    } else if (!this.shouldUseLocalEngine()) {
      this.logger.error(
        `[StepFunctionsAdapter] ${arnResult.error.message}`,
      );
      return Result.err(arnResult.error);
    }

    const fallbackExec = this.aslLocalEngine.startExecution(wrapper);
    this.executionArnByOrder.set(
      wrapper.payload.orderId,
      fallbackExec.workflowInstanceId,
    );
    return Result.ok(fallbackExec);
  }

  async completeWorkflowStep(params: {
    correlationToken: string;
    output: unknown;
  }): Promise<Result<void, DomainError>> {
    if (params.correlationToken.startsWith('task_token_')) {
      this.aslLocalEngine.handleSendTaskSuccess(
        params.correlationToken,
        params.output,
      );
      return Result.ok(undefined);
    }

    const arnResult = await this.resolveStateMachineArn();
    if (arnResult.isErr() && this.shouldUseLocalEngine()) {
      this.aslLocalEngine.handleSendTaskSuccess(
        params.correlationToken,
        params.output,
      );
      return Result.ok(undefined);
    }

    try {
      const commandInput = this.translator.toSendTaskSuccessInput(params);
      await this.sfnClient.send(new SendTaskSuccessCommand(commandInput));
      this.logger.log(
        `[StepFunctionsAdapter] SendTaskSuccess sent to Step Functions (token=${params.correlationToken.slice(0, 16)}...)`,
      );
      return Result.ok(undefined);
    } catch (err) {
      this.logger.error(
        `[StepFunctionsAdapter] SendTaskSuccess failed: ${err instanceof Error ? err.message : String(err)}`,
      );
      return Result.err(new StepFunctionsInfraError('SendTaskSuccess', err));
    }
  }

  async failWorkflowStep(params: {
    correlationToken: string;
    error: DomainError;
  }): Promise<Result<void, DomainError>> {
    if (params.correlationToken.startsWith('task_token_')) {
      this.aslLocalEngine.handleSendTaskFailure(
        params.correlationToken,
        params.error,
      );
      return Result.ok(undefined);
    }

    const arnResult = await this.resolveStateMachineArn();
    if (arnResult.isErr() && this.shouldUseLocalEngine()) {
      this.aslLocalEngine.handleSendTaskFailure(
        params.correlationToken,
        params.error,
      );
      return Result.ok(undefined);
    }

    try {
      const commandInput = this.translator.toSendTaskFailureInput(params);
      await this.sfnClient.send(new SendTaskFailureCommand(commandInput));
      this.logger.warn(
        `[StepFunctionsAdapter] SendTaskFailure sent to Step Functions (error=${params.error.code}, token=${params.correlationToken.slice(0, 16)}...)`,
      );
      return Result.ok(undefined);
    } catch (err) {
      this.logger.error(
        `[StepFunctionsAdapter] SendTaskFailure failed: ${err instanceof Error ? err.message : String(err)}`,
      );
      return Result.err(new StepFunctionsInfraError('SendTaskFailure', err));
    }
  }

  async getWorkflowStatus(
    orderId: string,
  ): Promise<Result<WorkflowExecutionSummary, DomainError>> {
    const localSummary = this.aslLocalEngine.getExecutionSummary(orderId);
    if (localSummary) {
      return Result.ok(localSummary);
    }

    let executionArn = this.executionArnByOrder.get(orderId);
    if (!executionArn) {
      const arnResult = await this.resolveStateMachineArn();
      if (arnResult.isOk()) {
        executionArn = `${arnResult.value.replace(':stateMachine:', ':execution:')}:${orderId}`;
      }
    }

    if (!executionArn) {
      return Result.ok({ status: 'NOT_STARTED' });
    }

    try {
      const desc = await this.sfnClient.send(
        new DescribeExecutionCommand({ executionArn }),
      );
      return Result.ok(
        this.translator.toWorkflowExecutionSummary(executionArn, desc),
      );
    } catch {
      return Result.ok({
        status: 'NOT_STARTED',
      });
    }
  }
}
