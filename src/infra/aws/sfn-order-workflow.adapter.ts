import type { SFNClient } from '@aws-sdk/client-sfn';
import { Inject, Injectable } from '@nestjs/common';
import type {
  OrderWorkflowPort,
  StartSagaPayload,
  WorkflowExecutionSummary,
} from '../../modules/order/domain/order-workflow.port.js';
import type { DomainError } from '../../shared/core/domain-error.js';
import { Result } from '../../shared/core/result.js';
import type { MessageWrapper } from '../../shared/messaging/message-wrapper.js';
import { SFN_CLIENT } from './aws-clients.provider.js';
import { AwsStepFunctionsTranslator } from './translators/sfn-acl.translator.js';

export class StepFunctionsNotImplementedError implements DomainError {
  readonly code = 'STEP_FUNCTIONS_NOT_IMPLEMENTED';
  readonly message: string;
  readonly retryable = false;

  constructor(operation: string) {
    this.message = `TODO: Implement '${operation}' in StepFunctionsOrderWorkflowAdapter`;
  }
}

@Injectable()
export class StepFunctionsOrderWorkflowAdapter implements OrderWorkflowPort {
  constructor(
    @Inject(SFN_CLIENT)
    private readonly _sfnClient: SFNClient,
    private readonly _translator: AwsStepFunctionsTranslator,
  ) {}

  async startOrderWorkflow(
    _wrapper: MessageWrapper<StartSagaPayload>,
  ): Promise<Result<{ workflowInstanceId: string }, DomainError>> {
    // TODO (Prática): Usar this._translator.toStartExecutionInput e enviar StartExecutionCommand
    return Result.err(
      new StepFunctionsNotImplementedError('startOrderWorkflow'),
    );
  }

  async completeWorkflowStep(_params: {
    correlationToken: string;
    output: unknown;
  }): Promise<Result<void, DomainError>> {
    // TODO (Prática): Usar this._translator.toSendTaskSuccessInput e enviar SendTaskSuccessCommand
    return Result.err(
      new StepFunctionsNotImplementedError('completeWorkflowStep'),
    );
  }

  async failWorkflowStep(_params: {
    correlationToken: string;
    error: DomainError;
  }): Promise<Result<void, DomainError>> {
    // TODO (Prática): Usar this._translator.toSendTaskFailureInput e enviar SendTaskFailureCommand
    return Result.err(new StepFunctionsNotImplementedError('failWorkflowStep'));
  }

  async getWorkflowStatus(
    _orderId: string,
  ): Promise<Result<WorkflowExecutionSummary, DomainError>> {
    // TODO (Prática): Enviar DescribeExecutionCommand e traduzir com this._translator.toWorkflowExecutionSummary
    return Result.ok({ status: 'NOT_STARTED' });
  }
}
