import type { DomainError } from '../../../shared/core/domain-error.js';
import type { Result } from '../../../shared/core/result.js';
import type { MessageWrapper } from '../../../shared/messaging/message-wrapper.js';

export interface StartSagaPayload {
  readonly orderId: string;
  readonly sku: string;
  readonly quantity: number;
  readonly amount: number;
  readonly zipCode: string;
}

export interface StepFunctionsExecutionSummary {
  readonly executionArn?: string;
  readonly status: 'RUNNING' | 'SUCCEEDED' | 'FAILED' | 'TIMED_OUT' | 'ABORTED' | 'NOT_STARTED';
  readonly startDate?: string;
  readonly stopDate?: string;
  readonly error?: string;
  readonly cause?: string;
}

export const ORDER_WORKFLOW_PORT = Symbol('ORDER_WORKFLOW_PORT');

export interface OrderWorkflowPort {
  startSagaExecution(
    wrapper: MessageWrapper<StartSagaPayload>,
  ): Promise<Result<{ executionArn: string }, DomainError>>;
  sendTaskSuccess(params: {
    taskToken: string;
    output: unknown;
  }): Promise<Result<void, DomainError>>;
  sendTaskFailure(params: {
    taskToken: string;
    error: DomainError;
  }): Promise<Result<void, DomainError>>;
  getExecutionStatus(
    orderId: string,
  ): Promise<Result<StepFunctionsExecutionSummary, DomainError>>;
}
