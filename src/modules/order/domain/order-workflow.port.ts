import type { DomainError } from '../../../shared/core/domain-error.js';
import type { Result } from '../../../shared/core/result.js';
import type {
  MessageWrapper,
  SagaStepName,
} from '../../../shared/messaging/message-wrapper.js';

export interface StartSagaPayload {
  readonly orderId: string;
  readonly sku: string;
  readonly quantity: number;
  readonly amount: number;
  readonly zipCode: string;
}

export interface WorkflowFailureDetails {
  readonly code?: string;
  readonly message?: string;
}

export interface SagaStepContextPayload extends StartSagaPayload {
  readonly failure?: WorkflowFailureDetails;
  readonly [key: string]: unknown;
}

/**
 * Agnostic command representing a single workflow step to be executed by the application.
 * Translated by the infrastructure ACL from whatever external orchestrator is used.
 */
export interface WorkflowStepCommand {
  readonly stepName: SagaStepName;
  readonly idempotencyKey: string;
  readonly correlationToken: string;
  readonly wrapper: MessageWrapper<SagaStepContextPayload>;
}

export type WorkflowExecutionStatus =
  | 'NOT_STARTED'
  | 'RUNNING'
  | 'COMPLETED'
  | 'ROLLED_BACK'
  | 'FAILED';

export interface WorkflowExecutionSummary {
  readonly workflowInstanceId?: string;
  readonly status: WorkflowExecutionStatus;
  readonly startedAt?: string;
  readonly finishedAt?: string;
  readonly failureCode?: string;
  readonly failureReason?: string;
}

export const ORDER_WORKFLOW_PORT = Symbol('ORDER_WORKFLOW_PORT');

/**
 * Orchestrator-agnostic Outbound Port.
 * Can be implemented by AWS Step Functions, Temporal, Camunda, Conductor, etc.
 */
export interface OrderWorkflowPort {
  startOrderWorkflow(
    wrapper: MessageWrapper<StartSagaPayload>,
  ): Promise<Result<{ workflowInstanceId: string }, DomainError>>;
  completeWorkflowStep(params: {
    correlationToken: string;
    output: unknown;
  }): Promise<Result<void, DomainError>>;
  failWorkflowStep(params: {
    correlationToken: string;
    error: DomainError;
  }): Promise<Result<void, DomainError>>;
  getWorkflowStatus(
    orderId: string,
  ): Promise<Result<WorkflowExecutionSummary, DomainError>>;
}
