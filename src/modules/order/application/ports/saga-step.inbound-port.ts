import type { DomainError } from '../../../../shared/core/domain-error.js';
import type { Result } from '../../../../shared/core/result.js';
import type {
  MessageWrapper,
  SagaStepName,
} from '../../../../shared/messaging/message-wrapper.js';
import type { StartSagaPayload } from './order-workflow.outbound-port.js';

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
 * Translated by the infrastructure ACL from whatever external orchestrator/queue is used.
 */
export interface WorkflowStepCommand {
  readonly stepName: SagaStepName;
  readonly idempotencyKey: string;
  readonly correlationToken: string;
  readonly wrapper: MessageWrapper<SagaStepContextPayload>;
}

/**
 * Business outcome of a step. A business failure (e.g. PAYMENT_DECLINED) is a *successful*
 * processing of the message (outcome = 'ERR'); only infrastructure failures return Result.err,
 * so the delivering infrastructure can retry the message.
 */
export interface ExecuteSagaStepOutput {
  readonly deduplicated: boolean;
  readonly outcome: 'OK' | 'ERR';
  readonly output?: unknown;
}

export const EXECUTE_SAGA_STEP_INBOUND_PORT = Symbol(
  'EXECUTE_SAGA_STEP_INBOUND_PORT',
);

/**
 * Orchestrator-agnostic Inbound (Driving) Port.
 * Consumed by driving adapters (e.g., SqsSagaStepWorker) and implemented by ExecuteSagaStepUseCase.
 */
export interface ExecuteSagaStepInboundPort {
  execute(
    command: WorkflowStepCommand,
  ): Promise<Result<ExecuteSagaStepOutput, DomainError>>;
}
