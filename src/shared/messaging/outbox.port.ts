import type { DomainError } from '../core/domain-error.js';
import type { MessageWrapper } from './message-wrapper.js';

/**
 * Cloud-agnostic domain outbox event types.
 * - ORDER_CREATED: requests the orchestrator to start the Order Saga.
 * - WORKFLOW_STEP_COMPLETED / WORKFLOW_STEP_FAILED: callback to the orchestrator carrying the
 *   step outcome, persisted in the SAME transaction as the Inbox record and the domain state change.
 */
export type OutboxEventType =
  | 'ORDER_CREATED'
  | 'WORKFLOW_STEP_COMPLETED'
  | 'WORKFLOW_STEP_FAILED'
  | (string & {});

export type OutboxStatus = 'PENDING' | 'PUBLISHED' | 'FAILED';

export interface OutboxMessage<TPayload = unknown> {
  readonly id: string;
  readonly orderId: string;
  readonly eventType: OutboxEventType;
  readonly status: OutboxStatus;
  readonly wrapper: MessageWrapper<TPayload>;
  readonly correlationToken?: string;
  readonly errorDetails?: DomainError;
  readonly createdAt: string;
  readonly publishedAt?: string;
  readonly workflowInstanceId?: string;
}

export const OUTBOX_REPOSITORY_PORT = Symbol('OUTBOX_REPOSITORY_PORT');

export interface OutboxRepositoryPort {
  save(message: OutboxMessage): Promise<void>;
  findPending(): Promise<OutboxMessage[]>;
  markPublished(
    id: string,
    metadata?: { workflowInstanceId?: string },
  ): Promise<void>;
  markFailed(id: string): Promise<void>;
  findByOrderId(orderId: string): Promise<OutboxMessage[]>;
  findAll(): Promise<OutboxMessage[]>;
}
