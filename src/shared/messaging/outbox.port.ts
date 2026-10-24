import type { DomainError } from '../core/domain-error.js';
import type { MessageWrapper } from './message-wrapper.js';

/**
 * Cloud-agnostic workflow outbox event types.
 */
export type OutboxEventType =
  | 'ORDER_WORKFLOW_REQUESTED'
  | 'WORKFLOW_STEP_COMPLETED'
  | 'WORKFLOW_STEP_FAILED';

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
