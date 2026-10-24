import type { DomainError } from '../core/domain-error.js';
import type { MessageWrapper } from './message-wrapper.js';

export type OutboxEventType =
  | 'START_ORDER_SAGA'
  | 'SFN_TASK_SUCCESS'
  | 'SFN_TASK_FAILURE';

export type OutboxStatus = 'PENDING' | 'PUBLISHED' | 'FAILED';

export interface OutboxMessage<TPayload = unknown> {
  readonly id: string;
  readonly orderId: string;
  readonly eventType: OutboxEventType;
  readonly status: OutboxStatus;
  readonly wrapper: MessageWrapper<TPayload>;
  readonly taskToken?: string;
  readonly errorDetails?: DomainError;
  readonly createdAt: string;
  readonly publishedAt?: string;
  readonly executionArn?: string;
}

export const OUTBOX_REPOSITORY_PORT = Symbol('OUTBOX_REPOSITORY_PORT');

export interface OutboxRepositoryPort {
  save(message: OutboxMessage): Promise<void>;
  findPending(): Promise<OutboxMessage[]>;
  markPublished(id: string, metadata?: { executionArn?: string }): Promise<void>;
  markFailed(id: string): Promise<void>;
  findByOrderId(orderId: string): Promise<OutboxMessage[]>;
  findAll(): Promise<OutboxMessage[]>;
}
