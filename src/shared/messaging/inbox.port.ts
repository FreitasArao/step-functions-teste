import type {
  MessageWrapper,
  SagaStepName,
} from './message-wrapper.js';

export interface InboxMessage {
  readonly idempotencyKey: string;
  readonly orderId: string;
  readonly stepName: SagaStepName;
  readonly outcome: 'OK' | 'ERR';
  readonly responsePayload: unknown;
  readonly wrapper: MessageWrapper<unknown>;
  readonly processedAt: string;
}

export const INBOX_REPOSITORY_PORT = Symbol('INBOX_REPOSITORY_PORT');

export interface InboxRepositoryPort {
  findByIdempotencyKey(idempotencyKey: string): Promise<InboxMessage | null>;
  save(message: InboxMessage): Promise<void>;
  findByOrderId(orderId: string): Promise<InboxMessage[]>;
  findAll(): Promise<InboxMessage[]>;
}
