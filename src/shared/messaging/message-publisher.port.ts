import type { DomainError } from '../core/domain-error.js';
import type { Result } from '../core/result.js';
import type { MessageWrapper } from './message-wrapper.js';

export interface PublishMessageInput<TPayload = unknown> {
  readonly messageId: string;
  readonly eventType: string;
  readonly aggregateId: string;
  readonly wrapper: MessageWrapper<TPayload>;
}

export const MESSAGE_PUBLISHER_PORT = Symbol('MESSAGE_PUBLISHER_PORT');

/**
 * Cloud-agnostic Message Publisher Port (Driven Port).
 * Used by the Generic Outbox Relay to publish domain events to a message broker (e.g., SQS, SNS, Kafka, RabbitMQ)
 * without knowing anything about workflows, sagas, or specific business modules.
 */
export interface MessagePublisherPort {
  publish(
    input: PublishMessageInput,
  ): Promise<Result<{ brokerMessageId: string }, DomainError>>;
}
