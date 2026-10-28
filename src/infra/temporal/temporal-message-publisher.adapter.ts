import { Injectable, Logger } from '@nestjs/common';
import type { DomainError } from '../../shared/core/domain-error.js';
import { Result } from '../../shared/core/result.js';
import type {
  MessagePublisherPort,
  PublishMessageInput,
} from '../../shared/messaging/message-publisher.port.js';

export interface PublishedEventEnvelope {
  readonly messageId: string;
  readonly eventType: string;
  readonly aggregateId: string;
  readonly wrapper: PublishMessageInput['wrapper'];
}

/**
 * Infrastructure Adapter implementing MessagePublisherPort without AWS SQS/LocalStack.
 * Receives events published by the agnostic OutboxRelayService and buffers them
 * for TemporalOrderEventsWorker to start the Temporal Saga Workflow.
 */
@Injectable()
export class TemporalEventPublisherAdapter implements MessagePublisherPort {
  private readonly logger = new Logger(TemporalEventPublisherAdapter.name);
  private readonly eventQueueBuffer: string[] = [];

  async publish(
    input: PublishMessageInput,
  ): Promise<Result<{ brokerMessageId: string }, DomainError>> {
    const envelope: PublishedEventEnvelope = {
      messageId: input.messageId,
      eventType: input.eventType,
      aggregateId: input.aggregateId,
      wrapper: input.wrapper,
    };
    const serialized = JSON.stringify(envelope);
    this.eventQueueBuffer.push(serialized);

    const brokerMessageId = `temporal_evt_${input.messageId}`;
    this.logger.log(
      `[TemporalEventPublisher] Published event '${input.eventType}' (outboxId=${input.messageId}, orderId=${input.aggregateId}) -> brokerMessageId=${brokerMessageId}`,
    );

    return Result.ok({ brokerMessageId });
  }

  dequeueEventBodies(max = 10): string[] {
    return this.eventQueueBuffer.splice(0, max);
  }
}
