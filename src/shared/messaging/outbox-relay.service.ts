import { Inject, Injectable, Logger } from '@nestjs/common';
import {
  TELEMETRY_PORT,
  type TelemetryPort,
} from '../../infra/telemetry/tracer.js';
import {
  MESSAGE_PUBLISHER_PORT,
  type MessagePublisherPort,
} from './message-publisher.port.js';
import { createMessageWrapper } from './message-wrapper.js';
import {
  OUTBOX_REPOSITORY_PORT,
  type OutboxMessage,
  type OutboxRepositoryPort,
} from './outbox.port.js';

/**
 * 100% Generic Outbox Relay Service.
 * Knows nothing about Order, Saga, or AWS Step Functions.
 * Simply reads PENDING messages from OutboxRepositoryPort, publishes them to MessagePublisherPort (e.g. SQS),
 * and marks them as PUBLISHED.
 */
@Injectable()
export class OutboxRelayService {
  private readonly logger = new Logger(OutboxRelayService.name);

  constructor(
    @Inject(OUTBOX_REPOSITORY_PORT)
    private readonly outboxRepository: OutboxRepositoryPort,
    @Inject(MESSAGE_PUBLISHER_PORT)
    private readonly messagePublisher: MessagePublisherPort,
    @Inject(TELEMETRY_PORT)
    private readonly telemetry: TelemetryPort,
  ) {}

  async executeBatch(): Promise<number> {
    let publishedCount = 0;
    const pendingMessages = await this.outboxRepository.findPending();
    for (const message of pendingMessages) {
      const ok = await this.publishOne(message);
      if (ok) {
        publishedCount += 1;
      }
    }
    return publishedCount;
  }

  private async publishOne(message: OutboxMessage): Promise<boolean> {
    return this.telemetry.runInSpan(
      {
        operationName: `outbox.publish.${message.eventType.toLowerCase()}`,
        resourceName: `OutboxRelayService.${message.eventType}`,
        correlationId: message.orderId,
        parentCarrier: message.wrapper.metadata.traceContext,
        tags: {
          'outbox.id': message.id,
          'outbox.event_type': message.eventType,
          'aggregate.id': message.orderId,
        },
      },
      async (traceCarrier) => {
        const tracedWrapper = createMessageWrapper({
          payload: message.wrapper.payload,
          correlationId: message.orderId,
          causationId: message.wrapper.metadata.messageId,
          source: `outbox-relay:${message.eventType}`,
          traceContext: traceCarrier,
        });

        const publishResult = await this.messagePublisher.publish({
          messageId: message.id,
          eventType: message.eventType,
          aggregateId: message.orderId,
          wrapper: tracedWrapper,
        });

        if (publishResult.isErr()) {
          this.logger.warn(
            `[OutboxRelay] Failed to publish outboxId=${message.id} (eventType=${message.eventType}, orderId=${message.orderId}): ${publishResult.error.message}`,
          );
          return false;
        }

        await this.outboxRepository.markPublished(message.id);
        this.logger.log(
          `[OutboxRelay] Published outboxId=${message.id} (eventType=${message.eventType}, orderId=${message.orderId}) -> brokerMessageId=${publishResult.value.brokerMessageId} [status=PUBLISHED]`,
        );
        return true;
      },
    );
  }
}
