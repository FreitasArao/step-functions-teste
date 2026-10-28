import { Inject, Injectable, Logger } from '@nestjs/common';
import {
  TELEMETRY_PORT,
  type TelemetryPort,
} from '../../infra/telemetry/tracer.js';
import type { DomainError } from '../core/domain-error.js';
import { Result } from '../core/result.js';
import {
  MESSAGE_PUBLISHER_PORT,
  type MessagePublisherPort,
} from './message-publisher.port.js';
import { createMessageWrapper } from './message-wrapper.js';
import type {
  OutboxMessage,
  OutboxRepositoryPort,
} from './outbox.port.js';
import {
  UNIT_OF_WORK_PORT,
  type UnitOfWorkPort,
} from './unit-of-work.port.js';

/**
 * 100% Generic Outbox Relay Service.
 * Knows nothing about Order, Saga, AWS Step Functions, or Temporal.
 * Claims PENDING domain events inside a UnitOfWork transaction (`FOR UPDATE SKIP LOCKED`),
 * publishes them via MessagePublisherPort, and marks them as PUBLISHED.
 */
@Injectable()
export class OutboxRelayService {
  private readonly logger = new Logger(OutboxRelayService.name);

  constructor(
    @Inject(UNIT_OF_WORK_PORT)
    private readonly unitOfWork: UnitOfWorkPort,
    @Inject(MESSAGE_PUBLISHER_PORT)
    private readonly messagePublisher: MessagePublisherPort,
    @Inject(TELEMETRY_PORT)
    private readonly telemetry: TelemetryPort,
  ) {}

  async executeBatch(): Promise<number> {
    const batchResult = await this.unitOfWork.execute<number, DomainError>(
      async ({ outbox }) => {
        const pendingMessages = await outbox.findPending();
        let publishedCount = 0;

        for (const message of pendingMessages) {
          const ok = await this.publishOne(outbox, message);
          if (ok) {
            publishedCount += 1;
          }
        }
        return Result.ok(publishedCount);
      },
    );

    return batchResult.isOk() ? batchResult.value : 0;
  }

  private async publishOne(
    outbox: OutboxRepositoryPort,
    message: OutboxMessage,
  ): Promise<boolean> {
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

        await outbox.markPublished(message.id);
        this.logger.log(
          `[OutboxRelay] Published outboxId=${message.id} (eventType=${message.eventType}, orderId=${message.orderId}) -> brokerMessageId=${publishResult.value.brokerMessageId} [status=PUBLISHED]`,
        );
        return true;
      },
    );
  }
}
