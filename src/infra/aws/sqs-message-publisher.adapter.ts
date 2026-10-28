import {
  GetQueueUrlCommand,
  SendMessageCommand,
  type SQSClient,
} from '@aws-sdk/client-sqs';
import { Inject, Injectable, Logger } from '@nestjs/common';
import type { DomainError } from '../../shared/core/domain-error.js';
import { Result } from '../../shared/core/result.js';
import type {
  MessagePublisherPort,
  PublishMessageInput,
} from '../../shared/messaging/message-publisher.port.js';
import { SQS_CLIENT } from './aws-clients.provider.js';

export interface SqsPublishedEventEnvelope {
  readonly messageId: string;
  readonly eventType: string;
  readonly aggregateId: string;
  readonly wrapper: PublishMessageInput['wrapper'];
}

export class SqsPublishInfraError implements DomainError {
  readonly code = 'SQS_PUBLISH_INFRA_ERROR';
  readonly message: string;
  readonly retryable = true;

  constructor(detail: string) {
    this.message = detail;
  }
}

/**
 * Infrastructure Adapter implementing MessagePublisherPort over AWS SQS ('order-events-queue').
 */
@Injectable()
export class SqsMessagePublisherAdapter implements MessagePublisherPort {
  private readonly logger = new Logger(SqsMessagePublisherAdapter.name);
  private cachedQueueUrl?: string;
  private readonly localEventBuffer: string[] = [];

  constructor(
    @Inject(SQS_CLIENT)
    private readonly sqsClient: SQSClient,
  ) {}

  private isTestFallbackEnabled(): boolean {
    return (
      process.env.NODE_ENV === 'test' ||
      process.env.USE_LOCAL_ASL_ENGINE === 'true'
    );
  }

  private async resolveEventsQueueUrl(): Promise<Result<string, DomainError>> {
    if (process.env.SQS_ORDER_EVENTS_QUEUE_URL) {
      return Result.ok(process.env.SQS_ORDER_EVENTS_QUEUE_URL);
    }
    if (this.cachedQueueUrl) {
      return Result.ok(this.cachedQueueUrl);
    }
    try {
      const response = await this.sqsClient.send(
        new GetQueueUrlCommand({ QueueName: 'order-events-queue' }),
      );
      if (response.QueueUrl) {
        this.cachedQueueUrl = response.QueueUrl;
        return Result.ok(response.QueueUrl);
      }
      return Result.err(
        new SqsPublishInfraError(
          "SQS queue 'order-events-queue' not found on LocalStack (run 'cd infra && terraform apply -auto-approve')",
        ),
      );
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      return Result.err(
        new SqsPublishInfraError(
          `Failed to resolve SQS queue 'order-events-queue' on LocalStack: ${msg}`,
        ),
      );
    }
  }

  async publish(
    input: PublishMessageInput,
  ): Promise<Result<{ brokerMessageId: string }, DomainError>> {
    const envelope: SqsPublishedEventEnvelope = {
      messageId: input.messageId,
      eventType: input.eventType,
      aggregateId: input.aggregateId,
      wrapper: input.wrapper,
    };
    const serialized = JSON.stringify(envelope);

    const queueUrlResult = await this.resolveEventsQueueUrl();
    if (queueUrlResult.isOk()) {
      try {
        const res = await this.sqsClient.send(
          new SendMessageCommand({
            QueueUrl: queueUrlResult.value,
            MessageBody: serialized,
          }),
        );
        const brokerMessageId = res.MessageId ?? input.messageId;
        this.logger.log(
          `[SQS Publisher] Sent event '${input.eventType}' (outboxId=${input.messageId}, orderId=${input.aggregateId}) -> QueueUrl=${queueUrlResult.value} (SQS MessageId=${brokerMessageId})`,
        );
        return Result.ok({ brokerMessageId });
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        if (!this.isTestFallbackEnabled()) {
          this.logger.error(
            `[SQS Publisher] SendMessage failed for QueueUrl=${queueUrlResult.value}: ${msg}`,
          );
          return Result.err(new SqsPublishInfraError(msg));
        }
      }
    } else if (!this.isTestFallbackEnabled()) {
      this.logger.error(`[SQS Publisher] ${queueUrlResult.error.message}`);
      return Result.err(queueUrlResult.error);
    }

    this.localEventBuffer.push(serialized);
    return Result.ok({ brokerMessageId: `local_${input.messageId}` });
  }

  dequeueLocalEventBodies(max = 10): string[] {
    return this.localEventBuffer.splice(0, max);
  }
}
