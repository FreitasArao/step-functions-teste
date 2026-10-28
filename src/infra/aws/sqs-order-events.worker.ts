import {
  DeleteMessageCommand,
  GetQueueUrlCommand,
  ReceiveMessageCommand,
  type SQSClient,
} from '@aws-sdk/client-sqs';
import {
  Inject,
  Injectable,
  Logger,
  type OnModuleDestroy,
  type OnModuleInit,
} from '@nestjs/common';
import {
  ORDER_WORKFLOW_PORT,
  type OrderWorkflowPort,
  type StartSagaPayload,
} from '../../modules/order/application/ports/order-workflow.outbound-port.js';
import type { MessageWrapper } from '../../shared/messaging/message-wrapper.js';
import {
  TELEMETRY_PORT,
  type TelemetryPort,
} from '../telemetry/tracer.js';
import { SQS_CLIENT } from './aws-clients.provider.js';
import {
  SqsMessagePublisherAdapter,
  type SqsPublishedEventEnvelope,
} from './sqs-message-publisher.adapter.js';

/**
 * Driving Worker that consumes 'ORDER_CREATED' events from SQS ('order-events-queue')
 * and starts the Order Saga Workflow via OrderWorkflowPort.
 * Completes the flow: Outbox -> Publish Message -> SQS -> Start Workflow
 */
@Injectable()
export class SqsOrderEventsWorker implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(SqsOrderEventsWorker.name);
  private timer?: ReturnType<typeof setInterval>;
  private isPolling = false;
  private cachedQueueUrl?: string;

  constructor(
    @Inject(SQS_CLIENT)
    private readonly sqsClient: SQSClient,
    @Inject(ORDER_WORKFLOW_PORT)
    private readonly orderWorkflow: OrderWorkflowPort,
    @Inject(TELEMETRY_PORT)
    private readonly telemetry: TelemetryPort,
    private readonly sqsPublisher: SqsMessagePublisherAdapter,
  ) {}

  onModuleInit(): void {
    if (process.env.DISABLE_BACKGROUND_WORKERS === 'true') {
      return;
    }
    const intervalMs = Number(process.env.SQS_POLL_INTERVAL_MS ?? 250);
    this.timer = setInterval(() => {
      void this.pollEventsQueueOnce();
    }, intervalMs);
  }

  onModuleDestroy(): void {
    if (this.timer) {
      clearInterval(this.timer);
    }
  }

  private async resolveEventsQueueUrl(): Promise<string | null> {
    if (process.env.SQS_ORDER_EVENTS_QUEUE_URL) {
      return process.env.SQS_ORDER_EVENTS_QUEUE_URL;
    }
    if (this.cachedQueueUrl) {
      return this.cachedQueueUrl;
    }
    try {
      const response = await this.sqsClient.send(
        new GetQueueUrlCommand({ QueueName: 'order-events-queue' }),
      );
      if (response.QueueUrl) {
        this.cachedQueueUrl = response.QueueUrl;
        return response.QueueUrl;
      }
      return null;
    } catch {
      return null;
    }
  }

  async pollEventsQueueOnce(): Promise<number> {
    if (this.isPolling) {
      return 0;
    }
    this.isPolling = true;
    let processedCount = 0;

    try {
      // 1. Drain local buffered events (when running unit tests without LocalStack)
      const localBodies = this.sqsPublisher.dequeueLocalEventBodies(10);
      for (const rawBody of localBodies) {
        const ok = await this.handleRawEventBody(rawBody, 'in-memory-buffer');
        if (ok) {
          processedCount += 1;
        }
      }

      // 2. Poll LocalStack SQS 'order-events-queue'
      const queueUrl = await this.resolveEventsQueueUrl();
      if (!queueUrl) {
        return processedCount;
      }

      const response = await this.sqsClient.send(
        new ReceiveMessageCommand({
          QueueUrl: queueUrl,
          MaxNumberOfMessages: 5,
          WaitTimeSeconds: 0,
        }),
      );

      for (const sqsMsg of response.Messages ?? []) {
        if (!sqsMsg.Body) {
          continue;
        }
        const ok = await this.handleRawEventBody(sqsMsg.Body, queueUrl);
        if (ok && sqsMsg.ReceiptHandle) {
          await this.sqsClient.send(
            new DeleteMessageCommand({
              QueueUrl: queueUrl,
              ReceiptHandle: sqsMsg.ReceiptHandle,
            }),
          );
          processedCount += 1;
        }
      }

      return processedCount;
    } catch {
      return processedCount;
    } finally {
      this.isPolling = false;
    }
  }

  private async handleRawEventBody(
    rawBody: string,
    sourceQueue: string,
  ): Promise<boolean> {
    const envelope = JSON.parse(rawBody) as SqsPublishedEventEnvelope;
    if (envelope.eventType !== 'ORDER_CREATED') {
      return true;
    }

    const wrapper = envelope.wrapper as MessageWrapper<StartSagaPayload>;

    return this.telemetry.runInSpan(
      {
        operationName: 'sqs.consume.order_created',
        resourceName: 'SqsOrderEventsWorker.StartWorkflow',
        correlationId: envelope.aggregateId,
        parentCarrier: wrapper.metadata.traceContext,
        tags: {
          'order.id': envelope.aggregateId,
          'event.type': envelope.eventType,
        },
      },
      async () => {
        this.logger.log(
          `[SqsOrderEventsWorker] Consumed '${envelope.eventType}' from ${sourceQueue} (orderId=${envelope.aggregateId}) -> Starting Step Functions workflow...`,
        );
        const startResult =
          await this.orderWorkflow.startOrderWorkflow(wrapper);
        if (startResult.isErr()) {
          this.logger.warn(
            `[SqsOrderEventsWorker] Failed to start workflow for order '${envelope.aggregateId}': ${startResult.error.message}`,
          );
          return false;
        }
        this.logger.log(
          `[SqsOrderEventsWorker] Workflow started for orderId=${envelope.aggregateId} -> workflowInstanceId=${startResult.value.workflowInstanceId}`,
        );
        return true;
      },
    );
  }
}
