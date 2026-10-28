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
import {
  type PublishedEventEnvelope,
  TemporalEventPublisherAdapter,
} from './temporal-message-publisher.adapter.js';

/**
 * Driving Worker that consumes 'ORDER_CREATED' events published by OutboxRelayService
 * and starts the Order Saga Workflow in Temporal via OrderWorkflowPort.
 */
@Injectable()
export class TemporalOrderEventsWorker
  implements OnModuleInit, OnModuleDestroy
{
  private readonly logger = new Logger(TemporalOrderEventsWorker.name);
  private timer?: ReturnType<typeof setInterval>;
  private isPolling = false;

  constructor(
    @Inject(ORDER_WORKFLOW_PORT)
    private readonly orderWorkflow: OrderWorkflowPort,
    @Inject(TELEMETRY_PORT)
    private readonly telemetry: TelemetryPort,
    private readonly eventPublisher: TemporalEventPublisherAdapter,
  ) {}

  onModuleInit(): void {
    if (process.env.DISABLE_BACKGROUND_WORKERS === 'true') {
      return;
    }
    const intervalMs = Number(process.env.EVENT_POLL_INTERVAL_MS ?? 250);
    this.timer = setInterval(() => {
      void this.pollEventsQueueOnce();
    }, intervalMs);
  }

  onModuleDestroy(): void {
    if (this.timer) {
      clearInterval(this.timer);
    }
  }

  async pollEventsQueueOnce(): Promise<number> {
    if (this.isPolling) {
      return 0;
    }
    this.isPolling = true;
    let processedCount = 0;

    try {
      const eventBodies = this.eventPublisher.dequeueEventBodies(10);
      for (const rawBody of eventBodies) {
        const ok = await this.handleRawEventBody(rawBody);
        if (ok) {
          processedCount += 1;
        }
      }
      return processedCount;
    } finally {
      this.isPolling = false;
    }
  }

  private async handleRawEventBody(rawBody: string): Promise<boolean> {
    const envelope = JSON.parse(rawBody) as PublishedEventEnvelope;
    if (envelope.eventType !== 'ORDER_CREATED') {
      return true;
    }

    const wrapper = envelope.wrapper as MessageWrapper<StartSagaPayload>;

    return this.telemetry.runInSpan(
      {
        operationName: 'temporal.consume.order_created',
        resourceName: 'TemporalOrderEventsWorker.StartWorkflow',
        correlationId: envelope.aggregateId,
        parentCarrier: wrapper.metadata.traceContext,
        tags: {
          'order.id': envelope.aggregateId,
          'event.type': envelope.eventType,
        },
      },
      async () => {
        this.logger.log(
          `[TemporalOrderEventsWorker] Consumed '${envelope.eventType}' (orderId=${envelope.aggregateId}) -> Starting Temporal workflow...`,
        );
        const startResult =
          await this.orderWorkflow.startOrderWorkflow(wrapper);
        if (startResult.isErr()) {
          this.logger.warn(
            `[TemporalOrderEventsWorker] Failed to start workflow for order '${envelope.aggregateId}': ${startResult.error.message}`,
          );
          return false;
        }
        this.logger.log(
          `[TemporalOrderEventsWorker] Workflow started for orderId=${envelope.aggregateId} -> workflowInstanceId=${startResult.value.workflowInstanceId}`,
        );
        return true;
      },
    );
  }
}
