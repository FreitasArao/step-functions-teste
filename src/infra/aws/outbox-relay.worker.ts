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
} from '../../modules/order/domain/order-workflow.port.js';
import { createMessageWrapper } from '../../shared/messaging/message-wrapper.js';
import {
  OUTBOX_REPOSITORY_PORT,
  type OutboxMessage,
  type OutboxRepositoryPort,
} from '../../shared/messaging/outbox.port.js';
import {
  TELEMETRY_PORT,
  type TelemetryPort,
} from '../telemetry/tracer.js';

@Injectable()
export class OutboxRelayWorker implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(OutboxRelayWorker.name);
  private timer?: ReturnType<typeof setInterval>;
  private isProcessing = false;

  constructor(
    @Inject(OUTBOX_REPOSITORY_PORT)
    private readonly outboxRepository: OutboxRepositoryPort,
    @Inject(ORDER_WORKFLOW_PORT)
    private readonly orderWorkflow: OrderWorkflowPort,
    @Inject(TELEMETRY_PORT)
    private readonly telemetry: TelemetryPort,
  ) {}

  onModuleInit(): void {
    if (process.env.DISABLE_BACKGROUND_WORKERS === 'true') {
      return;
    }
    const intervalMs = Number(process.env.OUTBOX_POLL_INTERVAL_MS ?? 250);
    this.timer = setInterval(() => {
      void this.processPendingOutbox();
    }, intervalMs);
  }

  onModuleDestroy(): void {
    if (this.timer) {
      clearInterval(this.timer);
    }
  }

  async processPendingOutbox(): Promise<number> {
    if (this.isProcessing) {
      return 0;
    }
    this.isProcessing = true;
    let publishedCount = 0;

    try {
      const pendingMessages = await this.outboxRepository.findPending();
      for (const message of pendingMessages) {
        const ok = await this.publishSingleMessage(message);
        if (ok) {
          publishedCount += 1;
        }
      }
      return publishedCount;
    } finally {
      this.isProcessing = false;
    }
  }

  private async publishSingleMessage(
    message: OutboxMessage,
  ): Promise<boolean> {
    return this.telemetry.runInSpan(
      {
        operationName: `outbox.relay.${message.eventType.toLowerCase()}`,
        resourceName: `OutboxRelayWorker.${message.eventType}`,
        correlationId: message.orderId,
        parentCarrier: message.wrapper.metadata.traceContext,
        tags: {
          'outbox.id': message.id,
          'outbox.event_type': message.eventType,
          'order.id': message.orderId,
        },
      },
      async (traceCarrier) => {
        if (message.eventType === 'START_ORDER_SAGA') {
          const updatedWrapper = createMessageWrapper<StartSagaPayload>({
            payload: message.wrapper.payload as StartSagaPayload,
            correlationId: message.orderId,
            causationId: message.wrapper.metadata.messageId,
            source: 'outbox-relay:StartOrderSaga',
            traceContext: traceCarrier,
          });

          const startResult =
            await this.orderWorkflow.startSagaExecution(updatedWrapper);
          if (startResult.isErr()) {
            this.logger.warn(
              `Outbox [${message.id}] START_ORDER_SAGA failed: ${startResult.error.message}`,
            );
            return false;
          }
          await this.outboxRepository.markPublished(message.id, {
            executionArn: startResult.value.executionArn,
          });
          return true;
        }

        if (message.eventType === 'SFN_TASK_SUCCESS') {
          if (!message.taskToken) {
            await this.outboxRepository.markFailed(message.id);
            return false;
          }
          const successResult = await this.orderWorkflow.sendTaskSuccess({
            taskToken: message.taskToken,
            output: message.wrapper.payload,
          });
          if (successResult.isErr()) {
            this.logger.warn(
              `Outbox [${message.id}] SFN_TASK_SUCCESS failed: ${successResult.error.message}`,
            );
            return false;
          }
          await this.outboxRepository.markPublished(message.id);
          return true;
        }

        if (message.eventType === 'SFN_TASK_FAILURE') {
          if (!message.taskToken || !message.errorDetails) {
            await this.outboxRepository.markFailed(message.id);
            return false;
          }
          const failureResult = await this.orderWorkflow.sendTaskFailure({
            taskToken: message.taskToken,
            error: message.errorDetails,
          });
          if (failureResult.isErr()) {
            this.logger.warn(
              `Outbox [${message.id}] SFN_TASK_FAILURE failed: ${failureResult.error.message}`,
            );
            return false;
          }
          await this.outboxRepository.markPublished(message.id);
          return true;
        }

        return false;
      },
    );
  }
}
