import { Inject, Injectable, Logger } from '@nestjs/common';
import {
  TELEMETRY_PORT,
  type TelemetryPort,
} from '../../../infra/telemetry/tracer.js';
import { createMessageWrapper } from '../../../shared/messaging/message-wrapper.js';
import {
  OUTBOX_REPOSITORY_PORT,
  type OutboxMessage,
  type OutboxRepositoryPort,
} from '../../../shared/messaging/outbox.port.js';
import {
  ORDER_WORKFLOW_PORT,
  type OrderWorkflowPort,
  type StartSagaPayload,
} from '../domain/order-workflow.port.js';

/**
 * Pure Application Layer Use Case that relays pending Outbox events
 * through the agnostic OrderWorkflowPort.
 * Knows nothing about AWS Step Functions.
 */
@Injectable()
export class RelayOutboxUseCase {
  private readonly logger = new Logger(RelayOutboxUseCase.name);

  constructor(
    @Inject(OUTBOX_REPOSITORY_PORT)
    private readonly outboxRepository: OutboxRepositoryPort,
    @Inject(ORDER_WORKFLOW_PORT)
    private readonly orderWorkflow: OrderWorkflowPort,
    @Inject(TELEMETRY_PORT)
    private readonly telemetry: TelemetryPort,
  ) {}

  async executeBatch(): Promise<number> {
    let publishedCount = 0;
    const pendingMessages = await this.outboxRepository.findPending();
    for (const message of pendingMessages) {
      const ok = await this.publishMessage(message);
      if (ok) {
        publishedCount += 1;
      }
    }
    return publishedCount;
  }

  private async publishMessage(message: OutboxMessage): Promise<boolean> {
    return this.telemetry.runInSpan(
      {
        operationName: `outbox.relay.${message.eventType.toLowerCase()}`,
        resourceName: `RelayOutboxUseCase.${message.eventType}`,
        correlationId: message.orderId,
        parentCarrier: message.wrapper.metadata.traceContext,
        tags: {
          'outbox.id': message.id,
          'outbox.event_type': message.eventType,
          'order.id': message.orderId,
        },
      },
      async (traceCarrier) => {
        if (message.eventType === 'ORDER_WORKFLOW_REQUESTED') {
          const updatedWrapper = createMessageWrapper<StartSagaPayload>({
            payload: message.wrapper.payload as StartSagaPayload,
            correlationId: message.orderId,
            causationId: message.wrapper.metadata.messageId,
            source: 'outbox-relay:StartOrderWorkflow',
            traceContext: traceCarrier,
          });

          const startResult =
            await this.orderWorkflow.startOrderWorkflow(updatedWrapper);
          if (startResult.isErr()) {
            this.logger.warn(
              `Outbox [${message.id}] ORDER_WORKFLOW_REQUESTED failed: ${startResult.error.message}`,
            );
            return false;
          }
          await this.outboxRepository.markPublished(message.id, {
            workflowInstanceId: startResult.value.workflowInstanceId,
          });
          return true;
        }

        if (message.eventType === 'WORKFLOW_STEP_COMPLETED') {
          if (!message.correlationToken) {
            await this.outboxRepository.markFailed(message.id);
            return false;
          }
          const completeResult = await this.orderWorkflow.completeWorkflowStep({
            correlationToken: message.correlationToken,
            output: message.wrapper.payload,
          });
          if (completeResult.isErr()) {
            this.logger.warn(
              `Outbox [${message.id}] WORKFLOW_STEP_COMPLETED failed: ${completeResult.error.message}`,
            );
            return false;
          }
          await this.outboxRepository.markPublished(message.id);
          return true;
        }

        if (message.eventType === 'WORKFLOW_STEP_FAILED') {
          if (!message.correlationToken || !message.errorDetails) {
            await this.outboxRepository.markFailed(message.id);
            return false;
          }
          const failResult = await this.orderWorkflow.failWorkflowStep({
            correlationToken: message.correlationToken,
            error: message.errorDetails,
          });
          if (failResult.isErr()) {
            this.logger.warn(
              `Outbox [${message.id}] WORKFLOW_STEP_FAILED failed: ${failResult.error.message}`,
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
