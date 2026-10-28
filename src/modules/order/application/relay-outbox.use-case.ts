import { Inject, Injectable, Logger } from '@nestjs/common';
import {
  TELEMETRY_PORT,
  type TelemetryPort,
} from '../../../infra/telemetry/tracer.js';
import type { DomainError } from '../../../shared/core/domain-error.js';
import { Result } from '../../../shared/core/result.js';
import {
  createMessageWrapper,
  type MessageWrapper,
} from '../../../shared/messaging/message-wrapper.js';
import type {
  OutboxMessage,
  OutboxRepositoryPort,
} from '../../../shared/messaging/outbox.port.js';
import {
  UNIT_OF_WORK_PORT,
  type UnitOfWorkPort,
} from '../../../shared/messaging/unit-of-work.port.js';
import {
  ORDER_WORKFLOW_PORT,
  type OrderWorkflowPort,
  type StartSagaPayload,
} from './ports/order-workflow.outbound-port.js';

/**
 * A message that can never be delivered (unknown type / missing data).
 * Marked as FAILED instead of being retried forever.
 */
export class OutboxPoisonMessageError implements DomainError {
  readonly code = 'OUTBOX_POISON_MESSAGE';
  readonly retryable = false;
  readonly message: string;

  constructor(outboxId: string, reason: string) {
    this.message = `Outbox message '${outboxId}' cannot be delivered: ${reason}`;
  }
}

/**
 * Transactional Outbox Relay (Application Layer, orchestrator-agnostic).
 *
 * Guarantees AT-LEAST-ONCE delivery of every outbox message to the orchestrator:
 * 1. Claims PENDING rows inside a transaction (`FOR UPDATE SKIP LOCKED` → safe with N relay instances).
 * 2. Delivers each message through OrderWorkflowPort (start / complete / fail).
 * 3. Marks the row PUBLISHED ONLY after the orchestrator acknowledged it.
 *    On failure the row stays PENDING and is retried on the next poll.
 *
 * Because delivery is at-least-once, the receiving side MUST be idempotent:
 * - start    -> deterministic workflowId + REJECT_DUPLICATE reuse policy
 * - complete -> a stale/already-used correlationToken is treated as a no-op
 */
@Injectable()
export class RelayOutboxUseCase {
  private readonly logger = new Logger(RelayOutboxUseCase.name);

  constructor(
    @Inject(UNIT_OF_WORK_PORT)
    private readonly unitOfWork: UnitOfWorkPort,
    @Inject(ORDER_WORKFLOW_PORT)
    private readonly orderWorkflow: OrderWorkflowPort,
    @Inject(TELEMETRY_PORT)
    private readonly telemetry: TelemetryPort,
  ) {}

  async executeBatch(): Promise<number> {
    const batchResult = await this.unitOfWork.execute<number, DomainError>(
      async ({ outbox }) => {
        // Row locks are held until COMMIT, so concurrent relays skip these rows
        const pendingMessages = await outbox.findPending();
        let deliveredCount = 0;

        for (const message of pendingMessages) {
          const delivered = await this.relayOne(outbox, message);
          if (delivered) {
            deliveredCount += 1;
          }
        }
        return Result.ok(deliveredCount);
      },
    );
    return batchResult.isOk() ? batchResult.value : 0;
  }

  private async relayOne(
    outbox: OutboxRepositoryPort,
    message: OutboxMessage,
  ): Promise<boolean> {
    return this.telemetry.runInSpan(
      {
        operationName: `outbox.relay.${message.eventType.toLowerCase()}`,
        resourceName: `RelayOutboxUseCase.${message.eventType}`,
        correlationId: message.orderId,
        parentCarrier: message.wrapper.metadata.traceContext,
        tags: {
          'outbox.id': message.id,
          'outbox.event_type': message.eventType,
          'aggregate.id': message.orderId,
        },
      },
      async (traceCarrier) => {
        const deliveryResult = await this.deliver(message, traceCarrier);

        if (deliveryResult.isErr()) {
          if (deliveryResult.error instanceof OutboxPoisonMessageError) {
            await outbox.markFailed(message.id);
            this.logger.error(
              `[OutboxRelay] ${deliveryResult.error.message} [status=FAILED]`,
            );
            return false;
          }
          this.logger.warn(
            `[OutboxRelay] Delivery failed for outboxId=${message.id} (eventType=${message.eventType}, orderId=${message.orderId}): ${deliveryResult.error.message} [status=PENDING, will retry]`,
          );
          return false;
        }

        await outbox.markPublished(message.id, deliveryResult.value);
        this.logger.log(
          `[OutboxRelay] Delivered outboxId=${message.id} (eventType=${message.eventType}, orderId=${message.orderId}) [status=PUBLISHED]`,
        );
        return true;
      },
    );
  }

  private async deliver(
    message: OutboxMessage,
    traceCarrier: MessageWrapper['metadata']['traceContext'],
  ): Promise<Result<{ workflowInstanceId?: string }, DomainError>> {
    switch (message.eventType) {
      case 'ORDER_CREATED': {
        const tracedWrapper = createMessageWrapper({
          payload: message.wrapper.payload as StartSagaPayload,
          correlationId: message.orderId,
          causationId: message.wrapper.metadata.messageId,
          source: `outbox-relay:${message.eventType}`,
          traceContext: traceCarrier,
        });
        const started =
          await this.orderWorkflow.startOrderWorkflow(tracedWrapper);
        return started.isOk()
          ? Result.ok({ workflowInstanceId: started.value.workflowInstanceId })
          : Result.err(started.error);
      }

      case 'WORKFLOW_STEP_COMPLETED': {
        if (!message.correlationToken) {
          return Result.err(
            new OutboxPoisonMessageError(message.id, 'missing correlationToken'),
          );
        }
        const completed = await this.orderWorkflow.completeWorkflowStep({
          correlationToken: message.correlationToken,
          output: message.wrapper.payload,
        });
        return completed.isOk() ? Result.ok({}) : Result.err(completed.error);
      }

      case 'WORKFLOW_STEP_FAILED': {
        if (!message.correlationToken || !message.errorDetails) {
          return Result.err(
            new OutboxPoisonMessageError(
              message.id,
              'missing correlationToken or errorDetails',
            ),
          );
        }
        const failed = await this.orderWorkflow.failWorkflowStep({
          correlationToken: message.correlationToken,
          error: message.errorDetails,
        });
        return failed.isOk() ? Result.ok({}) : Result.err(failed.error);
      }

      default:
        return Result.err(
          new OutboxPoisonMessageError(
            message.id,
            `unsupported eventType '${message.eventType}'`,
          ),
        );
    }
  }
}
