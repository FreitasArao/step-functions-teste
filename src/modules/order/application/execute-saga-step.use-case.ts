import { Inject, Injectable, Logger } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import {
  TELEMETRY_PORT,
  type TelemetryPort,
} from '../../../infra/telemetry/tracer.js';
import type { DomainError } from '../../../shared/core/domain-error.js';
import { Result } from '../../../shared/core/result.js';
import {
  createMessageWrapper,
  type SagaStepName,
} from '../../../shared/messaging/message-wrapper.js';
import {
  UNIT_OF_WORK_PORT,
  type UnitOfWorkPort,
} from '../../../shared/messaging/unit-of-work.port.js';
import { ProcessPaymentUseCase } from '../../payment/application/process-payment.use-case.js';
import { RefundPaymentUseCase } from '../../payment/application/refund-payment.use-case.js';
import { CreateShipmentUseCase } from '../../shipping/application/create-shipment.use-case.js';
import { ReleaseStockUseCase } from '../../stock/application/release-stock.use-case.js';
import { ReserveStockUseCase } from '../../stock/application/reserve-stock.use-case.js';
import { CancelOrderUseCase } from './cancel-order.use-case.js';
import { ConfirmOrderUseCase } from './confirm-order.use-case.js';
import type {
  ExecuteSagaStepInboundPort,
  ExecuteSagaStepOutput,
  SagaStepContextPayload,
  WorkflowStepCommand,
} from './ports/saga-step.inbound-port.js';

/**
 * Pure Application Layer Use Case that executes a single Saga step.
 * 100% agnostic of AWS, SQS, Step Functions or Temporal.
 *
 * Everything happens inside ONE ACID transaction (UnitOfWork):
 * 1. Transactional Inbox  -> deduplicates the incoming message (idempotencyKey = orderId:stepName).
 * 2. Domain Use Case      -> changes the business state (only on first delivery).
 * 3. Inbox record         -> stores the outcome ('OK' | 'ERR') so redeliveries replay it.
 * 4. Transactional Outbox -> stores the callback to the orchestrator
 *                            (WORKFLOW_STEP_COMPLETED | WORKFLOW_STEP_FAILED + correlationToken).
 *
 * The callback is NEVER sent over the network from here (that would be a dual-write).
 * The Outbox Relay (RelayOutboxUseCase) delivers it through OrderWorkflowPort after COMMIT.
 *
 * On a duplicate delivery (e.g. the orchestrator retried the step with a NEW correlationToken
 * because the previous callback was lost), the stored outcome is replayed into a NEW outbox
 * message bound to the current correlationToken — the domain is NOT executed again.
 */
@Injectable()
export class ExecuteSagaStepUseCase implements ExecuteSagaStepInboundPort {
  private readonly logger = new Logger(ExecuteSagaStepUseCase.name);

  constructor(
    @Inject(UNIT_OF_WORK_PORT)
    private readonly unitOfWork: UnitOfWorkPort,
    @Inject(TELEMETRY_PORT)
    private readonly telemetry: TelemetryPort,
    private readonly reserveStockUseCase: ReserveStockUseCase,
    private readonly releaseStockUseCase: ReleaseStockUseCase,
    private readonly processPaymentUseCase: ProcessPaymentUseCase,
    private readonly refundPaymentUseCase: RefundPaymentUseCase,
    private readonly createShipmentUseCase: CreateShipmentUseCase,
    private readonly confirmOrderUseCase: ConfirmOrderUseCase,
    private readonly cancelOrderUseCase: CancelOrderUseCase,
  ) {}

  async execute(
    command: WorkflowStepCommand,
  ): Promise<Result<ExecuteSagaStepOutput, DomainError>> {
    const { stepName, idempotencyKey, correlationToken, wrapper } = command;
    const orderId = wrapper.payload.orderId;

    return this.telemetry.runInSpan(
      {
        operationName: `saga.step.${stepName.toLowerCase()}`,
        resourceName: `ExecuteSagaStepUseCase.${stepName}`,
        correlationId: orderId,
        parentCarrier: wrapper.metadata?.traceContext,
        tags: {
          'order.id': orderId,
          'saga.step': stepName,
          'idempotency.key': idempotencyKey,
        },
      },
      async (stepTraceCarrier) =>
        this.unitOfWork.execute<ExecuteSagaStepOutput, DomainError>(
          async ({ inbox, outbox }) => {
            let deduplicated: boolean;
            let outcome: 'OK' | 'ERR';
            let responsePayload: unknown;

            // 1. Transactional Inbox: deduplicate
            const existingInbox =
              await inbox.findByIdempotencyKey(idempotencyKey);

            if (existingInbox) {
              this.logger.log(
                `Idempotent hit in Inbox for key '${idempotencyKey}' - replaying stored outcome '${existingInbox.outcome}' without re-executing the domain`,
              );
              deduplicated = true;
              outcome = existingInbox.outcome;
              responsePayload = existingInbox.responsePayload;
            } else {
              // 2. Domain Use Case (same transaction)
              const stepResult = await this.dispatchDomainUseCase(
                stepName,
                wrapper.payload,
              );
              deduplicated = false;
              outcome = stepResult.isOk() ? 'OK' : 'ERR';
              responsePayload = stepResult.isOk()
                ? stepResult.value
                : toDomainErrorSnapshot(stepResult.error);

              // 3. Inbox record (PRIMARY KEY guarantees a concurrent duplicate rolls back)
              await inbox.save({
                idempotencyKey,
                orderId,
                stepName,
                outcome,
                responsePayload,
                wrapper: createMessageWrapper<SagaStepContextPayload>({
                  payload: wrapper.payload,
                  correlationId: orderId,
                  causationId: wrapper.metadata?.messageId,
                  source: `saga-step-executor:${stepName}`,
                  traceContext: stepTraceCarrier,
                  stepName,
                  idempotencyKey,
                }),
                processedAt: new Date().toISOString(),
              });
            }

            // 4. Transactional Outbox: callback to the orchestrator (same transaction)
            await outbox.save({
              id: `out_${randomUUID()}`,
              orderId,
              eventType:
                outcome === 'OK'
                  ? 'WORKFLOW_STEP_COMPLETED'
                  : 'WORKFLOW_STEP_FAILED',
              status: 'PENDING',
              correlationToken,
              ...(outcome === 'ERR'
                ? { errorDetails: responsePayload as DomainError }
                : {}),
              wrapper: createMessageWrapper({
                payload: responsePayload,
                correlationId: orderId,
                causationId: wrapper.metadata?.messageId,
                source: `saga-step-executor:${stepName}`,
                traceContext: stepTraceCarrier,
                stepName,
                idempotencyKey,
              }),
              createdAt: new Date().toISOString(),
            });

            return Result.ok({
              deduplicated,
              outcome,
              output: responsePayload,
            });
          },
        ),
    );
  }

  private async dispatchDomainUseCase(
    stepName: SagaStepName,
    payload: SagaStepContextPayload,
  ): Promise<Result<unknown, DomainError>> {
    switch (stepName) {
      case 'RESERVE_STOCK':
        return this.reserveStockUseCase.execute({
          orderId: payload.orderId,
          sku: payload.sku,
          quantity: payload.quantity,
        });

      case 'PROCESS_PAYMENT':
        return this.processPaymentUseCase.execute({
          orderId: payload.orderId,
          amount: payload.amount,
        });

      case 'CREATE_SHIPMENT':
        return this.createShipmentUseCase.execute({
          orderId: payload.orderId,
          zipCode: payload.zipCode,
        });

      case 'CONFIRM_ORDER':
        return this.confirmOrderUseCase.execute({
          orderId: payload.orderId,
        });

      case 'REFUND_PAYMENT':
        return this.refundPaymentUseCase.execute({
          orderId: payload.orderId,
        });

      case 'RELEASE_STOCK':
        return this.releaseStockUseCase.execute({
          orderId: payload.orderId,
        });

      case 'CANCEL_ORDER': {
        const reason = payload.failure?.code
          ? `${payload.failure.code}: ${payload.failure.message ?? ''}`
          : (payload.failure?.message ?? 'Order cancelled by Saga rollback');
        return this.cancelOrderUseCase.execute({
          orderId: payload.orderId,
          reason,
        });
      }
    }
  }
}

function toDomainErrorSnapshot(error: DomainError): DomainError {
  return {
    code: error.code,
    message: error.message,
    retryable: error.retryable ?? false,
    metadata: error.metadata ?? {},
  };
}
