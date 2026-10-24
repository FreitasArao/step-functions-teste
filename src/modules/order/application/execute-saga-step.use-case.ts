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
import type {
  SagaStepContextPayload,
  WorkflowStepCommand,
} from '../domain/order-workflow.port.js';
import { CancelOrderUseCase } from './cancel-order.use-case.js';
import { ConfirmOrderUseCase } from './confirm-order.use-case.js';

/**
 * Pure Application Layer Use Case that executes a single Saga step.
 * 100% agnostic of AWS, SQS, or Step Functions.
 * Coordinates: Distributed Trace Span -> UnitOfWork(Inbox -> Domain UseCase -> Outbox).
 */
@Injectable()
export class ExecuteSagaStepUseCase {
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
  ): Promise<
    Result<{ deduplicated: boolean; output?: unknown }, DomainError>
  > {
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
      async (stepTraceCarrier) => {
        return this.unitOfWork.execute(async ({ inbox, outbox }) => {
          // 1. Transactional Inbox check (Idempotency)
          const existingInbox =
            await inbox.findByIdempotencyKey(idempotencyKey);
          if (existingInbox) {
            this.logger.log(
              `Idempotent hit in Inbox for key '${idempotencyKey}' - skipping duplicate domain execution`,
            );
            return Result.ok({
              deduplicated: true,
              output: existingInbox.responsePayload,
            });
          }

          // 2. Dispatch pure Domain Use Case returning Result<T, DomainError>
          const stepResult = await this.dispatchDomainUseCase(
            stepName,
            wrapper.payload,
          );

          const tracedIncomingWrapper =
            createMessageWrapper<SagaStepContextPayload>({
              payload: wrapper.payload,
              correlationId: orderId,
              causationId: wrapper.metadata?.messageId,
              source: `saga-step-executor:${stepName}`,
              traceContext: stepTraceCarrier,
              stepName,
              idempotencyKey,
            });

          // 3. Persist in Transactional Inbox
          await inbox.save({
            idempotencyKey,
            orderId,
            stepName,
            outcome: stepResult.isOk() ? 'OK' : 'ERR',
            responsePayload: stepResult.isOk()
              ? stepResult.value
              : stepResult.error,
            wrapper: tracedIncomingWrapper,
            processedAt: new Date().toISOString(),
          });

          // 4. Persist agnostic outcome in Transactional Outbox
          if (stepResult.isOk()) {
            const outboxWrapper = createMessageWrapper({
              payload: stepResult.value,
              correlationId: orderId,
              causationId: tracedIncomingWrapper.metadata.messageId,
              source: `saga-step-completed:${stepName}`,
              traceContext: stepTraceCarrier,
              stepName,
              idempotencyKey,
            });

            await outbox.save({
              id: `out_${randomUUID().slice(0, 8)}`,
              orderId,
              eventType: 'WORKFLOW_STEP_COMPLETED',
              status: 'PENDING',
              wrapper: outboxWrapper,
              correlationToken,
              createdAt: new Date().toISOString(),
            });

            return Result.ok({
              deduplicated: false,
              output: stepResult.value,
            });
          }

          const outboxFailureWrapper = createMessageWrapper({
            payload: stepResult.error,
            correlationId: orderId,
            causationId: tracedIncomingWrapper.metadata.messageId,
            source: `saga-step-failed:${stepName}`,
            traceContext: stepTraceCarrier,
            stepName,
            idempotencyKey,
          });

          await outbox.save({
            id: `out_${randomUUID().slice(0, 8)}`,
            orderId,
            eventType: 'WORKFLOW_STEP_FAILED',
            status: 'PENDING',
            wrapper: outboxFailureWrapper,
            correlationToken,
            errorDetails: stepResult.error,
            createdAt: new Date().toISOString(),
          });

          return Result.err(stepResult.error);
        });
      },
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
