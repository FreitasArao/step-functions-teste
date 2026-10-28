import { Inject, Injectable, Logger } from '@nestjs/common';
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
import {
  ORDER_WORKFLOW_PORT,
  type OrderWorkflowPort,
} from './ports/order-workflow.outbound-port.js';
import type {
  ExecuteSagaStepInboundPort,
  ExecuteSagaStepOutput,
  SagaStepContextPayload,
  WorkflowStepCommand,
} from './ports/saga-step.inbound-port.js';

/**
 * Pure Application Layer Use Case that executes a single Saga step.
 * 100% agnostic of AWS, SQS, or Step Functions.
 * Coordinates:
 * 1. Distributed Trace Span
 * 2. UnitOfWork (Transactional Inbox check -> Domain UseCase -> Save in Inbox)
 * 3. Direct callback to OrderWorkflowPort (completeWorkflowStep / failWorkflowStep),
 *    including safe re-delivery if a duplicate SQS message arrives after a transient network error.
 */
@Injectable()
export class ExecuteSagaStepUseCase implements ExecuteSagaStepInboundPort {
  private readonly logger = new Logger(ExecuteSagaStepUseCase.name);

  constructor(
    @Inject(UNIT_OF_WORK_PORT)
    private readonly unitOfWork: UnitOfWorkPort,
    @Inject(ORDER_WORKFLOW_PORT)
    private readonly orderWorkflow: OrderWorkflowPort,
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
      async (stepTraceCarrier) => {
        // 1. Execute Domain UseCase and persist outcome in Transactional Inbox (ACID)
        const txOutcome = await this.unitOfWork.execute<
          {
            deduplicated: boolean;
            outcome: 'OK' | 'ERR';
            responsePayload: unknown;
          },
          DomainError
        >(async ({ inbox }) => {
          const existingInbox =
            await inbox.findByIdempotencyKey(idempotencyKey);
          if (existingInbox) {
            this.logger.log(
              `Idempotent hit in Inbox for key '${idempotencyKey}' - skipping duplicate domain execution`,
            );
            return Result.ok({
              deduplicated: true,
              outcome: existingInbox.outcome,
              responsePayload: existingInbox.responsePayload,
            });
          }

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

          const outcome: 'OK' | 'ERR' = stepResult.isOk() ? 'OK' : 'ERR';
          const responsePayload = stepResult.isOk()
            ? stepResult.value
            : stepResult.error;

          await inbox.save({
            idempotencyKey,
            orderId,
            stepName,
            outcome,
            responsePayload,
            wrapper: tracedIncomingWrapper,
            processedAt: new Date().toISOString(),
          });

          return Result.ok({
            deduplicated: false,
            outcome,
            responsePayload,
          });
        });

        if (txOutcome.isErr()) {
          return Result.err(txOutcome.error);
        }

        const { deduplicated, outcome, responsePayload } = txOutcome.value;

        // 2. Notify Orchestrator via OrderWorkflowPort (even on duplicate delivery if previous callback timed out)
        if (outcome === 'OK') {
          const callbackResult = await this.orderWorkflow.completeWorkflowStep({
            correlationToken,
            output: responsePayload,
          });
          if (callbackResult.isErr() && !deduplicated) {
            return Result.err(callbackResult.error);
          }
          return Result.ok({
            deduplicated,
            output: responsePayload,
          });
        }

        const domainError = responsePayload as DomainError;
        const failCallbackResult = await this.orderWorkflow.failWorkflowStep({
          correlationToken,
          error: domainError,
        });
        if (failCallbackResult.isErr() && !deduplicated) {
          return Result.err(failCallbackResult.error);
        }

        if (deduplicated) {
          return Result.ok({
            deduplicated: true,
            output: responsePayload,
          });
        }

        return Result.err(domainError);
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
