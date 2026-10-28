import { Inject, Injectable } from '@nestjs/common';
import {
  type RecordedSpan,
  TELEMETRY_PORT,
  type TelemetryPort,
} from '../../../infra/telemetry/tracer.js';
import type { DomainError } from '../../../shared/core/domain-error.js';
import { Result } from '../../../shared/core/result.js';
import {
  INBOX_REPOSITORY_PORT,
  type InboxMessage,
  type InboxRepositoryPort,
} from '../../../shared/messaging/inbox.port.js';
import {
  OUTBOX_REPOSITORY_PORT,
  type OutboxMessage,
  type OutboxRepositoryPort,
} from '../../../shared/messaging/outbox.port.js';
import type { PaymentProps } from '../../payment/domain/payment.entity.js';
import {
  PAYMENT_REPOSITORY_PORT,
  type PaymentRepositoryPort,
} from '../../payment/domain/payment.repository.port.js';
import type { ShipmentProps } from '../../shipping/domain/shipment.entity.js';
import {
  SHIPMENT_REPOSITORY_PORT,
  type ShipmentRepositoryPort,
} from '../../shipping/domain/shipment.repository.port.js';
import type {
  StockItemProps,
  StockReservationProps,
} from '../../stock/domain/stock-item.entity.js';
import {
  STOCK_REPOSITORY_PORT,
  type StockRepositoryPort,
} from '../../stock/domain/stock.repository.port.js';
import {
  ORDER_WORKFLOW_PORT,
  type OrderWorkflowPort,
  type WorkflowExecutionSummary,
} from './ports/order-workflow.outbound-port.js';
import type { OrderProps } from '../domain/order.entity.js';
import { OrderNotFoundError } from '../domain/order.errors.js';
import {
  ORDER_REPOSITORY_PORT,
  type OrderRepositoryPort,
} from '../domain/order.repository.port.js';

export interface OrderDiagnosticsOutput {
  readonly order: OrderProps;
  readonly domainState: {
    readonly stockReservation: StockReservationProps | null;
    readonly currentSkuStock: StockItemProps | null;
    readonly payment: PaymentProps | null;
    readonly shipment: ShipmentProps | null;
  };
  readonly workflowExecution: WorkflowExecutionSummary;
  readonly transactionalMessaging: {
    readonly inboxProcessedSteps: InboxMessage[];
    readonly outboxEvents: OutboxMessage[];
  };
  readonly distributedTracing: {
    readonly traceIds: string[];
    readonly totalSpans: number;
    readonly spans: RecordedSpan[];
  };
}

@Injectable()
export class GetOrderDiagnosticsUseCase {
  constructor(
    @Inject(ORDER_REPOSITORY_PORT)
    private readonly orderRepository: OrderRepositoryPort,
    @Inject(STOCK_REPOSITORY_PORT)
    private readonly stockRepository: StockRepositoryPort,
    @Inject(PAYMENT_REPOSITORY_PORT)
    private readonly paymentRepository: PaymentRepositoryPort,
    @Inject(SHIPMENT_REPOSITORY_PORT)
    private readonly shipmentRepository: ShipmentRepositoryPort,
    @Inject(INBOX_REPOSITORY_PORT)
    private readonly inboxRepository: InboxRepositoryPort,
    @Inject(OUTBOX_REPOSITORY_PORT)
    private readonly outboxRepository: OutboxRepositoryPort,
    @Inject(ORDER_WORKFLOW_PORT)
    private readonly orderWorkflow: OrderWorkflowPort,
    @Inject(TELEMETRY_PORT)
    private readonly telemetry: TelemetryPort,
  ) {}

  async execute(
    orderId: string,
  ): Promise<Result<OrderDiagnosticsOutput, DomainError>> {
    const order = await this.orderRepository.findById(orderId);
    if (!order) {
      return Result.err(new OrderNotFoundError(orderId));
    }

    const [
      stockReservation,
      currentSkuStock,
      payment,
      shipment,
      inboxMessages,
      outboxMessages,
      workflowStatusResult,
    ] = await Promise.all([
      this.stockRepository.findReservationByOrderId(orderId),
      this.stockRepository.findBySku(order.sku),
      this.paymentRepository.findByOrderId(orderId),
      this.shipmentRepository.findByOrderId(orderId),
      this.inboxRepository.findByOrderId(orderId),
      this.outboxRepository.findByOrderId(orderId),
      this.orderWorkflow.getWorkflowStatus(orderId),
    ]);

    const spans = this.telemetry.getSpansByCorrelationId(orderId);
    const traceIds = Array.from(new Set(spans.map((s) => s.traceId)));

    return Result.ok({
      order: order.toJSON(),
      domainState: {
        stockReservation: stockReservation ? stockReservation.toJSON() : null,
        currentSkuStock: currentSkuStock ? currentSkuStock.toJSON() : null,
        payment: payment ? payment.toJSON() : null,
        shipment: shipment ? shipment.toJSON() : null,
      },
      workflowExecution: workflowStatusResult.isOk()
        ? workflowStatusResult.value
        : { status: 'NOT_STARTED' },
      transactionalMessaging: {
        inboxProcessedSteps: inboxMessages,
        outboxEvents: outboxMessages,
      },
      distributedTracing: {
        traceIds,
        totalSpans: spans.length,
        spans,
      },
    });
  }
}
