import {
  BadRequestException,
  Body,
  Controller,
  Get,
  Headers,
  HttpCode,
  HttpStatus,
  Inject,
  NotFoundException,
  Param,
  Post,
  Query,
} from '@nestjs/common';
import { SagaPipelineRunnerService } from '../../../../infra/temporal/saga-pipeline-runner.service.js';
import { PostgresDatabaseService } from '../../../../infra/database/postgres-client.provider.js';
import {
  TELEMETRY_PORT,
  type TelemetryPort,
} from '../../../../infra/telemetry/tracer.js';
import {
  InMemoryInboxRepository,
  InMemoryOutboxRepository,
} from '../../../../shared/messaging/in-memory-messaging.store.js';
import {
  INBOX_REPOSITORY_PORT,
  type InboxRepositoryPort,
} from '../../../../shared/messaging/inbox.port.js';
import type { TraceContextCarrier } from '../../../../shared/messaging/message-wrapper.js';
import {
  OUTBOX_REPOSITORY_PORT,
  type OutboxRepositoryPort,
} from '../../../../shared/messaging/outbox.port.js';
import {
  PAYMENT_REPOSITORY_PORT,
  type PaymentRepositoryPort,
} from '../../../payment/domain/payment.repository.port.js';
import { InMemoryPaymentRepository } from '../../../payment/infra/in-memory-payment.repository.js';
import {
  SHIPMENT_REPOSITORY_PORT,
  type ShipmentRepositoryPort,
} from '../../../shipping/domain/shipment.repository.port.js';
import { InMemoryShipmentRepository } from '../../../shipping/infra/in-memory-shipment.repository.js';
import {
  STOCK_REPOSITORY_PORT,
  type StockRepositoryPort,
} from '../../../stock/domain/stock.repository.port.js';
import { InMemoryStockRepository } from '../../../stock/infra/in-memory-stock.repository.js';
import {
  CreateOrderUseCase,
  type CreateOrderInput,
} from '../../application/create-order.use-case.js';
import { GetOrderDiagnosticsUseCase } from '../../application/get-order-diagnostics.use-case.js';
import {
  ORDER_REPOSITORY_PORT,
  type OrderRepositoryPort,
} from '../../domain/order.repository.port.js';
import { InMemoryOrderRepository } from '../in-memory-order.repository.js';

@Controller()
export class OrderController {
  constructor(
    private readonly createOrderUseCase: CreateOrderUseCase,
    private readonly getOrderDiagnosticsUseCase: GetOrderDiagnosticsUseCase,
    private readonly pipelineRunner: SagaPipelineRunnerService,
    private readonly postgresDb: PostgresDatabaseService,
    private readonly memoryOrders: InMemoryOrderRepository,
    private readonly memoryStock: InMemoryStockRepository,
    private readonly memoryPayments: InMemoryPaymentRepository,
    private readonly memoryShipments: InMemoryShipmentRepository,
    private readonly memoryInbox: InMemoryInboxRepository,
    private readonly memoryOutbox: InMemoryOutboxRepository,
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
    @Inject(TELEMETRY_PORT)
    private readonly telemetry: TelemetryPort,
  ) {}

  @Post('orders')
  @HttpCode(HttpStatus.ACCEPTED)
  async createOrder(
    @Body() body: Omit<CreateOrderInput, 'parentTraceCarrier'>,
    @Headers() headers: Record<string, string | undefined>,
    @Query('wait') wait?: string,
  ) {
    const parentTraceCarrier: TraceContextCarrier = {};
    if (headers['x-datadog-trace-id']) {
      parentTraceCarrier['x-datadog-trace-id'] = headers['x-datadog-trace-id'];
    }
    if (headers['x-datadog-parent-id']) {
      parentTraceCarrier['x-datadog-parent-id'] =
        headers['x-datadog-parent-id'];
    }
    if (headers.traceparent) {
      parentTraceCarrier.traceparent = headers.traceparent;
    }

    const result = await this.createOrderUseCase.execute({
      sku: body.sku,
      quantity: body.quantity,
      amount: body.amount,
      zipCode: body.zipCode,
      ...(Object.keys(parentTraceCarrier).length > 0
        ? { parentTraceCarrier }
        : {}),
    });

    if (result.isErr()) {
      throw new BadRequestException({
        code: result.error.code,
        message: result.error.message,
      });
    }

    if (wait !== 'false') {
      await this.pipelineRunner.drainUntilSettled();
    }

    return result.value;
  }

  @Get('orders/:id')
  async getOrderDiagnostics(@Param('id') id: string) {
    const result = await this.getOrderDiagnosticsUseCase.execute(id);
    if (result.isErr()) {
      throw new NotFoundException({
        code: result.error.code,
        message: result.error.message,
      });
    }
    return result.value;
  }

  @Get('stock')
  async getStockOverview() {
    const [items, reservations] = await Promise.all([
      this.stockRepository.findAllItems(),
      this.stockRepository.findAllReservations(),
    ]);
    return {
      items: items.map((item) => item.toJSON()),
      reservations: reservations.map((res) => res.toJSON()),
    };
  }

  @Get('db')
  async getDatabaseDump() {
    const [
      orders,
      stockItems,
      stockReservations,
      payments,
      shipments,
      inboxMessages,
      outboxMessages,
    ] = await Promise.all([
      this.orderRepository.findAll(),
      this.stockRepository.findAllItems(),
      this.stockRepository.findAllReservations(),
      this.paymentRepository.findAll(),
      this.shipmentRepository.findAll(),
      this.inboxRepository.findAll(),
      this.outboxRepository.findAll(),
    ]);

    return {
      databaseEngine: this.postgresDb.isPostgresActive()
        ? 'postgresql'
        : 'in-memory',
      tables: {
        orders: orders.map((o) => o.toJSON()),
        stock_items: stockItems.map((i) => i.toJSON()),
        stock_reservations: stockReservations.map((r) => r.toJSON()),
        payments: payments.map((p) => p.toJSON()),
        shipments: shipments.map((s) => s.toJSON()),
        inbox_messages: inboxMessages,
        outbox_messages: outboxMessages,
        telemetry_spans: this.telemetry.getAllSpans(),
      },
    };
  }

  @Post('db/reset')
  @HttpCode(HttpStatus.OK)
  async resetDatabase() {
    await this.postgresDb.resetAllTables();
    this.memoryOrders.clear();
    this.memoryStock.seedDefaults();
    this.memoryPayments.clear();
    this.memoryShipments.clear();
    this.memoryInbox.clear();
    this.memoryOutbox.clear();
    this.telemetry.clear();

    return {
      status: 'RESET_COMPLETED',
      databaseEngine: this.postgresDb.isPostgresActive()
        ? 'postgresql'
        : 'in-memory',
      message:
        'All database tables, Inbox, Outbox, and Telemetry spans reset to initial state.',
    };
  }
}
