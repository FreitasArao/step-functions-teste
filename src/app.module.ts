import { Module } from '@nestjs/common';
import { createObserveModule } from '@nestjs/observe';
import { AppController } from './app.controller.js';
import { AppService } from './app.service.js';
import { AslLocalEngineService } from './infra/aws/asl-local-engine.service.js';
import {
  createSfnClient,
  createSqsClient,
  SFN_CLIENT,
  SQS_CLIENT,
} from './infra/aws/aws-clients.provider.js';
import { OutboxRelayWorker } from './infra/aws/outbox-relay.worker.js';
import { SagaPipelineRunnerService } from './infra/aws/saga-pipeline-runner.service.js';
import { StepFunctionsOrderWorkflowAdapter } from './infra/aws/sfn-order-workflow.adapter.js';
import { SqsSagaStepWorker } from './infra/aws/sqs-saga-step.worker.js';
import { AwsStepFunctionsTranslator } from './infra/aws/translators/sfn-acl.translator.js';
import { PostgresDatabaseService } from './infra/database/postgres-client.provider.js';
import {
  PostgresInboxRepository,
  PostgresOutboxRepository,
  PostgresUnitOfWork,
} from './infra/database/postgres-messaging.repositories.js';
import {
  DatadogTelemetryService,
  TELEMETRY_PORT,
} from './infra/telemetry/tracer.js';
import { CancelOrderUseCase } from './modules/order/application/cancel-order.use-case.js';
import { ConfirmOrderUseCase } from './modules/order/application/confirm-order.use-case.js';
import { CreateOrderUseCase } from './modules/order/application/create-order.use-case.js';
import { ExecuteSagaStepUseCase } from './modules/order/application/execute-saga-step.use-case.js';
import { GetOrderDiagnosticsUseCase } from './modules/order/application/get-order-diagnostics.use-case.js';
import { RelayOutboxUseCase } from './modules/order/application/relay-outbox.use-case.js';
import { ORDER_WORKFLOW_PORT } from './modules/order/domain/order-workflow.port.js';
import { ORDER_REPOSITORY_PORT } from './modules/order/domain/order.repository.port.js';
import { OrderController } from './modules/order/infra/http/order.controller.js';
import { InMemoryOrderRepository } from './modules/order/infra/in-memory-order.repository.js';
import { PostgresOrderRepository } from './modules/order/infra/postgres-order.repository.js';
import { ProcessPaymentUseCase } from './modules/payment/application/process-payment.use-case.js';
import { RefundPaymentUseCase } from './modules/payment/application/refund-payment.use-case.js';
import { PAYMENT_REPOSITORY_PORT } from './modules/payment/domain/payment.repository.port.js';
import { InMemoryPaymentRepository } from './modules/payment/infra/in-memory-payment.repository.js';
import { PostgresPaymentRepository } from './modules/payment/infra/postgres-payment.repository.js';
import { CreateShipmentUseCase } from './modules/shipping/application/create-shipment.use-case.js';
import { SHIPMENT_REPOSITORY_PORT } from './modules/shipping/domain/shipment.repository.port.js';
import { InMemoryShipmentRepository } from './modules/shipping/infra/in-memory-shipment.repository.js';
import { PostgresShipmentRepository } from './modules/shipping/infra/postgres-shipment.repository.js';
import { ReleaseStockUseCase } from './modules/stock/application/release-stock.use-case.js';
import { ReserveStockUseCase } from './modules/stock/application/reserve-stock.use-case.js';
import { STOCK_REPOSITORY_PORT } from './modules/stock/domain/stock.repository.port.js';
import { InMemoryStockRepository } from './modules/stock/infra/in-memory-stock.repository.js';
import { PostgresStockRepository } from './modules/stock/infra/postgres-stock.repository.js';
import {
  InMemoryInboxRepository,
  InMemoryOutboxRepository,
} from './shared/messaging/in-memory-messaging.store.js';
import { INBOX_REPOSITORY_PORT } from './shared/messaging/inbox.port.js';
import { OUTBOX_REPOSITORY_PORT } from './shared/messaging/outbox.port.js';
import { UNIT_OF_WORK_PORT } from './shared/messaging/unit-of-work.port.js';

export const { ObserveModule, ObserveInstrument } = createObserveModule();

@Module({
  imports: [
    ObserveModule.forRoot({
      appKey: 'YOUR_APP_KEY',
      appSecret: 'YOUR_APP_SECRET',
      serviceId: 'step-function',
    }),
  ],
  controllers: [AppController, OrderController],
  providers: [
    AppService,

    // Telemetry (Datadog dd-trace + Span Recorder)
    DatadogTelemetryService,
    {
      provide: TELEMETRY_PORT,
      useExisting: DatadogTelemetryService,
    },

    // PostgreSQL Connection Pool & AsyncLocalStorage Transaction Manager + InMemory Fallbacks
    PostgresDatabaseService,
    InMemoryInboxRepository,
    InMemoryOutboxRepository,
    InMemoryOrderRepository,
    InMemoryStockRepository,
    InMemoryPaymentRepository,
    InMemoryShipmentRepository,

    // Transactional Inbox, Outbox & Unit of Work (PostgreSQL ACID with InMemory fallback)
    PostgresInboxRepository,
    PostgresOutboxRepository,
    PostgresUnitOfWork,
    {
      provide: INBOX_REPOSITORY_PORT,
      useExisting: PostgresInboxRepository,
    },
    {
      provide: OUTBOX_REPOSITORY_PORT,
      useExisting: PostgresOutboxRepository,
    },
    {
      provide: UNIT_OF_WORK_PORT,
      useExisting: PostgresUnitOfWork,
    },

    // Domain Repositories (Ports -> Postgres Adapters)
    PostgresOrderRepository,
    {
      provide: ORDER_REPOSITORY_PORT,
      useExisting: PostgresOrderRepository,
    },
    PostgresStockRepository,
    {
      provide: STOCK_REPOSITORY_PORT,
      useExisting: PostgresStockRepository,
    },
    PostgresPaymentRepository,
    {
      provide: PAYMENT_REPOSITORY_PORT,
      useExisting: PostgresPaymentRepository,
    },
    PostgresShipmentRepository,
    {
      provide: SHIPMENT_REPOSITORY_PORT,
      useExisting: PostgresShipmentRepository,
    },

    // Domain & Application Use Cases (100% Agnostic of AWS/StepFunctions/SQS)
    CreateOrderUseCase,
    ConfirmOrderUseCase,
    CancelOrderUseCase,
    GetOrderDiagnosticsUseCase,
    ExecuteSagaStepUseCase,
    RelayOutboxUseCase,
    ReserveStockUseCase,
    ReleaseStockUseCase,
    ProcessPaymentUseCase,
    RefundPaymentUseCase,
    CreateShipmentUseCase,

    // AWS Translation Layer (ACL) + Step Functions & SQS Adapters + Workers
    {
      provide: SFN_CLIENT,
      useFactory: createSfnClient,
    },
    {
      provide: SQS_CLIENT,
      useFactory: createSqsClient,
    },
    AwsStepFunctionsTranslator,
    AslLocalEngineService,
    StepFunctionsOrderWorkflowAdapter,
    {
      provide: ORDER_WORKFLOW_PORT,
      useExisting: StepFunctionsOrderWorkflowAdapter,
    },
    OutboxRelayWorker,
    SqsSagaStepWorker,
    SagaPipelineRunnerService,
  ],
})
export class AppModule {}
