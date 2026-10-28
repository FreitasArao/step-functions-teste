import { Test, type TestingModule } from '@nestjs/testing';
import { AppModule } from './app.module.js';
import { SagaPipelineRunnerService } from './infra/temporal/saga-pipeline-runner.service.js';
import { TemporalAclTranslator } from './infra/temporal/translators/temporal-acl.translator.js';
import { CancelOrderUseCase } from './modules/order/application/cancel-order.use-case.js';
import { ConfirmOrderUseCase } from './modules/order/application/confirm-order.use-case.js';
import { CreateOrderUseCase } from './modules/order/application/create-order.use-case.js';
import { ExecuteSagaStepUseCase } from './modules/order/application/execute-saga-step.use-case.js';
import { GetOrderDiagnosticsUseCase } from './modules/order/application/get-order-diagnostics.use-case.js';
import { OrderController } from './modules/order/infra/http/order.controller.js';
import { ProcessPaymentUseCase } from './modules/payment/application/process-payment.use-case.js';
import { RefundPaymentUseCase } from './modules/payment/application/refund-payment.use-case.js';
import { CreateShipmentUseCase } from './modules/shipping/application/create-shipment.use-case.js';
import { ReleaseStockUseCase } from './modules/stock/application/release-stock.use-case.js';
import { ReserveStockUseCase } from './modules/stock/application/reserve-stock.use-case.js';
import {
  STOCK_REPOSITORY_PORT,
  type StockRepositoryPort,
} from './modules/stock/domain/stock.repository.port.js';
import { createMessageWrapper } from './shared/messaging/message-wrapper.js';
import {
  UNIT_OF_WORK_PORT,
  type UnitOfWorkPort,
} from './shared/messaging/unit-of-work.port.js';

describe('Domain Logic, Use Cases (Result Pattern), Temporal.io Adapter & Database UnitOfWork', () => {
  let moduleRef: TestingModule;
  let createOrderUseCase: CreateOrderUseCase;
  let confirmOrderUseCase: ConfirmOrderUseCase;
  let cancelOrderUseCase: CancelOrderUseCase;
  let reserveStockUseCase: ReserveStockUseCase;
  let releaseStockUseCase: ReleaseStockUseCase;
  let processPaymentUseCase: ProcessPaymentUseCase;
  let refundPaymentUseCase: RefundPaymentUseCase;
  let createShipmentUseCase: CreateShipmentUseCase;
  let executeSagaStepUseCase: ExecuteSagaStepUseCase;
  let getOrderDiagnosticsUseCase: GetOrderDiagnosticsUseCase;
  let temporalTranslator: TemporalAclTranslator;
  let pipelineRunner: SagaPipelineRunnerService;
  let stockRepository: StockRepositoryPort;
  let unitOfWork: UnitOfWorkPort;
  let orderController: OrderController;

  beforeAll(async () => {
    process.env.DISABLE_BACKGROUND_WORKERS = 'true';
    process.env.USE_LOCAL_TEMPORAL_ENGINE = 'true';

    moduleRef = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();

    createOrderUseCase = moduleRef.get(CreateOrderUseCase);
    confirmOrderUseCase = moduleRef.get(ConfirmOrderUseCase);
    cancelOrderUseCase = moduleRef.get(CancelOrderUseCase);
    reserveStockUseCase = moduleRef.get(ReserveStockUseCase);
    releaseStockUseCase = moduleRef.get(ReleaseStockUseCase);
    processPaymentUseCase = moduleRef.get(ProcessPaymentUseCase);
    refundPaymentUseCase = moduleRef.get(RefundPaymentUseCase);
    createShipmentUseCase = moduleRef.get(CreateShipmentUseCase);
    executeSagaStepUseCase = moduleRef.get(ExecuteSagaStepUseCase);
    getOrderDiagnosticsUseCase = moduleRef.get(GetOrderDiagnosticsUseCase);
    temporalTranslator = moduleRef.get(TemporalAclTranslator);
    pipelineRunner = moduleRef.get(SagaPipelineRunnerService);
    stockRepository = moduleRef.get(STOCK_REPOSITORY_PORT);
    unitOfWork = moduleRef.get(UNIT_OF_WORK_PORT);
    orderController = moduleRef.get(OrderController);
  });

  beforeEach(async () => {
    await orderController.resetDatabase();
  });

  afterAll(async () => {
    await moduleRef.close();
  });

  it('validates Stock Use Cases (ReserveStock & ReleaseStock) with Result<T, E>', async () => {
    const reserveOk = await reserveStockUseCase.execute({
      orderId: 'ord_1',
      sku: 'SKU-100',
      quantity: 4,
    });
    expect(reserveOk.isOk()).toBe(true);
    if (reserveOk.isOk()) {
      expect(reserveOk.value.remainingAvailable).toBe(6);
      expect(reserveOk.value.status).toBe('RESERVED');
    }

    const reserveFail = await reserveStockUseCase.execute({
      orderId: 'ord_2',
      sku: 'SKU-100',
      quantity: 20,
    });
    expect(reserveFail.isErr()).toBe(true);
    if (reserveFail.isErr()) {
      expect(reserveFail.error.code).toBe('INSUFFICIENT_STOCK');
    }

    const releaseOk = await releaseStockUseCase.execute({ orderId: 'ord_1' });
    expect(releaseOk.isOk()).toBe(true);
    if (releaseOk.isOk()) {
      expect(releaseOk.value.availableQuantityAfterRelease).toBe(10);
      expect(releaseOk.value.status).toBe('RELEASED');
    }
  });

  it('validates Payment Use Cases (ProcessPayment & RefundPayment) with Result<T, E>', async () => {
    const paymentOk = await processPaymentUseCase.execute({
      orderId: 'ord_1',
      amount: 800,
    });
    expect(paymentOk.isOk()).toBe(true);
    if (paymentOk.isOk()) {
      expect(paymentOk.value.status).toBe('APPROVED');
    }

    const refundOk = await refundPaymentUseCase.execute({ orderId: 'ord_1' });
    expect(refundOk.isOk()).toBe(true);
    if (refundOk.isOk()) {
      expect(refundOk.value.status).toBe('REFUNDED');
    }

    const paymentDeclined = await processPaymentUseCase.execute({
      orderId: 'ord_2',
      amount: 1500,
    });
    expect(paymentDeclined.isErr()).toBe(true);
    if (paymentDeclined.isErr()) {
      expect(paymentDeclined.error.code).toBe('PAYMENT_DECLINED');
    }
  });

  it('validates Shipping & Order Use Cases with Result<T, E>', async () => {
    const created = await createOrderUseCase.execute({
      sku: 'SKU-100',
      quantity: 1,
      amount: 200,
      zipCode: '01001-000',
    });
    expect(created.isOk()).toBe(true);
    if (!created.isOk()) return;

    const orderId = created.value.order.id;

    const shipmentOk = await createShipmentUseCase.execute({
      orderId,
      zipCode: '01001-000',
    });
    expect(shipmentOk.isOk()).toBe(true);

    const shipmentFail = await createShipmentUseCase.execute({
      orderId: 'ord_bad_zip',
      zipCode: '99000-000',
    });
    expect(shipmentFail.isErr()).toBe(true);
    if (shipmentFail.isErr()) {
      expect(shipmentFail.error.code).toBe('SHIPPING_UNAVAILABLE');
    }

    const confirmed = await confirmOrderUseCase.execute({ orderId });
    expect(confirmed.isOk()).toBe(true);

    const cannotCancelConfirmed = await cancelOrderUseCase.execute({
      orderId,
      reason: 'Too late',
    });
    expect(cannotCancelConfirmed.isErr()).toBe(true);
  });

  it('rolls back both Inbox and Outbox writes if an unexpected database failure happens mid-transaction', async () => {
    const wrapper = createMessageWrapper({
      payload: { orderId: 'ord_tx_rollback' },
      correlationId: 'ord_tx_rollback',
      source: 'test-tx-rollback',
      traceContext: { 'x-datadog-trace-id': '5555555555555555555' },
      stepName: 'RESERVE_STOCK',
    });

    await expect(
      unitOfWork.execute(async ({ inbox, outbox }) => {
        await inbox.save({
          idempotencyKey: 'ord_tx_rollback:RESERVE_STOCK',
          orderId: 'ord_tx_rollback',
          stepName: 'RESERVE_STOCK',
          outcome: 'OK',
          responsePayload: {},
          wrapper,
          processedAt: new Date().toISOString(),
        });

        await outbox.save({
          id: 'out_tx_rollback_1',
          orderId: 'ord_tx_rollback',
          eventType: 'WORKFLOW_STEP_COMPLETED',
          status: 'PENDING',
          wrapper,
          createdAt: new Date().toISOString(),
        });

        throw new Error('Simulated DB crash before COMMIT');
      }),
    ).rejects.toThrow('Simulated DB crash before COMMIT');

    const dump = await orderController.getDatabaseDump();
    expect(dump.tables.inbox_messages).toHaveLength(0);
    expect(dump.tables.outbox_messages).toHaveLength(0);
  });

  describe('Saga Completa com Outbox Agnóstico, Temporal ACL Translator, Temporal Workflow (Async Activity Completion) e Inbox', () => {
    it('Scenario 1 (Happy Path): executes ReserveStock -> ProcessPayment -> CreateShipment -> ConfirmOrder', async () => {
      const createResult = await createOrderUseCase.execute({
        sku: 'SKU-100',
        quantity: 2,
        amount: 450,
        zipCode: '01001-000',
        parentTraceCarrier: {
          'x-datadog-trace-id': '1000000000000000001',
          'x-datadog-parent-id': '2000000000000000001',
        },
      });
      expect(createResult.isOk()).toBe(true);
      if (!createResult.isOk()) return;

      const orderId = createResult.value.order.id;
      await pipelineRunner.drainUntilSettled();

      const diagResult = await getOrderDiagnosticsUseCase.execute(orderId);
      expect(diagResult.isOk()).toBe(true);
      if (!diagResult.isOk()) return;

      expect(diagResult.value.order.status).toBe('CONFIRMED');
      expect(diagResult.value.workflowExecution.status).toBe('COMPLETED');
      expect(
        diagResult.value.transactionalMessaging.inboxProcessedSteps.map(
          (s) => s.stepName,
        ),
      ).toEqual([
        'RESERVE_STOCK',
        'PROCESS_PAYMENT',
        'CREATE_SHIPMENT',
        'CONFIRM_ORDER',
      ]);
      expect(
        diagResult.value.transactionalMessaging.outboxEvents.map(
          (e) => e.eventType,
        ),
      ).toEqual([
        'ORDER_CREATED',
        'WORKFLOW_STEP_COMPLETED',
        'WORKFLOW_STEP_COMPLETED',
        'WORKFLOW_STEP_COMPLETED',
        'WORKFLOW_STEP_COMPLETED',
      ]);
      expect(
        diagResult.value.transactionalMessaging.outboxEvents.every(
          (e) => e.status === 'PUBLISHED',
        ),
      ).toBe(true);
      expect(diagResult.value.distributedTracing.traceIds).toContain(
        '1000000000000000001',
      );
    });

    it('Scenario 2 (Fail at Step 1): ReserveStock [ERR] -> CancelOrder', async () => {
      const createResult = await createOrderUseCase.execute({
        sku: 'SKU-100',
        quantity: 50,
        amount: 450,
        zipCode: '01001-000',
      });
      expect(createResult.isOk()).toBe(true);
      if (!createResult.isOk()) return;

      const orderId = createResult.value.order.id;
      await pipelineRunner.drainUntilSettled();

      const diagResult = await getOrderDiagnosticsUseCase.execute(orderId);
      expect(diagResult.isOk()).toBe(true);
      if (!diagResult.isOk()) return;

      expect(diagResult.value.order.status).toBe('CANCELLED');
      expect(diagResult.value.order.cancellationReason).toContain(
        'INSUFFICIENT_STOCK',
      );
      expect(diagResult.value.workflowExecution.status).toBe('ROLLED_BACK');
    });

    it('Scenario 3 (1-Level Rollback): ReserveStock [OK] -> ProcessPayment [ERR] -> ReleaseStock -> CancelOrder', async () => {
      const createResult = await createOrderUseCase.execute({
        sku: 'SKU-100',
        quantity: 3,
        amount: 2500,
        zipCode: '01001-000',
      });
      expect(createResult.isOk()).toBe(true);
      if (!createResult.isOk()) return;

      const orderId = createResult.value.order.id;
      await pipelineRunner.drainUntilSettled();

      const diagResult = await getOrderDiagnosticsUseCase.execute(orderId);
      expect(diagResult.isOk()).toBe(true);
      if (!diagResult.isOk()) return;

      expect(diagResult.value.order.status).toBe('CANCELLED');
      expect(diagResult.value.domainState.stockReservation?.status).toBe(
        'RELEASED',
      );
      expect(diagResult.value.domainState.currentSkuStock?.availableQuantity).toBe(
        10,
      );
      expect(
        diagResult.value.transactionalMessaging.inboxProcessedSteps.map(
          (s) => s.stepName,
        ),
      ).toEqual([
        'RESERVE_STOCK',
        'PROCESS_PAYMENT',
        'RELEASE_STOCK',
        'CANCEL_ORDER',
      ]);
    });

    it('Scenario 4 (2-Level Cascading Rollback): ReserveStock [OK] -> ProcessPayment [OK] -> CreateShipment [ERR] -> RefundPayment -> ReleaseStock -> CancelOrder', async () => {
      const createResult = await createOrderUseCase.execute({
        sku: 'SKU-100',
        quantity: 4,
        amount: 800,
        zipCode: '99000-000',
      });
      expect(createResult.isOk()).toBe(true);
      if (!createResult.isOk()) return;

      const orderId = createResult.value.order.id;
      await pipelineRunner.drainUntilSettled();

      const diagResult = await getOrderDiagnosticsUseCase.execute(orderId);
      expect(diagResult.isOk()).toBe(true);
      if (!diagResult.isOk()) return;

      expect(diagResult.value.order.status).toBe('CANCELLED');
      expect(diagResult.value.domainState.payment?.status).toBe('REFUNDED');
      expect(diagResult.value.domainState.stockReservation?.status).toBe(
        'RELEASED',
      );
      expect(diagResult.value.domainState.currentSkuStock?.availableQuantity).toBe(
        10,
      );
      expect(
        diagResult.value.transactionalMessaging.inboxProcessedSteps.map(
          (s) => s.stepName,
        ),
      ).toEqual([
        'RESERVE_STOCK',
        'PROCESS_PAYMENT',
        'CREATE_SHIPMENT',
        'REFUND_PAYMENT',
        'RELEASE_STOCK',
        'CANCEL_ORDER',
      ]);
    });

    it('Scenario 5 (Transactional Inbox Idempotency + Temporal ACL Translation): duplicate Temporal Activity delivery does not reserve stock twice', async () => {
      const rawTemporalEnvelope = JSON.stringify({
        stepName: 'RESERVE_STOCK',
        idempotencyKey: 'ord_idempotent_test:RESERVE_STOCK',
        taskToken: Buffer.from(
          'temporal_local_token:ord_idempotent_test:RESERVE_STOCK:dup1',
          'utf8',
        ).toString('base64'),
        workflowId: 'order-saga-ord_idempotent_test',
        runId: 'run_dup_1',
        metadata: createMessageWrapper({
          payload: {},
          correlationId: 'ord_idempotent_test',
          source: 'temporal-workflow',
          traceContext: {
            'x-datadog-trace-id': '1234567890123456789',
            'x-datadog-parent-id': '9876543210987654321',
          },
        }).metadata,
        payload: {
          orderId: 'ord_idempotent_test',
          sku: 'SKU-100',
          quantity: 3,
          amount: 300,
          zipCode: '01001-000',
        },
      });

      const agnosticCommand =
        temporalTranslator.toWorkflowStepCommand(rawTemporalEnvelope);
      const firstCall = await executeSagaStepUseCase.execute(agnosticCommand);
      expect(firstCall.isOk()).toBe(true);
      if (firstCall.isOk()) {
        expect(firstCall.value.deduplicated).toBe(false);
        expect(firstCall.value.outcome).toBe('OK');
      }

      // Simulate Temporal Activity Retry (same idempotencyKey, NEW taskToken)
      const retryEnvelope = JSON.stringify({
        ...JSON.parse(rawTemporalEnvelope),
        taskToken: Buffer.from(
          'temporal_local_token:ord_idempotent_test:RESERVE_STOCK:dup2_new_token',
          'utf8',
        ).toString('base64'),
      });
      const retryCommand =
        temporalTranslator.toWorkflowStepCommand(retryEnvelope);
      const secondCall = await executeSagaStepUseCase.execute(retryCommand);
      expect(secondCall.isOk()).toBe(true);
      if (secondCall.isOk()) {
        expect(secondCall.value.deduplicated).toBe(true);
        expect(secondCall.value.outcome).toBe('OK');
      }

      const skuItem = await stockRepository.findBySku('SKU-100');
      expect(skuItem?.availableQuantity).toBe(7);

      const dump = await orderController.getDatabaseDump();
      const outboxForOrder = dump.tables.outbox_messages.filter(
        (m) => m.orderId === 'ord_idempotent_test',
      );
      expect(outboxForOrder).toHaveLength(2);
      expect(outboxForOrder[0]?.correlationToken).not.toBe(
        outboxForOrder[1]?.correlationToken,
      );
    });
  });
});
