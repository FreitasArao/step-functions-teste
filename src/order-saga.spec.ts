import { Test, type TestingModule } from '@nestjs/testing';
import { AppModule } from './app.module.js';
import { SagaPipelineRunnerService } from './infra/aws/saga-pipeline-runner.service.js';
import { SqsSagaStepWorker } from './infra/aws/sqs-saga-step.worker.js';
import { CreateOrderUseCase } from './modules/order/application/create-order.use-case.js';
import { GetOrderDiagnosticsUseCase } from './modules/order/application/get-order-diagnostics.use-case.js';
import { OrderController } from './modules/order/infra/http/order.controller.js';
import {
  STOCK_REPOSITORY_PORT,
  type StockRepositoryPort,
} from './modules/stock/domain/stock.repository.port.js';
import { createMessageWrapper } from './shared/messaging/message-wrapper.js';
import {
  UNIT_OF_WORK_PORT,
  type UnitOfWorkPort,
} from './shared/messaging/unit-of-work.port.js';

describe('Order Saga with Step Functions, SQS waitForTaskToken, Outbox/Inbox, Result Pattern & dd-trace', () => {
  let moduleRef: TestingModule;
  let createOrderUseCase: CreateOrderUseCase;
  let getOrderDiagnosticsUseCase: GetOrderDiagnosticsUseCase;
  let pipelineRunner: SagaPipelineRunnerService;
  let sqsWorker: SqsSagaStepWorker;
  let stockRepository: StockRepositoryPort;
  let unitOfWork: UnitOfWorkPort;
  let orderController: OrderController;

  beforeAll(async () => {
    process.env.DISABLE_BACKGROUND_WORKERS = 'true';
    process.env.USE_LOCAL_ASL_ENGINE = 'true';

    moduleRef = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();

    createOrderUseCase = moduleRef.get(CreateOrderUseCase);
    getOrderDiagnosticsUseCase = moduleRef.get(GetOrderDiagnosticsUseCase);
    pipelineRunner = moduleRef.get(SagaPipelineRunnerService);
    sqsWorker = moduleRef.get(SqsSagaStepWorker);
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

  it('Scenario 1 (Happy Path): executes ReserveStock -> ProcessPayment -> CreateShipment -> ConfirmOrder and propagates a single dd-trace traceId across all Outbox/SQS/Inbox messages', async () => {
    const createResult = await createOrderUseCase.execute({
      sku: 'SKU-100',
      quantity: 2,
      amount: 450,
      zipCode: '01001-000',
    });

    expect(createResult.isOk()).toBe(true);
    if (!createResult.isOk()) return;

    const orderId = createResult.value.order.id;
    const rootTraceId =
      createResult.value.traceContext['x-datadog-trace-id'];
    expect(rootTraceId).toBeDefined();

    await pipelineRunner.drainUntilSettled();

    const diagResult = await getOrderDiagnosticsUseCase.execute(orderId);
    expect(diagResult.isOk()).toBe(true);
    if (!diagResult.isOk()) return;

    const diag = diagResult.value;
    expect(diag.order.status).toBe('CONFIRMED');
    expect(diag.domainState.stockReservation?.status).toBe('RESERVED');
    expect(diag.domainState.currentSkuStock?.availableQuantity).toBe(8);
    expect(diag.domainState.currentSkuStock?.reservedQuantity).toBe(2);
    expect(diag.domainState.payment?.status).toBe('APPROVED');
    expect(diag.domainState.shipment?.status).toBe('DISPATCHED');
    expect(diag.stepFunctionsExecution.status).toBe('SUCCEEDED');

    // Verify Inbox steps
    const inboxSteps = diag.transactionalMessaging.inboxProcessedSteps.map(
      (i) => i.stepName,
    );
    expect(inboxSteps).toEqual([
      'RESERVE_STOCK',
      'PROCESS_PAYMENT',
      'CREATE_SHIPMENT',
      'CONFIRM_ORDER',
    ]);

    // Verify Outbox events were all PUBLISHED
    expect(
      diag.transactionalMessaging.outboxEvents.every(
        (o) => o.status === 'PUBLISHED',
      ),
    ).toBe(true);

    // Verify Distributed Tracing: single shared traceId across the entire flow
    expect(diag.distributedTracing.traceIds).toEqual([rootTraceId]);
    expect(diag.distributedTracing.totalSpans).toBeGreaterThanOrEqual(9);
    for (const inboxMsg of diag.transactionalMessaging.inboxProcessedSteps) {
      expect(
        inboxMsg.wrapper.metadata.traceContext['x-datadog-trace-id'],
      ).toBe(rootTraceId);
    }
  });

  it('Scenario 2 (Step 1 Failure - Insufficient Stock): fails at ReserveStock and transitions directly to CancelOrder without touching stock balance', async () => {
    const createResult = await createOrderUseCase.execute({
      sku: 'SKU-100',
      quantity: 25, // Only 10 available
      amount: 500,
      zipCode: '01001-000',
    });

    expect(createResult.isOk()).toBe(true);
    if (!createResult.isOk()) return;

    const orderId = createResult.value.order.id;
    await pipelineRunner.drainUntilSettled();

    const diagResult = await getOrderDiagnosticsUseCase.execute(orderId);
    expect(diagResult.isOk()).toBe(true);
    if (!diagResult.isOk()) return;

    const diag = diagResult.value;
    expect(diag.order.status).toBe('CANCELLED');
    expect(diag.order.cancellationReason).toContain('INSUFFICIENT_STOCK');
    expect(diag.domainState.currentSkuStock?.availableQuantity).toBe(10);
    expect(diag.domainState.currentSkuStock?.reservedQuantity).toBe(0);
    expect(diag.domainState.payment).toBeNull();
    expect(diag.domainState.shipment).toBeNull();

    const inboxSteps = diag.transactionalMessaging.inboxProcessedSteps.map(
      (i) => ({ step: i.stepName, outcome: i.outcome }),
    );
    expect(inboxSteps).toEqual([
      { step: 'RESERVE_STOCK', outcome: 'ERR' },
      { step: 'CANCEL_ORDER', outcome: 'OK' },
    ]);
  });

  it('Scenario 3 (Level 1 Rollback - Payment Declined): reserves stock, fails at ProcessPayment, compensates via ReleaseStock and cancels order', async () => {
    const createResult = await createOrderUseCase.execute({
      sku: 'SKU-100',
      quantity: 3,
      amount: 2500, // Exceeds limit of 1000 -> PAYMENT_DECLINED
      zipCode: '01001-000',
    });

    expect(createResult.isOk()).toBe(true);
    if (!createResult.isOk()) return;

    const orderId = createResult.value.order.id;
    await pipelineRunner.drainUntilSettled();

    const diagResult = await getOrderDiagnosticsUseCase.execute(orderId);
    expect(diagResult.isOk()).toBe(true);
    if (!diagResult.isOk()) return;

    const diag = diagResult.value;
    expect(diag.order.status).toBe('CANCELLED');
    expect(diag.order.cancellationReason).toContain('PAYMENT_DECLINED');

    // Prove stock was reserved and then released back to 10!
    expect(diag.domainState.stockReservation?.status).toBe('RELEASED');
    expect(diag.domainState.currentSkuStock?.availableQuantity).toBe(10);
    expect(diag.domainState.currentSkuStock?.reservedQuantity).toBe(0);
    expect(diag.domainState.payment?.status).toBe('DECLINED');
    expect(diag.domainState.shipment).toBeNull();

    const inboxSteps = diag.transactionalMessaging.inboxProcessedSteps.map(
      (i) => ({ step: i.stepName, outcome: i.outcome }),
    );
    expect(inboxSteps).toEqual([
      { step: 'RESERVE_STOCK', outcome: 'OK' },
      { step: 'PROCESS_PAYMENT', outcome: 'ERR' },
      { step: 'RELEASE_STOCK', outcome: 'OK' },
      { step: 'CANCEL_ORDER', outcome: 'OK' },
    ]);
  });

  it('Scenario 4 (Level 2 Cascading Rollback - Shipping Unavailable): executes ReserveStock -> ProcessPayment -> CreateShipment(ERR) -> RefundPayment -> ReleaseStock -> CancelOrder', async () => {
    const createResult = await createOrderUseCase.execute({
      sku: 'SKU-100',
      quantity: 4,
      amount: 800,
      zipCode: '99123-000', // Prefix 99 is unsupported -> SHIPPING_UNAVAILABLE
    });

    expect(createResult.isOk()).toBe(true);
    if (!createResult.isOk()) return;

    const orderId = createResult.value.order.id;
    const rootTraceId =
      createResult.value.traceContext['x-datadog-trace-id'];

    await pipelineRunner.drainUntilSettled();

    const diagResult = await getOrderDiagnosticsUseCase.execute(orderId);
    expect(diagResult.isOk()).toBe(true);
    if (!diagResult.isOk()) return;

    const diag = diagResult.value;
    expect(diag.order.status).toBe('CANCELLED');
    expect(diag.order.cancellationReason).toContain('SHIPPING_UNAVAILABLE');

    // Prove Level 2 Compensation (RefundPayment) AND Level 1 Compensation (ReleaseStock) both ran!
    expect(diag.domainState.payment?.status).toBe('REFUNDED');
    expect(diag.domainState.stockReservation?.status).toBe('RELEASED');
    expect(diag.domainState.currentSkuStock?.availableQuantity).toBe(10);
    expect(diag.domainState.currentSkuStock?.reservedQuantity).toBe(0);
    expect(diag.domainState.shipment?.status).toBe('FAILED');
    expect(diag.stepFunctionsExecution.status).toBe('FAILED');

    const inboxSteps = diag.transactionalMessaging.inboxProcessedSteps.map(
      (i) => ({ step: i.stepName, outcome: i.outcome }),
    );
    expect(inboxSteps).toEqual([
      { step: 'RESERVE_STOCK', outcome: 'OK' },
      { step: 'PROCESS_PAYMENT', outcome: 'OK' },
      { step: 'CREATE_SHIPMENT', outcome: 'ERR' },
      { step: 'REFUND_PAYMENT', outcome: 'OK' },
      { step: 'RELEASE_STOCK', outcome: 'OK' },
      { step: 'CANCEL_ORDER', outcome: 'OK' },
    ]);

    // Distributed trace id is preserved even through all cascading rollback steps
    expect(diag.distributedTracing.traceIds).toEqual([rootTraceId]);
  });

  it('Scenario 5 (Transactional Inbox Idempotency): duplicate SQS message delivery does not reserve stock twice', async () => {
    const duplicateWrapper = createMessageWrapper({
      payload: {
        orderId: 'ord_idempotent_test',
        sku: 'SKU-100',
        quantity: 3,
        amount: 300,
        zipCode: '01001-000',
      },
      correlationId: 'ord_idempotent_test',
      source: 'test-sqs-redelivery',
      traceContext: {
        'x-datadog-trace-id': '1234567890123456789',
        'x-datadog-parent-id': '9876543210987654321',
      },
      stepName: 'RESERVE_STOCK',
      idempotencyKey: 'ord_idempotent_test:RESERVE_STOCK',
      taskToken: 'task_token_dup_1',
    });

    const firstCall = await sqsWorker.handleStepMessage(duplicateWrapper);
    expect(firstCall.isOk()).toBe(true);
    if (firstCall.isOk()) {
      expect(firstCall.value.deduplicated).toBe(false);
    }

    const secondCall = await sqsWorker.handleStepMessage(duplicateWrapper);
    expect(secondCall.isOk()).toBe(true);
    if (secondCall.isOk()) {
      expect(secondCall.value.deduplicated).toBe(true);
    }

    const skuItem = await stockRepository.findBySku('SKU-100');
    expect(skuItem?.availableQuantity).toBe(7); // Deducted 3 only ONCE (10 - 3 = 7)
    expect(skuItem?.reservedQuantity).toBe(3);
  });

  it('Scenario 6 (UnitOfWork Atomic Rollback): rolls back both Inbox and Outbox writes if an unexpected database failure happens mid-transaction', async () => {
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
          eventType: 'SFN_TASK_SUCCESS',
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
});
