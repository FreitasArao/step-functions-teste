import {
  DeleteMessageCommand,
  GetQueueUrlCommand,
  ReceiveMessageCommand,
  type SQSClient,
} from '@aws-sdk/client-sqs';
import {
  Inject,
  Injectable,
  Logger,
  type OnModuleDestroy,
  type OnModuleInit,
} from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { CancelOrderUseCase } from '../../modules/order/application/cancel-order.use-case.js';
import { ConfirmOrderUseCase } from '../../modules/order/application/confirm-order.use-case.js';
import { ProcessPaymentUseCase } from '../../modules/payment/application/process-payment.use-case.js';
import { RefundPaymentUseCase } from '../../modules/payment/application/refund-payment.use-case.js';
import { CreateShipmentUseCase } from '../../modules/shipping/application/create-shipment.use-case.js';
import { ReleaseStockUseCase } from '../../modules/stock/application/release-stock.use-case.js';
import { ReserveStockUseCase } from '../../modules/stock/application/reserve-stock.use-case.js';
import type { DomainError } from '../../shared/core/domain-error.js';
import { Result } from '../../shared/core/result.js';
import {
  createMessageWrapper,
  type MessageWrapper,
  type SagaStepName,
} from '../../shared/messaging/message-wrapper.js';
import {
  UNIT_OF_WORK_PORT,
  type UnitOfWorkPort,
} from '../../shared/messaging/unit-of-work.port.js';
import {
  TELEMETRY_PORT,
  type TelemetryPort,
} from '../telemetry/tracer.js';
import { AslLocalEngineService } from './asl-local-engine.service.js';
import { SQS_CLIENT } from './aws-clients.provider.js';

export interface SagaStatePayload {
  readonly orderId: string;
  readonly sku: string;
  readonly quantity: number;
  readonly amount: number;
  readonly zipCode: string;
  readonly error?: {
    readonly Error?: string;
    readonly Cause?: string;
  };
  readonly [key: string]: unknown;
}

@Injectable()
export class SqsSagaStepWorker implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(SqsSagaStepWorker.name);
  private timer?: ReturnType<typeof setInterval>;
  private isPolling = false;
  private cachedQueueUrl?: string;

  constructor(
    @Inject(SQS_CLIENT)
    private readonly sqsClient: SQSClient,
    @Inject(UNIT_OF_WORK_PORT)
    private readonly unitOfWork: UnitOfWorkPort,
    @Inject(TELEMETRY_PORT)
    private readonly telemetry: TelemetryPort,
    private readonly aslLocalEngine: AslLocalEngineService,
    private readonly reserveStockUseCase: ReserveStockUseCase,
    private readonly releaseStockUseCase: ReleaseStockUseCase,
    private readonly processPaymentUseCase: ProcessPaymentUseCase,
    private readonly refundPaymentUseCase: RefundPaymentUseCase,
    private readonly createShipmentUseCase: CreateShipmentUseCase,
    private readonly confirmOrderUseCase: ConfirmOrderUseCase,
    private readonly cancelOrderUseCase: CancelOrderUseCase,
  ) {}

  onModuleInit(): void {
    if (process.env.DISABLE_BACKGROUND_WORKERS === 'true') {
      return;
    }
    const intervalMs = Number(process.env.SQS_POLL_INTERVAL_MS ?? 300);
    this.timer = setInterval(() => {
      void this.pollQueueOnce();
    }, intervalMs);
  }

  onModuleDestroy(): void {
    if (this.timer) {
      clearInterval(this.timer);
    }
  }

  private async resolveQueueUrl(): Promise<string | null> {
    if (process.env.SQS_QUEUE_URL) {
      return process.env.SQS_QUEUE_URL;
    }
    if (this.cachedQueueUrl) {
      return this.cachedQueueUrl;
    }
    try {
      const response = await this.sqsClient.send(
        new GetQueueUrlCommand({ QueueName: 'order-saga-tasks-queue' }),
      );
      if (response.QueueUrl) {
        this.cachedQueueUrl = response.QueueUrl;
        return response.QueueUrl;
      }
      return null;
    } catch {
      return null;
    }
  }

  async pollQueueOnce(): Promise<number> {
    if (this.isPolling) {
      return 0;
    }
    this.isPolling = true;
    let processedCount = 0;

    try {
      // 1. Drain any locally buffered ASL engine messages first
      const localMessages = this.aslLocalEngine.dequeueMessages(5);
      for (const localWrapper of localMessages) {
        await this.handleStepMessage(localWrapper);
        processedCount += 1;
      }

      // 2. Poll MiniStack SQS queue if reachable
      const queueUrl = await this.resolveQueueUrl();
      if (!queueUrl) {
        return processedCount;
      }

      const response = await this.sqsClient.send(
        new ReceiveMessageCommand({
          QueueUrl: queueUrl,
          MaxNumberOfMessages: 5,
          WaitTimeSeconds: 0,
        }),
      );

      for (const sqsMsg of response.Messages ?? []) {
        if (!sqsMsg.Body) {
          continue;
        }
        const wrapper = JSON.parse(
          sqsMsg.Body,
        ) as MessageWrapper<SagaStatePayload>;
        await this.handleStepMessage(wrapper);

        if (sqsMsg.ReceiptHandle) {
          await this.sqsClient.send(
            new DeleteMessageCommand({
              QueueUrl: queueUrl,
              ReceiptHandle: sqsMsg.ReceiptHandle,
            }),
          );
        }
        processedCount += 1;
      }

      return processedCount;
    } catch {
      return processedCount;
    } finally {
      this.isPolling = false;
    }
  }

  /**
   * Executes a single SQS Saga Step MessageWrapper using:
   * 1. Datadog Distributed Trace context extraction (child span of the Saga trace)
   * 2. Transactional Inbox check (idempotency per orderId:stepName)
   * 3. Pure Domain Use Case returning Result<T, DomainError>
   * 4. Transactional Outbox insertion (SFN_TASK_SUCCESS or SFN_TASK_FAILURE)
   */
  async handleStepMessage(
    wrapper: MessageWrapper<SagaStatePayload>,
  ): Promise<
    Result<{ deduplicated: boolean; output?: unknown }, DomainError>
  > {
    const stepName = wrapper.stepName ?? 'RESERVE_STOCK';
    const orderId = wrapper.payload.orderId;
    const idempotencyKey =
      wrapper.idempotencyKey ?? `${orderId}:${stepName}`;
    const taskToken = wrapper.taskToken ?? `token_${orderId}_${stepName}`;

    return this.telemetry.runInSpan(
      {
        operationName: `saga.step.${stepName.toLowerCase()}`,
        resourceName: `SqsSagaStepWorker.${stepName}`,
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
          // 1. Check Transactional Inbox for duplicate SQS delivery
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

          // 2. Execute Pure Domain Use Case (Result<T, DomainError>, zero exceptions)
          const stepResult = await this.dispatchUseCase(
            stepName,
            wrapper.payload,
          );

          const tracedIncomingWrapper = createMessageWrapper<SagaStatePayload>({
            payload: wrapper.payload,
            correlationId: orderId,
            causationId: wrapper.metadata?.messageId,
            source: `sqs-worker:${stepName}`,
            traceContext: stepTraceCarrier,
            stepName,
            idempotencyKey,
            taskToken,
          });

          // 3. Save in Transactional Inbox
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

          // 4. Save callback in Transactional Outbox (SendTaskSuccess or SendTaskFailure)
          if (stepResult.isOk()) {
            const outboxWrapper = createMessageWrapper({
              payload: stepResult.value,
              correlationId: orderId,
              causationId: tracedIncomingWrapper.metadata.messageId,
              source: `saga-step-ok:${stepName}`,
              traceContext: stepTraceCarrier,
              stepName,
              idempotencyKey,
              taskToken,
            });

            await outbox.save({
              id: `out_${randomUUID().slice(0, 8)}`,
              orderId,
              eventType: 'SFN_TASK_SUCCESS',
              status: 'PENDING',
              wrapper: outboxWrapper,
              taskToken,
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
            source: `saga-step-err:${stepName}`,
            traceContext: stepTraceCarrier,
            stepName,
            idempotencyKey,
            taskToken,
          });

          await outbox.save({
            id: `out_${randomUUID().slice(0, 8)}`,
            orderId,
            eventType: 'SFN_TASK_FAILURE',
            status: 'PENDING',
            wrapper: outboxFailureWrapper,
            taskToken,
            errorDetails: stepResult.error,
            createdAt: new Date().toISOString(),
          });

          return Result.err(stepResult.error);
        });
      },
    );
  }

  private async dispatchUseCase(
    stepName: SagaStepName,
    payload: SagaStatePayload,
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
        let reason = 'Order cancelled by Saga rollback';
        if (payload.error?.Cause) {
          try {
            const parsedCause = JSON.parse(payload.error.Cause) as {
              code?: string;
              message?: string;
            };
            reason = parsedCause.code
              ? `${parsedCause.code}: ${parsedCause.message ?? ''}`
              : payload.error.Cause;
          } catch {
            reason = payload.error.Cause;
          }
        } else if (payload.error?.Error) {
          reason = payload.error.Error;
        }
        return this.cancelOrderUseCase.execute({
          orderId: payload.orderId,
          reason,
        });
      }
    }
  }
}
