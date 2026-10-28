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
import {
  EXECUTE_SAGA_STEP_INBOUND_PORT,
  type ExecuteSagaStepInboundPort,
  type ExecuteSagaStepOutput,
  type WorkflowStepCommand,
} from '../../modules/order/application/ports/saga-step.inbound-port.js';
import type { DomainError } from '../../shared/core/domain-error.js';
import type { Result } from '../../shared/core/result.js';
import { AslLocalEngineService } from './asl-local-engine.service.js';
import { SQS_CLIENT } from './aws-clients.provider.js';
import { AwsStepFunctionsTranslator } from './translators/sfn-acl.translator.js';

@Injectable()
export class SqsSagaStepWorker implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(SqsSagaStepWorker.name);
  private timer?: ReturnType<typeof setInterval>;
  private isPolling = false;
  private cachedQueueUrl?: string;

  constructor(
    @Inject(SQS_CLIENT)
    private readonly sqsClient: SQSClient,
    private readonly translator: AwsStepFunctionsTranslator,
    @Inject(EXECUTE_SAGA_STEP_INBOUND_PORT)
    private readonly executeSagaStepPort: ExecuteSagaStepInboundPort,
    private readonly aslLocalEngine: AslLocalEngineService,
  ) {}

  private isLocalEngineMode(): boolean {
    return (
      process.env.USE_LOCAL_ASL_ENGINE === 'true' ||
      process.env.NODE_ENV === 'test'
    );
  }

  onModuleInit(): void {
    if (process.env.DISABLE_BACKGROUND_WORKERS === 'true') {
      return;
    }
    const intervalMs = Number(process.env.SQS_POLL_INTERVAL_MS ?? 250);
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
      if (this.isLocalEngineMode()) {
        // 1. Drain locally buffered raw SQS JSON bodies in test mode
        const localBodies = this.aslLocalEngine.dequeueRawSqsBodies(5);
        for (const rawBody of localBodies) {
          const command = this.translator.toWorkflowStepCommand(rawBody);
          const stepResult = await this.executeSagaStepPort.execute(command);
          if (stepResult.isErr()) {
            this.aslLocalEngine.requeueRawSqsBody(rawBody);
            continue;
          }
          processedCount += 1;
        }
        return processedCount;
      }

      // 2. Poll LocalStack SQS queue in live mode
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
        const command = this.translator.toWorkflowStepCommand(sqsMsg.Body);
        this.logger.log(
          `[SqsSagaStepWorker] Received step='${command.stepName}' (orderId=${command.wrapper.payload.orderId}, idempotencyKey=${command.idempotencyKey}) from QueueUrl=${queueUrl}`,
        );

        const stepResult = await this.executeSagaStepPort.execute(command);
        if (stepResult.isErr()) {
          // Do NOT delete the SQS message if the database transaction failed;
          // SQS will redeliver it after visibility timeout and the Inbox guarantees idempotency.
          this.logger.error(
            `[SqsSagaStepWorker] Failed to persist step='${command.stepName}' (orderId=${command.wrapper.payload.orderId}): ${stepResult.error.message} — keeping message in SQS for retry`,
          );
          continue;
        }

        if (sqsMsg.ReceiptHandle) {
          await this.sqsClient.send(
            new DeleteMessageCommand({
              QueueUrl: queueUrl,
              ReceiptHandle: sqsMsg.ReceiptHandle,
            }),
          );
          this.logger.log(
            `[SqsSagaStepWorker] Committed Inbox+Outbox for step='${command.stepName}' (orderId=${command.wrapper.payload.orderId}, outcome=${stepResult.value.outcome}) and deleted message from SQS`,
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

  async handleCommand(
    command: WorkflowStepCommand,
  ): Promise<Result<ExecuteSagaStepOutput, DomainError>> {
    return this.executeSagaStepPort.execute(command);
  }
}
