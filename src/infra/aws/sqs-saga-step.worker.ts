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
      // 1. Drain any locally buffered raw SQS JSON bodies through the AWS ACL Translator (unit tests)
      const localBodies = this.aslLocalEngine.dequeueRawSqsBodies(5);
      for (const rawBody of localBodies) {
        const command = this.translator.toWorkflowStepCommand(rawBody);
        await this.executeSagaStepPort.execute(command);
        processedCount += 1;
      }

      // 2. Poll LocalStack SQS queue if reachable
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

        await this.executeSagaStepPort.execute(command);

        if (sqsMsg.ReceiptHandle) {
          await this.sqsClient.send(
            new DeleteMessageCommand({
              QueueUrl: queueUrl,
              ReceiptHandle: sqsMsg.ReceiptHandle,
            }),
          );
          this.logger.log(
            `[SqsSagaStepWorker] Finished step='${command.stepName}' (orderId=${command.wrapper.payload.orderId}) and deleted message from SQS`,
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
