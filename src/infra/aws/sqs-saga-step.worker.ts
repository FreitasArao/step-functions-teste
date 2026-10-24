import {
  DeleteMessageCommand,
  GetQueueUrlCommand,
  ReceiveMessageCommand,
  type SQSClient,
} from '@aws-sdk/client-sqs';
import {
  Inject,
  Injectable,
  type OnModuleDestroy,
  type OnModuleInit,
} from '@nestjs/common';
import { ExecuteSagaStepUseCase } from '../../modules/order/application/execute-saga-step.use-case.js';
import type { WorkflowStepCommand } from '../../modules/order/domain/order-workflow.port.js';
import type { DomainError } from '../../shared/core/domain-error.js';
import type { Result } from '../../shared/core/result.js';
import { AslLocalEngineService } from './asl-local-engine.service.js';
import { SQS_CLIENT } from './aws-clients.provider.js';
import { AwsStepFunctionsTranslator } from './translators/sfn-acl.translator.js';

/**
 * Primary / Driving Adapter for AWS SQS.
 * Receives raw SQS messages from Step Functions (.waitForTaskToken),
 * translates them via AwsStepFunctionsTranslator (ACL) into agnostic WorkflowStepCommands,
 * and invokes ExecuteSagaStepUseCase.
 */
@Injectable()
export class SqsSagaStepWorker implements OnModuleInit, OnModuleDestroy {
  private timer?: ReturnType<typeof setInterval>;
  private isPolling = false;
  private cachedQueueUrl?: string;

  constructor(
    @Inject(SQS_CLIENT)
    private readonly sqsClient: SQSClient,
    private readonly translator: AwsStepFunctionsTranslator,
    private readonly executeSagaStepUseCase: ExecuteSagaStepUseCase,
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
      // 1. Drain any locally buffered raw SQS JSON bodies through the AWS ACL Translator
      const localBodies = this.aslLocalEngine.dequeueRawSqsBodies(5);
      for (const rawBody of localBodies) {
        const command = this.translator.toWorkflowStepCommand(rawBody);
        await this.executeSagaStepUseCase.execute(command);
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
        // Translate AWS Step Functions SQS JSON -> Agnostic WorkflowStepCommand
        const command = this.translator.toWorkflowStepCommand(sqsMsg.Body);
        await this.executeSagaStepUseCase.execute(command);

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

  async handleCommand(
    command: WorkflowStepCommand,
  ): Promise<
    Result<{ deduplicated: boolean; output?: unknown }, DomainError>
  > {
    return this.executeSagaStepUseCase.execute(command);
  }
}
