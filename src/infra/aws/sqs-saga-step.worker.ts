import type { SQSClient } from '@aws-sdk/client-sqs';
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
import { SQS_CLIENT } from './aws-clients.provider.js';
import { AwsStepFunctionsTranslator } from './translators/sfn-acl.translator.js';

@Injectable()
export class SqsSagaStepWorker implements OnModuleInit, OnModuleDestroy {
  private timer?: ReturnType<typeof setInterval>;

  constructor(
    @Inject(SQS_CLIENT)
    private readonly _sqsClient: SQSClient,
    private readonly _translator: AwsStepFunctionsTranslator,
    private readonly executeSagaStepUseCase: ExecuteSagaStepUseCase,
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

  async pollQueueOnce(): Promise<number> {
    // TODO (Prática - SQS Adapter + ACL Translator):
    // 1. Ler mensagens da fila SQS 'order-saga-tasks-queue' (ReceiveMessageCommand)
    // 2. Traduzir o JSON da AWS para o comando agnóstico:
    //    `const command = this._translator.toWorkflowStepCommand(sqsMsg.Body)`
    // 3. Executar o caso de uso agnóstico:
    //    `await this.executeSagaStepUseCase.execute(command)`
    // 4. Deletar mensagem da fila (DeleteMessageCommand)
    return 0;
  }

  async handleCommand(
    command: WorkflowStepCommand,
  ): Promise<
    Result<{ deduplicated: boolean; output?: unknown }, DomainError>
  > {
    return this.executeSagaStepUseCase.execute(command);
  }
}
