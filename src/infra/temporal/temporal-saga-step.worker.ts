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
import { TemporalLocalEngineService } from './temporal-local-engine.service.js';
import { TemporalAclTranslator } from './translators/temporal-acl.translator.js';

/**
 * Driving Worker that drains Temporal Activity tasks (carrying Temporal's base64 taskToken),
 * translates them via TemporalAclTranslator into the cloud-agnostic WorkflowStepCommand,
 * and invokes ExecuteSagaStepInboundPort (which runs Transactional Inbox + Domain UseCase + AsyncCompletionClient callback).
 */
@Injectable()
export class TemporalSagaStepWorker implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(TemporalSagaStepWorker.name);
  private timer?: ReturnType<typeof setInterval>;
  private isPolling = false;

  constructor(
    private readonly translator: TemporalAclTranslator,
    @Inject(EXECUTE_SAGA_STEP_INBOUND_PORT)
    private readonly executeSagaStepPort: ExecuteSagaStepInboundPort,
    private readonly localEngine: TemporalLocalEngineService,
  ) {}

  onModuleInit(): void {
    if (process.env.DISABLE_BACKGROUND_WORKERS === 'true') {
      return;
    }
    const intervalMs = Number(process.env.TEMPORAL_STEP_POLL_INTERVAL_MS ?? 200);
    this.timer = setInterval(() => {
      void this.pollActivityQueueOnce();
    }, intervalMs);
  }

  onModuleDestroy(): void {
    if (this.timer) {
      clearInterval(this.timer);
    }
  }

  async pollActivityQueueOnce(): Promise<number> {
    if (this.isPolling) {
      return 0;
    }
    this.isPolling = true;
    let processedCount = 0;

    try {
      const rawEnvelopes = this.localEngine.dequeueRawActivityEnvelopes(5);
      for (const rawJson of rawEnvelopes) {
        const command = this.translator.toWorkflowStepCommand(rawJson);
        this.logger.log(
          `[TemporalSagaStepWorker] Executing activity step='${command.stepName}' (orderId=${command.wrapper.payload.orderId}, idempotencyKey=${command.idempotencyKey})`,
        );

        await this.executeSagaStepPort.execute(command);

        this.logger.log(
          `[TemporalSagaStepWorker] Completed activity step='${command.stepName}' (orderId=${command.wrapper.payload.orderId})`,
        );
        processedCount += 1;
      }

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
