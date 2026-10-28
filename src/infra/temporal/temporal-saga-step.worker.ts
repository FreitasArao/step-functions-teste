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
import { TemporalConnectionService } from './temporal-client.provider.js';
import { TemporalLocalEngineService } from './temporal-local-engine.service.js';
import { TemporalSagaStepActivityHandler } from './temporal-saga-step-activity.handler.js';

/**
 * Driving Worker used ONLY in local emulator test mode (`NODE_ENV=test` / `USE_LOCAL_TEMPORAL_ENGINE=true`)
 * to drain activity tasks from `TemporalLocalEngineService` and invoke `TemporalSagaStepActivityHandler`.
 *
 * When connected to a real Temporal Server, the official `@temporalio/worker` invokes
 * `TemporalSagaStepActivityHandler.handle(envelope)` directly inside the Activity (Temporal Server
 * itself is the durable queue, with zero in-memory intermediate buffer).
 */
@Injectable()
export class TemporalSagaStepWorker implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(TemporalSagaStepWorker.name);
  private timer?: ReturnType<typeof setInterval>;
  private isPolling = false;

  constructor(
    private readonly activityHandler: TemporalSagaStepActivityHandler,
    @Inject(EXECUTE_SAGA_STEP_INBOUND_PORT)
    private readonly executeSagaStepPort: ExecuteSagaStepInboundPort,
    private readonly localEngine: TemporalLocalEngineService,
    private readonly temporalConnection: TemporalConnectionService,
  ) {}

  onModuleInit(): void {
    if (
      process.env.DISABLE_BACKGROUND_WORKERS === 'true' ||
      !this.temporalConnection.isLocalEngineMode()
    ) {
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
    if (!this.temporalConnection.isLocalEngineMode() || this.isPolling) {
      return 0;
    }
    this.isPolling = true;
    let processedCount = 0;

    try {
      const rawEnvelopes = this.localEngine.dequeueRawActivityEnvelopes(5);
      for (const rawJson of rawEnvelopes) {
        try {
          await this.activityHandler.handle(rawJson);
          processedCount += 1;
        } catch (err) {
          this.logger.warn(
            `[TemporalSagaStepWorker] Activity execution failed (${err instanceof Error ? err.message : String(err)}) — requeueing for retry.`,
          );
          this.localEngine.requeueActivityEnvelope(rawJson);
        }
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
