import {
  Inject,
  Injectable,
  Logger,
  type OnModuleDestroy,
  type OnModuleInit,
} from '@nestjs/common';
import { NativeConnection, Worker } from '@temporalio/worker';
import { fileURLToPath } from 'node:url';
import {
  EXECUTE_SAGA_STEP_INBOUND_PORT,
  type ExecuteSagaStepInboundPort,
  type ExecuteSagaStepOutput,
  type WorkflowStepCommand,
} from '../../modules/order/application/ports/saga-step.inbound-port.js';
import type { DomainError } from '../../shared/core/domain-error.js';
import type { Result } from '../../shared/core/result.js';
import { createOrderSagaActivities } from './activities/order-saga.activities.js';
import { TemporalConnectionService } from './temporal-client.provider.js';
import { TemporalLocalEngineService } from './temporal-local-engine.service.js';
import { TemporalSagaStepActivityHandler } from './temporal-saga-step-activity.handler.js';
import { TEMPORAL_ORDER_SAGA_TASK_QUEUE } from './translators/temporal-acl.translator.js';

/**
 * Driving Worker for Temporal Saga Steps:
 * - In live Temporal mode, starts the official `@temporalio/worker` polling `order-saga-task-queue`.
 * - In local emulator test mode (`NODE_ENV=test` / `USE_LOCAL_TEMPORAL_ENGINE=true`), drains
 *   activity tasks from `TemporalLocalEngineService`.
 */
@Injectable()
export class TemporalSagaStepWorker implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(TemporalSagaStepWorker.name);
  private timer?: ReturnType<typeof setInterval>;
  private isPolling = false;
  private nativeConnection?: NativeConnection;
  private worker?: Worker;
  private workerRunPromise?: Promise<void>;

  constructor(
    private readonly activityHandler: TemporalSagaStepActivityHandler,
    @Inject(EXECUTE_SAGA_STEP_INBOUND_PORT)
    private readonly executeSagaStepPort: ExecuteSagaStepInboundPort,
    private readonly localEngine: TemporalLocalEngineService,
    private readonly temporalConnection: TemporalConnectionService,
  ) {}

  async onModuleInit(): Promise<void> {
    if (process.env.DISABLE_BACKGROUND_WORKERS === 'true') {
      return;
    }

    if (this.temporalConnection.isLocalEngineMode()) {
      const intervalMs = Number(
        process.env.TEMPORAL_STEP_POLL_INTERVAL_MS ?? 200,
      );
      this.timer = setInterval(() => {
        void this.pollActivityQueueOnce();
      }, intervalMs);
      return;
    }

    if (await this.temporalConnection.isServerReachable()) {
      await this.startEmbeddedWorker();
    }
  }

  private async startEmbeddedWorker(): Promise<void> {
    try {
      this.nativeConnection = await NativeConnection.connect({
        address: this.temporalConnection.address,
      });
      const workflowsPath = fileURLToPath(
        new URL('./workflows/order-saga.workflow.js', import.meta.url),
      );

      this.worker = await Worker.create({
        connection: this.nativeConnection,
        namespace: this.temporalConnection.namespace,
        taskQueue: TEMPORAL_ORDER_SAGA_TASK_QUEUE,
        workflowsPath,
        activities: createOrderSagaActivities((envelope) =>
          this.activityHandler.handle(envelope),
        ),
      });

      this.workerRunPromise = this.worker.run().catch((err) => {
        this.logger.error(
          `[Temporal Worker] Stopped with error: ${err instanceof Error ? err.message : String(err)}`,
        );
      });

      this.logger.log(
        `[Temporal Worker] Polling TaskQueue='${TEMPORAL_ORDER_SAGA_TASK_QUEUE}' for Workflows & Activities`,
      );
    } catch (err) {
      this.logger.error(
        `[Temporal Worker] Could not start Temporal Worker: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
  }

  async onModuleDestroy(): Promise<void> {
    if (this.timer) {
      clearInterval(this.timer);
    }
    try {
      if (this.worker) {
        this.worker.shutdown();
        await this.workerRunPromise;
      }
      if (this.nativeConnection) {
        await this.nativeConnection.close();
      }
    } catch {
      // Ignore cleanup errors on shutdown
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
