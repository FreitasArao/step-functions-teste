import {
  Injectable,
  Logger,
  type OnModuleDestroy,
  type OnModuleInit,
} from '@nestjs/common';
import {
  AsyncCompletionClient,
  Client,
  Connection,
  WorkflowClient,
} from '@temporalio/client';
import { NativeConnection, Worker } from '@temporalio/worker';
import { Socket } from 'node:net';
import { fileURLToPath } from 'node:url';
import { createOrderSagaActivities } from './activities/order-saga.activities.js';
import { TemporalSagaStepActivityHandler } from './temporal-saga-step-activity.handler.js';
import { TEMPORAL_ORDER_SAGA_TASK_QUEUE } from './translators/temporal-acl.translator.js';

const RECONNECT_INTERVAL_MS = 5_000;

/**
 * The in-process Temporal emulator is an explicit TEST DOUBLE.
 * It is never used as a silent fallback when the real Temporal Server is down
 * (that would let the Outbox mark messages as PUBLISHED into volatile memory).
 */
export function isLocalTemporalEngineMode(): boolean {
  return (
    process.env.NODE_ENV === 'test' ||
    process.env.USE_LOCAL_TEMPORAL_ENGINE === 'true'
  );
}

@Injectable()
export class TemporalConnectionService
  implements OnModuleInit, OnModuleDestroy
{
  private readonly logger = new Logger(TemporalConnectionService.name);
  private connection?: Connection;
  private client?: Client;
  private nativeConnection?: NativeConnection;
  private worker?: Worker;
  private workerRunPromise?: Promise<void>;
  private connecting?: Promise<boolean>;
  private lastAttemptAt = 0;

  constructor(
    private readonly activityHandler: TemporalSagaStepActivityHandler,
  ) {}

  get address(): string {
    return process.env.TEMPORAL_ADDRESS ?? 'localhost:7233';
  }

  get namespace(): string {
    return process.env.TEMPORAL_NAMESPACE ?? 'default';
  }

  isLocalEngineMode(): boolean {
    return isLocalTemporalEngineMode();
  }

  async onModuleInit(): Promise<void> {
    if (this.isLocalEngineMode()) {
      this.logger.log(
        '[Temporal] Local in-process Temporal emulator enabled (test mode).',
      );
      return;
    }
    await this.ensureConnected();
  }

  /**
   * Returns the WorkflowClient or null when Temporal is unreachable.
   * Callers MUST return Result.err in that case so the Outbox keeps the message PENDING.
   */
  async getWorkflowClient(): Promise<WorkflowClient | null> {
    return (await this.ensureConnected()) ? (this.client?.workflow ?? null) : null;
  }

  async getAsyncCompletionClient(): Promise<AsyncCompletionClient | null> {
    return (await this.ensureConnected()) ? (this.client?.activity ?? null) : null;
  }

  private async ensureConnected(): Promise<boolean> {
    if (this.client) {
      return true;
    }
    if (this.connecting) {
      return this.connecting;
    }
    if (Date.now() - this.lastAttemptAt < RECONNECT_INTERVAL_MS) {
      return false;
    }
    this.lastAttemptAt = Date.now();
    this.connecting = this.connect().finally(() => {
      this.connecting = undefined;
    });
    return this.connecting;
  }

  private async connect(): Promise<boolean> {
    const reachable = await this.checkTcpReachable(this.address, 800);
    if (!reachable) {
      this.logger.warn(
        `[Temporal] Temporal Server at ${this.address} is not reachable — Outbox messages stay PENDING and will be retried (reconnect every ${RECONNECT_INTERVAL_MS / 1000}s).`,
      );
      return false;
    }

    try {
      const connection = await Connection.connect({ address: this.address });
      this.connection = connection;
      this.client = new Client({ connection, namespace: this.namespace });
      this.logger.log(
        `[Temporal] Connected to Temporal Server at ${this.address} (namespace=${this.namespace})`,
      );

      if (process.env.DISABLE_BACKGROUND_WORKERS !== 'true') {
        await this.startEmbeddedWorker();
      }
      return true;
    } catch (err) {
      this.client = undefined;
      this.logger.warn(
        `[Temporal] Failed to connect (${err instanceof Error ? err.message : String(err)}) — Outbox messages stay PENDING and will be retried.`,
      );
      return false;
    }
  }

  private async startEmbeddedWorker(): Promise<void> {
    try {
      this.nativeConnection = await NativeConnection.connect({
        address: this.address,
      });
      const workflowsPath = fileURLToPath(
        new URL('./workflows/order-saga.workflow.js', import.meta.url),
      );

      this.worker = await Worker.create({
        connection: this.nativeConnection,
        namespace: this.namespace,
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
    try {
      if (this.worker) {
        this.worker.shutdown();
        await this.workerRunPromise;
      }
      if (this.nativeConnection) {
        await this.nativeConnection.close();
      }
      if (this.connection) {
        await this.connection.close();
      }
    } catch {
      // Ignore cleanup errors on shutdown
    }
  }

  private checkTcpReachable(
    hostPort: string,
    timeoutMs: number,
  ): Promise<boolean> {
    return new Promise((resolve) => {
      const [host = 'localhost', portStr = '7233'] = hostPort.split(':');
      const port = Number(portStr);
      const socket = new Socket();
      let settled = false;

      const finish = (ok: boolean) => {
        if (!settled) {
          settled = true;
          socket.destroy();
          resolve(ok);
        }
      };

      socket.setTimeout(timeoutMs);
      socket.once('connect', () => finish(true));
      socket.once('timeout', () => finish(false));
      socket.once('error', () => finish(false));
      socket.connect(port, host);
    });
  }
}
