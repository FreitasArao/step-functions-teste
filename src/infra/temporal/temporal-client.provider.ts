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
import { TemporalLocalEngineService } from './temporal-local-engine.service.js';
import { TEMPORAL_ORDER_SAGA_TASK_QUEUE } from './translators/temporal-acl.translator.js';

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
  private connected = false;

  constructor(private readonly localEngine: TemporalLocalEngineService) {}

  get address(): string {
    return process.env.TEMPORAL_ADDRESS ?? 'localhost:7233';
  }

  get namespace(): string {
    return process.env.TEMPORAL_NAMESPACE ?? 'default';
  }

  isTemporalServerActive(): boolean {
    return (
      this.connected &&
      process.env.USE_LOCAL_TEMPORAL_ENGINE !== 'true' &&
      process.env.NODE_ENV !== 'test'
    );
  }

  getWorkflowClient(): WorkflowClient | null {
    return this.client?.workflow ?? null;
  }

  getAsyncCompletionClient(): AsyncCompletionClient | null {
    return this.client?.activity ?? null;
  }

  async onModuleInit(): Promise<void> {
    if (
      process.env.NODE_ENV === 'test' ||
      process.env.USE_LOCAL_TEMPORAL_ENGINE === 'true'
    ) {
      return;
    }

    const reachable = await this.checkTcpReachable(this.address, 800);
    if (!reachable) {
      this.logger.warn(
        `[Temporal] Temporal Server at ${this.address} is not reachable — using local Temporal engine fallback.`,
      );
      return;
    }

    try {
      this.connection = await Connection.connect({ address: this.address });
      this.client = new Client({
        connection: this.connection,
        namespace: this.namespace,
      });
      this.connected = true;
      this.logger.log(
        `[Temporal] Connected to Temporal Server at ${this.address} (namespace=${this.namespace})`,
      );

      if (process.env.DISABLE_BACKGROUND_WORKERS !== 'true') {
        await this.startEmbeddedWorker();
      }
    } catch (err) {
      this.connected = false;
      this.logger.warn(
        `[Temporal] Failed to initialize Temporal client/worker (${err instanceof Error ? err.message : String(err)}) — falling back to local Temporal engine.`,
      );
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
        activities: createOrderSagaActivities((envelope) => {
          this.localEngine.enqueueLiveActivityEnvelope(envelope);
        }),
      });

      this.workerRunPromise = this.worker.run().catch((err) => {
        this.logger.error(
          `[Temporal Worker] Stopped with error: ${err instanceof Error ? err.message : String(err)}`,
        );
      });

      this.logger.log(
        `[Temporal Worker] Polling TaskQueue='${TEMPORAL_ORDER_SAGA_TASK_QUEUE}' for Workflows & Async Activities`,
      );
    } catch (err) {
      this.logger.warn(
        `[Temporal Worker] Could not start native Temporal Worker in current runtime (${err instanceof Error ? err.message : String(err)}).`,
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
