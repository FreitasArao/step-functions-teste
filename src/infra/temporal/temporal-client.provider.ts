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
import { Socket } from 'node:net';

const RECONNECT_INTERVAL_MS = 5_000;

/**
 * The in-process Temporal emulator is an explicit TEST DOUBLE.
 * It is never used as a silent fallback when the real Temporal Server is down.
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
  private connecting?: Promise<boolean>;
  private lastAttemptAt = 0;

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

  async getWorkflowClient(): Promise<WorkflowClient | null> {
    return (await this.ensureConnected()) ? (this.client?.workflow ?? null) : null;
  }

  async getAsyncCompletionClient(): Promise<AsyncCompletionClient | null> {
    return (await this.ensureConnected()) ? (this.client?.activity ?? null) : null;
  }

  async isServerReachable(): Promise<boolean> {
    return this.ensureConnected();
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
      return true;
    } catch (err) {
      this.client = undefined;
      this.logger.warn(
        `[Temporal] Failed to connect (${err instanceof Error ? err.message : String(err)}) — Outbox messages stay PENDING and will be retried.`,
      );
      return false;
    }
  }

  async onModuleDestroy(): Promise<void> {
    try {
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
