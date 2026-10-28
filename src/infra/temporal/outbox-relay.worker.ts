import {
  Injectable,
  type OnModuleDestroy,
  type OnModuleInit,
} from '@nestjs/common';
import { RelayOutboxUseCase } from '../../modules/order/application/relay-outbox.use-case.js';

@Injectable()
export class OutboxRelayWorker implements OnModuleInit, OnModuleDestroy {
  private timer?: ReturnType<typeof setInterval>;
  private isProcessing = false;

  constructor(private readonly relayOutboxUseCase: RelayOutboxUseCase) {}

  onModuleInit(): void {
    if (process.env.DISABLE_BACKGROUND_WORKERS === 'true') {
      return;
    }
    const intervalMs = Number(process.env.OUTBOX_POLL_INTERVAL_MS ?? 200);
    this.timer = setInterval(() => {
      void this.processPendingOutbox();
    }, intervalMs);
  }

  onModuleDestroy(): void {
    if (this.timer) {
      clearInterval(this.timer);
    }
  }

  async processPendingOutbox(): Promise<number> {
    if (this.isProcessing) {
      return 0;
    }
    this.isProcessing = true;
    try {
      return await this.relayOutboxUseCase.executeBatch();
    } finally {
      this.isProcessing = false;
    }
  }
}
