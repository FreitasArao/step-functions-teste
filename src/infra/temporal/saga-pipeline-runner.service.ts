import { Injectable } from '@nestjs/common';
import { OutboxRelayWorker } from './outbox-relay.worker.js';
import { TemporalConnectionService } from './temporal-client.provider.js';
import { TemporalSagaStepWorker } from './temporal-saga-step.worker.js';

@Injectable()
export class SagaPipelineRunnerService {
  constructor(
    private readonly outboxRelay: OutboxRelayWorker,
    private readonly sagaStepWorker: TemporalSagaStepWorker,
    private readonly temporalConnection: TemporalConnectionService,
  ) {}

  /**
   * Runs the full production pipeline deterministically:
   * 1. Outbox Relay (`OutboxRelayService`) -> Publishes pending domain events (`ORDER_CREATED`)
   *    via `MessagePublisherPort` (`TemporalEventPublisherAdapter` -> starts Temporal Workflow).
   * 2. Activity Execution -> Executes `ExecuteSagaStepUseCase` (Transactional Inbox + Domain UseCase
   *    in a single ACID transaction, then `AsyncCompletionClient.complete` / `fail` callback).
   */
  async drainUntilSettled(maxCycles = 16): Promise<void> {
    const isLocalMode = this.temporalConnection.isLocalEngineMode();

    for (let i = 0; i < maxCycles; i += 1) {
      const published = await this.outboxRelay.processPendingOutbox();

      if (isLocalMode) {
        const stepsProcessed =
          await this.sagaStepWorker.pollActivityQueueOnce();
        if (published === 0 && stepsProcessed === 0) {
          break;
        }
      } else {
        await new Promise((r) => setTimeout(r, 200));
        if (published === 0 && i >= 4) {
          break;
        }
      }
    }
  }
}
