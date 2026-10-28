import { Injectable } from '@nestjs/common';
import { OutboxRelayWorker } from './outbox-relay.worker.js';
import { TemporalConnectionService } from './temporal-client.provider.js';
import { TemporalOrderEventsWorker } from './temporal-order-events.worker.js';
import { TemporalSagaStepWorker } from './temporal-saga-step.worker.js';

@Injectable()
export class SagaPipelineRunnerService {
  constructor(
    private readonly outboxRelay: OutboxRelayWorker,
    private readonly orderEventsWorker: TemporalOrderEventsWorker,
    private readonly sagaStepWorker: TemporalSagaStepWorker,
    private readonly temporalConnection: TemporalConnectionService,
  ) {}

  /**
   * Runs the full production pipeline deterministically:
   * 1. Outbox Relay -> Publishes pending domain events via MessagePublisherPort
   * 2. Temporal Order Events Worker -> Consumes 'ORDER_CREATED' and starts Temporal Workflow
   * 3. Temporal Saga Step Worker -> Consumes Async Activity tasks (with base64 taskToken),
   *    executes UseCase with Transactional Inbox, and calls AsyncCompletionClient.complete / fail
   */
  async drainUntilSettled(maxCycles = 16): Promise<void> {
    const isLiveTemporal = this.temporalConnection.isTemporalServerActive();

    for (let i = 0; i < maxCycles; i += 1) {
      const published = await this.outboxRelay.processPendingOutbox();
      const eventsStarted =
        await this.orderEventsWorker.pollEventsQueueOnce();

      if (isLiveTemporal && (published > 0 || eventsStarted > 0)) {
        // Give Temporal Server a brief window to schedule the next Activity task onto the Worker
        await new Promise((r) => setTimeout(r, 150));
      }

      const stepsProcessed =
        await this.sagaStepWorker.pollActivityQueueOnce();

      if (isLiveTemporal && stepsProcessed > 0) {
        await new Promise((r) => setTimeout(r, 150));
        continue;
      }

      if (published === 0 && eventsStarted === 0 && stepsProcessed === 0) {
        break;
      }
    }
  }
}
