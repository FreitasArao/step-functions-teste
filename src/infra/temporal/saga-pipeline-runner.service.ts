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
   * Runs the full Transactional Outbox + Inbox pipeline deterministically:
   * 1. Outbox Relay (`RelayOutboxUseCase`) -> Delivers PENDING outbox messages
   *    (`ORDER_CREATED`, `WORKFLOW_STEP_COMPLETED`, `WORKFLOW_STEP_FAILED`) to `OrderWorkflowPort`
   *    and marks them PUBLISHED only after Temporal acknowledges them.
   * 2. Activity Execution -> Executes `ExecuteSagaStepUseCase` (Transactional Inbox + Domain UseCase +
   *    Transactional Outbox callback in a single ACID transaction).
   * 3. Next Outbox Relay cycle -> Delivers the `WORKFLOW_STEP_COMPLETED` / `WORKFLOW_STEP_FAILED`
   *    outbox message via `AsyncCompletionClient.complete` / `fail`.
   */
  async drainUntilSettled(maxCycles = 24): Promise<void> {
    const isLocalMode = this.temporalConnection.isLocalEngineMode();

    for (let i = 0; i < maxCycles; i += 1) {
      const published = await this.outboxRelay.processPendingOutbox();

      if (isLocalMode) {
        const stepsProcessed =
          await this.sagaStepWorker.pollActivityQueueOnce();
        const callbacksPublished =
          await this.outboxRelay.processPendingOutbox();

        if (
          published === 0 &&
          stepsProcessed === 0 &&
          callbacksPublished === 0
        ) {
          break;
        }
      } else {
        // In live Temporal mode, the Temporal Worker executes the Activity asynchronously
        // and writes the callback event into outbox_messages.
        await new Promise((r) => setTimeout(r, 180));
        const callbacksPublished =
          await this.outboxRelay.processPendingOutbox();

        if (published === 0 && callbacksPublished === 0 && i >= 3) {
          break;
        }
      }
    }
  }
}
