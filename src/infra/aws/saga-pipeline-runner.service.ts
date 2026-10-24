import { Injectable } from '@nestjs/common';
import { OutboxRelayWorker } from './outbox-relay.worker.js';
import { SqsSagaStepWorker } from './sqs-saga-step.worker.js';

@Injectable()
export class SagaPipelineRunnerService {
  constructor(
    private readonly outboxRelay: OutboxRelayWorker,
    private readonly sqsWorker: SqsSagaStepWorker,
  ) {}

  /**
   * Runs Outbox Relay <-> SQS Saga Worker cycles until no more pending messages
   * remain in the local/MiniStack pipeline (up to maxCycles).
   */
  async drainUntilSettled(maxCycles = 12): Promise<void> {
    for (let i = 0; i < maxCycles; i += 1) {
      const published = await this.outboxRelay.processPendingOutbox();
      const processed = await this.sqsWorker.pollQueueOnce();
      if (published === 0 && processed === 0) {
        break;
      }
    }
  }
}
