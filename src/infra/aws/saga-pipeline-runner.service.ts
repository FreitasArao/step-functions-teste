import { Injectable } from '@nestjs/common';
import { OutboxRelayWorker } from './outbox-relay.worker.js';
import { SqsOrderEventsWorker } from './sqs-order-events.worker.js';
import { SqsSagaStepWorker } from './sqs-saga-step.worker.js';

@Injectable()
export class SagaPipelineRunnerService {
  constructor(
    private readonly outboxRelay: OutboxRelayWorker,
    private readonly sqsEventsWorker: SqsOrderEventsWorker,
    private readonly sqsStepWorker: SqsSagaStepWorker,
  ) {}

  /**
   * Runs the full production pipeline deterministically:
   * 1. Outbox Relay -> Publishes pending domain events to SQS ('order-events-queue')
   * 2. SQS Order Events Worker -> Consumes 'ORDER_CREATED' from SQS and starts Step Functions workflow
   * 3. SQS Saga Step Worker -> Consumes step tasks from SQS ('order-saga-tasks-queue'), executes UseCase with Inbox, and sends callback to Step Functions
   */
  async drainUntilSettled(maxCycles = 12): Promise<void> {
    for (let i = 0; i < maxCycles; i += 1) {
      const published = await this.outboxRelay.processPendingOutbox();
      const eventsStarted = await this.sqsEventsWorker.pollEventsQueueOnce();
      const stepsProcessed = await this.sqsStepWorker.pollQueueOnce();
      if (published === 0 && eventsStarted === 0 && stepsProcessed === 0) {
        break;
      }
    }
  }
}
