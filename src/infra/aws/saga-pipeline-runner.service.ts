import { Injectable } from '@nestjs/common';
import { OutboxRelayWorker } from './outbox-relay.worker.js';
import { StepFunctionsOrderWorkflowAdapter } from './sfn-order-workflow.adapter.js';
import { SqsSagaStepWorker } from './sqs-saga-step.worker.js';

@Injectable()
export class SagaPipelineRunnerService {
  constructor(
    private readonly outboxRelay: OutboxRelayWorker,
    private readonly sqsStepWorker: SqsSagaStepWorker,
    private readonly sfnAdapter: StepFunctionsOrderWorkflowAdapter,
  ) {}

  /**
   * Runs the full Transactional Outbox + Inbox pipeline deterministically:
   * 1. Outbox Relay (`RelayOutboxUseCase`) -> Delivers PENDING outbox messages
   *    (`ORDER_CREATED`, `WORKFLOW_STEP_COMPLETED`, `WORKFLOW_STEP_FAILED`) to `OrderWorkflowPort`
   *    and marks them PUBLISHED only after AWS Step Functions acknowledges them.
   * 2. SQS Saga Step Worker -> Consumes step tasks from SQS (`order-saga-tasks-queue`),
   *    executes `ExecuteSagaStepUseCase` (Transactional Inbox + Domain + Transactional Outbox callback
   *    in a single ACID transaction), and deletes the SQS message only after COMMIT.
   * 3. Next Outbox Relay cycle -> Delivers the `WORKFLOW_STEP_COMPLETED` / `WORKFLOW_STEP_FAILED`
   *    outbox message via `SendTaskSuccessCommand` / `SendTaskFailureCommand`.
   */
  async drainUntilSettled(maxCycles = 24): Promise<void> {
    const isLocalMode = this.sfnAdapter.isLocalEngineMode();

    for (let i = 0; i < maxCycles; i += 1) {
      const published = await this.outboxRelay.processPendingOutbox();

      if (!isLocalMode && published > 0) {
        // Give LocalStack Step Functions a brief window to publish the next task to SQS
        await new Promise((r) => setTimeout(r, 150));
      }

      const stepsProcessed = await this.sqsStepWorker.pollQueueOnce();
      const callbacksPublished = await this.outboxRelay.processPendingOutbox();

      if (!isLocalMode && (stepsProcessed > 0 || callbacksPublished > 0)) {
        await new Promise((r) => setTimeout(r, 150));
        continue;
      }

      if (published === 0 && stepsProcessed === 0 && callbacksPublished === 0) {
        break;
      }
    }
  }
}
