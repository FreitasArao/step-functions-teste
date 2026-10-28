import { activityInfo, CompleteAsyncError } from '@temporalio/activity';
import type { TemporalActivityEnvelope } from '../translators/temporal-acl.translator.js';
import type { TemporalStepActivityInput } from '../workflows/order-saga.workflow.js';

export interface OrderSagaActivities {
  dispatchSagaStepActivity(input: TemporalStepActivityInput): Promise<unknown>;
}

/**
 * Factory that creates the Temporal Activity implementation using Async Activity Completion
 * combined with the Transactional Inbox + Outbox.
 *
 * When Temporal invokes `dispatchSagaStepActivity`:
 * 1. Extracts `activityInfo().taskToken` (Uint8Array) and encodes it as base64 (`correlationToken`).
 *    Each retry ATTEMPT receives a NEW taskToken.
 * 2. Runs the step synchronously through `handleStep` (Inbox -> Domain -> Outbox callback in ONE transaction).
 *    If it throws (infra failure), the activity fails and Temporal retries it.
 * 3. Throws `CompleteAsyncError`: the result is delivered later by the Outbox Relay through
 *    `AsyncCompletionClient.complete / fail` (TemporalOrderWorkflowAdapter).
 */
export function createOrderSagaActivities(
  handleStep: (envelope: TemporalActivityEnvelope) => Promise<unknown>,
): OrderSagaActivities {
  return {
    async dispatchSagaStepActivity(
      input: TemporalStepActivityInput,
    ): Promise<unknown> {
      const info = activityInfo();
      const taskTokenBase64 = Buffer.from(info.taskToken).toString('base64');

      const envelope: TemporalActivityEnvelope = {
        stepName: input.stepName,
        idempotencyKey: input.idempotencyKey,
        taskToken: taskTokenBase64,
        ...(info.workflowExecution?.workflowId
          ? { workflowId: info.workflowExecution.workflowId }
          : {}),
        ...(info.workflowExecution?.runId
          ? { runId: info.workflowExecution.runId }
          : {}),
        metadata: input.metadata,
        payload: input.payload,
      };

      await handleStep(envelope);

      throw new CompleteAsyncError();
    },
  };
}
