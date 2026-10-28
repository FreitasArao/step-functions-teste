import { activityInfo, CompleteAsyncError } from '@temporalio/activity';
import type { TemporalActivityEnvelope } from '../translators/temporal-acl.translator.js';
import type { TemporalStepActivityInput } from '../workflows/order-saga.workflow.js';

export interface OrderSagaActivities {
  dispatchSagaStepActivity(input: TemporalStepActivityInput): Promise<unknown>;
}

/**
 * Factory that creates the Temporal Activity implementation using Async Activity Completion.
 *
 * When Temporal invokes `dispatchSagaStepActivity`:
 * 1. Extracts `activityInfo().taskToken` (Uint8Array) and encodes it as base64 (`correlationToken`).
 * 2. Enqueues the `TemporalActivityEnvelope` for the `TemporalSagaStepWorker` to translate via
 *    `TemporalAclTranslator` and execute `ExecuteSagaStepInboundPort` (Transactional Inbox + Domain UseCase).
 * 3. Throws `CompleteAsyncError` so Temporal pauses the Activity waiting for
 *    `TemporalOrderWorkflowAdapter.completeWorkflowStep` / `failWorkflowStep` (`AsyncCompletionClient`)!
 */
export function createOrderSagaActivities(
  enqueueActivityEnvelope: (envelope: TemporalActivityEnvelope) => void,
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

      enqueueActivityEnvelope(envelope);

      throw new CompleteAsyncError();
    },
  };
}
