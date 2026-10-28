import {
  ActivityFailure,
  ApplicationFailure,
  proxyActivities,
} from '@temporalio/workflow';
import type {
  MessageMetadata,
  SagaStepName,
} from '../../../shared/messaging/message-wrapper.js';
import type { OrderSagaActivities } from '../activities/order-saga.activities.js';
import type { TemporalWorkflowInput } from '../translators/temporal-acl.translator.js';

export interface TemporalStepActivityInput {
  readonly stepName: SagaStepName;
  readonly idempotencyKey: string;
  readonly metadata: MessageMetadata;
  readonly payload: {
    readonly orderId: string;
    readonly sku: string;
    readonly quantity: number;
    readonly amount: number;
    readonly zipCode: string;
    readonly error?: {
      readonly type?: string;
      readonly message?: string;
      readonly details?: string;
    };
    readonly [key: string]: unknown;
  };
}

const { dispatchSagaStepActivity } = proxyActivities<OrderSagaActivities>({
  startToCloseTimeout: '30 seconds',
  retry: {
    // Domain/business errors are deterministic and handled by Saga compensations
    maximumAttempts: 1,
  },
});

/**
 * Temporal.io Order Saga Workflow (replaces AWS Step Functions 180-line order-saga.asl.json).
 * Orchestrates the 4 forward steps and 2-level cascading compensations in pure TypeScript:
 *
 * Forward path:
 *   RESERVE_STOCK -> PROCESS_PAYMENT -> CREATE_SHIPMENT -> CONFIRM_ORDER
 *
 * Compensation stack (LIFO):
 *   - Failure at RESERVE_STOCK   -> CANCEL_ORDER
 *   - Failure at PROCESS_PAYMENT -> RELEASE_STOCK (Level 1) -> CANCEL_ORDER
 *   - Failure at CREATE_SHIPMENT -> REFUND_PAYMENT (Level 2) -> RELEASE_STOCK (Level 1) -> CANCEL_ORDER
 */
export async function orderSagaWorkflow(
  input: TemporalWorkflowInput,
): Promise<Record<string, unknown>> {
  const { metadata, payload } = input;
  const orderId = payload.orderId;

  const statePayload: Record<string, unknown> & {
    orderId: string;
    sku: string;
    quantity: number;
    amount: number;
    zipCode: string;
    error?: {
      type?: string;
      message?: string;
      details?: string;
    };
  } = {
    ...payload,
  };

  // LIFO stack of compensation steps registered as forward steps succeed
  const compensations: Array<() => Promise<void>> = [];

  try {
    // Step 1: Reserve Stock
    const stockOutput = await dispatchSagaStepActivity({
      stepName: 'RESERVE_STOCK',
      idempotencyKey: `${orderId}:RESERVE_STOCK`,
      metadata,
      payload: statePayload,
    });
    statePayload.stock = stockOutput;

    // Register Level 1 Compensation: Release Stock
    compensations.unshift(async () => {
      const releaseOutput = await dispatchSagaStepActivity({
        stepName: 'RELEASE_STOCK',
        idempotencyKey: `${orderId}:RELEASE_STOCK`,
        metadata,
        payload: statePayload,
      });
      statePayload.stockRelease = releaseOutput;
    });

    // Step 2: Process Payment
    const paymentOutput = await dispatchSagaStepActivity({
      stepName: 'PROCESS_PAYMENT',
      idempotencyKey: `${orderId}:PROCESS_PAYMENT`,
      metadata,
      payload: statePayload,
    });
    statePayload.payment = paymentOutput;

    // Register Level 2 Compensation: Refund Payment (runs BEFORE Release Stock)
    compensations.unshift(async () => {
      const refundOutput = await dispatchSagaStepActivity({
        stepName: 'REFUND_PAYMENT',
        idempotencyKey: `${orderId}:REFUND_PAYMENT`,
        metadata,
        payload: statePayload,
      });
      statePayload.paymentRefund = refundOutput;
    });

    // Step 3: Create Shipment
    const shipmentOutput = await dispatchSagaStepActivity({
      stepName: 'CREATE_SHIPMENT',
      idempotencyKey: `${orderId}:CREATE_SHIPMENT`,
      metadata,
      payload: statePayload,
    });
    statePayload.shipment = shipmentOutput;

    // Step 4: Confirm Order
    const confirmOutput = await dispatchSagaStepActivity({
      stepName: 'CONFIRM_ORDER',
      idempotencyKey: `${orderId}:CONFIRM_ORDER`,
      metadata,
      payload: statePayload,
    });
    statePayload.confirmation = confirmOutput;

    return statePayload;
  } catch (err) {
    const errorInfo = extractTemporalActivityError(err);
    statePayload.error = errorInfo;

    // Run registered compensations in reverse order (Level 2 -> Level 1)
    for (const runCompensation of compensations) {
      await runCompensation();
    }

    // Final Rollback Step: Cancel Order with failure reason
    const cancelOutput = await dispatchSagaStepActivity({
      stepName: 'CANCEL_ORDER',
      idempotencyKey: `${orderId}:CANCEL_ORDER`,
      metadata,
      payload: statePayload,
    });
    statePayload.cancellation = cancelOutput;

    throw ApplicationFailure.nonRetryable(
      'Order saga failed and all compensations were executed.',
      'ORDER_SAGA_ROLLED_BACK',
      JSON.stringify(errorInfo),
    );
  }
}

function extractTemporalActivityError(err: unknown): {
  type: string;
  message: string;
  details?: string;
} {
  if (
    err instanceof ActivityFailure &&
    err.cause instanceof ApplicationFailure
  ) {
    const appFailure = err.cause;
    const firstDetail = appFailure.details?.[0];
    return {
      type: appFailure.type ?? 'DOMAIN_ERROR',
      message: appFailure.message,
      ...(typeof firstDetail === 'string' ? { details: firstDetail } : {}),
    };
  }
  if (err instanceof ApplicationFailure) {
    const firstDetail = err.details?.[0];
    return {
      type: err.type ?? 'DOMAIN_ERROR',
      message: err.message,
      ...(typeof firstDetail === 'string' ? { details: firstDetail } : {}),
    };
  }
  const msg = err instanceof Error ? err.message : String(err);
  return {
    type: 'WORKFLOW_STEP_ERROR',
    message: msg,
  };
}
