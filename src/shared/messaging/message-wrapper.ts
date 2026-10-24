import { randomUUID } from 'node:crypto';

export interface TraceContextCarrier {
  'x-datadog-trace-id'?: string;
  'x-datadog-parent-id'?: string;
  'x-datadog-sampling-priority'?: string;
  'x-datadog-tags'?: string;
  traceparent?: string;
  tracestate?: string;
  [key: string]: string | undefined;
}

export interface MessageMetadata {
  readonly messageId: string;
  readonly correlationId: string;
  readonly causationId: string;
  readonly timestamp: string;
  readonly source: string;
  readonly traceContext: TraceContextCarrier;
}

export type SagaStepName =
  | 'RESERVE_STOCK'
  | 'PROCESS_PAYMENT'
  | 'CREATE_SHIPMENT'
  | 'CONFIRM_ORDER'
  | 'REFUND_PAYMENT'
  | 'RELEASE_STOCK'
  | 'CANCEL_ORDER';

/**
 * Orchestrator-agnostic MessageWrapper.
 * Knows nothing about AWS Step Functions or SQS tokens.
 */
export interface MessageWrapper<TPayload = unknown> {
  readonly stepName?: SagaStepName;
  readonly idempotencyKey?: string;
  readonly metadata: MessageMetadata;
  readonly payload: TPayload;
}

export function createMessageWrapper<TPayload>(params: {
  payload: TPayload;
  correlationId: string;
  causationId?: string;
  source: string;
  traceContext: TraceContextCarrier;
  stepName?: SagaStepName;
  idempotencyKey?: string;
}): MessageWrapper<TPayload> {
  const messageId = randomUUID();
  return {
    ...(params.stepName ? { stepName: params.stepName } : {}),
    ...(params.idempotencyKey ? { idempotencyKey: params.idempotencyKey } : {}),
    metadata: {
      messageId,
      correlationId: params.correlationId,
      causationId: params.causationId ?? messageId,
      timestamp: new Date().toISOString(),
      source: params.source,
      traceContext: { ...params.traceContext },
    },
    payload: params.payload,
  };
}
