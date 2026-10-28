import { Injectable } from '@nestjs/common';
import type { WorkflowExecutionDescription } from '@temporalio/client';
import { ApplicationFailure } from '@temporalio/common';
import type {
  StartSagaPayload,
  WorkflowExecutionSummary,
} from '../../../modules/order/application/ports/order-workflow.outbound-port.js';
import type {
  SagaStepContextPayload,
  WorkflowFailureDetails,
  WorkflowStepCommand,
} from '../../../modules/order/application/ports/saga-step.inbound-port.js';
import type { DomainError } from '../../../shared/core/domain-error.js';
import {
  createMessageWrapper,
  type MessageMetadata,
  type MessageWrapper,
  type SagaStepName,
} from '../../../shared/messaging/message-wrapper.js';

export const TEMPORAL_ORDER_SAGA_TASK_QUEUE =
  process.env.TEMPORAL_TASK_QUEUE ?? 'order-saga-task-queue';

export interface TemporalWorkflowInput {
  readonly metadata: MessageMetadata;
  readonly payload: StartSagaPayload;
}

/**
 * Raw Temporal Activity task envelope produced when Temporal Worker dispatches an Activity
 * with Async Activity Completion (taskToken in base64).
 */
export interface TemporalActivityEnvelope {
  readonly stepName: SagaStepName;
  readonly idempotencyKey: string;
  readonly taskToken: string;
  readonly workflowId?: string;
  readonly runId?: string;
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

/**
 * Anti-Corruption Layer (ACL) / Translation Layer between:
 * - Temporal.io (WorkflowId, RunId, Uint8Array Activity TaskToken, ApplicationFailure)
 * - Agnostic Application & Domain Contracts (MessageWrapper, WorkflowStepCommand, DomainError, WorkflowExecutionSummary)
 */
@Injectable()
export class TemporalAclTranslator {
  /**
   * Translates an agnostic MessageWrapper<StartSagaPayload> into Temporal WorkflowClient.start options.
   */
  toStartWorkflowInput(
    taskQueue: string,
    wrapper: MessageWrapper<StartSagaPayload>,
  ): {
    workflowId: string;
    taskQueue: string;
    args: [TemporalWorkflowInput];
    memo: Record<string, unknown>;
  } {
    return {
      workflowId: `order-saga-${wrapper.payload.orderId}`,
      taskQueue,
      args: [
        {
          metadata: wrapper.metadata,
          payload: wrapper.payload,
        },
      ],
      memo: {
        orderId: wrapper.payload.orderId,
        correlationId: wrapper.metadata.correlationId,
        causationId: wrapper.metadata.causationId,
        source: wrapper.metadata.source,
        traceContext: wrapper.metadata.traceContext,
      },
    };
  }

  /**
   * Encodes Temporal's binary Uint8Array taskToken into an agnostic string correlationToken.
   */
  encodeTaskToken(taskToken: Uint8Array): string {
    return Buffer.from(taskToken).toString('base64');
  }

  /**
   * Decodes an agnostic correlationToken back into Temporal's binary Uint8Array taskToken.
   */
  decodeTaskToken(correlationToken: string): Uint8Array {
    return new Uint8Array(Buffer.from(correlationToken, 'base64'));
  }

  /**
   * Translates an agnostic step completion into Temporal AsyncCompletionClient.complete arguments.
   */
  toAsyncActivityCompleteInput(params: {
    correlationToken: string;
    output: unknown;
  }): {
    taskToken: Uint8Array;
    result: unknown;
  } {
    return {
      taskToken: this.decodeTaskToken(params.correlationToken),
      result: params.output ?? {},
    };
  }

  /**
   * Translates a DomainError into Temporal AsyncCompletionClient.fail arguments (ApplicationFailure).
   */
  toAsyncActivityFailureInput(params: {
    correlationToken: string;
    error: DomainError;
  }): {
    taskToken: Uint8Array;
    failure: ApplicationFailure;
  } {
    const serializedCause = JSON.stringify({
      code: params.error.code,
      message: params.error.message,
      retryable: params.error.retryable ?? false,
      metadata: params.error.metadata ?? {},
    });

    const failure = ApplicationFailure.nonRetryable(
      params.error.message,
      params.error.code,
      serializedCause,
    );

    return {
      taskToken: this.decodeTaskToken(params.correlationToken),
      failure,
    };
  }

  /**
   * Translates a raw Temporal Activity Envelope (or JSON string) into a pure,
   * orchestrator-agnostic WorkflowStepCommand for the Application Layer.
   */
  toWorkflowStepCommand(
    rawEnvelopeOrJson: string | TemporalActivityEnvelope,
  ): WorkflowStepCommand {
    const raw =
      typeof rawEnvelopeOrJson === 'string'
        ? (JSON.parse(rawEnvelopeOrJson) as TemporalActivityEnvelope)
        : rawEnvelopeOrJson;

    const failure = this.extractFailureFromTemporalError(raw.payload.error);

    const agnosticPayload: SagaStepContextPayload = {
      ...raw.payload,
      ...(failure ? { failure } : {}),
    };

    const wrapper: MessageWrapper<SagaStepContextPayload> = {
      stepName: raw.stepName,
      idempotencyKey: raw.idempotencyKey,
      metadata:
        raw.metadata ??
        createMessageWrapper({
          payload: agnosticPayload,
          correlationId: raw.payload.orderId,
          source: 'temporal-acl',
          traceContext: {},
        }).metadata,
      payload: agnosticPayload,
    };

    return {
      stepName: raw.stepName,
      idempotencyKey:
        raw.idempotencyKey ?? `${raw.payload.orderId}:${raw.stepName}`,
      correlationToken: raw.taskToken,
      wrapper,
    };
  }

  /**
   * Translates Temporal's WorkflowExecutionDescription into an agnostic WorkflowExecutionSummary.
   */
  toWorkflowExecutionSummary(
    workflowId: string,
    description: WorkflowExecutionDescription,
    failureMetadata?: { code?: string; reason?: string },
  ): WorkflowExecutionSummary {
    const statusName = description.status.name;
    let status: WorkflowExecutionSummary['status'] = 'RUNNING';

    if (statusName === 'COMPLETED') {
      status = 'COMPLETED';
    } else if (statusName === 'FAILED') {
      status =
        failureMetadata?.code === 'ORDER_SAGA_ROLLED_BACK' ||
        failureMetadata?.code === 'ApplicationFailure'
          ? 'ROLLED_BACK'
          : 'ROLLED_BACK';
    } else if (
      statusName === 'TIMED_OUT' ||
      statusName === 'TERMINATED' ||
      statusName === 'CANCELLED'
    ) {
      status = 'FAILED';
    }

    return {
      workflowInstanceId: `${workflowId}:${description.runId}`,
      status,
      ...(description.startTime
        ? { startedAt: description.startTime.toISOString() }
        : {}),
      ...(description.closeTime
        ? { finishedAt: description.closeTime.toISOString() }
        : {}),
      ...(failureMetadata?.code ? { failureCode: failureMetadata.code } : {}),
      ...(failureMetadata?.reason
        ? { failureReason: failureMetadata.reason }
        : {}),
    };
  }

  private extractFailureFromTemporalError(temporalError?: {
    readonly type?: string;
    readonly message?: string;
    readonly details?: string;
  }): WorkflowFailureDetails | undefined {
    if (!temporalError) {
      return undefined;
    }
    if (temporalError.details) {
      try {
        const parsed = JSON.parse(temporalError.details) as {
          code?: string;
          message?: string;
        };
        return {
          code: parsed.code ?? temporalError.type,
          message: parsed.message ?? temporalError.message,
        };
      } catch {
        return {
          code: temporalError.type,
          message: temporalError.message ?? temporalError.details,
        };
      }
    }
    return {
      code: temporalError.type,
      message: temporalError.message ?? temporalError.type,
    };
  }
}
