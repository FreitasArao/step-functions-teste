import { Injectable } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import type {
  StartSagaPayload,
  WorkflowExecutionSummary,
} from '../../modules/order/application/ports/order-workflow.outbound-port.js';
import type { DomainError } from '../../shared/core/domain-error.js';
import type {
  MessageWrapper,
  SagaStepName,
} from '../../shared/messaging/message-wrapper.js';
import type { TemporalActivityEnvelope } from './translators/temporal-acl.translator.js';

interface ActiveTemporalExecution {
  readonly workflowId: string;
  readonly runId: string;
  readonly orderId: string;
  currentStep: SagaStepName;
  status: WorkflowExecutionSummary['status'];
  stateJson: {
    metadata: MessageWrapper<StartSagaPayload>['metadata'];
    payload: TemporalActivityEnvelope['payload'];
  };
  readonly startedAt: string;
  finishedAt?: string;
  failureCode?: string;
  failureReason?: string;
}

/**
 * Buffers incoming Temporal Activity envelopes (both from live Temporal Worker activities
 * and from the in-process Temporal Workflow emulator used during unit tests when Temporal Server is offline).
 */
@Injectable()
export class TemporalLocalEngineService {
  private readonly executionsByOrder = new Map<
    string,
    ActiveTemporalExecution
  >();
  private readonly executionsByToken = new Map<
    string,
    ActiveTemporalExecution
  >();
  private readonly activityEnvelopeBuffer: string[] = [];

  enqueueLiveActivityEnvelope(envelope: TemporalActivityEnvelope): void {
    this.activityEnvelopeBuffer.push(JSON.stringify(envelope));
  }

  startLocalWorkflowExecution(wrapper: MessageWrapper<StartSagaPayload>): {
    workflowInstanceId: string;
  } {
    const orderId = wrapper.payload.orderId;
    const workflowId = `order-saga-${orderId}`;
    const runId = randomUUID();

    const execution: ActiveTemporalExecution = {
      workflowId,
      runId,
      orderId,
      currentStep: 'RESERVE_STOCK',
      status: 'RUNNING',
      stateJson: {
        metadata: { ...wrapper.metadata },
        payload: { ...wrapper.payload },
      },
      startedAt: new Date().toISOString(),
    };

    this.executionsByOrder.set(orderId, execution);
    this.enqueueLocalStep(execution, 'RESERVE_STOCK');
    return { workflowInstanceId: `${workflowId}:${runId}` };
  }

  private enqueueLocalStep(
    execution: ActiveTemporalExecution,
    stepName: SagaStepName,
  ): void {
    execution.currentStep = stepName;
    const rawToken = `temporal_local_token:${execution.orderId}:${stepName}:${randomUUID().slice(0, 8)}`;
    const taskTokenBase64 = Buffer.from(rawToken, 'utf8').toString('base64');
    this.executionsByToken.set(taskTokenBase64, execution);

    const rawEnvelope: TemporalActivityEnvelope = {
      stepName,
      idempotencyKey: `${execution.orderId}:${stepName}`,
      taskToken: taskTokenBase64,
      workflowId: execution.workflowId,
      runId: execution.runId,
      metadata: { ...execution.stateJson.metadata },
      payload: structuredClone(execution.stateJson.payload),
    };

    this.activityEnvelopeBuffer.push(JSON.stringify(rawEnvelope));
  }

  isLocalTaskToken(correlationToken: string): boolean {
    if (this.executionsByToken.has(correlationToken)) {
      return true;
    }
    try {
      const decoded = Buffer.from(correlationToken, 'base64').toString('utf8');
      return decoded.startsWith('temporal_local_token:');
    } catch {
      return false;
    }
  }

  dequeueRawActivityEnvelopes(max = 5): string[] {
    return this.activityEnvelopeBuffer.splice(0, max);
  }

  handleAsyncActivityComplete(correlationToken: string, output: unknown): void {
    const execution = this.executionsByToken.get(correlationToken);
    if (!execution || execution.status !== 'RUNNING') {
      return;
    }
    this.executionsByToken.delete(correlationToken);

    const step = execution.currentStep;
    const payload = execution.stateJson.payload as Record<string, unknown>;

    switch (step) {
      case 'RESERVE_STOCK':
        payload.stock = output;
        this.enqueueLocalStep(execution, 'PROCESS_PAYMENT');
        break;

      case 'PROCESS_PAYMENT':
        payload.payment = output;
        this.enqueueLocalStep(execution, 'CREATE_SHIPMENT');
        break;

      case 'CREATE_SHIPMENT':
        payload.shipment = output;
        this.enqueueLocalStep(execution, 'CONFIRM_ORDER');
        break;

      case 'CONFIRM_ORDER':
        payload.confirmation = output;
        execution.status = 'COMPLETED';
        execution.finishedAt = new Date().toISOString();
        break;

      case 'REFUND_PAYMENT':
        payload.paymentRefund = output;
        this.enqueueLocalStep(execution, 'RELEASE_STOCK');
        break;

      case 'RELEASE_STOCK':
        payload.stockRelease = output;
        this.enqueueLocalStep(execution, 'CANCEL_ORDER');
        break;

      case 'CANCEL_ORDER':
        payload.cancellation = output;
        execution.status = 'ROLLED_BACK';
        execution.failureCode = 'ORDER_SAGA_ROLLED_BACK';
        execution.failureReason =
          'Order saga failed and all compensations were executed.';
        execution.finishedAt = new Date().toISOString();
        break;
    }
  }

  handleAsyncActivityFailure(
    correlationToken: string,
    error: DomainError,
  ): void {
    const execution = this.executionsByToken.get(correlationToken);
    if (!execution || execution.status !== 'RUNNING') {
      return;
    }
    this.executionsByToken.delete(correlationToken);

    const step = execution.currentStep;
    const payload = execution.stateJson.payload as Record<string, unknown>;
    const detailsStr = JSON.stringify({
      code: error.code,
      message: error.message,
      retryable: error.retryable ?? false,
      metadata: error.metadata ?? {},
    });

    payload.error = {
      type: error.code,
      message: error.message,
      details: detailsStr,
    };

    switch (step) {
      case 'RESERVE_STOCK':
        this.enqueueLocalStep(execution, 'CANCEL_ORDER');
        break;

      case 'PROCESS_PAYMENT':
        this.enqueueLocalStep(execution, 'RELEASE_STOCK');
        break;

      case 'CREATE_SHIPMENT':
        this.enqueueLocalStep(execution, 'REFUND_PAYMENT');
        break;

      default:
        execution.status = 'FAILED';
        execution.failureCode = error.code;
        execution.failureReason = detailsStr;
        execution.finishedAt = new Date().toISOString();
        break;
    }
  }

  getExecutionSummary(orderId: string): WorkflowExecutionSummary | null {
    const exec = this.executionsByOrder.get(orderId);
    if (!exec) {
      return null;
    }
    return {
      workflowInstanceId: `${exec.workflowId}:${exec.runId}`,
      status: exec.status,
      startedAt: exec.startedAt,
      ...(exec.finishedAt ? { finishedAt: exec.finishedAt } : {}),
      ...(exec.failureCode ? { failureCode: exec.failureCode } : {}),
      ...(exec.failureReason ? { failureReason: exec.failureReason } : {}),
    };
  }

  clear(): void {
    this.executionsByOrder.clear();
    this.executionsByToken.clear();
    this.activityEnvelopeBuffer.length = 0;
  }
}
