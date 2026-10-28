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
import type { AwsSfnSqsTaskEnvelope } from './translators/sfn-acl.translator.js';

interface ActiveExecution {
  readonly executionArn: string;
  readonly orderId: string;
  currentStep: SagaStepName;
  status: WorkflowExecutionSummary['status'];
  stateJson: {
    metadata: MessageWrapper<StartSagaPayload>['metadata'];
    payload: AwsSfnSqsTaskEnvelope['payload'];
  };
  readonly startedAt: string;
  finishedAt?: string;
  failureCode?: string;
  failureReason?: string;
}

/**
 * Local in-process interpreter for infra/statemachine/order-saga.asl.json
 * Used as fallback when LocalStack/MiniStack is offline during unit tests.
 */
@Injectable()
export class AslLocalEngineService {
  private readonly executionsByOrder = new Map<string, ActiveExecution>();
  private readonly executionsByToken = new Map<string, ActiveExecution>();
  private readonly rawSqsQueueBuffer: string[] = [];

  startExecution(wrapper: MessageWrapper<StartSagaPayload>): {
    workflowInstanceId: string;
  } {
    const orderId = wrapper.payload.orderId;
    const executionArn = `arn:aws:states:us-east-1:000000000000:execution:order-saga-state-machine:${orderId}`;

    const execution: ActiveExecution = {
      executionArn,
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
    this.enqueueStep(execution, 'RESERVE_STOCK');
    return { workflowInstanceId: executionArn };
  }

  private enqueueStep(
    execution: ActiveExecution,
    stepName: SagaStepName,
  ): void {
    execution.currentStep = stepName;
    const taskToken = `task_token_${execution.orderId}_${stepName}_${randomUUID().slice(0, 6)}`;
    this.executionsByToken.set(taskToken, execution);

    const rawEnvelope: AwsSfnSqsTaskEnvelope = {
      stepName,
      idempotencyKey: `${execution.orderId}:${stepName}`,
      taskToken,
      metadata: { ...execution.stateJson.metadata },
      payload: structuredClone(execution.stateJson.payload),
    };

    this.rawSqsQueueBuffer.push(JSON.stringify(rawEnvelope));
  }

  dequeueRawSqsBodies(max = 5): string[] {
    return this.rawSqsQueueBuffer.splice(0, max);
  }

  handleSendTaskSuccess(taskToken: string, output: unknown): void {
    const execution = this.executionsByToken.get(taskToken);
    if (!execution || execution.status !== 'RUNNING') {
      return;
    }
    this.executionsByToken.delete(taskToken);

    const step = execution.currentStep;
    const payload = execution.stateJson.payload as Record<string, unknown>;

    switch (step) {
      case 'RESERVE_STOCK':
        payload.stock = output;
        this.enqueueStep(execution, 'PROCESS_PAYMENT');
        break;

      case 'PROCESS_PAYMENT':
        payload.payment = output;
        this.enqueueStep(execution, 'CREATE_SHIPMENT');
        break;

      case 'CREATE_SHIPMENT':
        payload.shipment = output;
        this.enqueueStep(execution, 'CONFIRM_ORDER');
        break;

      case 'CONFIRM_ORDER':
        payload.confirmation = output;
        execution.status = 'COMPLETED';
        execution.finishedAt = new Date().toISOString();
        break;

      case 'REFUND_PAYMENT':
        payload.paymentRefund = output;
        this.enqueueStep(execution, 'RELEASE_STOCK');
        break;

      case 'RELEASE_STOCK':
        payload.stockRelease = output;
        this.enqueueStep(execution, 'CANCEL_ORDER');
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

  handleSendTaskFailure(taskToken: string, error: DomainError): void {
    const execution = this.executionsByToken.get(taskToken);
    if (!execution || execution.status !== 'RUNNING') {
      return;
    }
    this.executionsByToken.delete(taskToken);

    const step = execution.currentStep;
    const payload = execution.stateJson.payload as Record<string, unknown>;
    const causeStr = JSON.stringify({
      code: error.code,
      message: error.message,
      retryable: error.retryable ?? false,
      metadata: error.metadata ?? {},
    });

    payload.error = {
      Error: error.code,
      Cause: causeStr,
    };

    switch (step) {
      case 'RESERVE_STOCK':
        this.enqueueStep(execution, 'CANCEL_ORDER');
        break;

      case 'PROCESS_PAYMENT':
        this.enqueueStep(execution, 'RELEASE_STOCK');
        break;

      case 'CREATE_SHIPMENT':
        this.enqueueStep(execution, 'REFUND_PAYMENT');
        break;

      default:
        execution.status = 'FAILED';
        execution.failureCode = error.code;
        execution.failureReason = causeStr;
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
      workflowInstanceId: exec.executionArn,
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
    this.rawSqsQueueBuffer.length = 0;
  }
}
