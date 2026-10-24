import { Injectable } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import type {
  StartSagaPayload,
  StepFunctionsExecutionSummary,
} from '../../modules/order/domain/order-workflow.port.js';
import type { DomainError } from '../../shared/core/domain-error.js';
import type {
  MessageWrapper,
  SagaStepName,
} from '../../shared/messaging/message-wrapper.js';
import type { SagaStatePayload } from './sqs-saga-step.worker.js';

interface ActiveExecution {
  readonly executionArn: string;
  readonly orderId: string;
  currentStep: SagaStepName;
  status: StepFunctionsExecutionSummary['status'];
  stateJson: {
    metadata: MessageWrapper<StartSagaPayload>['metadata'];
    payload: SagaStatePayload;
  };
  readonly startDate: string;
  stopDate?: string;
  error?: string;
  cause?: string;
}

/**
 * Interprets the exact transitions of infra/statemachine/order-saga.asl.json
 * when MiniStack container is offline or during in-process E2E verification,
 * enqueueing the identical SQS MessageWrapper envelopes with taskTokens.
 */
@Injectable()
export class AslLocalEngineService {
  private readonly executionsByOrder = new Map<string, ActiveExecution>();
  private readonly executionsByToken = new Map<string, ActiveExecution>();
  private readonly localSqsBuffer: MessageWrapper<SagaStatePayload>[] = [];

  startExecution(wrapper: MessageWrapper<StartSagaPayload>): {
    executionArn: string;
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
      startDate: new Date().toISOString(),
    };

    this.executionsByOrder.set(orderId, execution);
    this.enqueueStep(execution, 'RESERVE_STOCK');
    return { executionArn };
  }

  private enqueueStep(
    execution: ActiveExecution,
    stepName: SagaStepName,
  ): void {
    execution.currentStep = stepName;
    const taskToken = `task_token_${execution.orderId}_${stepName}_${randomUUID().slice(0, 6)}`;
    this.executionsByToken.set(taskToken, execution);

    const sqsMessageBody: MessageWrapper<SagaStatePayload> = {
      stepName,
      idempotencyKey: `${execution.orderId}:${stepName}`,
      taskToken,
      metadata: { ...execution.stateJson.metadata },
      payload: structuredClone(execution.stateJson.payload),
    };

    this.localSqsBuffer.push(sqsMessageBody);
  }

  dequeueMessages(max = 5): MessageWrapper<SagaStatePayload>[] {
    return this.localSqsBuffer.splice(0, max);
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
        execution.status = 'SUCCEEDED';
        execution.stopDate = new Date().toISOString();
        break;

      case 'REFUND_PAYMENT':
        // Rollback Level 2 cascades into Rollback Level 1 (ReleaseStock)
        payload.paymentRefund = output;
        this.enqueueStep(execution, 'RELEASE_STOCK');
        break;

      case 'RELEASE_STOCK':
        // Rollback Level 1 cascades into CancelOrder
        payload.stockRelease = output;
        this.enqueueStep(execution, 'CANCEL_ORDER');
        break;

      case 'CANCEL_ORDER':
        payload.cancellation = output;
        execution.status = 'FAILED';
        execution.error = 'ORDER_SAGA_ROLLED_BACK';
        execution.cause =
          'Order saga failed and all compensations were executed.';
        execution.stopDate = new Date().toISOString();
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

    // ASL Catch with ResultPath: "$.payload.error"
    payload.error = {
      Error: error.code,
      Cause: causeStr,
    };

    switch (step) {
      case 'RESERVE_STOCK':
        // Step 1 Failure -> goes directly to CancelOrder
        this.enqueueStep(execution, 'CANCEL_ORDER');
        break;

      case 'PROCESS_PAYMENT':
        // Step 2 Failure -> Rollback Level 1: ReleaseStock -> CancelOrder
        this.enqueueStep(execution, 'RELEASE_STOCK');
        break;

      case 'CREATE_SHIPMENT':
        // Step 3 Failure -> Rollback Level 2 (Cascade): RefundPayment -> ReleaseStock -> CancelOrder
        this.enqueueStep(execution, 'REFUND_PAYMENT');
        break;

      default:
        execution.status = 'FAILED';
        execution.error = error.code;
        execution.cause = causeStr;
        execution.stopDate = new Date().toISOString();
        break;
    }
  }

  getExecutionSummary(orderId: string): StepFunctionsExecutionSummary | null {
    const exec = this.executionsByOrder.get(orderId);
    if (!exec) {
      return null;
    }
    return {
      executionArn: exec.executionArn,
      status: exec.status,
      startDate: exec.startDate,
      ...(exec.stopDate ? { stopDate: exec.stopDate } : {}),
      ...(exec.error ? { error: exec.error } : {}),
      ...(exec.cause ? { cause: exec.cause } : {}),
    };
  }

  getAllExecutions(): StepFunctionsExecutionSummary[] {
    return Array.from(this.executionsByOrder.keys())
      .map((orderId) => this.getExecutionSummary(orderId))
      .filter((item): item is StepFunctionsExecutionSummary => item !== null);
  }

  clear(): void {
    this.executionsByOrder.clear();
    this.executionsByToken.clear();
    this.localSqsBuffer.length = 0;
  }
}
