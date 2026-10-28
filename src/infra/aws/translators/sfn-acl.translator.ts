import type {
  DescribeExecutionCommandOutput,
  SendTaskFailureCommandInput,
  SendTaskSuccessCommandInput,
  StartExecutionCommandInput,
} from '@aws-sdk/client-sfn';
import { Injectable } from '@nestjs/common';
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

/**
 * Raw AWS Step Functions -> SQS envelope defined in order-saga.asl.json
 */
export interface AwsSfnSqsTaskEnvelope {
  readonly stepName: SagaStepName;
  readonly idempotencyKey: string;
  readonly taskToken: string;
  readonly metadata: MessageMetadata;
  readonly payload: {
    readonly orderId: string;
    readonly sku: string;
    readonly quantity: number;
    readonly amount: number;
    readonly zipCode: string;
    readonly error?: {
      readonly Error?: string;
      readonly Cause?: string;
    };
    readonly [key: string]: unknown;
  };
}

/**
 * Anti-Corruption Layer (ACL) / Translation Layer between:
 * - AWS Step Functions + SQS (TaskToken, ExecutionArn, ASL Error/Cause format)
 * - Agnostic Application & Domain Contracts (MessageWrapper, WorkflowStepCommand, DomainError, WorkflowExecutionSummary)
 */
@Injectable()
export class AwsStepFunctionsTranslator {
  /**
   * Translates an agnostic MessageWrapper<StartSagaPayload> into AWS StartExecutionCommandInput.
   */
  toStartExecutionInput(
    stateMachineArn: string,
    wrapper: MessageWrapper<StartSagaPayload>,
  ): StartExecutionCommandInput {
    return {
      stateMachineArn,
      name: wrapper.payload.orderId,
      input: JSON.stringify({
        metadata: wrapper.metadata,
        payload: wrapper.payload,
      }),
    };
  }

  /**
   * Translates an agnostic step completion into AWS SendTaskSuccessCommandInput.
   */
  toSendTaskSuccessInput(params: {
    correlationToken: string;
    output: unknown;
  }): SendTaskSuccessCommandInput {
    return {
      taskToken: params.correlationToken,
      output: JSON.stringify(params.output ?? {}),
    };
  }

  /**
   * Translates a DomainError into AWS SendTaskFailureCommandInput.
   */
  toSendTaskFailureInput(params: {
    correlationToken: string;
    error: DomainError;
  }): SendTaskFailureCommandInput {
    return {
      taskToken: params.correlationToken,
      error: params.error.code,
      cause: JSON.stringify({
        code: params.error.code,
        message: params.error.message,
        retryable: params.error.retryable ?? false,
        metadata: params.error.metadata ?? {},
      }),
    };
  }

  /**
   * Translates a raw AWS SQS MessageBody emitted by Step Functions (.waitForTaskToken)
   * into a pure, cloud-agnostic WorkflowStepCommand for the Application Layer.
   */
  toWorkflowStepCommand(rawSqsBody: string): WorkflowStepCommand {
    const raw = JSON.parse(rawSqsBody) as AwsSfnSqsTaskEnvelope;
    const failure = this.extractFailureFromAslError(raw.payload.error);

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
          source: 'aws-sfn-acl',
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
   * Translates AWS DescribeExecutionCommandOutput into an agnostic WorkflowExecutionSummary.
   */
  toWorkflowExecutionSummary(
    executionArn: string,
    awsOutput: DescribeExecutionCommandOutput,
  ): WorkflowExecutionSummary {
    const awsStatus = awsOutput.status;
    let status: WorkflowExecutionSummary['status'] = 'RUNNING';

    if (awsStatus === 'SUCCEEDED') {
      status = 'COMPLETED';
    } else if (awsStatus === 'FAILED') {
      status =
        awsOutput.error === 'ORDER_SAGA_ROLLED_BACK'
          ? 'ROLLED_BACK'
          : 'FAILED';
    } else if (awsStatus === 'TIMED_OUT' || awsStatus === 'ABORTED') {
      status = 'FAILED';
    }

    return {
      workflowInstanceId: executionArn,
      status,
      ...(awsOutput.startDate
        ? { startedAt: awsOutput.startDate.toISOString() }
        : {}),
      ...(awsOutput.stopDate
        ? { finishedAt: awsOutput.stopDate.toISOString() }
        : {}),
      ...(awsOutput.error ? { failureCode: awsOutput.error } : {}),
      ...(awsOutput.cause ? { failureReason: awsOutput.cause } : {}),
    };
  }

  private extractFailureFromAslError(aslError?: {
    readonly Error?: string;
    readonly Cause?: string;
  }): WorkflowFailureDetails | undefined {
    if (!aslError) {
      return undefined;
    }
    if (aslError.Cause) {
      try {
        const parsed = JSON.parse(aslError.Cause) as {
          code?: string;
          message?: string;
        };
        return {
          code: parsed.code ?? aslError.Error,
          message: parsed.message ?? aslError.Cause,
        };
      } catch {
        return {
          code: aslError.Error,
          message: aslError.Cause,
        };
      }
    }
    return {
      code: aslError.Error,
      message: aslError.Error,
    };
  }
}
