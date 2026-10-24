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
  WorkflowStepCommand,
} from '../../../modules/order/domain/order-workflow.port.js';
import type { DomainError } from '../../../shared/core/domain-error.js';
import {
  createMessageWrapper,
  type MessageMetadata,
  type MessageWrapper,
  type SagaStepName,
} from '../../../shared/messaging/message-wrapper.js';

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
 * TODO (Prática - Camada de Tradução / Anti-Corruption Layer AWS):
 * Traduz entre os contratos específicos da AWS (Step Functions + SQS)
 * e os contratos agnósticos da camada de Aplicação/Domínio.
 */
@Injectable()
export class AwsStepFunctionsTranslator {
  toStartExecutionInput(
    stateMachineArn: string,
    wrapper: MessageWrapper<StartSagaPayload>,
  ): StartExecutionCommandInput {
    // TODO: Traduzir MessageWrapper agnóstico para StartExecutionCommandInput do Step Functions
    return {
      stateMachineArn,
      name: wrapper.payload.orderId,
      input: JSON.stringify(wrapper),
    };
  }

  toSendTaskSuccessInput(params: {
    correlationToken: string;
    output: unknown;
  }): SendTaskSuccessCommandInput {
    // TODO: Traduzir correlationToken -> taskToken e serializar output
    return {
      taskToken: params.correlationToken,
      output: JSON.stringify(params.output ?? {}),
    };
  }

  toSendTaskFailureInput(params: {
    correlationToken: string;
    error: DomainError;
  }): SendTaskFailureCommandInput {
    // TODO: Traduzir DomainError (code, message) -> error e cause do Step Functions
    return {
      taskToken: params.correlationToken,
      error: params.error.code,
      cause: JSON.stringify(params.error),
    };
  }

  toWorkflowStepCommand(rawSqsBody: string): WorkflowStepCommand {
    // TODO: Traduzir o JSON bruto recebido do SQS (AwsSfnSqsTaskEnvelope com taskToken e payload.error)
    // para o comando agnóstico WorkflowStepCommand (com correlationToken e payload.failure)
    const raw = JSON.parse(rawSqsBody) as AwsSfnSqsTaskEnvelope;
    return {
      stepName: raw.stepName,
      idempotencyKey: raw.idempotencyKey,
      correlationToken: raw.taskToken,
      wrapper: createMessageWrapper({
        payload: raw.payload,
        correlationId: raw.payload.orderId,
        source: 'aws-sfn-acl',
        traceContext: raw.metadata?.traceContext ?? {},
        stepName: raw.stepName,
        idempotencyKey: raw.idempotencyKey,
      }),
    };
  }

  toWorkflowExecutionSummary(
    executionArn: string,
    _awsOutput: DescribeExecutionCommandOutput,
  ): WorkflowExecutionSummary {
    // TODO: Traduzir status do Step Functions (SUCCEEDED, FAILED, RUNNING)
    // para o status agnóstico do domínio (COMPLETED, ROLLED_BACK, FAILED, RUNNING)
    return {
      workflowInstanceId: executionArn,
      status: 'RUNNING',
    };
  }
}
