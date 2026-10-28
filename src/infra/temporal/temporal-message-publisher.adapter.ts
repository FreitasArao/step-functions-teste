import { Inject, Injectable, Logger } from '@nestjs/common';
import {
  ORDER_WORKFLOW_PORT,
  type OrderWorkflowPort,
  type StartSagaPayload,
} from '../../modules/order/application/ports/order-workflow.outbound-port.js';
import type { DomainError } from '../../shared/core/domain-error.js';
import { Result } from '../../shared/core/result.js';
import type {
  MessagePublisherPort,
  PublishMessageInput,
} from '../../shared/messaging/message-publisher.port.js';
import type { MessageWrapper } from '../../shared/messaging/message-wrapper.js';

/**
 * Infrastructure Adapter implementing the generic MessagePublisherPort for Temporal.
 *
 * Unlike an in-memory buffer (which would let the Outbox mark 'ORDER_CREATED' as PUBLISHED
 * before Temporal ever received it), this adapter starts the Temporal Workflow synchronously
 * and only returns `Result.ok` after the Temporal Server acknowledges the execution.
 */
@Injectable()
export class TemporalEventPublisherAdapter implements MessagePublisherPort {
  private readonly logger = new Logger(TemporalEventPublisherAdapter.name);

  constructor(
    @Inject(ORDER_WORKFLOW_PORT)
    private readonly orderWorkflow: OrderWorkflowPort,
  ) {}

  async publish(
    input: PublishMessageInput,
  ): Promise<Result<{ brokerMessageId: string }, DomainError>> {
    if (input.eventType !== 'ORDER_CREATED') {
      return Result.ok({ brokerMessageId: `ignored_${input.messageId}` });
    }

    const wrapper = input.wrapper as MessageWrapper<StartSagaPayload>;
    const startResult = await this.orderWorkflow.startOrderWorkflow(wrapper);
    if (startResult.isErr()) {
      return Result.err(startResult.error);
    }

    const brokerMessageId = startResult.value.workflowInstanceId;
    this.logger.log(
      `[TemporalEventPublisher] Delivered Outbox event '${input.eventType}' (outboxId=${input.messageId}, orderId=${input.aggregateId}) -> workflowInstanceId=${brokerMessageId}`,
    );

    return Result.ok({ brokerMessageId });
  }
}
