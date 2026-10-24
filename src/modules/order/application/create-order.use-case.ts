import { Inject, Injectable } from '@nestjs/common';
import {
  TELEMETRY_PORT,
  type TelemetryPort,
} from '../../../infra/telemetry/tracer.js';
import type { DomainError } from '../../../shared/core/domain-error.js';
import { Result } from '../../../shared/core/result.js';
import type { TraceContextCarrier } from '../../../shared/messaging/message-wrapper.js';
import {
  UNIT_OF_WORK_PORT,
  type UnitOfWorkPort,
} from '../../../shared/messaging/unit-of-work.port.js';
import { Order, type OrderProps } from '../domain/order.entity.js';
import {
  ORDER_REPOSITORY_PORT,
  type OrderRepositoryPort,
} from '../domain/order.repository.port.js';

export interface CreateOrderInput {
  readonly sku: string;
  readonly quantity: number;
  readonly amount: number;
  readonly zipCode: string;
  readonly parentTraceCarrier?: TraceContextCarrier;
}

export interface CreateOrderOutput {
  readonly order: OrderProps;
  readonly outboxMessageId: string;
  readonly correlationId: string;
  readonly traceContext: TraceContextCarrier;
}

@Injectable()
export class CreateOrderUseCase {
  constructor(
    @Inject(ORDER_REPOSITORY_PORT)
    private readonly orderRepository: OrderRepositoryPort,
    @Inject(UNIT_OF_WORK_PORT)
    private readonly unitOfWork: UnitOfWorkPort,
    @Inject(TELEMETRY_PORT)
    private readonly telemetry: TelemetryPort,
  ) {}

  async execute(
    input: CreateOrderInput,
  ): Promise<Result<CreateOrderOutput, DomainError>> {
    const orderResult = Order.create({
      sku: input.sku,
      quantity: input.quantity,
      amount: input.amount,
      zipCode: input.zipCode,
    });

    if (orderResult.isErr()) {
      return Result.err(orderResult.error);
    }

    const order = orderResult.value;

    return this.telemetry.runInSpan(
      {
        operationName: 'order.create',
        resourceName: 'CreateOrderUseCase.execute',
        correlationId: order.id,
        parentCarrier: input.parentTraceCarrier,
        tags: {
          'order.id': order.id,
          'order.sku': order.sku,
          'order.quantity': order.quantity,
          'order.amount': order.amount,
          'order.zip_code': order.zipCode,
        },
      },
      async (traceCarrier) => {
        return this.unitOfWork.execute(async ({ outbox: _outbox }) => {
          await this.orderRepository.save(order);

          // TODO (Prática - Transactional Outbox Agnóstico + MessageWrapper):
          // 1. Monte o StartSagaPayload e crie um MessageWrapper com `createMessageWrapper(...)`
          // 2. Grave na Outbox (`_outbox.save`) o evento agnóstico:
          //    - eventType: 'ORDER_WORKFLOW_REQUESTED'
          //    - status: 'PENDING'
          //    - wrapper

          return Result.ok({
            order: order.toJSON(),
            outboxMessageId: 'todo_outbox_id',
            correlationId: order.id,
            traceContext: traceCarrier,
          });
        });
      },
    );
  }
}
