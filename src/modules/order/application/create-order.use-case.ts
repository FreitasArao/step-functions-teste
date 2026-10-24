import { Inject, Injectable } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import {
  TELEMETRY_PORT,
  type TelemetryPort,
} from '../../../infra/telemetry/tracer.js';
import type { DomainError } from '../../../shared/core/domain-error.js';
import { Result } from '../../../shared/core/result.js';
import {
  createMessageWrapper,
  type TraceContextCarrier,
} from '../../../shared/messaging/message-wrapper.js';
import {
  UNIT_OF_WORK_PORT,
  type UnitOfWorkPort,
} from '../../../shared/messaging/unit-of-work.port.js';
import type { StartSagaPayload } from '../domain/order-workflow.port.js';
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
        return this.unitOfWork.execute(async ({ outbox }) => {
          await this.orderRepository.save(order);

          const sagaPayload: StartSagaPayload = {
            orderId: order.id,
            sku: order.sku,
            quantity: order.quantity,
            amount: order.amount,
            zipCode: order.zipCode,
          };

          const wrapper = createMessageWrapper<StartSagaPayload>({
            payload: sagaPayload,
            correlationId: order.id,
            source: 'order-module:CreateOrderUseCase',
            traceContext: traceCarrier,
          });

          const outboxMessageId = `out_${randomUUID().slice(0, 8)}`;
          await outbox.save({
            id: outboxMessageId,
            orderId: order.id,
            eventType: 'ORDER_WORKFLOW_REQUESTED',
            status: 'PENDING',
            wrapper,
            createdAt: new Date().toISOString(),
          });

          return Result.ok({
            order: order.toJSON(),
            outboxMessageId,
            correlationId: order.id,
            traceContext: wrapper.metadata.traceContext,
          });
        });
      },
    );
  }
}
