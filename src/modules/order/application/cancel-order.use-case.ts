import { Inject, Injectable } from '@nestjs/common';
import type { DomainError } from '../../../shared/core/domain-error.js';
import { Result } from '../../../shared/core/result.js';
import type { OrderStatus } from '../domain/order.entity.js';
import { OrderNotFoundError } from '../domain/order.errors.js';
import {
  ORDER_REPOSITORY_PORT,
  type OrderRepositoryPort,
} from '../domain/order.repository.port.js';

export interface CancelOrderInput {
  readonly orderId: string;
  readonly reason: string;
}

export interface CancelOrderOutput {
  readonly orderId: string;
  readonly status: OrderStatus;
  readonly cancellationReason: string;
  readonly cancelledAt: string;
}

@Injectable()
export class CancelOrderUseCase {
  constructor(
    @Inject(ORDER_REPOSITORY_PORT)
    private readonly orderRepository: OrderRepositoryPort,
  ) {}

  async execute(
    input: CancelOrderInput,
  ): Promise<Result<CancelOrderOutput, DomainError>> {
    const order = await this.orderRepository.findById(input.orderId);
    if (!order) {
      return Result.err(new OrderNotFoundError(input.orderId));
    }

    const cancelResult = order.cancel(input.reason);
    if (cancelResult.isErr()) {
      return Result.err(cancelResult.error);
    }

    await this.orderRepository.save(cancelResult.value);

    return Result.ok({
      orderId: order.id,
      status: cancelResult.value.status,
      cancellationReason: input.reason,
      cancelledAt: new Date().toISOString(),
    });
  }
}
