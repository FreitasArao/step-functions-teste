import { Inject, Injectable } from '@nestjs/common';
import type { DomainError } from '../../../shared/core/domain-error.js';
import { Result } from '../../../shared/core/result.js';
import type { OrderStatus } from '../domain/order.entity.js';
import { OrderNotFoundError } from '../domain/order.errors.js';
import {
  ORDER_REPOSITORY_PORT,
  type OrderRepositoryPort,
} from '../domain/order.repository.port.js';

export interface ConfirmOrderInput {
  readonly orderId: string;
}

export interface ConfirmOrderOutput {
  readonly orderId: string;
  readonly status: OrderStatus;
  readonly confirmedAt: string;
}

@Injectable()
export class ConfirmOrderUseCase {
  constructor(
    @Inject(ORDER_REPOSITORY_PORT)
    private readonly orderRepository: OrderRepositoryPort,
  ) {}

  async execute(
    input: ConfirmOrderInput,
  ): Promise<Result<ConfirmOrderOutput, DomainError>> {
    const order = await this.orderRepository.findById(input.orderId);
    if (!order) {
      return Result.err(new OrderNotFoundError(input.orderId));
    }

    const confirmResult = order.confirm();
    if (confirmResult.isErr()) {
      return Result.err(confirmResult.error);
    }

    await this.orderRepository.save(confirmResult.value);

    return Result.ok({
      orderId: order.id,
      status: confirmResult.value.status,
      confirmedAt: new Date().toISOString(),
    });
  }
}
