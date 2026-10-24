import { Inject, Injectable } from '@nestjs/common';
import type { DomainError } from '../../../shared/core/domain-error.js';
import { Result } from '../../../shared/core/result.js';
import type { PaymentStatus } from '../domain/payment.entity.js';
import { PaymentNotFoundError } from '../domain/payment.errors.js';
import {
  PAYMENT_REPOSITORY_PORT,
  type PaymentRepositoryPort,
} from '../domain/payment.repository.port.js';

export interface RefundPaymentInput {
  readonly orderId: string;
}

export interface RefundPaymentOutput {
  readonly paymentId: string;
  readonly orderId: string;
  readonly refundedAmount: number;
  readonly status: PaymentStatus;
}

@Injectable()
export class RefundPaymentUseCase {
  constructor(
    @Inject(PAYMENT_REPOSITORY_PORT)
    private readonly paymentRepository: PaymentRepositoryPort,
  ) {}

  async execute(
    input: RefundPaymentInput,
  ): Promise<Result<RefundPaymentOutput, DomainError>> {
    const payment = await this.paymentRepository.findByOrderId(input.orderId);
    if (!payment) {
      return Result.err(new PaymentNotFoundError(input.orderId));
    }

    const refundResult = payment.refund();
    if (refundResult.isErr()) {
      return Result.err(refundResult.error);
    }

    await this.paymentRepository.save(refundResult.value);

    return Result.ok({
      paymentId: payment.id,
      orderId: payment.orderId,
      refundedAmount: payment.amount,
      status: refundResult.value.status,
    });
  }
}
