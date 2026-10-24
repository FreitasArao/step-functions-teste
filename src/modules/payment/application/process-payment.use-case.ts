import { Inject, Injectable } from '@nestjs/common';
import type { DomainError } from '../../../shared/core/domain-error.js';
import { Result } from '../../../shared/core/result.js';
import { Payment, type PaymentStatus } from '../domain/payment.entity.js';
import {
  PAYMENT_REPOSITORY_PORT,
  type PaymentRepositoryPort,
} from '../domain/payment.repository.port.js';

export interface ProcessPaymentInput {
  readonly orderId: string;
  readonly amount: number;
}

export interface ProcessPaymentOutput {
  readonly paymentId: string;
  readonly orderId: string;
  readonly amount: number;
  readonly status: PaymentStatus;
}

@Injectable()
export class ProcessPaymentUseCase {
  constructor(
    @Inject(PAYMENT_REPOSITORY_PORT)
    private readonly paymentRepository: PaymentRepositoryPort,
  ) {}

  async execute(
    input: ProcessPaymentInput,
  ): Promise<Result<ProcessPaymentOutput, DomainError>> {
    const paymentResult = Payment.process({
      orderId: input.orderId,
      amount: input.amount,
    });

    if (paymentResult.isErr()) {
      const declined = Payment.createDeclinedRecord({
        orderId: input.orderId,
        amount: input.amount,
        reason: paymentResult.error.message,
      });
      await this.paymentRepository.save(declined);
      return Result.err(paymentResult.error);
    }

    const payment = paymentResult.value;
    await this.paymentRepository.save(payment);

    return Result.ok({
      paymentId: payment.id,
      orderId: payment.orderId,
      amount: payment.amount,
      status: payment.status,
    });
  }
}
