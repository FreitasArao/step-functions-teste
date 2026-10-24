import { randomUUID } from 'node:crypto';
import type { DomainError } from '../../../shared/core/domain-error.js';
import { Result } from '../../../shared/core/result.js';
import { PaymentDeclinedError } from './payment.errors.js';

export type PaymentStatus = 'APPROVED' | 'DECLINED' | 'REFUNDED';

export interface PaymentProps {
  readonly id: string;
  readonly orderId: string;
  readonly amount: number;
  readonly status: PaymentStatus;
  readonly reason?: string;
  readonly processedAt: string;
  readonly refundedAt?: string;
}

export class Payment {
  static readonly MAX_APPROVED_AMOUNT = 1000;

  private constructor(private props: PaymentProps) {}

  static rehydrate(props: PaymentProps): Payment {
    return new Payment({ ...props });
  }

  static process(params: {
    orderId: string;
    amount: number;
  }): Result<Payment, DomainError> {
    if (params.amount <= 0) {
      return Result.err(
        new PaymentDeclinedError(
          params.orderId,
          params.amount,
          'Amount must be greater than zero',
        ),
      );
    }

    if (params.amount > Payment.MAX_APPROVED_AMOUNT) {
      return Result.err(
        new PaymentDeclinedError(
          params.orderId,
          params.amount,
          `Amount exceeds limit of ${Payment.MAX_APPROVED_AMOUNT}`,
        ),
      );
    }

    return Result.ok(
      new Payment({
        id: `pay_${randomUUID().slice(0, 8)}`,
        orderId: params.orderId,
        amount: params.amount,
        status: 'APPROVED',
        processedAt: new Date().toISOString(),
      }),
    );
  }

  static createDeclinedRecord(params: {
    orderId: string;
    amount: number;
    reason: string;
  }): Payment {
    return new Payment({
      id: `pay_${randomUUID().slice(0, 8)}`,
      orderId: params.orderId,
      amount: params.amount,
      status: 'DECLINED',
      reason: params.reason,
      processedAt: new Date().toISOString(),
    });
  }

  refund(): Result<Payment, DomainError> {
    if (this.props.status === 'REFUNDED') {
      return Result.ok(this);
    }
    this.props = {
      ...this.props,
      status: 'REFUNDED',
      refundedAt: new Date().toISOString(),
    };
    return Result.ok(this);
  }

  get id(): string {
    return this.props.id;
  }

  get orderId(): string {
    return this.props.orderId;
  }

  get amount(): number {
    return this.props.amount;
  }

  get status(): PaymentStatus {
    return this.props.status;
  }

  toJSON(): PaymentProps {
    return { ...this.props };
  }
}
