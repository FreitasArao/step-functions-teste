import type { DomainError } from '../../../shared/core/domain-error.js';

export class PaymentDeclinedError implements DomainError {
  readonly code = 'PAYMENT_DECLINED';
  readonly message: string;
  readonly retryable = false;
  readonly metadata: Record<string, unknown>;

  constructor(orderId: string, amount: number, reason: string) {
    this.message = `Payment of ${amount} for order '${orderId}' was declined: ${reason}`;
    this.metadata = { orderId, amount, reason };
  }
}

export class PaymentNotFoundError implements DomainError {
  readonly code = 'PAYMENT_NOT_FOUND';
  readonly message: string;
  readonly retryable = false;

  constructor(orderId: string) {
    this.message = `Payment for order '${orderId}' was not found`;
  }
}
