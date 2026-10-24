import type { DomainError } from '../../../shared/core/domain-error.js';

export class InvalidOrderInputError implements DomainError {
  readonly code = 'INVALID_ORDER_INPUT';
  readonly message: string;
  readonly retryable = false;

  constructor(reason: string) {
    this.message = `Invalid order input: ${reason}`;
  }
}

export class OrderNotFoundError implements DomainError {
  readonly code = 'ORDER_NOT_FOUND';
  readonly message: string;
  readonly retryable = false;

  constructor(orderId: string) {
    this.message = `Order '${orderId}' was not found`;
  }
}

export class InvalidOrderTransitionError implements DomainError {
  readonly code = 'INVALID_ORDER_TRANSITION';
  readonly message: string;
  readonly retryable = false;

  constructor(orderId: string, currentStatus: string, targetStatus: string) {
    this.message = `Order '${orderId}' cannot transition from '${currentStatus}' to '${targetStatus}'`;
  }
}
