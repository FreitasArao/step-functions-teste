import type { DomainError } from '../../../shared/core/domain-error.js';

export class ShippingUnavailableError implements DomainError {
  readonly code = 'SHIPPING_UNAVAILABLE';
  readonly message: string;
  readonly retryable = false;
  readonly metadata: Record<string, unknown>;

  constructor(orderId: string, zipCode: string, reason: string) {
    this.message = `Shipping unavailable for order '${orderId}' at zipCode '${zipCode}': ${reason}`;
    this.metadata = { orderId, zipCode, reason };
  }
}
