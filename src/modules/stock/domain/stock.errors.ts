import type { DomainError } from '../../../shared/core/domain-error.js';

export class InsufficientStockError implements DomainError {
  readonly code = 'INSUFFICIENT_STOCK';
  readonly message: string;
  readonly retryable = false;
  readonly metadata: Record<string, unknown>;

  constructor(sku: string, requested: number, available: number) {
    this.message = `Insufficient stock for SKU '${sku}': requested ${requested}, available ${available}`;
    this.metadata = { sku, requested, available };
  }
}

export class StockSkuNotFoundError implements DomainError {
  readonly code = 'STOCK_SKU_NOT_FOUND';
  readonly message: string;
  readonly retryable = false;

  constructor(sku: string) {
    this.message = `Stock SKU '${sku}' was not found`;
  }
}

export class StockReservationNotFoundError implements DomainError {
  readonly code = 'STOCK_RESERVATION_NOT_FOUND';
  readonly message: string;
  readonly retryable = false;

  constructor(identifier: string) {
    this.message = `Stock reservation for '${identifier}' was not found`;
  }
}
