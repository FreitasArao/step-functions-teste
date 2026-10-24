import { randomUUID } from 'node:crypto';
import type { DomainError } from '../../../shared/core/domain-error.js';
import { Result } from '../../../shared/core/result.js';
import {
  InvalidOrderInputError,
  InvalidOrderTransitionError,
} from './order.errors.js';

export type OrderStatus = 'PENDING' | 'CONFIRMED' | 'CANCELLED';

export interface OrderProps {
  readonly id: string;
  readonly sku: string;
  readonly quantity: number;
  readonly amount: number;
  readonly zipCode: string;
  readonly status: OrderStatus;
  readonly cancellationReason?: string;
  readonly createdAt: string;
  readonly updatedAt: string;
}

export class Order {
  private constructor(private props: OrderProps) {}

  static rehydrate(props: OrderProps): Order {
    return new Order({ ...props });
  }

  static create(params: {
    id?: string;
    sku: string;
    quantity: number;
    amount: number;
    zipCode: string;
  }): Result<Order, DomainError> {
    if (!params.sku || params.sku.trim().length === 0) {
      return Result.err(new InvalidOrderInputError('sku is required'));
    }
    if (!Number.isInteger(params.quantity) || params.quantity <= 0) {
      return Result.err(
        new InvalidOrderInputError('quantity must be a positive integer'),
      );
    }
    if (typeof params.amount !== 'number' || params.amount <= 0) {
      return Result.err(
        new InvalidOrderInputError('amount must be a positive number'),
      );
    }
    if (!params.zipCode || params.zipCode.trim().length === 0) {
      return Result.err(new InvalidOrderInputError('zipCode is required'));
    }

    const now = new Date().toISOString();
    return Result.ok(
      new Order({
        id: params.id ?? `ord_${randomUUID().slice(0, 8)}`,
        sku: params.sku.trim(),
        quantity: params.quantity,
        amount: params.amount,
        zipCode: params.zipCode.trim(),
        status: 'PENDING',
        createdAt: now,
        updatedAt: now,
      }),
    );
  }

  confirm(): Result<Order, DomainError> {
    if (this.props.status === 'CONFIRMED') {
      return Result.ok(this);
    }
    if (this.props.status === 'CANCELLED') {
      return Result.err(
        new InvalidOrderTransitionError(
          this.props.id,
          this.props.status,
          'CONFIRMED',
        ),
      );
    }
    this.props = {
      ...this.props,
      status: 'CONFIRMED',
      updatedAt: new Date().toISOString(),
    };
    return Result.ok(this);
  }

  cancel(reason: string): Result<Order, DomainError> {
    if (this.props.status === 'CANCELLED') {
      return Result.ok(this);
    }
    if (this.props.status === 'CONFIRMED') {
      return Result.err(
        new InvalidOrderTransitionError(
          this.props.id,
          this.props.status,
          'CANCELLED',
        ),
      );
    }
    this.props = {
      ...this.props,
      status: 'CANCELLED',
      cancellationReason: reason,
      updatedAt: new Date().toISOString(),
    };
    return Result.ok(this);
  }

  get id(): string {
    return this.props.id;
  }

  get sku(): string {
    return this.props.sku;
  }

  get quantity(): number {
    return this.props.quantity;
  }

  get amount(): number {
    return this.props.amount;
  }

  get zipCode(): string {
    return this.props.zipCode;
  }

  get status(): OrderStatus {
    return this.props.status;
  }

  get cancellationReason(): string | undefined {
    return this.props.cancellationReason;
  }

  toJSON(): OrderProps {
    return { ...this.props };
  }
}
