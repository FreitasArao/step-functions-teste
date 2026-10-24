import { randomUUID } from 'node:crypto';
import type { DomainError } from '../../../shared/core/domain-error.js';
import { Result } from '../../../shared/core/result.js';
import { InsufficientStockError } from './stock.errors.js';

export type ReservationStatus = 'RESERVED' | 'RELEASED';

export interface StockReservationProps {
  readonly id: string;
  readonly orderId: string;
  readonly sku: string;
  readonly quantity: number;
  readonly status: ReservationStatus;
  readonly createdAt: string;
  readonly releasedAt?: string;
}

export class StockReservation {
  private constructor(private props: StockReservationProps) {}

  static rehydrate(props: StockReservationProps): StockReservation {
    return new StockReservation({ ...props });
  }

  static create(params: {
    orderId: string;
    sku: string;
    quantity: number;
  }): StockReservation {
    return new StockReservation({
      id: `res_${randomUUID().slice(0, 8)}`,
      orderId: params.orderId,
      sku: params.sku,
      quantity: params.quantity,
      status: 'RESERVED',
      createdAt: new Date().toISOString(),
    });
  }

  markReleased(): Result<StockReservation, DomainError> {
    if (this.props.status === 'RELEASED') {
      return Result.ok(this);
    }
    this.props = {
      ...this.props,
      status: 'RELEASED',
      releasedAt: new Date().toISOString(),
    };
    return Result.ok(this);
  }

  get id(): string {
    return this.props.id;
  }

  get orderId(): string {
    return this.props.orderId;
  }

  get sku(): string {
    return this.props.sku;
  }

  get quantity(): number {
    return this.props.quantity;
  }

  get status(): ReservationStatus {
    return this.props.status;
  }

  toJSON(): StockReservationProps {
    return { ...this.props };
  }
}

export interface StockItemProps {
  readonly sku: string;
  readonly name: string;
  readonly availableQuantity: number;
  readonly reservedQuantity: number;
}

export class StockItem {
  private constructor(private props: StockItemProps) {}

  static create(props: StockItemProps): StockItem {
    return new StockItem({ ...props });
  }

  reserve(
    orderId: string,
    quantity: number,
  ): Result<StockReservation, DomainError> {
    if (quantity <= 0) {
      return Result.err(
        new InsufficientStockError(
          this.props.sku,
          quantity,
          this.props.availableQuantity,
        ),
      );
    }
    if (this.props.availableQuantity < quantity) {
      return Result.err(
        new InsufficientStockError(
          this.props.sku,
          quantity,
          this.props.availableQuantity,
        ),
      );
    }

    this.props = {
      ...this.props,
      availableQuantity: this.props.availableQuantity - quantity,
      reservedQuantity: this.props.reservedQuantity + quantity,
    };

    const reservation = StockReservation.create({
      orderId,
      sku: this.props.sku,
      quantity,
    });
    return Result.ok(reservation);
  }

  releaseReservation(
    reservation: StockReservation,
  ): Result<StockReservation, DomainError> {
    if (reservation.status === 'RELEASED') {
      return Result.ok(reservation);
    }

    this.props = {
      ...this.props,
      availableQuantity: this.props.availableQuantity + reservation.quantity,
      reservedQuantity: Math.max(
        0,
        this.props.reservedQuantity - reservation.quantity,
      ),
    };

    return reservation.markReleased();
  }

  get sku(): string {
    return this.props.sku;
  }

  get availableQuantity(): number {
    return this.props.availableQuantity;
  }

  get reservedQuantity(): number {
    return this.props.reservedQuantity;
  }

  toJSON(): StockItemProps {
    return { ...this.props };
  }
}
