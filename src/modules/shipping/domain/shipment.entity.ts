import { randomUUID } from 'node:crypto';
import type { DomainError } from '../../../shared/core/domain-error.js';
import { Result } from '../../../shared/core/result.js';
import { ShippingUnavailableError } from './shipping.errors.js';

export type ShipmentStatus = 'DISPATCHED' | 'FAILED';

export interface ShipmentProps {
  readonly id: string;
  readonly orderId: string;
  readonly zipCode: string;
  readonly trackingCode?: string;
  readonly status: ShipmentStatus;
  readonly failureReason?: string;
  readonly createdAt: string;
}

export class Shipment {
  static readonly UNSUPPORTED_ZIP_PREFIX = '99';

  private constructor(private props: ShipmentProps) {}

  static rehydrate(props: ShipmentProps): Shipment {
    return new Shipment({ ...props });
  }

  static create(params: {
    orderId: string;
    zipCode: string;
  }): Result<Shipment, DomainError> {
    const cleanZip = params.zipCode.trim();
    if (!cleanZip || cleanZip.startsWith(Shipment.UNSUPPORTED_ZIP_PREFIX)) {
      return Result.err(
        new ShippingUnavailableError(
          params.orderId,
          cleanZip,
          `Region with zip prefix '${Shipment.UNSUPPORTED_ZIP_PREFIX}' is outside carrier coverage`,
        ),
      );
    }

    return Result.ok(
      new Shipment({
        id: `shp_${randomUUID().slice(0, 8)}`,
        orderId: params.orderId,
        zipCode: cleanZip,
        trackingCode: `TRK-${randomUUID().slice(0, 6).toUpperCase()}`,
        status: 'DISPATCHED',
        createdAt: new Date().toISOString(),
      }),
    );
  }

  static createFailedRecord(params: {
    orderId: string;
    zipCode: string;
    reason: string;
  }): Shipment {
    return new Shipment({
      id: `shp_${randomUUID().slice(0, 8)}`,
      orderId: params.orderId,
      zipCode: params.zipCode,
      status: 'FAILED',
      failureReason: params.reason,
      createdAt: new Date().toISOString(),
    });
  }

  get id(): string {
    return this.props.id;
  }

  get orderId(): string {
    return this.props.orderId;
  }

  get zipCode(): string {
    return this.props.zipCode;
  }

  get trackingCode(): string | undefined {
    return this.props.trackingCode;
  }

  get status(): ShipmentStatus {
    return this.props.status;
  }

  toJSON(): ShipmentProps {
    return { ...this.props };
  }
}
