import { Inject, Injectable } from '@nestjs/common';
import type { DomainError } from '../../../shared/core/domain-error.js';
import { Result } from '../../../shared/core/result.js';
import { Shipment, type ShipmentStatus } from '../domain/shipment.entity.js';
import {
  SHIPMENT_REPOSITORY_PORT,
  type ShipmentRepositoryPort,
} from '../domain/shipment.repository.port.js';

export interface CreateShipmentInput {
  readonly orderId: string;
  readonly zipCode: string;
}

export interface CreateShipmentOutput {
  readonly shipmentId: string;
  readonly orderId: string;
  readonly zipCode: string;
  readonly trackingCode: string;
  readonly status: ShipmentStatus;
}

@Injectable()
export class CreateShipmentUseCase {
  constructor(
    @Inject(SHIPMENT_REPOSITORY_PORT)
    private readonly shipmentRepository: ShipmentRepositoryPort,
  ) {}

  async execute(
    input: CreateShipmentInput,
  ): Promise<Result<CreateShipmentOutput, DomainError>> {
    const shipmentResult = Shipment.create({
      orderId: input.orderId,
      zipCode: input.zipCode,
    });

    if (shipmentResult.isErr()) {
      const failedRecord = Shipment.createFailedRecord({
        orderId: input.orderId,
        zipCode: input.zipCode,
        reason: shipmentResult.error.message,
      });
      await this.shipmentRepository.save(failedRecord);
      return Result.err(shipmentResult.error);
    }

    const shipment = shipmentResult.value;
    await this.shipmentRepository.save(shipment);

    return Result.ok({
      shipmentId: shipment.id,
      orderId: shipment.orderId,
      zipCode: shipment.zipCode,
      trackingCode: shipment.trackingCode ?? '',
      status: shipment.status,
    });
  }
}
