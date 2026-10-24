import { Inject, Injectable } from '@nestjs/common';
import type { DomainError } from '../../../shared/core/domain-error.js';
import { Result } from '../../../shared/core/result.js';
import type { ReservationStatus } from '../domain/stock-item.entity.js';
import {
  InsufficientStockError,
  StockSkuNotFoundError,
} from '../domain/stock.errors.js';
import {
  STOCK_REPOSITORY_PORT,
  type StockRepositoryPort,
} from '../domain/stock.repository.port.js';

export interface ReserveStockInput {
  readonly orderId: string;
  readonly sku: string;
  readonly quantity: number;
}

export interface ReserveStockOutput {
  readonly reservationId: string;
  readonly sku: string;
  readonly reservedQuantity: number;
  readonly remainingAvailable: number;
  readonly status: ReservationStatus;
}

@Injectable()
export class ReserveStockUseCase {
  constructor(
    @Inject(STOCK_REPOSITORY_PORT)
    private readonly stockRepository: StockRepositoryPort,
  ) {}

  async execute(
    input: ReserveStockInput,
  ): Promise<Result<ReserveStockOutput, DomainError>> {
    const item = await this.stockRepository.findBySku(input.sku);
    if (!item) {
      return Result.err(new StockSkuNotFoundError(input.sku));
    }

    const reservationResult = item.reserve(input.orderId, input.quantity);
    if (reservationResult.isErr()) {
      return Result.err(
        reservationResult.error ??
          new InsufficientStockError(
            input.sku,
            input.quantity,
            item.availableQuantity,
          ),
      );
    }

    const reservation = reservationResult.value;
    await this.stockRepository.saveItem(item);
    await this.stockRepository.saveReservation(reservation);

    return Result.ok({
      reservationId: reservation.id,
      sku: item.sku,
      reservedQuantity: reservation.quantity,
      remainingAvailable: item.availableQuantity,
      status: reservation.status,
    });
  }
}
