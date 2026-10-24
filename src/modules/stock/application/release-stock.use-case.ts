import { Inject, Injectable } from '@nestjs/common';
import type { DomainError } from '../../../shared/core/domain-error.js';
import { Result } from '../../../shared/core/result.js';
import type { ReservationStatus } from '../domain/stock-item.entity.js';
import {
  StockReservationNotFoundError,
  StockSkuNotFoundError,
} from '../domain/stock.errors.js';
import {
  STOCK_REPOSITORY_PORT,
  type StockRepositoryPort,
} from '../domain/stock.repository.port.js';

export interface ReleaseStockInput {
  readonly orderId: string;
}

export interface ReleaseStockOutput {
  readonly reservationId: string;
  readonly sku: string;
  readonly releasedQuantity: number;
  readonly availableQuantityAfterRelease: number;
  readonly status: ReservationStatus;
}

@Injectable()
export class ReleaseStockUseCase {
  constructor(
    @Inject(STOCK_REPOSITORY_PORT)
    private readonly stockRepository: StockRepositoryPort,
  ) {}

  async execute(
    input: ReleaseStockInput,
  ): Promise<Result<ReleaseStockOutput, DomainError>> {
    const reservation = await this.stockRepository.findReservationByOrderId(
      input.orderId,
    );
    if (!reservation) {
      return Result.err(new StockReservationNotFoundError(input.orderId));
    }

    const item = await this.stockRepository.findBySku(reservation.sku);
    if (!item) {
      return Result.err(new StockSkuNotFoundError(reservation.sku));
    }

    const releaseResult = item.releaseReservation(reservation);
    if (releaseResult.isErr()) {
      return Result.err(releaseResult.error);
    }

    await this.stockRepository.saveItem(item);
    await this.stockRepository.saveReservation(releaseResult.value);

    return Result.ok({
      reservationId: reservation.id,
      sku: item.sku,
      releasedQuantity: reservation.quantity,
      availableQuantityAfterRelease: item.availableQuantity,
      status: releaseResult.value.status,
    });
  }
}
