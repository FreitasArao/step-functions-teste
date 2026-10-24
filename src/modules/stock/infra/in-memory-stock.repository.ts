import { Injectable } from '@nestjs/common';
import {
  StockItem,
  type StockReservation,
} from '../domain/stock-item.entity.js';
import type { StockRepositoryPort } from '../domain/stock.repository.port.js';

@Injectable()
export class InMemoryStockRepository implements StockRepositoryPort {
  private readonly items = new Map<string, StockItem>();
  private readonly reservationsByOrder = new Map<string, StockReservation>();

  constructor() {
    this.seedDefaults();
  }

  seedDefaults(): void {
    this.items.clear();
    this.reservationsByOrder.clear();
    this.items.set(
      'SKU-100',
      StockItem.create({
        sku: 'SKU-100',
        name: 'Mechanical Keyboard Pro',
        availableQuantity: 10,
        reservedQuantity: 0,
      }),
    );
    this.items.set(
      'SKU-200',
      StockItem.create({
        sku: 'SKU-200',
        name: 'Ultrawide Monitor 34"',
        availableQuantity: 5,
        reservedQuantity: 0,
      }),
    );
  }

  async findBySku(sku: string): Promise<StockItem | null> {
    return this.items.get(sku) ?? null;
  }

  async saveItem(item: StockItem): Promise<void> {
    this.items.set(item.sku, item);
  }

  async saveReservation(reservation: StockReservation): Promise<void> {
    this.reservationsByOrder.set(reservation.orderId, reservation);
  }

  async findReservationByOrderId(
    orderId: string,
  ): Promise<StockReservation | null> {
    return this.reservationsByOrder.get(orderId) ?? null;
  }

  async findAllItems(): Promise<StockItem[]> {
    return Array.from(this.items.values());
  }

  async findAllReservations(): Promise<StockReservation[]> {
    return Array.from(this.reservationsByOrder.values());
  }
}
