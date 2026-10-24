import type {
  StockItem,
  StockReservation,
} from './stock-item.entity.js';

export const STOCK_REPOSITORY_PORT = Symbol('STOCK_REPOSITORY_PORT');

export interface StockRepositoryPort {
  findBySku(sku: string): Promise<StockItem | null>;
  saveItem(item: StockItem): Promise<void>;
  saveReservation(reservation: StockReservation): Promise<void>;
  findReservationByOrderId(orderId: string): Promise<StockReservation | null>;
  findAllItems(): Promise<StockItem[]>;
  findAllReservations(): Promise<StockReservation[]>;
}
