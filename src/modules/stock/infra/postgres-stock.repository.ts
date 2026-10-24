import { Injectable } from '@nestjs/common';
import { PostgresDatabaseService } from '../../../infra/database/postgres-client.provider.js';
import {
  type ReservationStatus,
  StockItem,
  StockReservation,
} from '../domain/stock-item.entity.js';
import type { StockRepositoryPort } from '../domain/stock.repository.port.js';
import { InMemoryStockRepository } from './in-memory-stock.repository.js';

interface StockItemRow {
  sku: string;
  name: string;
  available_quantity: number;
  reserved_quantity: number;
}

interface StockReservationRow {
  id: string;
  order_id: string;
  sku: string;
  quantity: number;
  status: ReservationStatus;
  created_at: Date | string;
  released_at: Date | string | null;
}

function mapStockItemRow(row: StockItemRow): StockItem {
  return StockItem.create({
    sku: row.sku,
    name: row.name,
    availableQuantity: Number(row.available_quantity),
    reservedQuantity: Number(row.reserved_quantity),
  });
}

function mapReservationRow(row: StockReservationRow): StockReservation {
  return StockReservation.rehydrate({
    id: row.id,
    orderId: row.order_id,
    sku: row.sku,
    quantity: Number(row.quantity),
    status: row.status,
    createdAt:
      row.created_at instanceof Date
        ? row.created_at.toISOString()
        : String(row.created_at),
    ...(row.released_at
      ? {
          releasedAt:
            row.released_at instanceof Date
              ? row.released_at.toISOString()
              : String(row.released_at),
        }
      : {}),
  });
}

@Injectable()
export class PostgresStockRepository implements StockRepositoryPort {
  constructor(
    private readonly db: PostgresDatabaseService,
    private readonly fallbackMemory: InMemoryStockRepository,
  ) {}

  async findBySku(sku: string): Promise<StockItem | null> {
    if (!this.db.isPostgresActive()) {
      return this.fallbackMemory.findBySku(sku);
    }
    const q = this.db.getQueryable();
    const res = await q.query<StockItemRow>(
      `SELECT * FROM stock_items WHERE sku = $1 FOR UPDATE`,
      [sku],
    );
    const row = res.rows[0];
    return row ? mapStockItemRow(row) : null;
  }

  async saveItem(item: StockItem): Promise<void> {
    if (!this.db.isPostgresActive()) {
      return this.fallbackMemory.saveItem(item);
    }
    const json = item.toJSON();
    const q = this.db.getQueryable();
    await q.query(
      `INSERT INTO stock_items (sku, name, available_quantity, reserved_quantity)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (sku) DO UPDATE SET
         name = EXCLUDED.name,
         available_quantity = EXCLUDED.available_quantity,
         reserved_quantity = EXCLUDED.reserved_quantity`,
      [json.sku, json.name, json.availableQuantity, json.reservedQuantity],
    );
  }

  async saveReservation(reservation: StockReservation): Promise<void> {
    if (!this.db.isPostgresActive()) {
      return this.fallbackMemory.saveReservation(reservation);
    }
    const json = reservation.toJSON();
    const q = this.db.getQueryable();
    await q.query(
      `INSERT INTO stock_reservations (id, order_id, sku, quantity, status, created_at, released_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7)
       ON CONFLICT (order_id) DO UPDATE SET
         status = EXCLUDED.status,
         released_at = EXCLUDED.released_at`,
      [
        json.id,
        json.orderId,
        json.sku,
        json.quantity,
        json.status,
        json.createdAt,
        json.releasedAt ?? null,
      ],
    );
  }

  async findReservationByOrderId(
    orderId: string,
  ): Promise<StockReservation | null> {
    if (!this.db.isPostgresActive()) {
      return this.fallbackMemory.findReservationByOrderId(orderId);
    }
    const q = this.db.getQueryable();
    const res = await q.query<StockReservationRow>(
      `SELECT * FROM stock_reservations WHERE order_id = $1 FOR UPDATE`,
      [orderId],
    );
    const row = res.rows[0];
    return row ? mapReservationRow(row) : null;
  }

  async findAllItems(): Promise<StockItem[]> {
    if (!this.db.isPostgresActive()) {
      return this.fallbackMemory.findAllItems();
    }
    const q = this.db.getQueryable();
    const res = await q.query<StockItemRow>(
      `SELECT * FROM stock_items ORDER BY sku ASC`,
    );
    return res.rows.map(mapStockItemRow);
  }

  async findAllReservations(): Promise<StockReservation[]> {
    if (!this.db.isPostgresActive()) {
      return this.fallbackMemory.findAllReservations();
    }
    const q = this.db.getQueryable();
    const res = await q.query<StockReservationRow>(
      `SELECT * FROM stock_reservations ORDER BY created_at ASC`,
    );
    return res.rows.map(mapReservationRow);
  }
}
