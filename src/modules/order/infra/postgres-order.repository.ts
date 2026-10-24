import { Injectable } from '@nestjs/common';
import { PostgresDatabaseService } from '../../../infra/database/postgres-client.provider.js';
import { Order, type OrderStatus } from '../domain/order.entity.js';
import type { OrderRepositoryPort } from '../domain/order.repository.port.js';
import { InMemoryOrderRepository } from './in-memory-order.repository.js';

interface OrderRow {
  id: string;
  sku: string;
  quantity: number;
  amount: string | number;
  zip_code: string;
  status: OrderStatus;
  cancellation_reason: string | null;
  created_at: Date | string;
  updated_at: Date | string;
}

function mapOrderRow(row: OrderRow): Order {
  return Order.rehydrate({
    id: row.id,
    sku: row.sku,
    quantity: Number(row.quantity),
    amount: Number(row.amount),
    zipCode: row.zip_code,
    status: row.status,
    ...(row.cancellation_reason
      ? { cancellationReason: row.cancellation_reason }
      : {}),
    createdAt:
      row.created_at instanceof Date
        ? row.created_at.toISOString()
        : String(row.created_at),
    updatedAt:
      row.updated_at instanceof Date
        ? row.updated_at.toISOString()
        : String(row.updated_at),
  });
}

@Injectable()
export class PostgresOrderRepository implements OrderRepositoryPort {
  constructor(
    private readonly db: PostgresDatabaseService,
    private readonly fallbackMemory: InMemoryOrderRepository,
  ) {}

  async save(order: Order): Promise<void> {
    if (!this.db.isPostgresActive()) {
      return this.fallbackMemory.save(order);
    }
    const json = order.toJSON();
    const q = this.db.getQueryable();
    await q.query(
      `INSERT INTO orders (
        id, sku, quantity, amount, zip_code, status, cancellation_reason, created_at, updated_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
      ON CONFLICT (id) DO UPDATE SET
        status = EXCLUDED.status,
        cancellation_reason = EXCLUDED.cancellation_reason,
        updated_at = EXCLUDED.updated_at`,
      [
        json.id,
        json.sku,
        json.quantity,
        json.amount,
        json.zipCode,
        json.status,
        json.cancellationReason ?? null,
        json.createdAt,
        json.updatedAt,
      ],
    );
  }

  async findById(orderId: string): Promise<Order | null> {
    if (!this.db.isPostgresActive()) {
      return this.fallbackMemory.findById(orderId);
    }
    const q = this.db.getQueryable();
    const res = await q.query<OrderRow>(
      `SELECT * FROM orders WHERE id = $1`,
      [orderId],
    );
    const row = res.rows[0];
    return row ? mapOrderRow(row) : null;
  }

  async findAll(): Promise<Order[]> {
    if (!this.db.isPostgresActive()) {
      return this.fallbackMemory.findAll();
    }
    const q = this.db.getQueryable();
    const res = await q.query<OrderRow>(
      `SELECT * FROM orders ORDER BY created_at ASC`,
    );
    return res.rows.map(mapOrderRow);
  }
}
