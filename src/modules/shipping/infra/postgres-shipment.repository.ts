import { Injectable } from '@nestjs/common';
import { PostgresDatabaseService } from '../../../infra/database/postgres-client.provider.js';
import { Shipment, type ShipmentStatus } from '../domain/shipment.entity.js';
import type { ShipmentRepositoryPort } from '../domain/shipment.repository.port.js';
import { InMemoryShipmentRepository } from './in-memory-shipment.repository.js';

interface ShipmentRow {
  id: string;
  order_id: string;
  zip_code: string;
  tracking_code: string | null;
  status: ShipmentStatus;
  failure_reason: string | null;
  created_at: Date | string;
}

function mapShipmentRow(row: ShipmentRow): Shipment {
  return Shipment.rehydrate({
    id: row.id,
    orderId: row.order_id,
    zipCode: row.zip_code,
    ...(row.tracking_code ? { trackingCode: row.tracking_code } : {}),
    status: row.status,
    ...(row.failure_reason ? { failureReason: row.failure_reason } : {}),
    createdAt:
      row.created_at instanceof Date
        ? row.created_at.toISOString()
        : String(row.created_at),
  });
}

@Injectable()
export class PostgresShipmentRepository implements ShipmentRepositoryPort {
  constructor(
    private readonly db: PostgresDatabaseService,
    private readonly fallbackMemory: InMemoryShipmentRepository,
  ) {}

  async save(shipment: Shipment): Promise<void> {
    if (!this.db.isPostgresActive()) {
      return this.fallbackMemory.save(shipment);
    }
    const json = shipment.toJSON();
    const q = this.db.getQueryable();
    await q.query(
      `INSERT INTO shipments (id, order_id, zip_code, tracking_code, status, failure_reason, created_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7)
       ON CONFLICT (order_id) DO UPDATE SET
         tracking_code = EXCLUDED.tracking_code,
         status = EXCLUDED.status,
         failure_reason = EXCLUDED.failure_reason`,
      [
        json.id,
        json.orderId,
        json.zipCode,
        json.trackingCode ?? null,
        json.status,
        json.failureReason ?? null,
        json.createdAt,
      ],
    );
  }

  async findByOrderId(orderId: string): Promise<Shipment | null> {
    if (!this.db.isPostgresActive()) {
      return this.fallbackMemory.findByOrderId(orderId);
    }
    const q = this.db.getQueryable();
    const res = await q.query<ShipmentRow>(
      `SELECT * FROM shipments WHERE order_id = $1 FOR UPDATE`,
      [orderId],
    );
    const row = res.rows[0];
    return row ? mapShipmentRow(row) : null;
  }

  async findAll(): Promise<Shipment[]> {
    if (!this.db.isPostgresActive()) {
      return this.fallbackMemory.findAll();
    }
    const q = this.db.getQueryable();
    const res = await q.query<ShipmentRow>(
      `SELECT * FROM shipments ORDER BY created_at ASC`,
    );
    return res.rows.map(mapShipmentRow);
  }
}
