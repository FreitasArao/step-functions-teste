import { Injectable } from '@nestjs/common';
import { PostgresDatabaseService } from '../../../infra/database/postgres-client.provider.js';
import { Payment, type PaymentStatus } from '../domain/payment.entity.js';
import type { PaymentRepositoryPort } from '../domain/payment.repository.port.js';
import { InMemoryPaymentRepository } from './in-memory-payment.repository.js';

interface PaymentRow {
  id: string;
  order_id: string;
  amount: string | number;
  status: PaymentStatus;
  reason: string | null;
  processed_at: Date | string;
  refunded_at: Date | string | null;
}

function mapPaymentRow(row: PaymentRow): Payment {
  return Payment.rehydrate({
    id: row.id,
    orderId: row.order_id,
    amount: Number(row.amount),
    status: row.status,
    ...(row.reason ? { reason: row.reason } : {}),
    processedAt:
      row.processed_at instanceof Date
        ? row.processed_at.toISOString()
        : String(row.processed_at),
    ...(row.refunded_at
      ? {
          refundedAt:
            row.refunded_at instanceof Date
              ? row.refunded_at.toISOString()
              : String(row.refunded_at),
        }
      : {}),
  });
}

@Injectable()
export class PostgresPaymentRepository implements PaymentRepositoryPort {
  constructor(
    private readonly db: PostgresDatabaseService,
    private readonly fallbackMemory: InMemoryPaymentRepository,
  ) {}

  async save(payment: Payment): Promise<void> {
    if (!this.db.isPostgresActive()) {
      return this.fallbackMemory.save(payment);
    }
    const json = payment.toJSON();
    const q = this.db.getQueryable();
    await q.query(
      `INSERT INTO payments (id, order_id, amount, status, reason, processed_at, refunded_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7)
       ON CONFLICT (order_id) DO UPDATE SET
         status = EXCLUDED.status,
         reason = EXCLUDED.reason,
         refunded_at = EXCLUDED.refunded_at`,
      [
        json.id,
        json.orderId,
        json.amount,
        json.status,
        json.reason ?? null,
        json.processedAt,
        json.refundedAt ?? null,
      ],
    );
  }

  async findByOrderId(orderId: string): Promise<Payment | null> {
    if (!this.db.isPostgresActive()) {
      return this.fallbackMemory.findByOrderId(orderId);
    }
    const q = this.db.getQueryable();
    const res = await q.query<PaymentRow>(
      `SELECT * FROM payments WHERE order_id = $1 FOR UPDATE`,
      [orderId],
    );
    const row = res.rows[0];
    return row ? mapPaymentRow(row) : null;
  }

  async findAll(): Promise<Payment[]> {
    if (!this.db.isPostgresActive()) {
      return this.fallbackMemory.findAll();
    }
    const q = this.db.getQueryable();
    const res = await q.query<PaymentRow>(
      `SELECT * FROM payments ORDER BY processed_at ASC`,
    );
    return res.rows.map(mapPaymentRow);
  }
}
