import { Injectable } from '@nestjs/common';
import type { DomainError } from '../../shared/core/domain-error.js';
import type { Result } from '../../shared/core/result.js';
import {
  InMemoryInboxRepository,
  InMemoryOutboxRepository,
} from '../../shared/messaging/in-memory-messaging.store.js';
import type {
  InboxMessage,
  InboxRepositoryPort,
} from '../../shared/messaging/inbox.port.js';
import type {
  MessageWrapper,
  SagaStepName,
} from '../../shared/messaging/message-wrapper.js';
import type {
  OutboxEventType,
  OutboxMessage,
  OutboxRepositoryPort,
  OutboxStatus,
} from '../../shared/messaging/outbox.port.js';
import type {
  TransactionalContext,
  UnitOfWorkPort,
} from '../../shared/messaging/unit-of-work.port.js';
import { PostgresDatabaseService } from './postgres-client.provider.js';

interface InboxRow {
  idempotency_key: string;
  order_id: string;
  step_name: SagaStepName;
  outcome: 'OK' | 'ERR';
  response_payload: unknown;
  wrapper: MessageWrapper<unknown>;
  processed_at: Date | string;
}

interface OutboxRow {
  id: string;
  order_id: string;
  event_type: OutboxEventType;
  status: OutboxStatus;
  task_token: string | null;
  error_details: DomainError | null;
  wrapper: MessageWrapper<unknown>;
  execution_arn: string | null;
  created_at: Date | string;
  published_at: Date | string | null;
}

function mapInboxRow(row: InboxRow): InboxMessage {
  return {
    idempotencyKey: row.idempotency_key,
    orderId: row.order_id,
    stepName: row.step_name,
    outcome: row.outcome,
    responsePayload: row.response_payload,
    wrapper: row.wrapper,
    processedAt:
      row.processed_at instanceof Date
        ? row.processed_at.toISOString()
        : String(row.processed_at),
  };
}

function mapOutboxRow(row: OutboxRow): OutboxMessage {
  return {
    id: row.id,
    orderId: row.order_id,
    eventType: row.event_type,
    status: row.status,
    wrapper: row.wrapper,
    ...(row.task_token ? { taskToken: row.task_token } : {}),
    ...(row.error_details ? { errorDetails: row.error_details } : {}),
    ...(row.execution_arn ? { executionArn: row.execution_arn } : {}),
    createdAt:
      row.created_at instanceof Date
        ? row.created_at.toISOString()
        : String(row.created_at),
    ...(row.published_at
      ? {
          publishedAt:
            row.published_at instanceof Date
              ? row.published_at.toISOString()
              : String(row.published_at),
        }
      : {}),
  };
}

@Injectable()
export class PostgresInboxRepository implements InboxRepositoryPort {
  constructor(
    private readonly db: PostgresDatabaseService,
    private readonly fallbackMemory: InMemoryInboxRepository,
  ) {}

  async findByIdempotencyKey(
    idempotencyKey: string,
  ): Promise<InboxMessage | null> {
    if (!this.db.isPostgresActive()) {
      return this.fallbackMemory.findByIdempotencyKey(idempotencyKey);
    }
    const q = this.db.getQueryable();
    const res = await q.query<InboxRow>(
      `SELECT * FROM inbox_messages WHERE idempotency_key = $1 FOR UPDATE`,
      [idempotencyKey],
    );
    const row = res.rows[0];
    return row ? mapInboxRow(row) : null;
  }

  async save(message: InboxMessage): Promise<void> {
    if (!this.db.isPostgresActive()) {
      return this.fallbackMemory.save(message);
    }
    const q = this.db.getQueryable();
    await q.query(
      `INSERT INTO inbox_messages (
        idempotency_key,
        order_id,
        step_name,
        outcome,
        response_payload,
        wrapper,
        processed_at
      ) VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb, $7)`,
      [
        message.idempotencyKey,
        message.orderId,
        message.stepName,
        message.outcome,
        JSON.stringify(message.responsePayload ?? {}),
        JSON.stringify(message.wrapper),
        message.processedAt,
      ],
    );
  }

  async findByOrderId(orderId: string): Promise<InboxMessage[]> {
    if (!this.db.isPostgresActive()) {
      return this.fallbackMemory.findByOrderId(orderId);
    }
    const q = this.db.getQueryable();
    const res = await q.query<InboxRow>(
      `SELECT * FROM inbox_messages WHERE order_id = $1 ORDER BY processed_at ASC`,
      [orderId],
    );
    return res.rows.map(mapInboxRow);
  }

  async findAll(): Promise<InboxMessage[]> {
    if (!this.db.isPostgresActive()) {
      return this.fallbackMemory.findAll();
    }
    const q = this.db.getQueryable();
    const res = await q.query<InboxRow>(
      `SELECT * FROM inbox_messages ORDER BY processed_at ASC`,
    );
    return res.rows.map(mapInboxRow);
  }
}

@Injectable()
export class PostgresOutboxRepository implements OutboxRepositoryPort {
  constructor(
    private readonly db: PostgresDatabaseService,
    private readonly fallbackMemory: InMemoryOutboxRepository,
  ) {}

  async save(message: OutboxMessage): Promise<void> {
    if (!this.db.isPostgresActive()) {
      return this.fallbackMemory.save(message);
    }
    const q = this.db.getQueryable();
    await q.query(
      `INSERT INTO outbox_messages (
        id,
        order_id,
        event_type,
        status,
        task_token,
        error_details,
        wrapper,
        execution_arn,
        created_at,
        published_at
      ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8, $9, $10)`,
      [
        message.id,
        message.orderId,
        message.eventType,
        message.status,
        message.taskToken ?? null,
        message.errorDetails ? JSON.stringify(message.errorDetails) : null,
        JSON.stringify(message.wrapper),
        message.executionArn ?? null,
        message.createdAt,
        message.publishedAt ?? null,
      ],
    );
  }

  async findPending(): Promise<OutboxMessage[]> {
    if (!this.db.isPostgresActive()) {
      return this.fallbackMemory.findPending();
    }
    const q = this.db.getQueryable();
    const res = await q.query<OutboxRow>(
      `SELECT * FROM outbox_messages
       WHERE status = 'PENDING'
       ORDER BY created_at ASC
       LIMIT 20
       FOR UPDATE SKIP LOCKED`,
    );
    return res.rows.map(mapOutboxRow);
  }

  async markPublished(
    id: string,
    metadata?: { executionArn?: string },
  ): Promise<void> {
    if (!this.db.isPostgresActive()) {
      return this.fallbackMemory.markPublished(id, metadata);
    }
    const q = this.db.getQueryable();
    await q.query(
      `UPDATE outbox_messages
       SET status = 'PUBLISHED',
           published_at = NOW(),
           execution_arn = COALESCE($2, execution_arn)
       WHERE id = $1`,
      [id, metadata?.executionArn ?? null],
    );
  }

  async markFailed(id: string): Promise<void> {
    if (!this.db.isPostgresActive()) {
      return this.fallbackMemory.markFailed(id);
    }
    const q = this.db.getQueryable();
    await q.query(
      `UPDATE outbox_messages SET status = 'FAILED' WHERE id = $1`,
      [id],
    );
  }

  async findByOrderId(orderId: string): Promise<OutboxMessage[]> {
    if (!this.db.isPostgresActive()) {
      return this.fallbackMemory.findByOrderId(orderId);
    }
    const q = this.db.getQueryable();
    const res = await q.query<OutboxRow>(
      `SELECT * FROM outbox_messages WHERE order_id = $1 ORDER BY created_at ASC`,
      [orderId],
    );
    return res.rows.map(mapOutboxRow);
  }

  async findAll(): Promise<OutboxMessage[]> {
    if (!this.db.isPostgresActive()) {
      return this.fallbackMemory.findAll();
    }
    const q = this.db.getQueryable();
    const res = await q.query<OutboxRow>(
      `SELECT * FROM outbox_messages ORDER BY created_at ASC`,
    );
    return res.rows.map(mapOutboxRow);
  }
}

@Injectable()
export class PostgresUnitOfWork implements UnitOfWorkPort {
  constructor(
    private readonly db: PostgresDatabaseService,
    private readonly inboxRepo: PostgresInboxRepository,
    private readonly outboxRepo: PostgresOutboxRepository,
    private readonly fallbackInboxMemory: InMemoryInboxRepository,
    private readonly fallbackOutboxMemory: InMemoryOutboxRepository,
  ) {}

  async execute<T, E = DomainError>(
    work: (ctx: TransactionalContext) => Promise<Result<T, E>>,
  ): Promise<Result<T, E>> {
    if (this.db.isPostgresActive()) {
      return this.db.runInTransaction(async () => {
        return work({
          inbox: this.inboxRepo,
          outbox: this.outboxRepo,
        });
      });
    }

    const inboxSnap = this.fallbackInboxMemory.snapshot();
    const outboxSnap = this.fallbackOutboxMemory.snapshot();
    try {
      return await work({
        inbox: this.inboxRepo,
        outbox: this.outboxRepo,
      });
    } catch (unexpectedError) {
      this.fallbackInboxMemory.restore(inboxSnap);
      this.fallbackOutboxMemory.restore(outboxSnap);
      throw unexpectedError;
    }
  }
}
