import {
  Injectable,
  Logger,
  type OnModuleDestroy,
  type OnModuleInit,
} from '@nestjs/common';
import { AsyncLocalStorage } from 'node:async_hooks';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import pg from 'pg';

const { Pool } = pg;

export type PgQueryable = pg.Pool | pg.PoolClient;

@Injectable()
export class PostgresDatabaseService
  implements OnModuleInit, OnModuleDestroy
{
  private readonly logger = new Logger(PostgresDatabaseService.name);
  private readonly pool: pg.Pool;
  private readonly txStorage = new AsyncLocalStorage<pg.PoolClient>();
  private postgresAvailable = false;

  constructor() {
    const connectionString =
      process.env.DATABASE_URL ??
      'postgresql://saga_user:saga_password@localhost:5432/order_saga_db';
    this.pool = new Pool({
      connectionString,
      max: 10,
      connectionTimeoutMillis: 1500,
    });
  }

  async onModuleInit(): Promise<void> {
    if (process.env.DB_DRIVER === 'memory') {
      this.postgresAvailable = false;
      return;
    }
    await this.checkAndInitializeSchema();
  }

  async onModuleDestroy(): Promise<void> {
    await this.pool.end().catch(() => undefined);
  }

  async checkAndInitializeSchema(): Promise<boolean> {
    if (process.env.DB_DRIVER === 'memory') {
      this.postgresAvailable = false;
      return false;
    }
    try {
      const client = await this.pool.connect();
      try {
        const initSqlPath = join(
          process.cwd(),
          'infra',
          'postgres',
          'init.sql',
        );
        const sql = readFileSync(initSqlPath, 'utf8');
        await client.query(sql);
        this.postgresAvailable = true;
        this.logger.log(
          'Connected to PostgreSQL and verified Transactional Outbox/Inbox schema',
        );
        return true;
      } finally {
        client.release();
      }
    } catch {
      this.postgresAvailable = false;
      return false;
    }
  }

  isPostgresActive(): boolean {
    if (process.env.DB_DRIVER === 'memory') {
      return false;
    }
    return this.postgresAvailable;
  }

  getQueryable(): PgQueryable {
    return this.txStorage.getStore() ?? this.pool;
  }

  async runInTransaction<T>(
    work: (client: pg.PoolClient) => Promise<T>,
  ): Promise<T> {
    const existingClient = this.txStorage.getStore();
    if (existingClient) {
      return work(existingClient);
    }

    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const result = await this.txStorage.run(client, () => work(client));
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  async resetAllTables(): Promise<void> {
    if (!this.isPostgresActive()) {
      return;
    }
    await this.runInTransaction(async (client) => {
      await client.query(`
        TRUNCATE TABLE
          orders,
          stock_reservations,
          payments,
          shipments,
          inbox_messages,
          outbox_messages
        RESTART IDENTITY CASCADE;
      `);
      await client.query(`
        DELETE FROM stock_items;
        INSERT INTO stock_items (sku, name, available_quantity, reserved_quantity)
        VALUES
          ('SKU-100', 'Mechanical Keyboard Pro', 10, 0),
          ('SKU-200', 'Ultrawide Monitor 34"', 5, 0);
      `);
    });
  }
}
