import { Injectable } from '@nestjs/common';
import type { DomainError } from '../core/domain-error.js';
import type { Result } from '../core/result.js';
import type { InboxMessage, InboxRepositoryPort } from './inbox.port.js';
import type { OutboxMessage, OutboxRepositoryPort } from './outbox.port.js';
import type {
  TransactionalContext,
  UnitOfWorkPort,
} from './unit-of-work.port.js';

@Injectable()
export class InMemoryInboxRepository implements InboxRepositoryPort {
  private readonly items = new Map<string, InboxMessage>();

  async findByIdempotencyKey(
    idempotencyKey: string,
  ): Promise<InboxMessage | null> {
    return this.items.get(idempotencyKey) ?? null;
  }

  async save(message: InboxMessage): Promise<void> {
    this.items.set(message.idempotencyKey, message);
  }

  async findByOrderId(orderId: string): Promise<InboxMessage[]> {
    return Array.from(this.items.values()).filter(
      (msg) => msg.orderId === orderId,
    );
  }

  async findAll(): Promise<InboxMessage[]> {
    return Array.from(this.items.values());
  }

  snapshot(): Map<string, InboxMessage> {
    return new Map(this.items);
  }

  restore(snap: Map<string, InboxMessage>): void {
    this.items.clear();
    for (const [k, v] of snap.entries()) {
      this.items.set(k, v);
    }
  }

  clear(): void {
    this.items.clear();
  }
}

@Injectable()
export class InMemoryOutboxRepository implements OutboxRepositoryPort {
  private readonly items = new Map<string, OutboxMessage>();

  async save(message: OutboxMessage): Promise<void> {
    this.items.set(message.id, message);
  }

  async findPending(): Promise<OutboxMessage[]> {
    return Array.from(this.items.values()).filter(
      (msg) => msg.status === 'PENDING',
    );
  }

  async markPublished(
    id: string,
    metadata?: { workflowInstanceId?: string },
  ): Promise<void> {
    const current = this.items.get(id);
    if (!current) {
      return;
    }
    this.items.set(id, {
      ...current,
      status: 'PUBLISHED',
      publishedAt: new Date().toISOString(),
      ...(metadata?.workflowInstanceId
        ? { workflowInstanceId: metadata.workflowInstanceId }
        : {}),
    });
  }

  async markFailed(id: string): Promise<void> {
    const current = this.items.get(id);
    if (!current) {
      return;
    }
    this.items.set(id, {
      ...current,
      status: 'FAILED',
    });
  }

  async findByOrderId(orderId: string): Promise<OutboxMessage[]> {
    return Array.from(this.items.values()).filter(
      (msg) => msg.orderId === orderId,
    );
  }

  async findAll(): Promise<OutboxMessage[]> {
    return Array.from(this.items.values());
  }

  snapshot(): Map<string, OutboxMessage> {
    return new Map(this.items);
  }

  restore(snap: Map<string, OutboxMessage>): void {
    this.items.clear();
    for (const [k, v] of snap.entries()) {
      this.items.set(k, v);
    }
  }

  clear(): void {
    this.items.clear();
  }
}

@Injectable()
export class InMemoryUnitOfWork implements UnitOfWorkPort {
  constructor(
    private readonly inboxRepository: InMemoryInboxRepository,
    private readonly outboxRepository: InMemoryOutboxRepository,
  ) {}

  async execute<T, E = DomainError>(
    work: (ctx: TransactionalContext) => Promise<Result<T, E>>,
  ): Promise<Result<T, E>> {
    const inboxSnap = this.inboxRepository.snapshot();
    const outboxSnap = this.outboxRepository.snapshot();
    try {
      return await work({
        inbox: this.inboxRepository,
        outbox: this.outboxRepository,
      });
    } catch (unexpectedError) {
      this.inboxRepository.restore(inboxSnap);
      this.outboxRepository.restore(outboxSnap);
      throw unexpectedError;
    }
  }
}
