import type { DomainError } from '../core/domain-error.js';
import type { Result } from '../core/result.js';
import type { InboxRepositoryPort } from './inbox.port.js';
import type { OutboxRepositoryPort } from './outbox.port.js';

export interface TransactionalContext {
  readonly inbox: InboxRepositoryPort;
  readonly outbox: OutboxRepositoryPort;
}

export const UNIT_OF_WORK_PORT = Symbol('UNIT_OF_WORK_PORT');

export interface UnitOfWorkPort {
  execute<T, E = DomainError>(
    work: (ctx: TransactionalContext) => Promise<Result<T, E>>,
  ): Promise<Result<T, E>>;
}
