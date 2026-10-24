import type { DomainError } from './domain-error.js';

export class Ok<T, E = DomainError> {
  readonly _tag = 'Ok' as const;

  constructor(readonly value: T) {
    Object.freeze(this);
  }

  isOk(): this is Ok<T, E> {
    return true;
  }

  isErr(): this is Err<T, E> {
    return false;
  }

  map<U>(fn: (value: T) => U): Result<U, E> {
    return Result.ok<U, E>(fn(this.value));
  }

  flatMap<U>(fn: (value: T) => Result<U, E>): Result<U, E> {
    return fn(this.value);
  }
}

export class Err<T = never, E = DomainError> {
  readonly _tag = 'Err' as const;

  constructor(readonly error: E) {
    Object.freeze(this);
  }

  isOk(): this is Ok<T, E> {
    return false;
  }

  isErr(): this is Err<T, E> {
    return true;
  }

  map<U>(_fn: (value: T) => U): Result<U, E> {
    return Result.err<E, U>(this.error);
  }

  flatMap<U>(_fn: (value: T) => Result<U, E>): Result<U, E> {
    return Result.err<E, U>(this.error);
  }
}

export type Result<T, E = DomainError> = Ok<T, E> | Err<T, E>;

export const Result = {
  ok<T, E = DomainError>(value: T): Result<T, E> {
    return new Ok<T, E>(value);
  },

  err<E = DomainError, T = never>(error: E): Result<T, E> {
    return new Err<T, E>(error);
  },
};
