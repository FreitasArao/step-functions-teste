export interface DomainError {
  readonly code: string;
  readonly message: string;
  readonly retryable?: boolean;
  readonly metadata?: Record<string, unknown>;
}
