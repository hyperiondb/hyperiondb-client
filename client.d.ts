export type Mode = 'read-write' | 'read-only' | 'prefer-standby' | 'any'

export type IsolationLevel = 'read-committed' | 'repeatable-read' | 'serializable'

export interface PoolOptions {
  /** One entry per cluster node. The pool races them per `mode`. */
  hosts: string[]
  /** Applied to every host. Defaults to 5432. */
  port?: number
  user: string
  password?: string
  database: string
  /** Routing. Defaults to `'read-write'` (follow the primary). */
  mode?: Mode
  /** Max pooled connections. Defaults to 10. */
  poolSize?: number
  /** Per-connection TCP/handshake timeout. */
  connectTimeoutMs?: number
  /** How long `query`/`begin` retry for a writable primary before throwing. Defaults to 5000. */
  acquireTimeoutMs?: number
  /** Server-enforced `statement_timeout` (ms) set on every pooled connection. */
  statementTimeoutMs?: number
  validationIntervalMs?: number
  applicationName?: string
  /** Called once per query with timing and outcome. Errors thrown here are ignored. */
  logger?: (event: QueryEvent) => void
  /** Retry policy for retryable reads, `transaction(cb)`, and idempotent `insert`. */
  retry?: RetryOptions
}

export interface RetryOptions {
  /** Max total attempts (1 = no retry). Defaults to 3. */
  maxAttempts?: number
  /** First backoff delay in ms (doubles per attempt, jittered). Defaults to 50. */
  baseDelayMs?: number
  /** Backoff cap in ms. Defaults to 1000. */
  maxDelayMs?: number
}

export interface QueryEvent {
  sql: string
  durationMs: number
  /** Present on success. */
  rowCount?: number
  /** Present on failure (carries `.code` for DB errors). */
  error?: DbError
}

export interface PoolStatus {
  maxSize: number
  size: number
  available: number
  inUse: number
  waiting: number
}

/** A result row: column name → decoded value. */
export type Row = Record<string, unknown>

/** A value accepted as a bound query parameter. */
export type Param =
  | null
  | boolean
  | number
  | bigint
  | string
  | Uint8Array
  | Date
  | Param[]
  | { [key: string]: unknown }

export interface QueryOptions {
  /** Cancel the in-flight query server-side after this many milliseconds. */
  timeoutMs?: number
  /** Abort the query (server-side cancel) when the signal fires. */
  signal?: AbortSignal
  /**
   * Retry on transient errors (serialization/deadlock, connection loss). Only safe for
   * reads or idempotent statements. Defaults to true on `read-only`/`prefer-standby` pools,
   * false otherwise — set explicitly to opt a specific query in or out.
   */
  retry?: boolean
}

export interface InsertOptions {
  /** Add `ON CONFLICT (<conflictTarget>) DO NOTHING`, making the insert safely retryable. */
  idempotency?: boolean
  /** Conflict key for idempotent inserts. Defaults to `'_id'`. */
  conflictTarget?: string | string[]
  timeoutMs?: number
  signal?: AbortSignal
}

export interface TransactionOptions {
  /** Max total attempts for the retry loop. Defaults to the pool's `retry.maxAttempts`. */
  maxAttempts?: number
  isolation?: IsolationLevel
}

export interface BeginOptions {
  isolation?: IsolationLevel
}

/**
 * A PostgreSQL `Error` carries the 5-character SQLSTATE on `.code`. A `transaction(cb)`
 * whose `COMMIT` is lost to a connection failure (outcome unknown) throws with
 * `code === 'IN_DOUBT'` and is never auto-retried.
 */
export interface DbError extends Error {
  code?: string
  cause?: unknown
}

export interface Transaction {
  query<T = Row>(sql: string, params?: Param[], opts?: QueryOptions): Promise<T[]>
  commit(): Promise<void>
  rollback(): Promise<void>
}

export interface Pool {
  query<T = Row>(sql: string, params?: Param[], opts?: QueryOptions): Promise<T[]>
  /**
   * Insert `row` into `table`, returning the inserted row(s). With `idempotency: true` a
   * re-applied row collapses to a no-op (`ON CONFLICT (<conflictTarget>) DO NOTHING`),
   * making the write safe to retry through the in-doubt window — a duplicate returns `[]`.
   */
  insert<T = Row>(table: string, row: Record<string, Param>, opts?: InsertOptions): Promise<T[]>
  /** Begin a transaction on a dedicated connection. Remember to `commit()` or `rollback()`. */
  begin(opts?: BeginOptions): Promise<Transaction>
  /**
   * Run `fn` inside a transaction, auto `COMMIT` on resolve and `ROLLBACK` on throw, retrying
   * the whole callback on serialization/deadlock and pre-commit connection failures. A failure
   * during `COMMIT` is surfaced as a `code === 'IN_DOUBT'` error and never auto-retried.
   */
  transaction<T>(fn: (tx: Transaction) => Promise<T>, opts?: TransactionOptions): Promise<T>
  /** Live pool counters: size, idle, in-use, and waiters. */
  status(): PoolStatus
  /** Drain and close the pool. */
  end(): Promise<void>
}

export function createPool(options: PoolOptions): Pool

export function hello(name: string): string
