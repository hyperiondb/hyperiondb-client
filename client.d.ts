export type Mode = 'read-write' | 'read-only' | 'prefer-standby' | 'any'

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
  applicationName?: string
  /** Called once per query with timing and outcome. Errors thrown here are ignored. */
  logger?: (event: QueryEvent) => void
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
}

/** A PostgreSQL `Error` carries the 5-character SQLSTATE on `.code`. */
export interface DbError extends Error {
  code?: string
}

export interface Transaction {
  query<T = Row>(sql: string, params?: Param[], opts?: QueryOptions): Promise<T[]>
  commit(): Promise<void>
  rollback(): Promise<void>
}

export interface Pool {
  query<T = Row>(sql: string, params?: Param[], opts?: QueryOptions): Promise<T[]>
  /** Begin a transaction on a dedicated connection. Remember to `commit()` or `rollback()`. */
  begin(): Promise<Transaction>
  /** Run `fn` inside a transaction, auto `COMMIT` on resolve and `ROLLBACK` on throw. */
  transaction<T>(fn: (tx: Transaction) => Promise<T>): Promise<T>
  /** Live pool counters: size, idle, in-use, and waiters. */
  status(): PoolStatus
  /** Drain and close the pool. */
  end(): Promise<void>
}

export function createPool(options: PoolOptions): Pool

export function hello(name: string): string
