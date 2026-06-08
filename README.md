# hyperiondb-client

Native Node.js client for a **HyperionDb / pg_replica** Postgres cluster: a primary-following
connection pool over the N nodes, written in Rust with [napi-rs](https://napi.rs) +
`tokio-postgres`. Routing, primary detection, pooling and failover recovery live in Rust — JS
never loops over connections.

Status: **in production**

## Install

```bash
npm install hyperiondb-client
```

Prebuilt binaries ship as per-platform optional dependencies
(`hyperiondb-client-<triple>`) for Windows x64, macOS x64/arm64, and Linux x64/arm64
(`gnu` and `musl`); npm installs only the one matching your platform. No Rust toolchain or
build step is needed to consume the package.

## Build (from source)

```bash
npm install
npm run build:debug # debug; or `npm run build` for release
```

`napi build` emits a platform-tagged `hyperiondb-client.<triple>.node` plus the generated
native binding `index.js`/`index.d.ts`. The package entry point is the hand-written
`client.js` (a thin ergonomic layer over the native binding) with hand-checked types in
`client.d.ts`.

## Smoke test (after build)

```js
const { hello } = require('./client.js')
console.log(hello('world')) // hello world from hyperiondb-client
```

## Usage

```js
const { createPool } = require('hyperiondb-client')

const pool = createPool({
  hosts: ['10.0.0.1', '10.0.0.2', '10.0.0.3'],
  port: 5432,
  user: 'app',
  password: '…',
  database: 'weido',
  mode: 'read-write',       // 'read-write' (default) | 'read-only' | 'prefer-standby' | 'any'
  poolSize: 10,
  connectTimeoutMs: 2000,   // per-connection TCP/handshake timeout
  acquireTimeoutMs: 5000,   // how long query() retries for a writable primary before throwing
  statementTimeoutMs: 30000,// server-enforced statement_timeout on every connection (optional)
  logger: (e) => console.log(e.sql, e.durationMs, e.rowCount ?? e.error?.code), // optional
})

const rows = await pool.query('select id, label from t where id = $1', [42])

await pool.end()
```

A `read-write` pool opens every connection with `target_session_attrs=read-write`, so it
resolves the current primary at connect time and follows a failover on reconnect — no proxy
and no application awareness of which node is primary. `query` returns rows as plain objects
keyed by column name; `end` drains the pool.

### Failover behavior

pg_replica fences a demoted primary **read-only without dropping its sessions**
(`default_transaction_read_only=on`). A `read-write` pool guards against that with a
checkout validation: before reusing a pooled connection it runs `SHOW transaction_read_only`
and evicts the connection if it is `on`, then opens a fresh one that re-resolves the new
primary. When no writable primary is reachable yet (mid-election), `query` retries with
backoff up to `acquireTimeoutMs` and then throws `no writable primary available after <ms>ms`.

### Read scaling

`mode: 'read-only'` routes strictly to standbys (`target_session_attrs=read-only`, random
host load-balancing) and fails if none is reachable. `'prefer-standby'` connects standby-first
and only falls back to the primary when no standby is reachable: each pooled connection first
attempts a `target_session_attrs=read-only` connect (standbys only) and, if that finds no host,
retries with `target_session_attrs=any`. `'any'` uses `target_session_attrs=any` with random
host load-balancing and does not favor standbys. (tokio-postgres 0.7.x has no server-side
`prefer-standby` attribute, so the fallback is done client-side over two connect attempts.)

## Transactions

```js
// Auto BEGIN / COMMIT, ROLLBACK on throw:
const id = await pool.transaction(async (tx) => {
  await tx.query('insert into t (x) values ($1)', [1])
  const rows = await tx.query('update t set x = x + 1 where x = $1 returning id', [1])
  return rows[0].id
})

// Or a checked-out connection you drive yourself:
const tx = await pool.begin()
try {
  await tx.query('insert into t (x) values ($1)', [2])
  await tx.commit()
} catch (e) {
  await tx.rollback()
  throw e
}
```

All statements in a transaction run on one dedicated connection. Repeated SQL reuses a
server-side prepared statement (deadpool `prepare_cached`).

## Retries & idempotency

`transaction(cb)` retries the whole callback on serialization/deadlock (`40001`/`40P01`) and
on connection failures that happen *before* `COMMIT`. A failure *during* `COMMIT` is ambiguous
(the write may have landed), so it is surfaced as `error.code === 'IN_DOUBT'` and **never**
auto-retried — handle that one idempotently.

```js
await pool.transaction(async (tx) => { /* ... */ }, { maxAttempts: 5 })
```

Reads can be retried too — on by default for `read-only`/`prefer-standby` pools (every query
is a read); on a `read-write` pool opt a specific read in with `{ retry: true }`:

```js
const rows = await pool.query('select …', [], { retry: true })
```

`insert(table, row, { idempotency: true })` makes a write safe to retry through the in-doubt
window by adding `ON CONFLICT (<conflictTarget>) DO NOTHING` (the conflict key defaults to
`_id`, which is any unique column — `varchar`/`text`, `uuid`, etc.). A re-applied row collapses to a
no-op and returns `[]`:

```js
const requestId = 'evt-2026-06-05-abc123' // any client-supplied unique key
const [order] = await pool.insert('orders', { _id: requestId, amount: 100 }, { idempotency: true })
// a duplicate retry returns []  — the row exists exactly once
```

Retry policy is per-pool: `createPool({ …, retry: { maxAttempts, baseDelayMs, maxDelayMs } })`
(defaults `3 / 50 / 1000`). This is the Postgres-side equivalent of MongoDB's retryable
writes — except the in-doubt commit needs your idempotency key, since Postgres has no built-in
per-statement dedup token.

## Cancellation & timeouts

`query` takes an optional third argument. Both a timeout and an `AbortSignal` cancel the
in-flight query **server-side** via the connection's `cancel_token`:

```js
await pool.query('select pg_sleep(10)', [], { timeoutMs: 2000 })   // throws after ~2s

const ac = new AbortController()
setTimeout(() => ac.abort(), 1000)
await pool.query('select pg_sleep(10)', [], { signal: ac.signal }) // throws on abort
```

## Errors

PostgreSQL errors are thrown as `Error` with the 5-character `SQLSTATE` on `.code`:

```js
try {
  await pool.query('insert into t (id) values (1)') // duplicate
} catch (e) {
  e.code // '23505'
}
```

## Observability

```js
pool.status() // { maxSize, size, available, inUse, waiting }
```

`size` is connections currently managed, `available` are idle, `inUse = size - available`,
and `waiting` is the number of queued checkouts. Pass a `logger` to `createPool` to receive
`{ sql, durationMs, rowCount?, error? }` once per query (errors thrown by the logger are
ignored). Set `statementTimeoutMs` to have the server cancel any statement that runs too long
(raised as a `57014` error).

## Type mapping

| PostgreSQL | JavaScript |
|------------|------------|
| `bool` | `boolean` |
| `int2`, `int4`, `oid` | `number` |
| `int8` / `bigint` | `BigInt` (lossless 64-bit) |
| `float4`, `float8` | `number` |
| `numeric` | `string` (arbitrary precision) |
| `text`, `varchar`, `bpchar`, `name` | `string` |
| `uuid` | `string` |
| `bytea` | `Buffer` |
| `json`, `jsonb` | parsed value (object/array/scalar) |
| `timestamptz`, `timestamp`, `date`, `time` | ISO 8601 `string` |
| any `T[]` | `Array` of the above |

Parameters accept the inverse: `null`, `boolean`, `number`, `BigInt`, `string`, `Buffer`
(→ `bytea`), `Date` (→ `timestamptz`), arrays (→ a Postgres array when the target column is
an array type, otherwise `jsonb`), and plain objects (→ `jsonb`). To write a `string` into a
`numeric`/`uuid` column, cast at the call site (`$1::text::numeric`).

A host entry may carry its own port (`'10.0.0.1:5433'`); otherwise `port` applies to all hosts.

## Testing

Tests use the built-in `node:test` runner and need a running cluster (connection + topology
from `HYPERION_*` env vars; defaults target the local dev cluster).

```bash
npm test         # type round-trips + the read-only-fence eviction path
npm run test:chaos   # stop the primary under write load, assert zero acked-write loss
```

`test:chaos` stops a node via `test/cluster.js` (local pgrx cluster through WSL `pg_ctl` by
default; override with `HYPERION_CTL` for the `docker/` cluster) and requires synchronous
replication for true zero-loss. Run `npm test` against a fresh cluster — the chaos test
relocates the primary.

## Releasing

`.github/workflows/release.yml` runs when a commit pushed to `main` contains `[cd]` in its
message. It cross-builds the seven native targets, bumps the patch version (syncing
`Cargo.toml`), commits the bump back, then publishes the per-platform
`hyperiondb-client-<triple>` packages and the main package to npm. The bump commit has no
`[cd]`, so it doesn't re-trigger a build/publish. Requires an `NPM_TOKEN` repository secret
with publish rights; the version bump is pushed with the workflow's `GITHUB_TOKEN` (allow it
in branch protection, or swap in a PAT).
