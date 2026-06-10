'use strict'

const native = require('./index.js')

const SQLSTATE = /^\[SQLSTATE ([0-9A-Za-z]{5})\] ([\s\S]*)$/

const SERIALIZATION = new Set(['40001', '40P01'])
const CONNECTION_SQLSTATE = new Set([
  '08000', '08003', '08006', '08001', '08004', '08007',
  '57P01', '57P02', '57P03', '53300', '53400',
])
const READ_MODES = new Set(['read-only', 'readonly', 'ro', 'prefer-standby', 'preferstandby'])
const DEFAULT_RETRY = { maxAttempts: 3, baseDelayMs: 50, maxDelayMs: 1000 }

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

function decorate(error) {
  if (error && typeof error.message === 'string') {
    const match = SQLSTATE.exec(error.message)
    if (match) {
      error.code = match[1]
      error.message = match[2]
    }
  }
  return error
}

function isSerialization(error) {
  return !!error && SERIALIZATION.has(error.code)
}

function isConnectionError(error) {
  return !!error && !!error.code && CONNECTION_SQLSTATE.has(error.code)
}

function isReadOnly(error) {
  return !!error && error.code === '25006'
}

function isRetryable(error) {
  return isSerialization(error) || isConnectionError(error) || isReadOnly(error)
}

function inDoubt(cause) {
  const error = new Error(`transaction commit outcome unknown (in doubt): ${cause.message}`)
  error.code = 'IN_DOUBT'
  error.cause = cause
  return error
}

function backoff(attempt, cfg) {
  const base = Math.min(cfg.baseDelayMs * 2 ** (attempt - 1), cfg.maxDelayMs)
  return Math.round(base * (0.5 + Math.random() * 0.5))
}

function quoteIdent(name) {
  return '"' + String(name).replace(/"/g, '""') + '"'
}

async function guard(run) {
  try {
    return await run()
  } catch (error) {
    throw decorate(error)
  }
}

function emit(logger, event) {
  if (logger) {
    try {
      logger(event)
    } catch {}
  }
}

async function runLogged(logger, sql, run) {
  const start = Date.now()
  try {
    const rows = await run()
    emit(logger, { sql, durationMs: Date.now() - start, rowCount: rows.length })
    return rows
  } catch (error) {
    const decorated = decorate(error)
    emit(logger, { sql, durationMs: Date.now() - start, error: decorated })
    throw decorated
  }
}

function checkAborted(opts) {
  if (opts && opts.signal && opts.signal.aborted) {
    const error = new Error('query: aborted by signal')
    error.code = 'ABORT_ERR'
    throw error
  }
}

class Transaction {
  constructor(inner, logger) {
    this._inner = inner
    this._logger = logger
  }

  async query(sql, params, opts) {
    checkAborted(opts)
    return runLogged(this._logger, sql, () =>
      this._inner.query(sql, params ?? null, opts?.timeoutMs ?? null, opts?.signal ?? null),
    )
  }

  commit() {
    return guard(() => this._inner.commit())
  }

  rollback() {
    return guard(() => this._inner.rollback())
  }
}

class Pool {
  constructor(options) {
    const { logger, retry, ...nativeOptions } = options
    this._logger = logger
    this._retry = { ...DEFAULT_RETRY, ...(retry || {}) }
    this._retryReads = READ_MODES.has(options.mode)
    this._inner = native.createPool(nativeOptions)
  }

  async query(sql, params, opts) {
    checkAborted(opts)
    const retry = opts?.retry ?? this._retryReads
    const max = retry ? this._retry.maxAttempts : 1
    for (let attempt = 1; ; attempt += 1) {
      try {
        return await runLogged(this._logger, sql, () =>
          this._inner.query(sql, params ?? null, opts?.timeoutMs ?? null, opts?.signal ?? null),
        )
      } catch (error) {
        if (retry && isRetryable(error) && attempt < max && !opts?.signal?.aborted) {
          await sleep(backoff(attempt, this._retry))
          continue
        }
        throw error
      }
    }
  }

  async insert(table, row, opts) {
    checkAborted(opts)
    const columns = Object.keys(row)
    const placeholders = columns.map((_, index) => '$' + (index + 1)).join(', ')
    const params = columns.map((column) => row[column])
    const idempotent = opts?.idempotency === true
    let onConflict = ''
    if (idempotent) {
      const target = opts?.conflictTarget ?? '_id'
      const targets = Array.isArray(target) ? target : [target]
      onConflict = ` ON CONFLICT (${targets.map(quoteIdent).join(', ')}) DO NOTHING`
    }
    const sql = `INSERT INTO ${quoteIdent(table)} (${columns.map(quoteIdent).join(', ')}) ` +
      `VALUES (${placeholders})${onConflict} RETURNING *`

    const max = idempotent ? this._retry.maxAttempts : 1
    for (let attempt = 1; ; attempt += 1) {
      try {
        return await runLogged(this._logger, sql, () =>
          this._inner.query(sql, params, opts?.timeoutMs ?? null, opts?.signal ?? null),
        )
      } catch (error) {
        if (idempotent && isRetryable(error) && attempt < max && !opts?.signal?.aborted) {
          await sleep(backoff(attempt, this._retry))
          continue
        }
        throw error
      }
    }
  }

  async begin(opts) {
    const inner = await guard(() => this._inner.begin(opts?.isolation ?? null))
    return new Transaction(inner, this._logger)
  }

  async transaction(callback, opts) {
    const max = opts?.maxAttempts ?? this._retry.maxAttempts
    for (let attempt = 1; ; attempt += 1) {
      let tx
      try {
        tx = await this.begin(opts)
        const result = await callback(tx)
        try {
          await tx.commit()
        } catch (commitError) {
          if (isSerialization(commitError) && attempt < max) {
            await sleep(backoff(attempt, this._retry))
            continue
          }
          if (isConnectionError(commitError)) throw inDoubt(commitError)
          throw commitError
        }
        return result
      } catch (error) {
        if (error.code === 'IN_DOUBT') throw error
        if (tx) {
          try {
            await tx.rollback()
          } catch {}
        }
        if (isRetryable(error) && attempt < max) {
          await sleep(backoff(attempt, this._retry))
          continue
        }
        throw error
      }
    }
  }

  status() {
    return this._inner.status()
  }

  end() {
    return guard(() => this._inner.end())
  }
}

function createPool(options) {
  return new Pool(options)
}

module.exports = { createPool, hello: native.hello }
