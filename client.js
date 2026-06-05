'use strict'

const native = require('./index.js')

const SQLSTATE = /^\[SQLSTATE ([0-9A-Za-z]{5})\] ([\s\S]*)$/

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
    } catch {
      // a logging hook must never break a query
    }
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
    const { logger, ...nativeOptions } = options
    this._logger = logger
    this._inner = native.createPool(nativeOptions)
  }

  async query(sql, params, opts) {
    checkAborted(opts)
    return runLogged(this._logger, sql, () =>
      this._inner.query(sql, params ?? null, opts?.timeoutMs ?? null, opts?.signal ?? null),
    )
  }

  async begin() {
    const inner = await guard(() => this._inner.begin())
    return new Transaction(inner, this._logger)
  }

  async transaction(callback) {
    const tx = await this.begin()
    try {
      const result = await callback(tx)
      await tx.commit()
      return result
    } catch (error) {
      try {
        await tx.rollback()
      } catch {
        // surface the original error, not a secondary rollback failure
      }
      throw error
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
