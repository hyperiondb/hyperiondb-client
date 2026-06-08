'use strict'

const { test, before, after } = require('node:test')
const assert = require('node:assert/strict')
const { singleNodePool } = require('./helpers.js')

let pool

before(async () => {
  pool = singleNodePool()
  await pool.query('drop table if exists t_retry')
  await pool.query('create table t_retry (_id varchar primary key, n int)')
})

after(async () => {
  if (pool) {
    await pool.query('drop table if exists t_retry')
    await pool.end()
  }
})

test('idempotent insert dedups by _id (exactly once)', async () => {
  await pool.query('truncate t_retry')
  const id = 'idem-aaa'

  const inserted = await pool.insert('t_retry', { _id: id, n: 1 }, { idempotency: true })
  assert.equal(inserted.length, 1)
  assert.equal(inserted[0]._id, id)
  assert.equal(inserted[0].n, 1)

  const duplicate = await pool.insert('t_retry', { _id: id, n: 2 }, { idempotency: true })
  assert.equal(duplicate.length, 0)

  const [row] = await pool.query('select n from t_retry where _id = $1', [id])
  assert.equal(row.n, 1)
})

test('non-idempotent duplicate throws 23505', async () => {
  const id = 'idem-bbb'
  await pool.insert('t_retry', { _id: id, n: 1 })
  await assert.rejects(
    () => pool.insert('t_retry', { _id: id, n: 2 }),
    (error) => error.code === '23505',
  )
})

test('transaction retries on serialization/deadlock, exactly once', async () => {
  const id = 'tx-ccc'
  let attempts = 0
  await pool.transaction(async (tx) => {
    attempts += 1
    await tx.query('insert into t_retry (_id, n) values ($1, $2)', [id, attempts])
    if (attempts === 1) {
      const error = new Error('synthetic serialization failure')
      error.code = '40001'
      throw error
    }
  })
  assert.equal(attempts, 2)
  const [row] = await pool.query('select count(*)::int c, max(n) m from t_retry where _id = $1', [id])
  assert.equal(row.c, 1)
  assert.equal(row.m, 2)
})

test('transaction does not retry a deterministic error', async () => {
  let attempts = 0
  await assert.rejects(
    () =>
      pool.transaction(async () => {
        attempts += 1
        const error = new Error('check violation')
        error.code = '23514'
        throw error
      }),
    (error) => error.code === '23514',
  )
  assert.equal(attempts, 1)
})

test('connection failure during COMMIT surfaces IN_DOUBT and is not retried', async () => {
  const realBegin = pool.begin.bind(pool)
  let begins = 0
  pool.begin = async () => {
    begins += 1
    const tx = await realBegin()
    tx.commit = async () => {
      await tx.rollback().catch(() => {})
      const error = new Error('server closed the connection unexpectedly')
      error.code = '08006'
      throw error
    }
    return tx
  }
  try {
    await assert.rejects(
      () => pool.transaction(async (tx) => { await tx.query('select 1') }),
      (error) => error.code === 'IN_DOUBT',
    )
    assert.equal(begins, 1)
  } finally {
    pool.begin = realBegin
  }
})
