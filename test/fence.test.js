'use strict'

const { test, before, after } = require('node:test')
const assert = require('node:assert/strict')
const { singleNodePool, sleep } = require('./helpers.js')

let writePool
let admin

async function showReadOnly() {
  const [row] = await admin.query('SHOW transaction_read_only')
  return row.transaction_read_only
}

async function waitReadOnly(value) {
  for (let i = 0; i < 30 && (await showReadOnly()) !== value; i += 1) await sleep(100)
}

before(async () => {
  writePool = singleNodePool(undefined, { mode: 'read-write', acquireTimeoutMs: 1500, poolSize: 3 })
  admin = singleNodePool(undefined, { mode: 'any', acquireTimeoutMs: 1500, poolSize: 2 })
  await admin.query('drop table if exists t_fence')
  await admin.query('create table t_fence (id serial primary key, who text)')
})

after(async () => {
  // never leave the primary fenced, even if an assertion failed
  try {
    await admin.query('alter system reset default_transaction_read_only')
    await admin.query('select pg_reload_conf()')
    await waitReadOnly('off')
  } catch {
    // best effort
  }
  try {
    await admin.query('drop table if exists t_fence')
  } catch {
    // best effort
  }
  await Promise.all([writePool?.end(), admin?.end()])
})

test('read-only fence evicts the connection and surfaces a typed error, then recovers', async () => {
  // baseline write works on the writable primary
  const [w] = await writePool.query('insert into t_fence (who) values ($1) returning inet_server_port() as port', ['before'])
  assert.ok(w.port > 0)

  // fence the primary read-only WITHOUT dropping sessions
  await admin.query('alter system set default_transaction_read_only = on')
  await admin.query('select pg_reload_conf()')
  await waitReadOnly('on')

  // the write pool must evict the now-read-only pooled connection and fail to find a writable primary
  let error = null
  const start = Date.now()
  try {
    await writePool.query('insert into t_fence (who) values ($1)', ['fenced'])
  } catch (e) {
    error = e
  }
  assert.ok(error, 'expected the fenced write to throw')
  assert.match(error.message, /no writable primary available/)
  assert.ok(Date.now() - start >= 1500, 'should retry up to acquireTimeoutMs before throwing')

  // un-fence and confirm the pool re-resolves a writable primary
  await admin.query('alter system reset default_transaction_read_only')
  await admin.query('select pg_reload_conf()')
  await waitReadOnly('off')

  const [w2] = await writePool.query('insert into t_fence (who) values ($1) returning id', ['after'])
  assert.ok(w2.id > 0)

  const rows = await admin.query("select who from t_fence where who in ('before','after') order by id")
  assert.deepEqual(rows.map((r) => r.who), ['before', 'after'])
})
