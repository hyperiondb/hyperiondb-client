'use strict'

const { test, before, after } = require('node:test')
const assert = require('node:assert/strict')
const { clusterPool, sleep } = require('./helpers.js')
const cluster = require('./cluster.js')

// Jepsen-style durability oracle, ported from packages/chaos-writer: stream monotonic ids
// through the primary-following pool, stop the primary mid-load, and assert every ACKED
// (committed) id survives the failover. Requires the ability to stop a node (cluster.js) and
// a synchronously-replicated cluster for true zero-loss.

let pool
let stoppedPort = null
let ctlAvailable = false

before(async () => {
  ctlAvailable = await cluster.available()
  if (!ctlAvailable) return
  pool = clusterPool()
  await pool.query('drop table if exists chaos')
  await pool.query('create table chaos (id bigint primary key)')
})

after(async () => {
  if (stoppedPort !== null) {
    try {
      await cluster.startNode(stoppedPort)
    } catch {
      // best effort restart so the cluster reconverges for later runs
    }
  }
  if (pool) {
    try {
      await pool.query('drop table if exists chaos')
    } catch {
      // best effort
    }
    await pool.end()
  }
})

test('acked writes survive a primary stop (zero loss)', { timeout: 90_000 }, async (t) => {
  if (!ctlAvailable) {
    t.skip('cluster control unavailable (set HYPERION_CTL / run against the dev cluster)')
    return
  }

  const acked = new Set()
  const portsSeen = new Set()
  let id = 0n
  let running = true
  let lastError = null

  const writer = (async () => {
    while (running) {
      id += 1n // a fresh id every iteration; a failed insert is never reused
      try {
        const [row] = await pool.query(
          'insert into chaos (id) values ($1) returning inet_server_port() as port',
          [id],
        )
        acked.add(id)
        portsSeen.add(row.port)
      } catch (e) {
        lastError = e
      }
      await sleep(2)
    }
  })()

  // build up load on the current primary
  await sleep(2000)
  const [{ port: primaryPort }] = await pool.query('select inet_server_port() as port')
  assert.ok(acked.size > 50, `expected steady writes before the fault, got ${acked.size}`)

  // stop the primary — the pool must follow the failover to a new writable primary
  stoppedPort = primaryPort
  await cluster.stopNode(primaryPort)

  // wait until acked writes land on a different node (proves reconnection to the new primary)
  const deadline = Date.now() + 45_000
  while (Date.now() < deadline && ![...portsSeen].some((p) => p !== primaryPort)) {
    await sleep(500)
  }
  const ackedAfterFailover = [...portsSeen].some((p) => p !== primaryPort)
  await sleep(1500) // a little more post-failover load
  running = false
  await writer

  assert.ok(
    ackedAfterFailover,
    `pool never reconnected to a new primary after stopping ${primaryPort}; last error: ${lastError?.message}`,
  )

  // ZERO LOSS: every acked id must be present in the surviving cluster
  const present = new Set()
  for (const r of await pool.query('select id from chaos')) present.add(r.id)
  const missing = [...acked].filter((value) => !present.has(value))
  assert.equal(
    missing.length,
    0,
    `${missing.length} of ${acked.size} acked writes were lost across the failover (first few: ${missing.slice(0, 5).join(',')})`,
  )

  // restart the stopped node so the cluster reconverges
  await cluster.startNode(stoppedPort)
  stoppedPort = null
})
