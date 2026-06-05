'use strict'

const { test, before, after } = require('node:test')
const assert = require('node:assert/strict')
const { singleNodePool } = require('./helpers.js')

let pool

before(async () => {
  pool = singleNodePool()
  await pool.query('drop table if exists t_types')
  await pool.query(`create table t_types (
    a int2, b int4, c int8, d float4, e float8, f bool, g text,
    h jsonb, i uuid, j bytea, k timestamptz, l numeric, m int4[], n text[])`)
})

after(async () => {
  if (pool) {
    await pool.query('drop table if exists t_types')
    await pool.end()
  }
})

test('scalars round-trip with the right JS types', async () => {
  const uuid = '550e8400-e29b-41d4-a716-446655440000'
  await pool.query('truncate t_types')
  await pool.query(
    `insert into t_types (a,b,c,d,e,f,g,h,i,j,k,l,m,n) values
     ($1,$2,$3,$4,$5,$6,$7,$8,$9::text::uuid,$10,$11,$12::text::numeric,$13,$14)`,
    [1, 2, 9007199254740993n, 1.5, 2.5, true, 'hi', { x: 1, y: [2, 3] }, uuid,
     Buffer.from([1, 2, 3, 255]), new Date('2026-06-05T12:00:00.000Z'), '12345.6789', [10, 20, 30], ['a', 'b']],
  )
  const [r] = await pool.query('select * from t_types')

  assert.equal(r.a, 1)
  assert.equal(r.b, 2)
  assert.equal(typeof r.c, 'bigint')
  assert.equal(r.c, 9007199254740993n) // lossless past 2^53
  assert.ok(Math.abs(r.d - 1.5) < 1e-6)
  assert.equal(r.e, 2.5)
  assert.equal(r.f, true)
  assert.equal(r.g, 'hi')
  assert.deepEqual(r.h, { x: 1, y: [2, 3] })
  assert.equal(r.i, uuid)
  assert.ok(Buffer.isBuffer(r.j))
  assert.deepEqual([...r.j], [1, 2, 3, 255])
  assert.equal(typeof r.k, 'string')
  assert.ok(r.k.startsWith('2026-06-05T12:00:00'))
  assert.equal(r.l, '12345.6789')
  assert.deepEqual(r.m, [10, 20, 30])
  assert.deepEqual(r.n, ['a', 'b'])
})

test('numeric keeps arbitrary precision as a string', async () => {
  const [r] = await pool.query(
    'select 0::numeric a, 100::numeric b, (-12.50)::numeric c, 0.000123::numeric d, 9999999999999999999999::numeric e',
  )
  assert.deepEqual(r, { a: '0', b: '100', c: '-12.50', d: '0.000123', e: '9999999999999999999999' })
})

test('NULLs decode to null across types', async () => {
  const [r] = await pool.query(
    'select null::int8 c, null::bytea j, null::numeric l, null::int4[] m, null::timestamptz k, null::jsonb h',
  )
  assert.deepEqual(r, { c: null, j: null, l: null, m: null, k: null, h: null })
})

test('date / time / void map to strings / null', async () => {
  const [r] = await pool.query("select date '2026-06-05' d, time '13:14:15' t, pg_sleep(0) v")
  assert.equal(r.d, '2026-06-05')
  assert.equal(r.t, '13:14:15')
  assert.equal(r.v, null)
})

test('parameter coercion: Date -> timestamptz, Buffer -> bytea, array -> int[] / text[]', async () => {
  const [r] = await pool.query(
    'select $1::timestamptz k, $2::bytea j, $3::int4[] m, $4::text[] n',
    [new Date('2026-01-02T03:04:05.000Z'), Buffer.from([9, 8, 7]), [1, 2], ['x', 'y']],
  )
  assert.ok(r.k.startsWith('2026-01-02T03:04:05'))
  assert.deepEqual([...r.j], [9, 8, 7])
  assert.deepEqual(r.m, [1, 2])
  assert.deepEqual(r.n, ['x', 'y'])
})

test('jsonb object and array parameters', async () => {
  const [r] = await pool.query('select $1::jsonb a, $2::jsonb b', [{ a: 1, nested: { b: [true, null] } }, [1, 'two', 3]])
  assert.deepEqual(r.a, { a: 1, nested: { b: [true, null] } })
  assert.deepEqual(r.b, [1, 'two', 3])
})

test('bigint array round-trips losslessly', async () => {
  const [r] = await pool.query('select $1::int8[] xs', [[1n, 9007199254740993n, -5n]])
  assert.deepEqual(r.xs, [1n, 9007199254740993n, -5n])
  assert.equal(typeof r.xs[1], 'bigint')
})
