'use strict'

const { createPool } = require('../client.js')

const HOST = process.env.HYPERION_HOST || '127.0.0.1'
const PRIMARY_PORT = Number(process.env.HYPERION_PORT || 54340)
const PORTS = (process.env.HYPERION_PORTS || '54340,54341,54342')
  .split(',')
  .map((value) => Number(value.trim()))

const auth = {
  user: process.env.HYPERION_USER || 'postgres',
  password: process.env.HYPERION_PASSWORD || 'supass',
  database: process.env.HYPERION_DB || 'postgres',
}

function singleNodePool(port = PRIMARY_PORT, extra = {}) {
  return createPool({
    hosts: [HOST],
    port,
    ...auth,
    connectTimeoutMs: 2000,
    acquireTimeoutMs: 5000,
    poolSize: 4,
    ...extra,
  })
}

function clusterPool(extra = {}) {
  return createPool({
    hosts: PORTS.map((port) => `${HOST}:${port}`),
    ...auth,
    connectTimeoutMs: 2000,
    acquireTimeoutMs: 8000,
    poolSize: 6,
    ...extra,
  })
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

module.exports = { HOST, PRIMARY_PORT, PORTS, auth, singleNodePool, clusterPool, sleep }
