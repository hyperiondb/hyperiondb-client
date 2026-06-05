'use strict'

const { execFile } = require('node:child_process')
const { promisify } = require('node:util')

const run = promisify(execFile)

// How to stop/start a single cluster node. Defaults to the local pgrx/WSL cluster used in
// development; override with HYPERION_CTL=docker (+ a compose mapping) in CI against docker/.
const MODE = process.env.HYPERION_CTL || 'wsl-pgctl'
const PGBIN = process.env.PGBIN || '/home/talai/.pgrx/18.4/pgrx-install/bin'
const ROOT = process.env.CLUSTER_ROOT || '/tmp/hyperion-repl'
const BASE_PORT = Number(process.env.HYPERION_BASE_PORT || 54340)

function dataDir(port) {
  return `${ROOT}/n${port - BASE_PORT + 1}`
}

async function wsl(command) {
  const { stdout } = await run('wsl.exe', ['-e', 'bash', '-lc', command])
  return stdout
}

async function available() {
  if (MODE !== 'wsl-pgctl') return false
  try {
    await run('wsl.exe', ['-e', 'true'])
    return true
  } catch {
    return false
  }
}

async function stopNode(port) {
  if (MODE === 'wsl-pgctl') return wsl(`${PGBIN}/pg_ctl -D ${dataDir(port)} stop -m fast`)
  throw new Error(`HYPERION_CTL=${MODE} is not supported by the test harness`)
}

async function startNode(port) {
  if (MODE === 'wsl-pgctl') {
    return wsl(`${PGBIN}/pg_ctl -D ${dataDir(port)} -l ${dataDir(port)}.log start`)
  }
  throw new Error(`HYPERION_CTL=${MODE} is not supported by the test harness`)
}

module.exports = { MODE, available, stopNode, startNode, dataDir }
