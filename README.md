# hyperiondb-client

Native Node.js client for a **HyperionDb / pg_replica** Postgres cluster: a primary-following
connection pool over the N nodes, written in Rust with [napi-rs](https://napi.rs) +
`tokio-postgres`. Routing, primary detection, pooling and failover recovery live in Rust — JS
never loops over connections.

Status: **in progress**

## Build

```bash
npm install
npm run build:debug # debug; or `npm run build` for release
```

`napi build` emits a platform-tagged `hyperiondb-client.<triple>.node` plus a generated
`index.js` and `index.d.ts` at the package root.

## Smoke test (after build)

```js
const { hello } = require('./index.js')
console.log(hello('world')) // hello world from hyperiondb-client
```
