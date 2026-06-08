use std::error::Error as StdError;
use std::future::{pending, Future};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use deadpool_postgres::{
    Connect, Hook, HookError, Manager, ManagerConfig, Object as PooledClient, Pool as DeadpoolPool,
    RecyclingMethod,
};
use napi::bindgen_prelude::*;
use napi::{sys, type_of, ValueType};
use napi_derive::napi;
use serde_json::{Number, Value};
use time::{Date as PgDate, OffsetDateTime, PrimitiveDateTime, Time as PgTime};
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;
use tokio_postgres::config::{LoadBalanceHosts, TargetSessionAttrs};
use tokio_postgres::types::{to_sql_checked, FromSql, IsNull, Kind, ToSql, Type};
use tokio_postgres::{Config, NoTls, Row};
use uuid::Uuid;

type SqlResult = std::result::Result<IsNull, Box<dyn StdError + Sync + Send>>;

#[napi]
pub fn hello(name: String) -> String {
    format!("hello {name} from hyperiondb-client")
}

#[napi(object)]
pub struct PoolOptions {
    pub hosts: Vec<String>,
    pub port: Option<u32>,
    pub user: String,
    pub password: Option<String>,
    pub database: String,
    pub pool_size: Option<u32>,
    pub connect_timeout_ms: Option<u32>,
    pub acquire_timeout_ms: Option<u32>,
    pub statement_timeout_ms: Option<u32>,
    pub mode: Option<String>,
    pub application_name: Option<String>,
}

#[napi(object)]
pub struct PoolStatus {
    pub max_size: u32,
    pub size: u32,
    pub available: u32,
    pub in_use: u32,
    pub waiting: u32,
}

#[derive(Clone, Copy)]
enum Routing {
    ReadWrite,
    ReadOnly,
    PreferStandby,
    Any,
}

fn split_host_port(entry: &str, default_port: u16) -> (&str, u16) {
    if let Some(index) = entry.rfind(':') {
        let (host, rest) = entry.split_at(index);
        if !host.is_empty() && !host.contains(':') {
            if let Ok(port) = rest[1..].parse::<u16>() {
                return (host, port);
            }
        }
    }
    (entry, default_port)
}

fn parse_mode(mode: Option<&str>) -> Result<Routing> {
    match mode.unwrap_or("read-write") {
        "read-write" | "readwrite" | "rw" => Ok(Routing::ReadWrite),
        "read-only" | "readonly" | "ro" => Ok(Routing::ReadOnly),
        "prefer-standby" | "preferstandby" => Ok(Routing::PreferStandby),
        "any" => Ok(Routing::Any),
        other => Err(Error::from_reason(format!(
            "createPool: unknown mode `{other}` (expected read-write | read-only | prefer-standby | any)"
        ))),
    }
}

#[napi]
pub struct Pool {
    inner: DeadpoolPool,
    acquire_timeout: Duration,
}

#[napi]
pub fn create_pool(options: PoolOptions) -> Result<Pool> {
    Pool::build(options)
}

#[napi]
impl Pool {
    fn build(options: PoolOptions) -> Result<Self> {
        if options.hosts.is_empty() {
            return Err(Error::from_reason(
                "createPool: `hosts` must list at least one host",
            ));
        }
        let routing = parse_mode(options.mode.as_deref())?;

        let mut config = Config::new();
        let default_port = options.port.unwrap_or(5432) as u16;
        for entry in &options.hosts {
            let (host, port) = split_host_port(entry, default_port);
            config.host(host);
            config.port(port);
        }
        config.user(options.user.as_str());
        if let Some(password) = &options.password {
            config.password(password.as_str());
        }
        config.dbname(options.database.as_str());
        if let Some(ms) = options.connect_timeout_ms {
            config.connect_timeout(Duration::from_millis(ms as u64));
        }
        if let Some(ms) = options.statement_timeout_ms {
            config.options(format!("-c statement_timeout={ms}"));
        }
        if let Some(name) = &options.application_name {
            config.application_name(name.as_str());
        }

        let manager_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };
        let manager = match routing {
            Routing::ReadWrite => {
                let mut config = config;
                config.target_session_attrs(TargetSessionAttrs::ReadWrite);
                Manager::from_config(config, NoTls, manager_config)
            }
            Routing::ReadOnly => {
                let mut config = config;
                config.target_session_attrs(TargetSessionAttrs::ReadOnly);
                config.load_balance_hosts(LoadBalanceHosts::Random);
                Manager::from_config(config, NoTls, manager_config)
            }
            Routing::Any => {
                let mut config = config;
                config.target_session_attrs(TargetSessionAttrs::Any);
                config.load_balance_hosts(LoadBalanceHosts::Random);
                Manager::from_config(config, NoTls, manager_config)
            }
            Routing::PreferStandby => {
                let mut standby = config.clone();
                standby.target_session_attrs(TargetSessionAttrs::ReadOnly);
                standby.load_balance_hosts(LoadBalanceHosts::Random);
                let mut fallback = config;
                fallback.target_session_attrs(TargetSessionAttrs::Any);
                fallback.load_balance_hosts(LoadBalanceHosts::Random);
                Manager::from_connect(fallback, PreferStandbyConnect { standby }, manager_config)
            }
        };
        let pool_size = options.pool_size.unwrap_or(10).max(1) as usize;
        let mut builder = DeadpoolPool::builder(manager).max_size(pool_size);
        if matches!(routing, Routing::ReadWrite) {
            builder = builder.pre_recycle(writable_check());
        }
        let inner = builder.build().map_err(|error| {
            Error::from_reason(format!("createPool: failed to build pool: {error}"))
        })?;

        let acquire_timeout = Duration::from_millis(options.acquire_timeout_ms.unwrap_or(5000) as u64);
        Ok(Pool {
            inner,
            acquire_timeout,
        })
    }

    #[napi(
        ts_args_type = "sql: string, params?: Array<any> | null, timeoutMs?: number | null, signal?: AbortSignal | null",
        ts_return_type = "Promise<Array<Record<string, any>>>"
    )]
    pub async fn query(
        &self,
        sql: String,
        params: Option<Vec<Param>>,
        timeout_ms: Option<u32>,
        signal: Option<AbortBridge>,
    ) -> Result<Vec<RowObject>> {
        let pool = self.inner.clone();
        let client = checkout(&pool, self.acquire_timeout).await?;
        run_query(&client, &sql, &params.unwrap_or_default(), timeout_ms, signal).await
    }

    #[napi]
    pub async fn begin(&self) -> Result<Transaction> {
        let pool = self.inner.clone();
        let client = checkout(&pool, self.acquire_timeout).await?;
        client.batch_execute("BEGIN").await.map_err(map_pg_error)?;
        Ok(Transaction {
            client: Arc::new(Mutex::new(Some(client))),
        })
    }

    #[napi]
    pub fn status(&self) -> PoolStatus {
        let status = self.inner.status();
        PoolStatus {
            max_size: status.max_size as u32,
            size: status.size as u32,
            available: status.available as u32,
            in_use: status.size.saturating_sub(status.available) as u32,
            waiting: status.waiting as u32,
        }
    }

    #[napi]
    pub async fn end(&self) -> Result<()> {
        self.inner.close();
        Ok(())
    }
}

#[napi]
pub struct Transaction {
    client: Arc<Mutex<Option<PooledClient>>>,
}

#[napi]
impl Transaction {
    #[napi(
        ts_args_type = "sql: string, params?: Array<any> | null, timeoutMs?: number | null, signal?: AbortSignal | null",
        ts_return_type = "Promise<Array<Record<string, any>>>"
    )]
    pub async fn query(
        &self,
        sql: String,
        params: Option<Vec<Param>>,
        timeout_ms: Option<u32>,
        signal: Option<AbortBridge>,
    ) -> Result<Vec<RowObject>> {
        let guard = self.client.lock().await;
        let client = guard.as_ref().ok_or_else(|| {
            Error::from_reason("transaction: already committed or rolled back")
        })?;
        run_query(client, &sql, &params.unwrap_or_default(), timeout_ms, signal).await
    }

    #[napi]
    pub async fn commit(&self) -> Result<()> {
        self.finish("COMMIT").await
    }

    #[napi]
    pub async fn rollback(&self) -> Result<()> {
        self.finish("ROLLBACK").await
    }
}

impl Transaction {
    async fn finish(&self, verb: &str) -> Result<()> {
        let mut guard = self.client.lock().await;
        let client = guard.take().ok_or_else(|| {
            Error::from_reason("transaction: already committed or rolled back")
        })?;
        client.batch_execute(verb).await.map_err(map_pg_error)?;
        Ok(())
    }
}

pub struct AbortBridge {
    notify: Arc<Notify>,
}

impl FromNapiValue for AbortBridge {
    unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> Result<Self> {
        let signal = unsafe { AbortSignal::from_napi_value(env, napi_val)? };
        let notify = Arc::new(Notify::new());
        let trigger = notify.clone();
        signal.on_abort(move || trigger.notify_one());
        Ok(AbortBridge { notify })
    }
}

async fn run_query(
    client: &PooledClient,
    sql: &str,
    params: &[Param],
    timeout_ms: Option<u32>,
    signal: Option<AbortBridge>,
) -> Result<Vec<RowObject>> {
    let statement = client.prepare_cached(sql).await.map_err(map_pg_error)?;
    let bind: Vec<&(dyn ToSql + Sync)> =
        params.iter().map(|param| param as &(dyn ToSql + Sync)).collect();
    let token = client.cancel_token();
    let abort = signal.map(|bridge| bridge.notify);
    let timeout = timeout_ms.map(|ms| Duration::from_millis(ms as u64));

    let query_future = client.query(&statement, &bind);
    tokio::pin!(query_future);

    let rows = tokio::select! {
        result = &mut query_future => result.map_err(map_pg_error)?,
        _ = sleep_opt(timeout) => {
            let _ = token.cancel_query(NoTls).await;
            let _ = (&mut query_future).await;
            return Err(Error::from_reason(format!(
                "query: cancelled after timeout of {}ms",
                timeout.map(|value| value.as_millis()).unwrap_or(0)
            )));
        }
        _ = notified_opt(&abort) => {
            let _ = token.cancel_query(NoTls).await;
            let _ = (&mut query_future).await;
            return Err(Error::from_reason("query: aborted by signal"));
        }
    };

    rows.iter().map(row_to_object).collect()
}

async fn sleep_opt(timeout: Option<Duration>) {
    match timeout {
        Some(duration) => tokio::time::sleep(duration).await,
        None => pending().await,
    }
}

async fn notified_opt(abort: &Option<Arc<Notify>>) {
    match abort {
        Some(notify) => notify.notified().await,
        None => pending().await,
    }
}

fn map_pg_error(error: tokio_postgres::Error) -> Error {
    if let Some(db) = error.as_db_error() {
        Error::from_reason(format!("[SQLSTATE {}] {}", db.code().code(), db.message()))
    } else if is_connection_error(&error) {
        Error::from_reason(format!("[SQLSTATE 08006] {error}"))
    } else {
        Error::from_reason(error.to_string())
    }
}

fn is_connection_error(error: &tokio_postgres::Error) -> bool {
    if error.is_closed() {
        return true;
    }
    let mut source = error.source();
    while let Some(inner) = source {
        if inner.downcast_ref::<std::io::Error>().is_some() {
            return true;
        }
        source = inner.source();
    }
    false
}

struct PreferStandbyConnect {
    standby: Config,
}

impl Connect for PreferStandbyConnect {
    fn connect<'a>(
        &'a self,
        fallback: &Config,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = std::result::Result<
                        (tokio_postgres::Client, JoinHandle<()>),
                        tokio_postgres::Error,
                    >,
                > + Send
                + 'a,
        >,
    > {
        let standby = self.standby.clone();
        let fallback = fallback.clone();
        Box::pin(async move {
            match connect_and_spawn(&standby).await {
                Ok(pair) => Ok(pair),
                Err(_) => connect_and_spawn(&fallback).await,
            }
        })
    }
}

async fn connect_and_spawn(
    config: &Config,
) -> std::result::Result<(tokio_postgres::Client, JoinHandle<()>), tokio_postgres::Error> {
    let (client, connection) = config.connect(NoTls).await?;
    let handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok((client, handle))
}

fn writable_check() -> Hook {
    Hook::async_fn(|client, _metrics| {
        Box::pin(async move {
            match client.query_one("SHOW transaction_read_only", &[]).await {
                Ok(row) => {
                    let read_only: String = row.get(0);
                    if read_only == "on" {
                        Err(HookError::message(
                            "connection is read-only (primary fenced or demoted); evicting",
                        ))
                    } else {
                        Ok(())
                    }
                }
                Err(error) => Err(HookError::Backend(error)),
            }
        })
    })
}

async fn checkout(pool: &DeadpoolPool, window: Duration) -> Result<PooledClient> {
    let start = Instant::now();
    let mut backoff = Duration::from_millis(50);
    loop {
        match pool.get().await {
            Ok(client) => return Ok(client),
            Err(error) => {
                if start.elapsed() >= window {
                    return Err(Error::from_reason(format!(
                        "[SQLSTATE 08006] query: no writable primary available after {}ms: {error}",
                        window.as_millis()
                    )));
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_millis(500));
            }
        }
    }
}

pub enum Cell {
    Null,
    Bool(bool),
    Int(i32),
    Double(f64),
    Big(i64),
    Text(String),
    Bytes(Vec<u8>),
    Json(Value),
    List(Vec<Cell>),
}

impl ToNapiValue for Cell {
    unsafe fn to_napi_value(env: sys::napi_env, value: Cell) -> Result<sys::napi_value> {
        match value {
            Cell::Null => unsafe { Null::to_napi_value(env, Null) },
            Cell::Bool(inner) => unsafe { bool::to_napi_value(env, inner) },
            Cell::Int(inner) => unsafe { i32::to_napi_value(env, inner) },
            Cell::Double(inner) => unsafe { f64::to_napi_value(env, inner) },
            Cell::Big(inner) => unsafe { BigInt::to_napi_value(env, BigInt::from(inner)) },
            Cell::Text(inner) => unsafe { String::to_napi_value(env, inner) },
            Cell::Bytes(inner) => unsafe { Buffer::to_napi_value(env, Buffer::from(inner)) },
            Cell::Json(inner) => unsafe { Value::to_napi_value(env, inner) },
            Cell::List(inner) => unsafe { Vec::<Cell>::to_napi_value(env, inner) },
        }
    }
}

pub struct RowObject(Vec<(String, Cell)>);

impl ToNapiValue for RowObject {
    unsafe fn to_napi_value(env: sys::napi_env, value: RowObject) -> Result<sys::napi_value> {
        let wrapper = Env::from_raw(env);
        let mut object = Object::new(&wrapper)?;
        for (name, cell) in value.0 {
            object.set(name.as_str(), cell)?;
        }
        unsafe { ToNapiValue::to_napi_value(env, &object) }
    }
}

fn row_to_object(row: &Row) -> Result<RowObject> {
    let columns = row.columns();
    let mut fields = Vec::with_capacity(columns.len());
    for (index, column) in columns.iter().enumerate() {
        fields.push((
            column.name().to_string(),
            cell_from_column(row, index, column.type_())?,
        ));
    }
    Ok(RowObject(fields))
}

fn cell_from_column(row: &Row, index: usize, ty: &Type) -> Result<Cell> {
    if let Kind::Array(member) = ty.kind() {
        return array_cell(row, index, member);
    }
    scalar_cell(row, index, ty.name())
}

fn scalar_cell(row: &Row, index: usize, name: &str) -> Result<Cell> {
    let cell = match name {
        "bool" => opt(get::<bool>(row, index)?, Cell::Bool),
        "int2" => opt(get::<i16>(row, index)?, |value| Cell::Int(value as i32)),
        "int4" => opt(get::<i32>(row, index)?, Cell::Int),
        "int8" => opt(get::<i64>(row, index)?, Cell::Big),
        "oid" => opt(get::<u32>(row, index)?, |value| Cell::Double(value as f64)),
        "float4" => opt(get::<f32>(row, index)?, |value| Cell::Double(value as f64)),
        "float8" => opt(get::<f64>(row, index)?, Cell::Double),
        "numeric" => opt(get::<PgNumeric>(row, index)?, |value| Cell::Text(value.0)),
        "text" | "varchar" | "bpchar" | "name" | "unknown" => {
            opt(get::<String>(row, index)?, Cell::Text)
        }
        "uuid" => opt(get::<Uuid>(row, index)?, |value| Cell::Text(value.to_string())),
        "bytea" => opt(get::<Vec<u8>>(row, index)?, Cell::Bytes),
        "json" | "jsonb" => get::<Value>(row, index)?.map(Cell::Json).unwrap_or(Cell::Null),
        "void" => Cell::Null,
        "timestamptz" => opt(get::<OffsetDateTime>(row, index)?, offset_cell),
        "timestamp" => opt(get::<PrimitiveDateTime>(row, index)?, primitive_cell),
        "date" => opt(get::<PgDate>(row, index)?, |value| Cell::Text(iso_date(value))),
        "time" => opt(get::<PgTime>(row, index)?, |value| Cell::Text(iso_time(value))),
        "geography" | "geometry" => opt(get::<RawWkb>(row, index)?, |value| Cell::Text(value.0)),
        other => {
            return Err(Error::from_reason(format!(
                "query: unsupported column type `{other}` at column `{}`",
                row.columns()[index].name()
            )))
        }
    };
    Ok(cell)
}

fn array_cell(row: &Row, index: usize, member: &Type) -> Result<Cell> {
    match member.name() {
        "bool" => list_of(row, index, Cell::Bool),
        "int2" => list_of(row, index, |value: i16| Cell::Int(value as i32)),
        "int4" => list_of(row, index, Cell::Int),
        "int8" => list_of(row, index, Cell::Big),
        "oid" => list_of(row, index, |value: u32| Cell::Double(value as f64)),
        "float4" => list_of(row, index, |value: f32| Cell::Double(value as f64)),
        "float8" => list_of(row, index, Cell::Double),
        "numeric" => list_of(row, index, |value: PgNumeric| Cell::Text(value.0)),
        "text" | "varchar" | "bpchar" | "name" => list_of(row, index, Cell::Text),
        "uuid" => list_of(row, index, |value: Uuid| Cell::Text(value.to_string())),
        "bytea" => list_of(row, index, Cell::Bytes),
        "json" | "jsonb" => list_of(row, index, Cell::Json),
        "timestamptz" => list_of(row, index, offset_cell),
        "timestamp" => list_of(row, index, primitive_cell),
        "date" => list_of(row, index, |value: PgDate| Cell::Text(iso_date(value))),
        "time" => list_of(row, index, |value: PgTime| Cell::Text(iso_time(value))),
        other => Err(Error::from_reason(format!(
            "query: unsupported array element type `{other}`"
        ))),
    }
}

fn get<'a, T: FromSql<'a>>(row: &'a Row, index: usize) -> Result<Option<T>> {
    row.try_get::<usize, Option<T>>(index)
        .map_err(|error| Error::from_reason(error.to_string()))
}

fn opt<T>(value: Option<T>, map: impl FnOnce(T) -> Cell) -> Cell {
    value.map(map).unwrap_or(Cell::Null)
}

struct RawWkb(String);

impl<'a> FromSql<'a> for RawWkb {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        use std::fmt::Write;
        let mut hex = String::with_capacity(raw.len() * 2);
        for byte in raw {
            let _ = write!(hex, "{byte:02X}");
        }
        Ok(RawWkb(hex))
    }

    fn accepts(ty: &Type) -> bool {
        matches!(ty.name(), "geography" | "geometry")
    }
}

fn list_of<'a, T, F>(row: &'a Row, index: usize, map: F) -> Result<Cell>
where
    T: FromSql<'a>,
    F: Fn(T) -> Cell,
{
    match get::<Vec<Option<T>>>(row, index)? {
        None => Ok(Cell::Null),
        Some(items) => Ok(Cell::List(
            items
                .into_iter()
                .map(|item| item.map(&map).unwrap_or(Cell::Null))
                .collect(),
        )),
    }
}

fn offset_cell(value: OffsetDateTime) -> Cell {
    Cell::Text(iso_offset(value))
}

fn primitive_cell(value: PrimitiveDateTime) -> Cell {
    Cell::Text(format!("{}T{}", iso_date(value.date()), iso_time(value.time())))
}

fn iso_offset(value: OffsetDateTime) -> String {
    let (hours, minutes, _) = value.offset().as_hms();
    let sign = if hours < 0 || minutes < 0 { '-' } else { '+' };
    format!(
        "{}T{}{}{:02}:{:02}",
        iso_date(value.date()),
        iso_time(value.time()),
        sign,
        hours.unsigned_abs(),
        minutes.unsigned_abs()
    )
}

fn iso_date(date: PgDate) -> String {
    format!(
        "{:04}-{:02}-{:02}",
        date.year(),
        u8::from(date.month()),
        date.day()
    )
}

fn iso_time(value: PgTime) -> String {
    let nanos = value.nanosecond();
    if nanos == 0 {
        format!("{:02}:{:02}:{:02}", value.hour(), value.minute(), value.second())
    } else {
        format!(
            "{:02}:{:02}:{:02}.{:06}",
            value.hour(),
            value.minute(),
            value.second(),
            nanos / 1_000
        )
    }
}

struct PgNumeric(String);

impl<'a> FromSql<'a> for PgNumeric {
    fn from_sql(_ty: &Type, raw: &'a [u8]) -> std::result::Result<Self, Box<dyn StdError + Sync + Send>> {
        Ok(PgNumeric(numeric_to_string(raw)?))
    }

    fn accepts(ty: &Type) -> bool {
        ty.name() == "numeric"
    }
}

fn numeric_to_string(raw: &[u8]) -> std::result::Result<String, Box<dyn StdError + Sync + Send>> {
    if raw.len() < 8 {
        return Err("numeric: truncated header".into());
    }
    let read = |at: usize| i16::from_be_bytes([raw[at], raw[at + 1]]);
    let ndigits = read(0) as usize;
    let weight = read(2) as i32;
    let sign = u16::from_be_bytes([raw[4], raw[5]]);
    let dscale = u16::from_be_bytes([raw[6], raw[7]]) as i32;

    match sign {
        0x0000 | 0x4000 => {}
        0xC000 => return Ok("NaN".to_string()),
        0xD000 => return Ok("Infinity".to_string()),
        0xF000 => return Ok("-Infinity".to_string()),
        _ => return Err("numeric: invalid sign".into()),
    }

    let mut groups = Vec::with_capacity(ndigits);
    for index in 0..ndigits {
        let at = 8 + index * 2;
        if at + 2 > raw.len() {
            return Err("numeric: truncated digits".into());
        }
        groups.push(read(at) as i32);
    }

    let mut out = String::new();
    if sign == 0x4000 {
        out.push('-');
    }

    if weight < 0 {
        out.push('0');
    } else {
        for position in 0..=weight {
            let group = groups.get(position as usize).copied().unwrap_or(0);
            if position == 0 {
                out.push_str(&group.to_string());
            } else {
                out.push_str(&format!("{group:04}"));
            }
        }
    }

    if dscale > 0 {
        out.push('.');
        let mut fraction = String::new();
        let mut position = weight + 1;
        while (fraction.len() as i32) < dscale {
            let group = if position >= 0 {
                groups.get(position as usize).copied().unwrap_or(0)
            } else {
                0
            };
            fraction.push_str(&format!("{group:04}"));
            position += 1;
        }
        fraction.truncate(dscale as usize);
        out.push_str(&fraction);
    }

    Ok(out)
}

#[derive(Clone, Debug)]
pub enum Param {
    Null,
    Bool(bool),
    Float(f64),
    Big(i64),
    Text(String),
    Bytes(Vec<u8>),
    Instant(OffsetDateTime),
    List(Vec<Param>),
    Json(Value),
}

impl FromNapiValue for Param {
    unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> Result<Self> {
        match type_of!(env, napi_val)? {
            ValueType::Null | ValueType::Undefined => Ok(Param::Null),
            ValueType::Boolean => Ok(Param::Bool(unsafe { bool::from_napi_value(env, napi_val)? })),
            ValueType::Number => Ok(Param::Float(unsafe { f64::from_napi_value(env, napi_val)? })),
            ValueType::BigInt => {
                let big = unsafe { BigInt::from_napi_value(env, napi_val)? };
                let (value, lossless) = big.get_i64();
                if !lossless {
                    return Err(Error::from_reason(
                        "query: BigInt parameter does not fit in a 64-bit integer",
                    ));
                }
                Ok(Param::Big(value))
            }
            ValueType::String => Ok(Param::Text(unsafe { String::from_napi_value(env, napi_val)? })),
            ValueType::Object => {
                let object = Object::from_raw(env, napi_val);
                if object.is_array()? {
                    Ok(Param::List(unsafe {
                        Vec::<Param>::from_napi_value(env, napi_val)?
                    }))
                } else if object.is_buffer()? || object.is_typedarray()? {
                    let buffer = unsafe { Buffer::from_napi_value(env, napi_val)? };
                    Ok(Param::Bytes(buffer.to_vec()))
                } else if js_is_date(env, napi_val)? {
                    let millis = js_date_ms(env, napi_val)?;
                    let nanos = (millis * 1_000_000.0) as i128;
                    let instant = OffsetDateTime::from_unix_timestamp_nanos(nanos).map_err(|error| {
                        Error::from_reason(format!("query: invalid Date parameter: {error}"))
                    })?;
                    Ok(Param::Instant(instant))
                } else {
                    Ok(Param::Json(unsafe { Value::from_napi_value(env, napi_val)? }))
                }
            }
            other => Err(Error::from_reason(format!(
                "query: unsupported parameter type {other:?}"
            ))),
        }
    }
}

impl ToSql for Param {
    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> SqlResult {
        match self {
            Param::Null => Ok(IsNull::Yes),
            Param::Bool(value) => value.to_sql(ty, out),
            Param::Float(value) => float_to_sql(*value, ty, out),
            Param::Big(value) => big_to_sql(*value, ty, out),
            Param::Text(value) => match ty.name() {
                "uuid" => Uuid::parse_str(value)
                    .map_err(|error| Box::new(error) as Box<dyn StdError + Sync + Send>)?
                    .to_sql(ty, out),
                _ => value.to_sql(ty, out),
            },
            Param::Bytes(value) => value.as_slice().to_sql(ty, out),
            Param::Instant(value) => value.to_sql(ty, out),
            Param::List(items) => list_to_sql(items, ty, out),
            Param::Json(value) => value.to_sql(ty, out),
        }
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }

    to_sql_checked!();
}

fn float_to_sql(value: f64, ty: &Type, out: &mut BytesMut) -> SqlResult {
    match ty.name() {
        "int2" => (value as i16).to_sql(ty, out),
        "int4" => (value as i32).to_sql(ty, out),
        "int8" => (value as i64).to_sql(ty, out),
        "oid" => (value as u32).to_sql(ty, out),
        "float4" => (value as f32).to_sql(ty, out),
        _ => value.to_sql(ty, out),
    }
}

fn big_to_sql(value: i64, ty: &Type, out: &mut BytesMut) -> SqlResult {
    match ty.name() {
        "int2" => (value as i16).to_sql(ty, out),
        "int4" => (value as i32).to_sql(ty, out),
        "oid" => (value as u32).to_sql(ty, out),
        "float4" => (value as f32).to_sql(ty, out),
        "float8" => (value as f64).to_sql(ty, out),
        _ => value.to_sql(ty, out),
    }
}

fn list_to_sql(items: &[Param], ty: &Type, out: &mut BytesMut) -> SqlResult {
    match ty.kind() {
        Kind::Array(_) => items.to_vec().to_sql(ty, out),
        _ => Value::Array(items.iter().map(param_to_json).collect()).to_sql(ty, out),
    }
}

fn param_to_json(param: &Param) -> Value {
    match param {
        Param::Null => Value::Null,
        Param::Bool(value) => Value::Bool(*value),
        Param::Float(value) => Number::from_f64(*value).map(Value::Number).unwrap_or(Value::Null),
        Param::Big(value) => Value::Number(Number::from(*value)),
        Param::Text(value) => Value::String(value.clone()),
        Param::Bytes(value) => {
            Value::Array(value.iter().map(|byte| Value::Number(Number::from(*byte))).collect())
        }
        Param::Instant(value) => Value::String(iso_offset(*value)),
        Param::List(items) => Value::Array(items.iter().map(param_to_json).collect()),
        Param::Json(value) => value.clone(),
    }
}

fn js_is_date(env: sys::napi_env, value: sys::napi_value) -> Result<bool> {
    let mut result = false;
    let status = unsafe { sys::napi_is_date(env, value, &mut result) };
    if status != sys::Status::napi_ok {
        return Err(Error::from_reason("query: napi_is_date failed"));
    }
    Ok(result)
}

fn js_date_ms(env: sys::napi_env, value: sys::napi_value) -> Result<f64> {
    let mut millis = 0.0_f64;
    let status = unsafe { sys::napi_get_date_value(env, value, &mut millis) };
    if status != sys::Status::napi_ok {
        return Err(Error::from_reason("query: napi_get_date_value failed"));
    }
    Ok(millis)
}
