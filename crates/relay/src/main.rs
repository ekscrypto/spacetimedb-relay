// SPDX-License-Identifier: MIT

mod stdb_mode;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use clap::{Parser, ValueEnum};
#[cfg(not(feature = "profile-heap"))]
use mimalloc::MiMalloc;
use tokio::sync::mpsc;
use tracing_subscriber::EnvFilter;
use url::Url;

// Heap-profiling builds replace mimalloc with dhat::Alloc so every
// allocation gets a backtrace. dhat is single-allocator: enabling
// `--features profile-heap` swaps the global allocator and starts a
// `Profiler` whose `Drop` writes `dhat-heap.json` on graceful shutdown.
#[cfg(feature = "profile-heap")]
#[global_allocator]
static GLOBAL: dhat::Alloc = dhat::Alloc;

#[cfg(not(feature = "profile-heap"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use relay_engine::Engine;
use relay_protocol::api_messages::websocket::common::{ByteListLen, RowListLen};
use relay_protocol::api_messages::websocket::v2::ServerMessage;
use relay_protocol::{decode_row, parse_schema, DecodedRow, MirroredSchema, MirroredType};
use relay_server::metrics::{Metrics, UpstreamMetrics};
use relay_server::ServerHandle;
use relay_storage::{Storage, StorageConfig};
use relay_upstream::{
    connect_and_run, fetch_schema, server_tag_name, Compression, ProtocolVersion, UpstreamCommand,
    UpstreamConfig, UpstreamEvent,
};

#[derive(Debug, Parser)]
#[command(name = "relay", version, about = "SpacetimeDB Relay")]
struct Args {
    /// Upstream SpacetimeDB host, e.g. wss://maincloud.spacetimedb.com
    #[arg(long, env = "RELAY_UPSTREAM")]
    upstream: Url,

    /// Upstream database name or identity (set via --database or RELAY_DATABASE).
    #[arg(long, env = "RELAY_DATABASE")]
    database: String,

    /// Optional bearer token for the upstream connection
    #[arg(long, env = "RELAY_UPSTREAM_TOKEN")]
    upstream_token: Option<String>,

    /// Filesystem directory for in-memory mirror snapshots. The relay
    /// writes a per-table file under `<data-dir>/<db_prefix>/` every
    /// `--snapshot-interval` and on graceful shutdown, then reloads
    /// them on the next startup so a restart doesn't need to refetch
    /// the whole dataset.
    #[arg(long, env = "RELAY_DATA_DIR", default_value = "data")]
    data_dir: PathBuf,

    /// How often the snapshotter persists the in-memory mirror to
    /// disk, in seconds. Snapshots also fire once on graceful shutdown.
    #[arg(
        long = "snapshot-interval",
        env = "RELAY_SNAPSHOT_INTERVAL",
        default_value_t = 60
    )]
    snapshot_interval_secs: u64,

    /// Address to bind the downstream WebSocket server
    #[arg(long, env = "RELAY_BIND", default_value = "0.0.0.0:3001")]
    bind: String,

    /// Tables to subscribe to upstream (`SELECT * FROM <table>`).
    /// Repeatable: `--subscribe-table message --subscribe-table user`
    #[arg(
        long = "subscribe-table",
        env = "RELAY_SUBSCRIBE_TABLES",
        value_delimiter = ','
    )]
    subscribe_tables: Vec<String>,

    /// Stop after N upstream frames (useful for smoke-testing)
    #[arg(long, env = "RELAY_FRAME_LIMIT")]
    frame_limit: Option<u64>,

    /// SpacetimeDB WebSocket subprotocol version of the upstream.
    /// `v2` (default) targets current SpacetimeDB. `v1` targets pre-2.0
    /// servers still on `v1.bsatn.spacetimedb`; v1 messages are
    /// translated to v2 internally.
    #[arg(long = "upstream-protocol", env = "RELAY_UPSTREAM_PROTOCOL", default_value_t = ProtocolVersion::V2)]
    upstream_protocol: ProtocolVersion,

    /// Mirror backend. `legacy` is the in-process MemStore + custom
    /// downstream WS server. `stdb` runs an embedded SpacetimeDB and
    /// publishes a generated mirror module to it; downstream clients
    /// connect directly to that local SpacetimeDB.
    #[arg(long = "mirror-mode", env = "RELAY_MIRROR_MODE", default_value_t = MirrorMode::Legacy, value_enum)]
    mirror_mode: MirrorMode,

    /// stdb mode: local SpacetimeDB URL the relay publishes the mirror
    /// module to and connects to as the writer.
    #[arg(long = "stdb-url", env = "RELAY_STDB_URL", default_value = "ws://127.0.0.1:3000")]
    stdb_url: Url,

    /// stdb mode: spacetime CLI server alias (run `spacetime server add ...` once).
    #[arg(long = "stdb-server-alias", env = "RELAY_STDB_SERVER_ALIAS", default_value = "local")]
    stdb_server_alias: String,

    /// stdb mode: name of the database to publish under.
    #[arg(long = "mirror-database", env = "RELAY_MIRROR_DATABASE")]
    mirror_database: Option<String>,

    /// stdb mode: bearer token for the writer identity. Should match
    /// the identity that ran `spacetime publish` so the wasm
    /// `assert_writer` gate accepts our calls.
    #[arg(long = "stdb-identity-token", env = "RELAY_STDB_IDENTITY_TOKEN")]
    stdb_identity_token: Option<String>,

    /// stdb mode: where to materialize the generated mirror crate.
    /// Defaults to `<data-dir>/mirror-publisher`.
    #[arg(long = "publisher-workdir", env = "RELAY_PUBLISHER_WORKDIR")]
    publisher_workdir: Option<PathBuf>,

    /// stdb mode: source directory holding `Cargo.toml` and
    /// `rust-toolchain.toml` for the mirror crate (defaults to
    /// `tools/mirror-template/` next to the relay binary).
    #[arg(long = "publisher-template-dir", env = "RELAY_PUBLISHER_TEMPLATE_DIR")]
    publisher_template_dir: Option<PathBuf>,

    /// stdb mode: path to the Python codegen script (defaults to
    /// `tools/codegen.py`).
    #[arg(long = "codegen-script", env = "RELAY_CODEGEN_SCRIPT")]
    codegen_script: Option<PathBuf>,

    /// stdb mode: path to the `spacetime` CLI binary.
    #[arg(long = "spacetime-bin", env = "RELAY_SPACETIME_BIN", default_value = "spacetime")]
    spacetime_bin: PathBuf,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum MirrorMode {
    Legacy,
    Stdb,
}

impl std::fmt::Display for MirrorMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Legacy => "legacy",
            Self::Stdb => "stdb",
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    #[cfg(feature = "profile-heap")]
    let _dhat = dhat::Profiler::new_heap();

    // Required for `wss://` upstreams: rustls 0.23 makes the
    // CryptoProvider an explicit choice, and tokio-tungstenite panics
    // on the first TLS handshake if no provider has been installed.
    let _ = rustls::crypto::ring::default_provider().install_default();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    tracing::info!(
        target: "relay",
        upstream = %args.upstream,
        database = %args.database,
        bind = %args.bind,
        protocol = %args.upstream_protocol,
        subscribe_tables = ?args.subscribe_tables,
        "spacetimedb-relay starting"
    );

    let raw_schema = fetch_schema(&args.upstream, &args.database).await?;
    let schema = parse_schema(&raw_schema)?;
    log_schema(&schema);

    if matches!(args.mirror_mode, MirrorMode::Stdb) {
        let mirror_database = args
            .mirror_database
            .clone()
            .unwrap_or_else(|| format!("relay-mirror-{}", sanitize_db_name(&args.database)));
        let publisher_workdir = args
            .publisher_workdir
            .clone()
            .unwrap_or_else(|| args.data_dir.join("mirror-publisher"));
        let template_dir = args
            .publisher_template_dir
            .clone()
            .unwrap_or_else(|| default_repo_path("tools/mirror-template"));
        let codegen_script = args
            .codegen_script
            .clone()
            .unwrap_or_else(|| default_repo_path("tools/codegen.py"));
        let cfg = stdb_mode::StdbModeConfig {
            upstream_host: args.upstream.clone(),
            upstream_database: args.database.clone(),
            upstream_token: args.upstream_token.clone(),
            upstream_protocol: args.upstream_protocol,
            frame_limit: args.frame_limit,
            subscribe_tables: args.subscribe_tables.clone(),
            stdb_url: args.stdb_url.clone(),
            mirror_database,
            identity_token: args.stdb_identity_token.clone(),
            stdb_server_alias: args.stdb_server_alias.clone(),
            publisher_workdir,
            publisher_template_dir: template_dir,
            codegen_script,
            spacetime_bin: args.spacetime_bin.clone(),
        };
        return stdb_mode::run(cfg, raw_schema.into(), Arc::new(schema)).await;
    }

    let storage = Storage::new(StorageConfig {
        upstream_host: args.upstream.to_string(),
        upstream_database: args.database.clone(),
    });
    storage.sync_schema(&schema)?;
    match storage.load_snapshots(&args.data_dir) {
        Ok(stats) if stats.tables_loaded > 0 => tracing::info!(
            target: "relay",
            tables = stats.tables_loaded,
            rows = stats.rows_loaded,
            "loaded snapshots from disk"
        ),
        Ok(_) => tracing::info!(target: "relay", "no usable snapshots on disk"),
        Err(e) => tracing::warn!(
            target: "relay",
            error = %e,
            "failed to load snapshots; starting empty"
        ),
    }
    let storage = Arc::new(storage);
    let schema = Arc::new(schema);
    let engine = Arc::new(Engine::new(schema.clone()));

    let snapshotter = spawn_snapshotter(
        storage.clone(),
        args.data_dir.clone(),
        Duration::from_secs(args.snapshot_interval_secs),
    );

    let server_handle = ServerHandle::new();
    let metrics = Metrics::new();
    let bind_addr: std::net::SocketAddr = args
        .bind
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid --bind {}: {e}", args.bind))?;
    {
        let storage = storage.clone();
        let engine = engine.clone();
        let database = args.database.clone();
        let handle = server_handle.clone();
        let metrics = metrics.clone();
        tokio::spawn(async move {
            if let Err(e) =
                relay_server::serve(bind_addr, storage, engine, database, handle, metrics).await
            {
                tracing::error!(target: "relay::server", error = %e, "downstream server exited");
            }
        });
    }

    let mut subscribe_tables = args.subscribe_tables.clone();
    if subscribe_tables.is_empty() {
        subscribe_tables = schema
            .tables
            .iter()
            .filter(|t| matches!(t.kind, relay_protocol::TableKind::User))
            .filter(|t| matches!(t.access, relay_protocol::TableAccess::Public))
            .map(|t| t.name.clone())
            .collect();
        tracing::info!(
            target: "relay",
            ?subscribe_tables,
            "no --subscribe-table given; defaulting to all user-public tables"
        );
    }
    let frame_limit = args.frame_limit;
    let cfg = UpstreamConfig {
        host: args.upstream,
        database: args.database,
        auth_token: args.upstream_token,
        compression: Compression::None,
        connect_timeout: Duration::from_secs(10),
        protocol: args.upstream_protocol,
    };

    let shutdown = std::pin::pin!(shutdown_signal());
    let mut shutdown = shutdown;
    let mut frames = 0u64;

    // Reconnect with exponential backoff. The in-memory mirror, engine
    // state, and snapshotter all outlive a single upstream session, so
    // the dashboard keeps serving the last known data while we're
    // disconnected. Re-Subscribe is automatic: the upstream sends a
    // fresh InitialConnection on reconnect and handle_server_message
    // replies with Subscribe.
    //
    // Backoff resets only when a session stayed up for STABLE_THRESHOLD.
    // BitCraft's "accept-then-RST" auth-rejection takes <1s — treating
    // such flaps as success would hammer the upstream forever.
    //
    // Note: we don't refetch the schema across reconnects. The engine
    // captured an Arc<MirroredSchema> at construction; live reload
    // would need a bigger refactor. Mid-run schema drift will surface
    // as decode warnings; restart the relay to pick up the new shape.
    const BACKOFF_MAX_SECS: u64 = 30;
    const STABLE_THRESHOLD: Duration = Duration::from_secs(5);
    let mut backoff_secs: u64 = 1;

    enum InnerExit {
        Shutdown,
        UpstreamGone,
        FrameLimitReached,
    }

    'reconnect: loop {
        let (event_tx, mut event_rx) = mpsc::channel(256);
        let (cmd_tx, cmd_rx) = mpsc::channel(32);
        let attempt_cfg = cfg.clone();
        let upstream_handle = tokio::spawn(async move {
            if let Err(e) = connect_and_run(attempt_cfg, event_tx, cmd_rx).await {
                tracing::error!(target: "relay::upstream", error = %e, "upstream task ended with error");
            }
        });

        let mut connected_at: Option<std::time::Instant> = None;
        let exit_reason = loop {
            let event = tokio::select! {
                biased;
                _ = shutdown.as_mut() => break InnerExit::Shutdown,
                event = event_rx.recv() => match event {
                    Some(e) => e,
                    None => break InnerExit::UpstreamGone,
                },
            };
            match event {
                UpstreamEvent::Connected => {
                    metrics.upstream.set_connected();
                    connected_at = Some(std::time::Instant::now());
                    tracing::info!(target: "relay", "upstream connected");
                }
                UpstreamEvent::Frame(frame) => {
                    frames += 1;
                    metrics.upstream.record_frame(frame.bsatn.len() as u64);
                    let tag = frame.server_tag();
                    let protocol = frame.protocol;
                    match frame.decode() {
                        Ok(message) => {
                            handle_server_message(
                                message,
                                &subscribe_tables,
                                &cmd_tx,
                                &storage,
                                &schema,
                                &engine,
                                &server_handle,
                                &metrics.upstream,
                            )
                            .await?
                        }
                        Err(e) => tracing::warn!(
                            target: "relay",
                            tag,
                            kind = server_tag_name(tag, protocol),
                            error = %e,
                            "failed to decode ServerMessage"
                        ),
                    }
                    if let Some(limit) = frame_limit {
                        if frames >= limit {
                            tracing::info!(target: "relay", frames, "frame limit reached, exiting");
                            let _ = cmd_tx.send(UpstreamCommand::Shutdown).await;
                            break InnerExit::FrameLimitReached;
                        }
                    }
                }
                UpstreamEvent::Ping => {
                    metrics.upstream.record_ping();
                }
                UpstreamEvent::Disconnected { reason } => {
                    metrics.upstream.set_disconnected();
                    tracing::warn!(target: "relay", %reason, "upstream disconnected");
                    break InnerExit::UpstreamGone;
                }
            }
        };

        metrics.upstream.set_disconnected();
        let _ = cmd_tx.send(UpstreamCommand::Shutdown).await;
        drop(cmd_tx);
        let _ = upstream_handle.await;

        match exit_reason {
            InnerExit::Shutdown => {
                tracing::info!(target: "relay", "received shutdown signal");
                break 'reconnect;
            }
            InnerExit::FrameLimitReached => break 'reconnect,
            InnerExit::UpstreamGone => {
                let stayed_up = connected_at.map(|t| t.elapsed()).unwrap_or_default();
                if stayed_up >= STABLE_THRESHOLD {
                    backoff_secs = 1;
                }
                let sleep_for = Duration::from_secs(backoff_secs);
                backoff_secs = (backoff_secs * 2).min(BACKOFF_MAX_SECS);
                tracing::warn!(
                    target: "relay",
                    backoff_secs = sleep_for.as_secs(),
                    stayed_up_ms = stayed_up.as_millis() as u64,
                    "upstream gone — reconnecting after backoff"
                );
                tokio::select! {
                    _ = shutdown.as_mut() => {
                        tracing::info!(target: "relay", "received shutdown signal during backoff");
                        break 'reconnect;
                    }
                    _ = tokio::time::sleep(sleep_for) => {}
                }
            }
        }
    }

    snapshotter.shutdown().await;
    Ok(())
}

struct SnapshotterHandle {
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    join: tokio::task::JoinHandle<()>,
}

impl SnapshotterHandle {
    async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        let _ = self.join.await;
    }
}

fn spawn_snapshotter(
    storage: Arc<Storage>,
    data_dir: PathBuf,
    interval: Duration,
) -> SnapshotterHandle {
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
    let join = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        ticker.tick().await; // skip the immediate first tick
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    write_snapshots_blocking(&storage, &data_dir).await;
                }
                _ = &mut shutdown_rx => {
                    tracing::info!(target: "relay::storage", "snapshotter: writing final snapshot");
                    write_snapshots_blocking(&storage, &data_dir).await;
                    break;
                }
            }
        }
    });
    SnapshotterHandle {
        shutdown: Some(shutdown_tx),
        join,
    }
}

async fn write_snapshots_blocking(storage: &Arc<Storage>, data_dir: &std::path::Path) {
    let storage = storage.clone();
    let data_dir = data_dir.to_path_buf();
    let res = tokio::task::spawn_blocking(move || storage.write_snapshots(&data_dir)).await;
    match res {
        Ok(Ok(stats)) => {
            if stats.tables_written > 0 {
                tracing::info!(
                    target: "relay::storage",
                    tables = stats.tables_written,
                    rows = stats.rows_written,
                    "snapshot written"
                );
            }
        }
        Ok(Err(e)) => tracing::warn!(target: "relay::storage", error = %e, "snapshot write failed"),
        Err(e) => tracing::error!(target: "relay::storage", error = %e, "snapshot task panicked"),
    }
}

fn sanitize_db_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for c in name.chars() {
        if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
            out.push(c);
        } else {
            out.push('-');
        }
    }
    if out.is_empty() {
        return "mirror".into();
    }
    out
}

fn default_repo_path(rel: &str) -> PathBuf {
    // CARGO_MANIFEST_DIR is `crates/relay`; the repo root is two up.
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest
        .parent()
        .and_then(|p| p.parent())
        .map(|root| root.join(rel))
        .unwrap_or_else(|| PathBuf::from(rel))
}

/// Resolve the first SIGINT or SIGTERM into a future the main loop can
/// `.await`. Falls back to ctrl_c-only on non-Unix.
pub(crate) async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(target: "relay", error = %e, "SIGTERM listener failed; using ctrl-c only");
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

async fn apply_transaction_update(
    tu: &relay_protocol::api_messages::websocket::v2::TransactionUpdate,
    storage: &Arc<Storage>,
    schema: &Arc<MirroredSchema>,
    engine: &Arc<Engine>,
    server_handle: &ServerHandle,
    upstream_metrics: &UpstreamMetrics,
) {
    use relay_protocol::api_messages::websocket::v2::TableUpdateRows;

    for set in tu.query_sets.iter() {
        for table in set.tables.iter() {
            let upstream_name: &str = table.table_name.as_ref();
            let Some(table_meta) = schema.tables.iter().find(|t| t.name == upstream_name) else {
                tracing::warn!(target: "relay", table = %upstream_name, "transaction update for unknown table");
                continue;
            };
            let Some(fields) = schema.table_product(table_meta) else {
                continue;
            };

            // Cells are only read by `engine.route_table_diff`. If no
            // downstream client subscribes to this table, skip the
            // BSATN→Cell decode entirely — storage's `pk_key` walks
            // raw bytes. Big CPU win on high-churn tables nobody
            // currently watches (typical for a freshly-started relay).
            let decode_cells = engine.has_table_subscribers(upstream_name);
            let decode_or_empty = |r: &bytes::Bytes| -> Vec<relay_protocol::Cell> {
                if decode_cells {
                    decode_row(r, fields, schema).unwrap_or_default()
                } else {
                    Vec::new()
                }
            };

            let mut deletes: Vec<DecodedRow> = Vec::new();
            let mut inserts_rows: Vec<DecodedRow> = Vec::new();
            for rows in table.rows.iter() {
                match rows {
                    TableUpdateRows::PersistentTable(p) => {
                        for r in p.deletes.into_iter() {
                            let cells = decode_or_empty(&r);
                            deletes.push(DecodedRow { cells, bsatn: r });
                        }
                        for r in p.inserts.into_iter() {
                            let cells = decode_or_empty(&r);
                            inserts_rows.push(DecodedRow { cells, bsatn: r });
                        }
                    }
                    TableUpdateRows::EventTable(e) => {
                        for r in e.events.into_iter() {
                            let cells = decode_or_empty(&r);
                            inserts_rows.push(DecodedRow { cells, bsatn: r });
                        }
                    }
                }
            }
            if deletes.is_empty() && inserts_rows.is_empty() {
                continue;
            }
            upstream_metrics.record_rows(inserts_rows.len() as u64, deletes.len() as u64);
            match storage.apply_diff(upstream_name, &deletes, &inserts_rows) {
                Ok(o) => tracing::debug!(
                    target: "relay",
                    table = %upstream_name,
                    deleted = o.deleted,
                    inserted = o.inserted,
                    "diff applied"
                ),
                Err(e) => tracing::error!(
                    target: "relay",
                    table = %upstream_name,
                    error = %e,
                    "diff apply failed"
                ),
            }

            let routed = engine.route_table_diff(upstream_name, &deletes, &inserts_rows);
            for diff in routed {
                server_handle.deliver(diff);
            }
        }
    }
}

async fn reconcile_table_snapshot(
    upstream_table: &str,
    rows: &relay_protocol::api_messages::websocket::common::BsatnRowList,
    storage: &std::sync::Arc<Storage>,
    schema: &std::sync::Arc<MirroredSchema>,
    engine: &std::sync::Arc<Engine>,
    server_handle: &ServerHandle,
    upstream_metrics: &UpstreamMetrics,
) -> Result<()> {
    let table_meta = schema
        .tables
        .iter()
        .find(|t| t.name == upstream_table)
        .ok_or_else(|| anyhow::anyhow!("upstream table {upstream_table} missing from schema"))?;
    let fields = schema
        .table_product(table_meta)
        .ok_or_else(|| anyhow::anyhow!("upstream table {upstream_table} has no product type"))?;

    let mut decoded: Vec<DecodedRow> = Vec::with_capacity(rows.len());
    let mut decode_failures = 0usize;
    for row_bytes in rows {
        match decode_row(&row_bytes, fields, schema) {
            Ok(cells) => decoded.push(DecodedRow {
                cells,
                bsatn: row_bytes,
            }),
            Err(e) => {
                decode_failures += 1;
                tracing::warn!(
                    target: "relay",
                    table = %upstream_table,
                    error = %e,
                    "row decode failed; skipping"
                );
            }
        }
    }

    let diff = storage.apply_snapshot_diff(upstream_table, &decoded, fields, schema)?;
    upstream_metrics.record_rows(diff.inserts.len() as u64, diff.deletes.len() as u64);
    tracing::info!(
        target: "relay",
        table = %upstream_table,
        snapshot_rows = decoded.len(),
        deleted = diff.deletes.len(),
        inserted = diff.inserts.len(),
        decode_failures,
        "snapshot reconciled"
    );

    if !diff.deletes.is_empty() || !diff.inserts.is_empty() {
        let routed = engine.route_table_diff(upstream_table, &diff.deletes, &diff.inserts);
        for client_diff in routed {
            server_handle.deliver(client_diff);
        }
    }
    Ok(())
}

fn log_schema(schema: &MirroredSchema) {
    tracing::info!(
        target: "relay",
        fingerprint = %schema.fingerprint_hex(),
        n_typespace = schema.typespace.len(),
        n_tables = schema.tables.len(),
        "schema loaded"
    );
    for table in &schema.tables {
        let columns = schema.table_product(table).unwrap_or(&[]);
        let cols: Vec<String> = columns
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let pk = if table.primary_key.contains(&(i as u16)) {
                    " PK"
                } else {
                    ""
                };
                let name = f.name.as_deref().unwrap_or("<unnamed>");
                format!("{name}: {}{pk}", describe_type(&f.ty, schema))
            })
            .collect();
        tracing::info!(
            target: "relay",
            table = %table.name,
            access = ?table.access,
            kind = ?table.kind,
            columns = %cols.join(", "),
            "  table"
        );
    }
}

fn describe_type(ty: &MirroredType, schema: &MirroredSchema) -> String {
    match schema.resolve(ty) {
        MirroredType::Bool => "Bool".into(),
        MirroredType::I8 => "I8".into(),
        MirroredType::I16 => "I16".into(),
        MirroredType::I32 => "I32".into(),
        MirroredType::I64 => "I64".into(),
        MirroredType::I128 => "I128".into(),
        MirroredType::I256 => "I256".into(),
        MirroredType::U8 => "U8".into(),
        MirroredType::U16 => "U16".into(),
        MirroredType::U32 => "U32".into(),
        MirroredType::U64 => "U64".into(),
        MirroredType::U128 => "U128".into(),
        MirroredType::U256 => "U256".into(),
        MirroredType::F32 => "F32".into(),
        MirroredType::F64 => "F64".into(),
        MirroredType::String => "String".into(),
        MirroredType::Product(fields) => format!(
            "{{{}}}",
            fields
                .iter()
                .map(|f| {
                    let n = f.name.as_deref().unwrap_or("_");
                    format!("{n}: {}", describe_type(&f.ty, schema))
                })
                .collect::<Vec<_>>()
                .join(", ")
        ),
        MirroredType::Sum(variants) => format!(
            "<{}>",
            variants
                .iter()
                .map(|v| {
                    let n = v.name.as_deref().unwrap_or("_");
                    format!("{n}: {}", describe_type(&v.ty, schema))
                })
                .collect::<Vec<_>>()
                .join(" | ")
        ),
        MirroredType::Array(inner) => format!("[{}]", describe_type(inner, schema)),
        MirroredType::Ref(n) => format!("Ref({n})"),
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_server_message(
    message: ServerMessage,
    subscribe_tables: &[String],
    cmd_tx: &mpsc::Sender<UpstreamCommand>,
    storage: &Arc<Storage>,
    schema: &Arc<MirroredSchema>,
    engine: &Arc<Engine>,
    server_handle: &ServerHandle,
    upstream_metrics: &UpstreamMetrics,
) -> Result<()> {
    match message {
        ServerMessage::InitialConnection(ic) => {
            tracing::info!(
                target: "relay",
                identity = %ic.identity.to_hex().as_str(),
                connection_id = %ic.connection_id.to_hex().as_str(),
                token_len = ic.token.len(),
                "InitialConnection"
            );
            if !subscribe_tables.is_empty() {
                let queries: Vec<String> = subscribe_tables
                    .iter()
                    .map(|t| format!("SELECT * FROM {t}"))
                    .collect();
                tracing::info!(target: "relay", ?queries, "sending Subscribe");
                cmd_tx
                    .send(UpstreamCommand::Subscribe {
                        request_id: 1,
                        query_set_id: 1,
                        queries,
                    })
                    .await
                    .map_err(|_| anyhow::anyhow!("upstream command channel closed"))?;
            }
        }
        ServerMessage::SubscribeApplied(sa) => {
            tracing::info!(
                target: "relay",
                request_id = sa.request_id,
                query_set_id = sa.query_set_id.id,
                n_tables = sa.rows.tables.len(),
                "SubscribeApplied"
            );
            for table in sa.rows.tables.iter() {
                let upstream_name: &str = table.table.as_ref();
                tracing::info!(
                    target: "relay",
                    table = %upstream_name,
                    rows = table.rows.len(),
                    bytes = table.rows.num_bytes(),
                    "  table"
                );
                if let Err(e) = reconcile_table_snapshot(
                    upstream_name,
                    &table.rows,
                    storage,
                    schema,
                    engine,
                    server_handle,
                    upstream_metrics,
                )
                .await
                {
                    tracing::error!(
                        target: "relay",
                        table = %upstream_name,
                        error = %e,
                        "failed to reconcile snapshot"
                    );
                }
            }
        }
        ServerMessage::TransactionUpdate(tu) => {
            tracing::debug!(
                target: "relay",
                n_query_sets = tu.query_sets.len(),
                "TransactionUpdate"
            );
            apply_transaction_update(
                &tu,
                storage,
                schema,
                engine,
                server_handle,
                upstream_metrics,
            )
            .await;
        }
        ServerMessage::ReducerResult(rr) => {
            tracing::info!(
                target: "relay",
                request_id = rr.request_id,
                ?rr.result,
                "ReducerResult"
            );
        }
        ServerMessage::SubscriptionError(err) => {
            tracing::error!(
                target: "relay",
                request_id = ?err.request_id,
                query_set_id = err.query_set_id.id,
                error = %err.error,
                "SubscriptionError"
            );
        }
        ServerMessage::UnsubscribeApplied(_)
        | ServerMessage::OneOffQueryResult(_)
        | ServerMessage::ProcedureResult(_) => {
            tracing::info!(target: "relay", ?message, "ServerMessage");
        }
    }
    Ok(())
}
