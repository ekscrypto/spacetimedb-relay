// SPDX-License-Identifier: MIT

mod dashboard;
mod stdb_mode;
mod stdb_spawn;

use relay_coordinator::CoordinatorClient;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};
use url::Url;

// Heap-profiling builds use dhat::Alloc so every allocation gets a
// backtrace. dhat is single-allocator: enabling `--features
// profile-heap` swaps the global allocator and starts a `Profiler`
// whose `Drop` writes `dhat-heap.json` on graceful shutdown.
// Default builds use the system allocator.
#[cfg(feature = "profile-heap")]
#[global_allocator]
static GLOBAL: dhat::Alloc = dhat::Alloc;

use relay_protocol::{parse_schema, MirroredSchema, MirroredType};
use relay_upstream::{fetch_schema, ProtocolVersion};

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

    /// Working directory the relay uses for state that's safe to lose
    /// (the publisher's mirror crate workdir lives under here by
    /// default). The mirrored data itself is owned by the local
    /// SpacetimeDB process; we never persist row data here.
    #[arg(long, env = "RELAY_DATA_DIR", default_value = "data")]
    data_dir: PathBuf,

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

    /// Split the upstream subscription across N parallel WS connections
    /// of at most this many tables each. Useful when a single
    /// 250-table InitialSubscription gets killed by middlebox / server
    /// timeouts during the (multi-minute, multi-hundred-MB) initial
    /// dump. `0` (default) means no chunking — one connection
    /// subscribes to all tables.
    #[arg(
        long = "subscribe-chunk-size",
        env = "RELAY_SUBSCRIBE_CHUNK_SIZE",
        default_value_t = 0
    )]
    subscribe_chunk_size: usize,

    /// SpacetimeDB WebSocket subprotocol version of the upstream.
    /// `v2` (default) targets current SpacetimeDB. `v1` targets pre-2.0
    /// servers still on `v1.bsatn.spacetimedb`; v1 messages are
    /// translated to v2 internally.
    #[arg(long = "upstream-protocol", env = "RELAY_UPSTREAM_PROTOCOL", default_value_t = ProtocolVersion::V2)]
    upstream_protocol: ProtocolVersion,

    /// Local SpacetimeDB URL the relay publishes the mirror module to
    /// and connects to as the writer.
    #[arg(
        long = "stdb-url",
        env = "RELAY_STDB_URL",
        default_value = "ws://127.0.0.1:3000"
    )]
    stdb_url: Url,

    /// spacetime CLI server alias (run `spacetime server add ...` once
    /// to register the local SpacetimeDB before running the relay).
    /// Ignored when `--stdb-spawn` is set; the spawned instance's HTTP
    /// URL is used directly.
    #[arg(
        long = "stdb-server-alias",
        env = "RELAY_STDB_SERVER_ALIAS",
        default_value = "local"
    )]
    stdb_server_alias: String,

    /// When set, the relay spawns its own `spacetime start` child process
    /// (using `--spacetime-bin`) on a free loopback port and manages its
    /// lifecycle. Each relay instance gets an isolated SpacetimeDB with
    /// data in `<data-dir>/stdb/`. Overrides `--stdb-url` and
    /// `--stdb-server-alias`. Requires SpacetimeDB CLI ≥ 2.x on PATH or
    /// pointed at by `--spacetime-bin`.
    #[arg(long = "stdb-spawn", env = "RELAY_STDB_SPAWN", default_value_t = false)]
    stdb_spawn: bool,

    /// Mirror database name. Defaults to a sanitized form of
    /// `relay-mirror-<upstream-database>`.
    #[arg(long = "mirror-database", env = "RELAY_MIRROR_DATABASE")]
    mirror_database: Option<String>,

    /// Bearer token for the writer identity. The same identity that
    /// runs `spacetime publish` (the spacetime CLI's logged-in
    /// identity) must be used at runtime — the wasm `assert_writer`
    /// gate compares `ctx.sender()` to the identity captured at module
    /// init.
    #[arg(long = "stdb-identity-token", env = "RELAY_STDB_IDENTITY_TOKEN")]
    stdb_identity_token: Option<String>,

    /// Where to materialize the generated mirror crate. Defaults to
    /// `<data-dir>/mirror-publisher`.
    #[arg(long = "publisher-workdir", env = "RELAY_PUBLISHER_WORKDIR")]
    publisher_workdir: Option<PathBuf>,

    /// Source directory holding `Cargo.toml` and `rust-toolchain.toml`
    /// for the mirror crate (defaults to `tools/mirror-template/`
    /// resolved against the relay binary's CARGO_MANIFEST_DIR).
    #[arg(long = "publisher-template-dir", env = "RELAY_PUBLISHER_TEMPLATE_DIR")]
    publisher_template_dir: Option<PathBuf>,

    /// Path to the Python codegen script (defaults to
    /// `tools/codegen.py`).
    #[arg(long = "codegen-script", env = "RELAY_CODEGEN_SCRIPT")]
    codegen_script: Option<PathBuf>,

    /// Path to the `spacetime` CLI binary.
    #[arg(
        long = "spacetime-bin",
        env = "RELAY_SPACETIME_BIN",
        default_value = "spacetime"
    )]
    spacetime_bin: PathBuf,

    /// Unix socket path of the relay-coordinator daemon. When set the
    /// relay acquires a permit from the coordinator before each stdb
    /// reconnect and releases it once the initial sequential subscribe
    /// completes. Absent = uncoordinated (stdb backoff only).
    #[arg(long = "coordinator-socket", env = "RELAY_COORDINATOR_SOCKET")]
    coordinator_socket: Option<PathBuf>,

    /// Bind address for the in-process dashboard (HTML + /metrics JSON).
    /// Empty string disables the dashboard.
    #[arg(
        long = "dashboard-bind",
        env = "RELAY_DASHBOARD_BIND",
        default_value = "127.0.0.1:3001"
    )]
    dashboard_bind: String,

    /// File where the relay persists the local-stdb identity token
    /// captured from `InitialConnection`. Loaded on startup so the
    /// relay reconnects as the same identity (and thus the bound
    /// writer) across restarts. Defaults to
    /// `<data-dir>/relay-stdb-identity.token`.
    #[arg(long = "identity-token-file", env = "RELAY_IDENTITY_TOKEN_FILE")]
    identity_token_file: Option<PathBuf>,

    /// Public-facing WebSocket bind address for the frontend proxy.
    /// Downstream clients connect here; the proxy forwards each
    /// connection to `--stdb-url` (loopback) and — for v1 clients —
    /// rewrites `relay_apply_<table>` `TransactionUpdate`s so they
    /// look like upstream's. Empty string disables the frontend; in
    /// that case downstream clients must connect directly to the
    /// local SpacetimeDB.
    #[arg(
        long = "frontend-bind",
        env = "RELAY_FRONTEND_BIND",
        default_value = "0.0.0.0:3009"
    )]
    frontend_bind: String,

    /// Maximum number of concurrent downstream clients on the
    /// frontend listener. Connections beyond this cap are dropped at
    /// accept time.
    #[arg(
        long = "frontend-max-clients",
        env = "RELAY_FRONTEND_MAX_CLIENTS",
        default_value_t = 1024
    )]
    frontend_max_clients: usize,

    /// How long the frontend waits between WS pings on idle client
    /// connections (seconds). Smaller values keep NAT/middleboxes
    /// from dropping idle TCP flows.
    #[arg(
        long = "frontend-idle-secs",
        env = "RELAY_FRONTEND_IDLE_SECS",
        default_value_t = 30
    )]
    frontend_idle_secs: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    #[cfg(feature = "profile-heap")]
    let _dhat = dhat::Profiler::new_heap();

    // Required for `wss://` upstreams: rustls 0.23 makes the
    // CryptoProvider an explicit choice, and tokio-tungstenite panics
    // on the first TLS handshake if no provider has been installed.
    let _ = rustls::crypto::ring::default_provider().install_default();

    // Two layers: an `fmt` layer for stderr (respects `RUST_LOG`) and
    // an `EventLogLayer` that always captures `relay=debug` into the
    // dashboard's in-process ring buffer. The dashboard view exists
    // precisely so we can see debug-level events without re-running
    // with a louder `RUST_LOG`.
    // 50K capacity is enough to hold ~12 minutes of BitCraft live
    // traffic at the observed ~64 events/sec frame rate without
    // evicting earlier milestones (Subscribe, SubscribeApplied, etc.).
    let event_ring = dashboard::EventRing::new(50_000);
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")));
    let event_layer = dashboard::EventLogLayer::new(event_ring.clone())
        .with_filter(EnvFilter::new("relay=debug"));
    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(event_layer)
        .init();

    let args = Args::parse();

    // Resolve the local-stdb URL and server reference (alias or HTTP URL
    // for `spacetime publish -s`) — either by spawning our own instance or
    // using the caller-supplied --stdb-url / --stdb-server-alias pair.
    // The StdbProcess handle must stay alive for the duration of main so
    // the child is only killed after stdb_mode::run returns.
    let (stdb_url, stdb_server_ref, _stdb_process);
    if args.stdb_spawn {
        let (url, http_base, proc, is_fresh) =
            stdb_spawn::spawn(&args.spacetime_bin, &args.data_dir).await?;
        stdb_url = url;
        stdb_server_ref = http_base;
        _stdb_process = Some(proc);
        // A fresh stdb data dir means the instance has no databases yet.
        // Delete the publisher fingerprint so publish_if_drifted always
        // republishes rather than skipping because the hash matches a
        // previous run against a different (shared) stdb instance.
        if is_fresh {
            let fp = args
                .publisher_workdir
                .clone()
                .unwrap_or_else(|| args.data_dir.join("mirror-publisher"))
                .join("fingerprint.json");
            if fp.exists() {
                tracing::info!(
                    target: "relay::stdb_spawn",
                    path = %fp.display(),
                    "fresh stdb data dir — deleting stale publisher fingerprint"
                );
                let _ = std::fs::remove_file(&fp);
            }
        }
    } else {
        stdb_url = args.stdb_url.clone();
        stdb_server_ref = args.stdb_server_alias.clone();
        _stdb_process = None;
    }

    tracing::info!(
        target: "relay",
        upstream = %args.upstream,
        database = %args.database,
        protocol = %args.upstream_protocol,
        stdb_url = %stdb_url,
        stdb_spawn = args.stdb_spawn,
        subscribe_tables = ?args.subscribe_tables,
        "spacetimedb-relay starting"
    );

    let raw_schema: Arc<[u8]> = Arc::from(fetch_schema(&args.upstream, &args.database).await?);
    let schema = parse_schema(&raw_schema)?;
    log_schema(&schema);

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
    let identity_token_file = args
        .identity_token_file
        .clone()
        .unwrap_or_else(|| args.data_dir.join("relay-stdb-identity.token"));

    // Default in-flight cap matches relay-mirror-driver's default; we
    // remember it here so the dashboard can show "used / max".
    const DEFAULT_MAX_IN_FLIGHT: u64 = 8000;
    let metrics = dashboard::Metrics::new(
        args.database.clone(),
        mirror_database.clone(),
        DEFAULT_MAX_IN_FLIGHT,
        event_ring,
    );

    // The relay-mirror-driver records (request_id, UpstreamReducerMeta)
    // here for every CallReducer it sends; the frontend proxy reads
    // entries to synthesise full v1 TransactionUpdates from local
    // stdb's TransactionUpdateLight broadcasts. Sweep periodically to
    // bound memory.
    let meta_registry = relay_mirror_driver::MetaRegistry::new();
    {
        let r = meta_registry.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(10));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                let evicted = r.sweep(relay_mirror_driver::MetaRegistry::DEFAULT_MAX_AGE);
                if evicted > 0 {
                    tracing::debug!(
                        target: "relay::meta_registry",
                        evicted,
                        live = r.len(),
                        "swept stale entries"
                    );
                }
            }
        });
    }

    if !args.frontend_bind.trim().is_empty() {
        match args.frontend_bind.parse::<std::net::SocketAddr>() {
            Ok(bind) => {
                let frontend_metrics =
                    relay_frontend::FrontendMetrics::new(args.frontend_bind.clone());
                let active_clients = relay_frontend::ActiveClients::new();
                metrics.install_frontend(dashboard::FrontendHandles {
                    metrics: frontend_metrics.clone(),
                    clients: active_clients.clone(),
                });
                let cfg = relay_frontend::Config {
                    bind,
                    local_url: stdb_url.clone(),
                    local_database: args
                        .mirror_database
                        .clone()
                        .unwrap_or_else(|| mirror_database.clone()),
                    local_token: None,
                    max_clients: args.frontend_max_clients,
                    idle_timeout: std::time::Duration::from_secs(args.frontend_idle_secs),
                    meta_registry: Some(meta_registry.clone()),
                    // Expose the cached upstream schema as plain HTTP on
                    // the frontend port so tokenless clients can
                    // discover the row shape the mirror is serving.
                    schema: Some(raw_schema.clone()),
                };
                tokio::spawn(async move {
                    if let Err(e) = relay_frontend::run(cfg, frontend_metrics, active_clients).await
                    {
                        tracing::error!(
                            target: "relay::frontend",
                            error = %e,
                            "frontend listener exited"
                        );
                    }
                });
            }
            Err(e) => tracing::warn!(
                target: "relay",
                bind = %args.frontend_bind,
                error = %e,
                "invalid --frontend-bind; frontend disabled"
            ),
        }
    }

    if !args.dashboard_bind.trim().is_empty() {
        match args.dashboard_bind.parse::<std::net::SocketAddr>() {
            Ok(bind) => {
                let m = metrics.clone();
                tokio::spawn(async move {
                    if let Err(e) = dashboard::serve(bind, m).await {
                        tracing::error!(target: "relay::dashboard", error = %e, "dashboard exited");
                    }
                });
            }
            Err(e) => tracing::warn!(
                target: "relay",
                bind = %args.dashboard_bind,
                error = %e,
                "invalid --dashboard-bind; dashboard disabled"
            ),
        }
    }

    let coordinator = args.coordinator_socket.map(|path| {
        CoordinatorClient::new(path, mirror_database.clone())
    });

    let cfg = stdb_mode::StdbModeConfig {
        upstream_host: args.upstream,
        upstream_database: args.database,
        upstream_token: args.upstream_token,
        upstream_protocol: args.upstream_protocol,
        frame_limit: args.frame_limit,
        subscribe_tables: args.subscribe_tables,
        subscribe_chunk_size: args.subscribe_chunk_size,
        stdb_url,
        mirror_database,
        identity_token: args.stdb_identity_token,
        identity_token_file,
        stdb_server_alias: stdb_server_ref,
        coordinator,
        publisher_workdir,
        publisher_template_dir: template_dir,
        codegen_script,
        spacetime_bin: args.spacetime_bin,
    };
    stdb_mode::run(
        cfg,
        raw_schema.clone(),
        Arc::new(schema),
        metrics,
        meta_registry,
    )
    .await
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
