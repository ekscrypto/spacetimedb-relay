// SPDX-License-Identifier: MIT

mod dashboard;
mod stdb_mode;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
#[cfg(not(feature = "profile-heap"))]
use mimalloc::MiMalloc;
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

    /// SpacetimeDB WebSocket subprotocol version of the upstream.
    /// `v2` (default) targets current SpacetimeDB. `v1` targets pre-2.0
    /// servers still on `v1.bsatn.spacetimedb`; v1 messages are
    /// translated to v2 internally.
    #[arg(long = "upstream-protocol", env = "RELAY_UPSTREAM_PROTOCOL", default_value_t = ProtocolVersion::V2)]
    upstream_protocol: ProtocolVersion,

    /// Local SpacetimeDB URL the relay publishes the mirror module to
    /// and connects to as the writer.
    #[arg(long = "stdb-url", env = "RELAY_STDB_URL", default_value = "ws://127.0.0.1:3000")]
    stdb_url: Url,

    /// spacetime CLI server alias (run `spacetime server add ...` once
    /// to register the local SpacetimeDB before running the relay).
    #[arg(long = "stdb-server-alias", env = "RELAY_STDB_SERVER_ALIAS", default_value = "local")]
    stdb_server_alias: String,

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
    #[arg(long = "spacetime-bin", env = "RELAY_SPACETIME_BIN", default_value = "spacetime")]
    spacetime_bin: PathBuf,

    /// Bind address for the in-process dashboard (HTML + /metrics JSON).
    /// Empty string disables the dashboard.
    #[arg(long = "dashboard-bind", env = "RELAY_DASHBOARD_BIND", default_value = "127.0.0.1:3001")]
    dashboard_bind: String,
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
        protocol = %args.upstream_protocol,
        stdb_url = %args.stdb_url,
        subscribe_tables = ?args.subscribe_tables,
        "spacetimedb-relay starting"
    );

    let raw_schema = fetch_schema(&args.upstream, &args.database).await?;
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

    // Default in-flight cap matches relay-mirror-driver's default; we
    // remember it here so the dashboard can show "used / max".
    const DEFAULT_MAX_IN_FLIGHT: u64 = 8000;
    let metrics = dashboard::Metrics::new(
        args.database.clone(),
        mirror_database.clone(),
        DEFAULT_MAX_IN_FLIGHT,
    );

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

    let cfg = stdb_mode::StdbModeConfig {
        upstream_host: args.upstream,
        upstream_database: args.database,
        upstream_token: args.upstream_token,
        upstream_protocol: args.upstream_protocol,
        frame_limit: args.frame_limit,
        subscribe_tables: args.subscribe_tables,
        stdb_url: args.stdb_url,
        mirror_database,
        identity_token: args.stdb_identity_token,
        stdb_server_alias: args.stdb_server_alias,
        publisher_workdir,
        publisher_template_dir: template_dir,
        codegen_script,
        spacetime_bin: args.spacetime_bin,
    };
    stdb_mode::run(cfg, raw_schema.into(), Arc::new(schema), metrics).await
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
