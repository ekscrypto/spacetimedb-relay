// SPDX-License-Identifier: MIT

//! `relay-cache` — same-host in-memory read cache over the relay fleet.
//!
//! Holds one long-lived v2 subscription per region frontend on loopback,
//! decodes BSATN rows into columnar in-memory storage (no JSON hop on the
//! read path), and serves HTTP queries on loopback (JSON default;
//! protobuf via `Accept: application/x-protobuf`):
//!   1. claim by PK / name substring (PK includes supplies/upkeep/tier)
//!   2. claim inventory rollup by building + dimension
//!   3. claim members / citizens / hexcoins
//!   4. player by PK / name substring
//!   5. player personal inventories (categorized bags)
//!   6. player housing (first house + interior buildings)
//!   7. player skills (XP → level)
//!   8. claim / player crafts (progressive + passive)
//!   9. Hexite Deposits (neutral claim_state + growth_state)

mod config;
mod decode;
mod discovery;
mod serve;
mod shard;
mod store;
mod wire;
mod xp;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use relay_protocol::{parse_schema, MirroredSchema};
use tracing_subscriber::EnvFilter;
use url::Url;

use crate::config::Args;
use crate::discovery::discover_regions;
use crate::serve::Fleet;
use crate::shard::spawn_shard;

#[tokio::main]
async fn main() -> Result<()> {
    // Required for HTTPS schema fetch (rustls 0.23 process-wide provider).
    let _ = rustls::crypto::ring::default_provider().install_default();

    let args = Args::parse();
    let default_filter = if args.debug {
        "relay_cache=debug"
    } else {
        "relay_cache=info"
    };
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(default_filter)),
        )
        .init();

    tracing::info!(
        target: "relay_cache",
        bind = %args.bind,
        unit_dir = %args.unit_dir.display(),
        schema_host = %args.schema_host,
        schema_db = %args.schema_db,
        mem_ceiling_bytes = args.mem_ceiling_bytes,
        debug_mode = args.debug,
        "starting"
    );

    let regions = discover_regions(&args.unit_dir)?;
    if regions.is_empty() {
        tracing::warn!(
            target: "relay_cache",
            unit_dir = %args.unit_dir.display(),
            "no regional relays discovered; HTTP will serve empty fan-outs"
        );
    } else {
        tracing::info!(
            target: "relay_cache",
            n_regions = regions.len(),
            "discovered regions"
        );
    }

    let schema = Arc::new(fetch_schema(&args.schema_host, &args.schema_db).await?);
    tracing::info!(
        target: "relay_cache",
        tables = schema.tables.len(),
        "schema loaded"
    );

    let mut shards = Vec::with_capacity(regions.len());
    for r in &regions {
        let bind_url = Url::parse(&format!("ws://127.0.0.1:{}", r.frontend_port))
            .context("build region bind URL")?;
        let handle = spawn_shard(
            r.region,
            r.database.clone(),
            bind_url,
            schema.clone(),
            args.debug,
            shutdown_signal_clone(),
        );
        shards.push(handle);
    }

    let memory_pressure = Arc::new(AtomicBool::new(false));
    {
        let pressure = memory_pressure.clone();
        let ceiling = args.mem_ceiling_bytes;
        let shutdown = shutdown_signal_clone();
        tokio::spawn(async move {
            run_memory_sampler(ceiling, pressure, shutdown).await;
        });
    }

    let fleet = Fleet {
        shards,
        memory_pressure,
    };

    let http_task = if args.bind.is_empty() {
        tracing::info!(target: "relay_cache", "HTTP bind empty — ingest-only mode");
        None
    } else {
        let addr: SocketAddr = args
            .bind
            .parse()
            .with_context(|| format!("parse --bind {}", args.bind))?;
        let fleet = fleet.clone();
        let shutdown = shutdown_signal_clone();
        Some(tokio::spawn(async move {
            if let Err(e) = serve::serve(addr, fleet, shutdown).await {
                tracing::error!(target: "relay_cache", error = %e, "HTTP server exited");
            }
        }))
    };

    shutdown_signal().await;
    tracing::info!(target: "relay_cache", "shutdown signal received");
    drop(http_task);
    // Give shard tasks a moment to observe their own shutdown futures.
    tokio::time::sleep(Duration::from_millis(200)).await;
    Ok(())
}

async fn fetch_schema(host_port: &str, database: &str) -> Result<MirroredSchema> {
    // Loopback frontends speak plain HTTP; public nginx frontends speak HTTPS.
    let scheme = if host_port.starts_with("127.0.0.1:") || host_port.starts_with("localhost:") {
        "http"
    } else {
        "https"
    };
    let url = format!("{scheme}://{host_port}/v1/database/{database}/schema?version=9");
    tracing::info!(target: "relay_cache", %url, "fetching schema");
    let response = reqwest::get(&url)
        .await
        .with_context(|| format!("GET {url}"))?;
    let status = response.status();
    if !status.is_success() {
        return Err(anyhow!("schema fetch returned HTTP {status}"));
    }
    let bytes = response.bytes().await?.to_vec();
    parse_schema(&bytes).context("parse schema")
}

async fn run_memory_sampler(
    ceiling_bytes: u64,
    pressure: Arc<AtomicBool>,
    mut shutdown: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
) {
    let mut sys = sysinfo::System::new();
    let pid = sysinfo::get_current_pid().ok();
    let mut interval = tokio::time::interval(Duration::from_secs(15));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => return,
            _ = interval.tick() => {
                let Some(pid) = pid else { continue };
                sys.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid]), true);
                let Some(proc) = sys.process(pid) else { continue };
                let rss = proc.memory(); // bytes on recent sysinfo
                let over = rss >= ceiling_bytes;
                let was = pressure.swap(over, Ordering::Relaxed);
                if over && !was {
                    tracing::warn!(
                        target: "relay_cache",
                        rss_bytes = rss,
                        ceiling_bytes,
                        "resident set at/above memory ceiling — /cache-health ready=false"
                    );
                } else if !over && was {
                    tracing::info!(
                        target: "relay_cache",
                        rss_bytes = rss,
                        ceiling_bytes,
                        "resident set back under memory ceiling"
                    );
                }
            }
        }
    }
}

fn shutdown_signal_clone() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
    Box::pin(shutdown_signal())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(
                    target: "relay_cache",
                    error = %e,
                    "SIGTERM listener failed; using ctrl-c only"
                );
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
