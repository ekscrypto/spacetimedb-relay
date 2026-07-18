// SPDX-License-Identifier: MIT

use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;

#[derive(Debug, Parser)]
#[command(
    name = "relay-coordinator",
    about = "Relay reconnect coordinator + /health aggregator daemon"
)]
struct Args {
    /// Unix socket path to listen on.
    #[arg(
        long,
        env = "RELAY_COORDINATOR_SOCKET",
        default_value = "/run/relay/coordinator.sock"
    )]
    socket: PathBuf,

    /// Maximum number of relays permitted to do their initial
    /// sequential subscribe simultaneously. Set to 2 if you want to
    /// allow pairs of regions to sync in parallel (halves total sync
    /// time on an idle stdb). Default 1 = fully serialised.
    #[arg(long, env = "RELAY_COORDINATOR_MAX_CONCURRENT", default_value_t = 1)]
    max_concurrent: usize,

    /// Bind address for the `/health` and `/` (dashboard) HTTP endpoint.
    /// Empty string disables the health aggregator. The default matches
    /// the address the retired BitCraft-Relay used (127.0.0.1:8082), so
    /// the existing nginx site can keep proxying 80/443 → 8082 unchanged.
    #[arg(long, env = "RELAY_HEALTH_BIND", default_value = "127.0.0.1:8082")]
    health_bind: String,

    /// Directory containing `relay-*.service` systemd unit files. Used
    /// by the `/health` aggregator to discover the fleet (each unit's
    /// `--frontend-bind` / `--dashboard-bind` / `--mirror-database`).
    #[arg(long, env = "RELAY_UNIT_DIR", default_value = "/etc/systemd/system")]
    unit_dir: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    // Empty string disables /health; otherwise the address must parse.
    let health_bind =
        if args.health_bind.trim().is_empty() {
            None
        } else {
            Some(args.health_bind.parse::<SocketAddr>().map_err(|e| {
                anyhow::anyhow!("invalid --health-bind {:?}: {e}", args.health_bind)
            })?)
        };

    let shutdown = async {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = term.recv() => {}
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
    };

    relay_coordinator::daemon::run(
        args.socket,
        args.max_concurrent,
        health_bind,
        args.unit_dir,
        shutdown,
    )
    .await
}
