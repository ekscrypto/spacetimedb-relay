// SPDX-License-Identifier: MIT

use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "relay-coordinator", about = "Relay reconnect coordinator daemon")]
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
    #[arg(
        long,
        env = "RELAY_COORDINATOR_MAX_CONCURRENT",
        default_value_t = 1
    )]
    max_concurrent: usize,
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

    relay_coordinator::daemon::run(args.socket, args.max_concurrent, shutdown).await
}
