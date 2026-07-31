// SPDX-License-Identifier: MIT

use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use relay_coordinator::fleet_sequencer::FleetSequencerConfig;
use relay_coordinator::health::NamingSpec;

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
    /// Empty string disables the health aggregator.
    #[arg(long, env = "RELAY_HEALTH_BIND", default_value = "127.0.0.1:8082")]
    health_bind: String,

    /// Public-mirror readiness URL (`GET /v1/mirrors`). When set (default),
    /// `/health` aggregates from this instead of legacy `relay-*.service`
    /// unit discovery. Empty string forces legacy unit-dir mode.
    #[arg(
        long,
        env = "RELAY_MIRRORS_URL",
        default_value = "http://127.0.0.1:3030/v1/mirrors"
    )]
    mirrors_url: String,

    /// Directory containing `relay-*.service` systemd unit files. Used
    /// by the `/health` aggregator only when `--mirrors-url` is empty
    /// (legacy per-relay fleet).
    #[arg(long, env = "RELAY_UNIT_DIR", default_value = "/etc/systemd/system")]
    unit_dir: PathBuf,

    /// Optional `format!`-style template projecting a discovered unit's
    /// stem into its `sources[*]` key in `/health`. `{stem}` is the
    /// only placeholder. Example: `live-{stem}`. Default unset = the
    /// unit stem passes through verbatim (`relay-region14` → `relay-region14`).
    #[arg(long, env = "RELAY_SOURCE_NAME_TEMPLATE")]
    source_name_template: Option<String>,

    /// Optional prefix stripped from the unit stem before substitution
    /// into `--source-name-template`. Example: with `stem_prefix =
    /// relay-region` and `template = live-{stem}`, `relay-region14`
    /// becomes `live-14`. Ignored when `--source-name-template` is unset.
    /// If the prefix is set but doesn't match a given unit, the full
    /// stem flows into the template.
    #[arg(long, env = "RELAY_SOURCE_NAME_STEM_PREFIX")]
    source_name_stem_prefix: Option<String>,

    /// Path to an HTML file served as the `/` dashboard page. If unset
    /// the coordinator serves a minimal stub that links to `/health`.
    /// The file is read once at startup; restart the coordinator to
    /// pick up edits.
    #[arg(long, env = "RELAY_INDEX_HTML")]
    index_html: Option<PathBuf>,

    /// Enable sequential `public-mirror@DATABASE` bootstrap from
    /// `--mirror-instances-file`. Requires passwordless sudo systemctl
    /// for the service user (see `relay-coordinator.sudoers`).
    #[arg(long, env = "RELAY_FLEET_SEQUENCER", default_value_t = false)]
    fleet_sequencer: bool,

    /// Newline-separated upstream database names to start as
    /// `public-mirror@DATABASE.service` (e.g. `bitcraft-live-7`).
    /// Empty disables the fleet sequencer even when `--fleet-sequencer`
    /// is set.
    #[arg(long, env = "RELAY_MIRROR_INSTANCES_FILE", default_value = "")]
    mirror_instances_file: String,

    /// argv prefix for systemd from the fleet sequencer
    /// (default: `sudo -n systemctl`).
    #[arg(
        long,
        env = "RELAY_FLEET_SYSTEMCTL",
        default_value = "sudo,-n,systemctl",
        value_delimiter = ','
    )]
    fleet_systemctl: Vec<String>,
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

    let index_html = match args.index_html {
        Some(ref path) => match std::fs::read_to_string(path) {
            Ok(content) => Some(content),
            Err(e) => {
                tracing::warn!(
                    target: "relay_coordinator",
                    path = %path.display(),
                    error = %e,
                    "--index-html file unreadable; serving stub page instead"
                );
                None
            }
        },
        None => None,
    };

    let mirrors_url = {
        let t = args.mirrors_url.trim();
        if t.is_empty() {
            None
        } else {
            Some(t.to_string())
        }
    };

    let fleet_sequencer = {
        let path = args.mirror_instances_file.trim();
        if args.fleet_sequencer && !path.is_empty() {
            Some(FleetSequencerConfig {
                instances_file: PathBuf::from(path),
                systemctl_argv: args.fleet_systemctl,
                poll_interval: relay_coordinator::fleet_sequencer::DEFAULT_POLL_INTERVAL,
                instance_timeout: relay_coordinator::fleet_sequencer::DEFAULT_INSTANCE_TIMEOUT,
                fetch_timeout: relay_coordinator::fleet_sequencer::DEFAULT_FETCH_TIMEOUT,
            })
        } else {
            if args.fleet_sequencer && path.is_empty() {
                tracing::warn!(
                    target: "relay_coordinator",
                    "--fleet-sequencer set but --mirror-instances-file empty; sequencer idle"
                );
            }
            None
        }
    };

    relay_coordinator::daemon::run(
        args.socket,
        args.max_concurrent,
        health_bind,
        mirrors_url,
        args.unit_dir,
        NamingSpec {
            template: args.source_name_template,
            stem_prefix: args.source_name_stem_prefix,
        },
        index_html,
        fleet_sequencer,
        shutdown,
    )
    .await
}
