// SPDX-License-Identifier: MIT

//! Sequential public-mirror fleet bootstrap.
//!
//! When `--mirror-instances-file` is set, the coordinator starts each
//! `public-mirror@DATABASE.service` one at a time and waits until that
//! instance reports `connectivity=live` with full table sync before
//! launching the next. Mirrors the old `relay-fleet-sequencer` behaviour
//! for the per-instance layout.

use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::Client;
use serde_json::Value;
use tokio::process::Command;
use tokio::time::{sleep, Instant};

use crate::health::mirror_status_port_for_database;

/// Poll interval while waiting for an instance to reach `live`.
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(5);
/// Per-instance wait budget (initial seed can take tens of minutes).
pub const DEFAULT_INSTANCE_TIMEOUT: Duration = Duration::from_secs(3600);
/// HTTP timeout for a single `/v1/mirrors` probe.
pub const DEFAULT_FETCH_TIMEOUT: Duration = Duration::from_secs(4);

#[derive(Clone, Debug)]
pub struct FleetSequencerConfig {
    pub instances_file: std::path::PathBuf,
    /// argv prefix for systemd, e.g. `["sudo", "-n", "systemctl"]`.
    pub systemctl_argv: Vec<String>,
    pub poll_interval: Duration,
    pub instance_timeout: Duration,
    pub fetch_timeout: Duration,
}

impl FleetSequencerConfig {
    pub fn unit_name(&self, database: &str) -> String {
        format!("public-mirror@{database}.service")
    }
}

/// Read newline-separated database names from the fleet manifest.
pub fn read_mirror_instances(path: &Path) -> Result<Vec<String>> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("read mirror instances file {}", path.display()))?;
    Ok(raw
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(str::to_string)
        .collect())
}

/// True when `/v1/mirrors` reports this database fully live.
pub fn mirror_row_is_live(body: &Value, database: &str) -> bool {
    let Some(arr) = body.get("mirrors").and_then(|v| v.as_array()) else {
        return false;
    };
    let Some(row) = arr.iter().find(|m| {
        m.get("database")
            .and_then(|v| v.as_str())
            .is_some_and(|d| d == database)
    }) else {
        return false;
    };
    row.get("connectivity").and_then(|v| v.as_str()) == Some("live")
        && row.get("tables_live").and_then(|v| v.as_u64())
            == row.get("tables_total").and_then(|v| v.as_u64())
        && row.get("tables_total").and_then(|v| v.as_u64()).is_some_and(|n| n > 0)
}

async fn fetch_mirrors(http: &Client, timeout: Duration, port: u16) -> Option<Value> {
    let url = format!("http://127.0.0.1:{port}/v1/mirrors");
    let resp = tokio::time::timeout(timeout, http.get(&url).send()).await;
    let resp = match resp {
        Ok(Ok(r)) if r.status().is_success() => r,
        _ => return None,
    };
    resp.json::<Value>().await.ok()
}

async fn systemctl(
    argv: &[String],
    subcommand: &str,
    unit: &str,
) -> Result<()> {
    anyhow::ensure!(!argv.is_empty(), "empty systemctl argv");
    let mut cmd = Command::new(&argv[0]);
    for arg in &argv[1..] {
        cmd.arg(arg);
    }
    cmd.arg(subcommand).arg(unit);
    let output = cmd
        .output()
        .await
        .with_context(|| format!("systemctl {subcommand} {unit}"))?;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    // `stop` on an inactive unit is not fatal.
    if subcommand == "stop" && stderr.contains("not loaded") {
        return Ok(());
    }
    anyhow::bail!(
        "systemctl {subcommand} {unit} failed (status={}): {}",
        output.status,
        stderr.trim()
    );
}

async fn unit_is_active(argv: &[String], unit: &str) -> bool {
    if argv.is_empty() {
        return false;
    }
    // Do not pass `--quiet`: sudoers matches exact argv shapes, and
    // `systemctl is-active UNIT --quiet` is not covered by the
    // `is-active public-mirror@*.service` rule.
    let mut cmd = Command::new(&argv[0]);
    for arg in &argv[1..] {
        cmd.arg(arg);
    }
    cmd.arg("is-active").arg(unit);
    match cmd.output().await {
        Ok(o) if o.status.success() => {
            let stdout = String::from_utf8_lossy(&o.stdout);
            stdout.trim() == "active"
        }
        _ => false,
    }
}

/// Stop every instance listed in the manifest (best-effort).
pub async fn stop_all_instances(cfg: &FleetSequencerConfig) -> Result<()> {
    let databases = read_mirror_instances(&cfg.instances_file)?;
    for database in databases.iter().rev() {
        let unit = cfg.unit_name(database);
        if let Err(e) = systemctl(&cfg.systemctl_argv, "stop", &unit).await {
            tracing::warn!(
                target: "relay_coordinator::fleet_sequencer",
                %database,
                error = %e,
                "stop failed (continuing)"
            );
        }
    }
    Ok(())
}

/// Start instances sequentially; wait for each to reach `live` before the next.
pub async fn run_fleet_sequencer(
    cfg: FleetSequencerConfig,
    shutdown: impl std::future::Future<Output = ()> + Send,
) {
    let mut shutdown = std::pin::pin!(shutdown);
    let databases = match read_mirror_instances(&cfg.instances_file) {
        Ok(d) if !d.is_empty() => d,
        Ok(_) => {
            tracing::warn!(
                target: "relay_coordinator::fleet_sequencer",
                path = %cfg.instances_file.display(),
                "instances file empty — fleet sequencer idle"
            );
            return;
        }
        Err(e) => {
            tracing::error!(
                target: "relay_coordinator::fleet_sequencer",
                error = %e,
                "failed to read instances file — fleet sequencer idle"
            );
            return;
        }
    };

    let http = match Client::builder().timeout(cfg.fetch_timeout).build() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(
                target: "relay_coordinator::fleet_sequencer",
                error = %e,
                "failed to build HTTP client"
            );
            return;
        }
    };

    tracing::info!(
        target: "relay_coordinator::fleet_sequencer",
        n = databases.len(),
        path = %cfg.instances_file.display(),
        "starting sequential public-mirror fleet bootstrap"
    );

    for (idx, database) in databases.iter().enumerate() {
        tokio::select! {
            biased;
            _ = &mut shutdown => {
                tracing::info!(
                    target: "relay_coordinator::fleet_sequencer",
                    "shutdown during fleet bootstrap"
                );
                return;
            }
            result = ensure_instance_live(&cfg, &http, database) => {
                match result {
                    Ok(()) => {
                        tracing::info!(
                            target: "relay_coordinator::fleet_sequencer",
                            database = %database,
                            progress = format!("{}/{}", idx + 1, databases.len()),
                            "mirror live — proceeding to next instance"
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            target: "relay_coordinator::fleet_sequencer",
                            database = %database,
                            error = %e,
                            "instance failed to reach live — continuing with next"
                        );
                    }
                }
            }
        }
    }

    tracing::info!(
        target: "relay_coordinator::fleet_sequencer",
        n = databases.len(),
        "sequential fleet bootstrap complete"
    );
}

async fn ensure_instance_live(
    cfg: &FleetSequencerConfig,
    http: &Client,
    database: &str,
) -> Result<()> {
    let port = mirror_status_port_for_database(database);
    let unit = cfg.unit_name(database);
    let started = Instant::now();

    loop {
        if let Some(body) = fetch_mirrors(http, cfg.fetch_timeout, port).await {
            if mirror_row_is_live(&body, database) {
                return Ok(());
            }
            if let Some(row) = body
                .get("mirrors")
                .and_then(|v| v.as_array())
                .and_then(|arr| arr.iter().find(|m| {
                    m.get("database")
                        .and_then(|v| v.as_str())
                        .is_some_and(|d| d == database)
                }))
            {
                let conn = row
                    .get("connectivity")
                    .and_then(|v| v.as_str())
                    .unwrap_or("?");
                let live = row.get("tables_live").and_then(|v| v.as_u64()).unwrap_or(0);
                let total = row.get("tables_total").and_then(|v| v.as_u64()).unwrap_or(0);
                tracing::debug!(
                    target: "relay_coordinator::fleet_sequencer",
                    database = %database,
                    connectivity = %conn,
                    tables = format!("{live}/{total}"),
                    elapsed_secs = started.elapsed().as_secs(),
                    "waiting for mirror live"
                );
            }
        }

        if started.elapsed() >= cfg.instance_timeout {
            anyhow::bail!(
                "timed out after {}s waiting for {database} to reach live",
                cfg.instance_timeout.as_secs()
            );
        }

        if !unit_is_active(&cfg.systemctl_argv, &unit).await {
            tracing::info!(
                target: "relay_coordinator::fleet_sequencer",
                database = %database,
                unit = %unit,
                "starting systemd unit"
            );
            systemctl(&cfg.systemctl_argv, "start", &unit).await.with_context(|| {
                format!("start {unit} (does the relay user have passwordless sudo for systemctl?)")
            })?;
        }

        sleep(cfg.poll_interval).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn live_requires_full_tables() {
        let body = json!({
            "mirrors": [{
                "database": "bitcraft-live-14",
                "connectivity": "live",
                "tables_live": 274,
                "tables_total": 274
            }]
        });
        assert!(mirror_row_is_live(&body, "bitcraft-live-14"));
        assert!(!mirror_row_is_live(&body, "bitcraft-live-3"));
    }

    #[test]
    fn subscribing_is_not_live() {
        let body = json!({
            "mirrors": [{
                "database": "bitcraft-live-14",
                "connectivity": "subscribing",
                "tables_live": 100,
                "tables_total": 274
            }]
        });
        assert!(!mirror_row_is_live(&body, "bitcraft-live-14"));
    }
}
