// SPDX-License-Identifier: MIT

//! Spawn a local `spacetimedb-standalone` instance as a child process and
//! wait until it is ready to accept connections.
//!
//! Used by the relay when `--stdb-spawn` is set. The returned
//! [`StdbProcess`] kills the child when dropped, so the local SpacetimeDB
//! always stops together with the relay process.

use std::path::Path;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use tokio::net::TcpListener;
use url::Url;

/// A running `spacetime start` child process. Sends SIGKILL on drop.
pub struct StdbProcess(tokio::process::Child);

impl Drop for StdbProcess {
    fn drop(&mut self) {
        // Best-effort — relay is shutting down anyway.
        let _ = self.0.start_kill();
    }
}

/// Spawn a local SpacetimeDB instance and wait for it to pass the health
/// check. Returns:
/// - the WebSocket URL to use as `stdb_url`
/// - the HTTP base URL to pass to `spacetime publish -s` (SpacetimeDB CLI
///   ≥ 2.x accepts a bare URL in the `-s` position without a pre-registered
///   alias)
/// - a [`StdbProcess`] that kills the child on drop
/// - `true` if the stdb data directory was newly created (i.e. this stdb
///   instance is fresh and contains no databases). Callers should delete
///   the publisher fingerprint when this is `true` so the mirror module is
///   always republished into the empty stdb.
///
/// The stdb data directory is `<data_dir>/stdb/` so each relay instance
/// keeps its SpacetimeDB state alongside its own publisher workdir and
/// identity token.
pub async fn spawn(
    spacetime_bin: &Path,
    data_dir: &Path,
) -> Result<(Url, String, StdbProcess, bool)> {
    let port = free_loopback_port().await?;
    let listen_addr = format!("127.0.0.1:{port}");
    let stdb_data_dir = data_dir.join("stdb");

    // Track whether the data dir already existed. A fresh dir means the
    // stdb has no databases, so the caller must republish unconditionally.
    let is_fresh = !stdb_data_dir.exists();
    tokio::fs::create_dir_all(&stdb_data_dir)
        .await
        .with_context(|| format!("create stdb data dir {}", stdb_data_dir.display()))?;

    tracing::info!(
        target: "relay::stdb_spawn",
        bin    = %spacetime_bin.display(),
        listen = %listen_addr,
        data   = %stdb_data_dir.display(),
        "spawning local SpacetimeDB instance"
    );

    // `spacetime start` is a foreground command; we own its lifetime.
    //
    // `--in-memory` disables on-disk persistence entirely: stdb keeps all
    // database pages in RAM and writes nothing to the data dir for them.
    // This is intentional for relay mirrors — every boot performs a full
    // resync from upstream, so there is nothing to recover and the disk
    // write path is pure overhead. Removing it eliminates the iowait that
    // bottlenecked per-region apply throughput under the per-mirror layout.
    let child = tokio::process::Command::new(spacetime_bin)
        .args([
            "start",
            "--in-memory",
            "--listen-addr",
            &listen_addr,
            "--data-dir",
        ])
        .arg(&stdb_data_dir)
        // Don't inherit our file descriptors — stdb emits its own log
        // format to stdout, which would interleave with the relay's
        // structured tracing output in the journal.
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("spawn {} start", spacetime_bin.display()))?;

    let http_base = format!("http://127.0.0.1:{port}");
    wait_for_health(&http_base)
        .await
        .context("spawned stdb did not become healthy within 60s")?;

    tracing::info!(
        target: "relay::stdb_spawn",
        listen = %listen_addr,
        "local SpacetimeDB instance ready"
    );

    let ws_url = Url::parse(&format!("ws://127.0.0.1:{port}")).expect("generated URL is valid");
    Ok((ws_url, http_base, StdbProcess(child), is_fresh))
}

/// Bind an ephemeral loopback port and immediately release it, returning
/// the port number for stdb to bind. There is a small TOCTOU window
/// between the drop and stdb's bind, but in practice this is negligible
/// on a loopback-only address in a controlled environment.
async fn free_loopback_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .context("bind ephemeral loopback port")?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

async fn wait_for_health(http_base: &str) -> Result<()> {
    let health_url = format!("{http_base}/v1/health");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .context("build health-check HTTP client")?;
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        if std::time::Instant::now() >= deadline {
            return Err(anyhow!(
                "stdb health check timed out after 60s — is {} a valid spacetime binary?",
                health_url
            ));
        }
        match client.get(&health_url).send().await {
            Ok(r) if r.status().is_success() => return Ok(()),
            _ => tokio::time::sleep(Duration::from_millis(500)).await,
        }
    }
}
