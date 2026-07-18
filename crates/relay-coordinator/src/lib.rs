// SPDX-License-Identifier: MIT

//! Client library for the relay reconnect coordinator.
//!
//! The coordinator is a small daemon that holds a bounded semaphore of
//! "reconnect permits". A relay process acquires a permit before
//! starting its initial sequential subscribe and releases it (by
//! dropping [`ReconnectPermit`]) once "all sequential subscriptions
//! applied". This serialises the flood of simultaneous initial syncs
//! that would otherwise saturate the shared local SpacetimeDB.
//!
//! Graceful degradation: every call that talks to the daemon returns
//! `None` (rather than an error) when the coordinator is absent or
//! unreachable. In that case the caller falls back to the exponential
//! stdb backoff that lives in `stdb_mode.rs`.

pub mod daemon;
pub mod health;
pub mod sys_metrics;

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Result;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

/// Framing constants for the newline-delimited JSON protocol.
///
/// ```text
/// Client → Daemon (once, immediately after connect):
///   {"relay_id":"bc12"}
///
/// Daemon → Client (when a slot is available):
///   {"status":"granted"}
///
/// Release: client closes the connection (Drop on ReconnectPermit).
/// Daemon sees EOF → releases the semaphore slot automatically.
/// ```
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const GRANT_TIMEOUT: Duration = Duration::from_secs(300); // 5 min max queue wait

/// Handle to the coordinator daemon. Cheap to clone.
#[derive(Clone, Debug)]
pub struct CoordinatorClient {
    socket_path: PathBuf,
    relay_id: String,
}

impl CoordinatorClient {
    pub fn new(socket_path: impl Into<PathBuf>, relay_id: impl Into<String>) -> Self {
        Self {
            socket_path: socket_path.into(),
            relay_id: relay_id.into(),
        }
    }

    /// Acquire a reconnect permit from the coordinator.
    ///
    /// Blocks until the daemon grants a slot. Returns `None` if the
    /// coordinator is unreachable — callers should treat that as
    /// "uncoordinated, proceed anyway".
    pub async fn acquire(&self) -> Option<ReconnectPermit> {
        match self.try_acquire().await {
            Ok(permit) => {
                tracing::info!(
                    target: "relay::coordinator",
                    relay_id = %self.relay_id,
                    "reconnect permit granted"
                );
                Some(permit)
            }
            Err(e) => {
                tracing::warn!(
                    target: "relay::coordinator",
                    relay_id = %self.relay_id,
                    error = %e,
                    "coordinator unreachable — proceeding without permit"
                );
                None
            }
        }
    }

    async fn try_acquire(&self) -> Result<ReconnectPermit> {
        let stream = tokio::time::timeout(CONNECT_TIMEOUT, UnixStream::connect(&self.socket_path))
            .await
            .map_err(|_| anyhow::anyhow!("connect timeout"))??;

        let (reader, mut writer) = stream.into_split();

        // Identify ourselves.
        let msg = format!("{{\"relay_id\":{}}}\n", serde_json::json!(self.relay_id));
        writer.write_all(msg.as_bytes()).await?;

        // Wait for the grant (blocks until the daemon has a free slot).
        let mut reader = BufReader::new(reader);
        let mut line = String::new();
        tokio::time::timeout(GRANT_TIMEOUT, reader.read_line(&mut line))
            .await
            .map_err(|_| anyhow::anyhow!("grant timeout after {}s", GRANT_TIMEOUT.as_secs()))??;

        if line.trim().is_empty() {
            return Err(anyhow::anyhow!(
                "coordinator closed connection before granting"
            ));
        }

        // Keep the writer alive — closing it signals the daemon to release.
        Ok(ReconnectPermit { _writer: writer })
    }
}

/// RAII guard for a coordinator reconnect slot. Dropping it releases
/// the slot so the next queued relay can proceed.
pub struct ReconnectPermit {
    // Keeping the write-half open signals to the daemon that we still
    // hold the permit. Drop closes the connection → daemon sees EOF →
    // releases the semaphore slot.
    _writer: tokio::net::unix::OwnedWriteHalf,
}

impl Drop for ReconnectPermit {
    fn drop(&mut self) {
        tracing::info!(target: "relay::coordinator", "reconnect permit released");
    }
}

/// Default socket path used by both the daemon and the client.
pub fn default_socket_path() -> PathBuf {
    PathBuf::from("/run/relay/coordinator.sock")
}

/// Check whether the coordinator socket exists (quick non-blocking probe).
pub fn socket_exists(path: &Path) -> bool {
    path.exists()
}
