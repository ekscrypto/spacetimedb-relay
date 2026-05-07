// SPDX-License-Identifier: MIT

//! Active-client registry shared between the listener task, per-client
//! tasks, and the dashboard.

use std::sync::Arc;

use dashmap::DashMap;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::metrics::{ClientSnapshot, ClientStats, FrontendSnapshot, MAX_CLIENTS_IN_SNAPSHOT};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ClientId(pub Uuid);

impl std::fmt::Display for ClientId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.as_hyphenated())
    }
}

/// One row in [`ActiveClients`]. The cancel token lets the dashboard
/// kill a misbehaving client without reaching into the per-client task.
pub struct ClientHandle {
    pub stats: Arc<ClientStats>,
    pub cancel: CancellationToken,
}

/// Registry of every currently-connected downstream client.
#[derive(Clone)]
pub struct ActiveClients {
    inner: Arc<DashMap<ClientId, ClientHandle>>,
}

impl ActiveClients {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
        }
    }

    pub fn insert(&self, handle: ClientHandle) {
        self.inner.insert(handle.stats.id, handle);
    }

    pub fn remove(&self, id: ClientId) -> Option<ClientHandle> {
        self.inner.remove(&id).map(|(_, v)| v)
    }

    pub fn get_cancel(&self, id: ClientId) -> Option<CancellationToken> {
        self.inner.get(&id).map(|h| h.cancel.clone())
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Render a per-client snapshot list for the dashboard. Sorted by
    /// total bytes out (the "biggest talkers" view) and capped at
    /// [`MAX_CLIENTS_IN_SNAPSHOT`].
    pub fn snapshot_clients(&self) -> Vec<ClientSnapshot> {
        let mut snaps: Vec<ClientSnapshot> = self
            .inner
            .iter()
            .map(|entry| entry.value().stats.snapshot())
            .collect();
        snaps.sort_by(|a, b| b.total_bytes_out.cmp(&a.total_bytes_out));
        snaps.truncate(MAX_CLIENTS_IN_SNAPSHOT);
        snaps
    }
}

impl Default for ActiveClients {
    fn default() -> Self {
        Self::new()
    }
}

/// Build the full frontend snapshot the dashboard reads as JSON.
pub fn snapshot(
    metrics: &crate::metrics::FrontendMetrics,
    clients: &ActiveClients,
) -> FrontendSnapshot {
    use std::sync::atomic::Ordering;
    FrontendSnapshot {
        bind: metrics.bind.clone(),
        started_at: metrics.started_at,
        active_clients: clients.len(),
        lifetime_connections: metrics.lifetime_connections.load(Ordering::Relaxed),
        lifetime_disconnects: metrics.lifetime_disconnects.load(Ordering::Relaxed),
        lifetime_rewrites: metrics.lifetime_rewrites.load(Ordering::Relaxed),
        total_bytes_in: metrics.total_bytes_in.load(Ordering::Relaxed),
        total_bytes_out: metrics.total_bytes_out.load(Ordering::Relaxed),
        bytes_in_1m: metrics.aggregate_bytes_in.last_minutes(1),
        bytes_in_5m: metrics.aggregate_bytes_in.last_minutes(5),
        bytes_in_30m: metrics.aggregate_bytes_in.last_minutes(30),
        bytes_out_1m: metrics.aggregate_bytes_out.last_minutes(1),
        bytes_out_5m: metrics.aggregate_bytes_out.last_minutes(5),
        bytes_out_30m: metrics.aggregate_bytes_out.last_minutes(30),
        frames_out_1m: metrics.aggregate_frames_out.last_minutes(1),
        frames_out_5m: metrics.aggregate_frames_out.last_minutes(5),
        frames_out_30m: metrics.aggregate_frames_out.last_minutes(30),
        clients: clients.snapshot_clients(),
    }
}
