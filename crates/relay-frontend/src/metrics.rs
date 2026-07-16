// SPDX-License-Identifier: MIT

//! Per-client and aggregate counters surfaced to the dashboard.
//!
//! Each connected downstream client owns one [`ClientStats`], plus the
//! frontend keeps a single aggregate [`LinkMetrics`-shaped] view across
//! all clients for the existing dashboard panels to render.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use serde::Serialize;
use uuid::Uuid;

use crate::state::ClientId;
use crate::Subprotocol;

const WINDOW_BUCKETS: usize = 30;
const BUCKET_SECS: u64 = 60;

fn epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn epoch_minute() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() / BUCKET_SECS)
        .unwrap_or(0)
}

/// 30-bucket sliding window with one bucket per minute. Identical
/// shape to the dashboard's existing counter so the JSON snapshot can
/// produce the same `{1m, 5m, 30m}` breakdown.
pub struct SlidingCounter {
    inner: Mutex<SlidingInner>,
}

struct SlidingInner {
    buckets: [u64; WINDOW_BUCKETS],
    last_minute: u64,
}

impl SlidingCounter {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(SlidingInner {
                buckets: [0; WINDOW_BUCKETS],
                last_minute: 0,
            }),
        }
    }

    pub fn record(&self, n: u64) {
        let now = epoch_minute();
        let mut inner = self.inner.lock();
        Self::advance(&mut inner, now);
        let idx = (now as usize) % WINDOW_BUCKETS;
        inner.buckets[idx] = inner.buckets[idx].saturating_add(n);
    }

    pub fn last_minutes(&self, minutes: usize) -> u64 {
        let now = epoch_minute();
        let mut inner = self.inner.lock();
        Self::advance(&mut inner, now);
        let take = minutes.min(WINDOW_BUCKETS);
        let mut sum = 0u64;
        for i in 0..take {
            let m = now.saturating_sub(i as u64);
            let idx = (m as usize) % WINDOW_BUCKETS;
            sum = sum.saturating_add(inner.buckets[idx]);
        }
        sum
    }

    fn advance(inner: &mut SlidingInner, now: u64) {
        if inner.last_minute == 0 {
            inner.last_minute = now;
            return;
        }
        if now <= inner.last_minute {
            return;
        }
        let gap = (now - inner.last_minute).min(WINDOW_BUCKETS as u64);
        for i in 1..=gap {
            let m = inner.last_minute + i;
            let idx = (m as usize) % WINDOW_BUCKETS;
            inner.buckets[idx] = 0;
        }
        inner.last_minute = now;
    }
}

impl Default for SlidingCounter {
    fn default() -> Self {
        Self::new()
    }
}

/// Per-client running state. One instance per active downstream
/// connection; dropped when the connection is torn down.
pub struct ClientStats {
    pub id: ClientId,
    pub remote_addr: SocketAddr,
    pub subprotocol: Subprotocol,
    pub connected_at: u64,
    pub last_activity: AtomicU64,

    pub bytes_in: SlidingCounter,
    pub bytes_out: SlidingCounter,
    pub frames_in: SlidingCounter,
    pub frames_out: SlidingCounter,

    pub total_bytes_in: AtomicU64,
    pub total_bytes_out: AtomicU64,
    pub total_frames_in: AtomicU64,
    pub total_frames_out: AtomicU64,

    pub one_off_queries: AtomicU64,
    pub subscriptions: Mutex<BTreeSet<String>>,

    /// Number of TransactionUpdate frames the proxy rewrote with
    /// upstream meta. v1 clients only — v2 clients always see zero
    /// here.
    pub rewrites: AtomicU64,

    /// CallReducer frames rejected at the frontend. The relay is
    /// read-only; reducer calls are never forwarded to local stdb
    /// (see `reject_call_reducer`). Counts both v1 and v2 clients.
    pub call_reducers: AtomicU64,

    /// CallProcedure frames rejected at the frontend, same rationale
    /// as `call_reducers`.
    pub call_procedures: AtomicU64,
}

impl ClientStats {
    pub fn new(remote_addr: SocketAddr, subprotocol: Subprotocol) -> Self {
        Self {
            id: ClientId(Uuid::new_v4()),
            remote_addr,
            subprotocol,
            connected_at: epoch_secs(),
            last_activity: AtomicU64::new(epoch_secs()),
            bytes_in: SlidingCounter::new(),
            bytes_out: SlidingCounter::new(),
            frames_in: SlidingCounter::new(),
            frames_out: SlidingCounter::new(),
            total_bytes_in: AtomicU64::new(0),
            total_bytes_out: AtomicU64::new(0),
            total_frames_in: AtomicU64::new(0),
            total_frames_out: AtomicU64::new(0),
            one_off_queries: AtomicU64::new(0),
            subscriptions: Mutex::new(BTreeSet::new()),
            rewrites: AtomicU64::new(0),
            call_reducers: AtomicU64::new(0),
            call_procedures: AtomicU64::new(0),
        }
    }

    pub fn record_inbound(&self, bytes: u64) {
        self.bytes_in.record(bytes);
        self.frames_in.record(1);
        self.total_bytes_in.fetch_add(bytes, Ordering::Relaxed);
        self.total_frames_in.fetch_add(1, Ordering::Relaxed);
        self.last_activity.store(epoch_secs(), Ordering::Relaxed);
    }

    pub fn record_outbound(&self, bytes: u64) {
        self.bytes_out.record(bytes);
        self.frames_out.record(1);
        self.total_bytes_out.fetch_add(bytes, Ordering::Relaxed);
        self.total_frames_out.fetch_add(1, Ordering::Relaxed);
        self.last_activity.store(epoch_secs(), Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> ClientSnapshot {
        let now = epoch_secs();
        let last = self.last_activity.load(Ordering::Relaxed);
        ClientSnapshot {
            id: self.id.to_string(),
            remote: self.remote_addr.to_string(),
            subprotocol: self.subprotocol.name(),
            connected_at: self.connected_at,
            idle_secs: now.saturating_sub(last),
            subscriptions: self.subscriptions.lock().iter().cloned().collect(),
            one_off_queries: self.one_off_queries.load(Ordering::Relaxed),
            rewrites: self.rewrites.load(Ordering::Relaxed),
            call_reducers: self.call_reducers.load(Ordering::Relaxed),
            call_procedures: self.call_procedures.load(Ordering::Relaxed),
            total_bytes_in: self.total_bytes_in.load(Ordering::Relaxed),
            total_bytes_out: self.total_bytes_out.load(Ordering::Relaxed),
            total_frames_in: self.total_frames_in.load(Ordering::Relaxed),
            total_frames_out: self.total_frames_out.load(Ordering::Relaxed),
            bytes_in_1m: self.bytes_in.last_minutes(1),
            bytes_in_5m: self.bytes_in.last_minutes(5),
            bytes_in_30m: self.bytes_in.last_minutes(30),
            bytes_out_1m: self.bytes_out.last_minutes(1),
            bytes_out_5m: self.bytes_out.last_minutes(5),
            bytes_out_30m: self.bytes_out.last_minutes(30),
            frames_out_1m: self.frames_out.last_minutes(1),
            frames_out_5m: self.frames_out.last_minutes(5),
            frames_out_30m: self.frames_out.last_minutes(30),
        }
    }
}

/// Top-level frontend metrics aggregate. Lives behind `Arc` so the
/// dashboard, listener, and per-client tasks all share it.
pub struct FrontendMetrics {
    pub bind: String,
    pub started_at: u64,
    pub aggregate_bytes_in: SlidingCounter,
    pub aggregate_bytes_out: SlidingCounter,
    pub aggregate_frames_in: SlidingCounter,
    pub aggregate_frames_out: SlidingCounter,
    pub total_bytes_in: AtomicU64,
    pub total_bytes_out: AtomicU64,
    pub lifetime_connections: AtomicU64,
    pub lifetime_disconnects: AtomicU64,
    pub lifetime_rewrites: AtomicU64,
}

impl FrontendMetrics {
    pub fn new(bind: String) -> Arc<Self> {
        Arc::new(Self {
            bind,
            started_at: epoch_secs(),
            aggregate_bytes_in: SlidingCounter::new(),
            aggregate_bytes_out: SlidingCounter::new(),
            aggregate_frames_in: SlidingCounter::new(),
            aggregate_frames_out: SlidingCounter::new(),
            total_bytes_in: AtomicU64::new(0),
            total_bytes_out: AtomicU64::new(0),
            lifetime_connections: AtomicU64::new(0),
            lifetime_disconnects: AtomicU64::new(0),
            lifetime_rewrites: AtomicU64::new(0),
        })
    }

    pub fn record_inbound(&self, bytes: u64) {
        self.aggregate_bytes_in.record(bytes);
        self.aggregate_frames_in.record(1);
        self.total_bytes_in.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_outbound(&self, bytes: u64) {
        self.aggregate_bytes_out.record(bytes);
        self.aggregate_frames_out.record(1);
        self.total_bytes_out.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_connect(&self) {
        self.lifetime_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_disconnect(&self) {
        self.lifetime_disconnects.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_rewrite(&self) {
        self.lifetime_rewrites.fetch_add(1, Ordering::Relaxed);
    }
}

/// Cap the per-snapshot client list so the JSON stays small even with
/// thousands of clients. Sorted by `total_bytes_out` desc — the
/// "biggest talkers" view operators usually want.
pub const MAX_CLIENTS_IN_SNAPSHOT: usize = 200;

#[derive(Serialize)]
pub struct FrontendSnapshot {
    pub bind: String,
    pub started_at: u64,
    pub active_clients: usize,
    pub lifetime_connections: u64,
    pub lifetime_disconnects: u64,
    pub lifetime_rewrites: u64,
    pub total_bytes_in: u64,
    pub total_bytes_out: u64,
    pub bytes_in_1m: u64,
    pub bytes_in_5m: u64,
    pub bytes_in_30m: u64,
    pub bytes_out_1m: u64,
    pub bytes_out_5m: u64,
    pub bytes_out_30m: u64,
    pub frames_out_1m: u64,
    pub frames_out_5m: u64,
    pub frames_out_30m: u64,
    pub clients: Vec<ClientSnapshot>,
}

#[derive(Serialize)]
pub struct ClientSnapshot {
    pub id: String,
    pub remote: String,
    pub subprotocol: &'static str,
    pub connected_at: u64,
    pub idle_secs: u64,
    pub subscriptions: Vec<String>,
    pub one_off_queries: u64,
    pub rewrites: u64,
    pub call_reducers: u64,
    pub call_procedures: u64,
    pub total_bytes_in: u64,
    pub total_bytes_out: u64,
    pub total_frames_in: u64,
    pub total_frames_out: u64,
    pub bytes_in_1m: u64,
    pub bytes_in_5m: u64,
    pub bytes_in_30m: u64,
    pub bytes_out_1m: u64,
    pub bytes_out_5m: u64,
    pub bytes_out_30m: u64,
    pub frames_out_1m: u64,
    pub frames_out_5m: u64,
    pub frames_out_30m: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sliding_counter_sums_recent_minutes() {
        let c = SlidingCounter::new();
        c.record(10);
        c.record(7);
        assert_eq!(c.last_minutes(1), 17);
        assert_eq!(c.last_minutes(30), 17);
    }

    #[test]
    fn client_stats_record_increments_totals() {
        let s = ClientStats::new("127.0.0.1:1".parse().unwrap(), Subprotocol::V2);
        s.record_inbound(100);
        s.record_outbound(50);
        assert_eq!(s.total_bytes_in.load(Ordering::Relaxed), 100);
        assert_eq!(s.total_bytes_out.load(Ordering::Relaxed), 50);
        assert_eq!(s.total_frames_in.load(Ordering::Relaxed), 1);
        assert_eq!(s.total_frames_out.load(Ordering::Relaxed), 1);
    }
}
