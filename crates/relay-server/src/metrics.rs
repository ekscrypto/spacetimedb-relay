// SPDX-License-Identifier: MIT

//! Lightweight in-memory metrics for the dashboard.
//!
//! Each counter is a one-second-bucket VecDeque pruned at 30 minutes —
//! deliberately bounded (≤1800 entries per counter) so a long-running
//! relay's footprint stays predictable.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use parking_lot::Mutex;
use sysinfo::{Pid, ProcessesToUpdate, System};

use relay_engine::ClientId;

const WINDOW_RETENTION_SECS: u64 = 30 * 60;

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[derive(Default)]
struct WindowedCounter {
    buckets: Mutex<std::collections::VecDeque<(u64, u64)>>,
}

impl WindowedCounter {
    fn record(&self, n: u64) {
        if n == 0 {
            return;
        }
        let now = now_secs();
        let mut g = self.buckets.lock();
        match g.back_mut() {
            Some(back) if back.0 == now => back.1 += n,
            _ => g.push_back((now, n)),
        }
        while let Some(front) = g.front() {
            if now.saturating_sub(front.0) > WINDOW_RETENTION_SECS {
                g.pop_front();
            } else {
                break;
            }
        }
    }

    fn sum_last(&self, secs: u64) -> u64 {
        let now = now_secs();
        let g = self.buckets.lock();
        g.iter()
            .filter(|(t, _)| now.saturating_sub(*t) < secs)
            .map(|(_, v)| *v)
            .sum()
    }

    fn windows(&self) -> WindowSnapshot {
        WindowSnapshot {
            last_1m: self.sum_last(60),
            last_5m: self.sum_last(300),
            last_30m: self.sum_last(1800),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, serde::Serialize)]
pub struct WindowSnapshot {
    pub last_1m: u64,
    pub last_5m: u64,
    pub last_30m: u64,
}

#[derive(Default)]
pub struct UpstreamMetrics {
    connected: AtomicBool,
    connected_since_ms: AtomicU64,
    last_frame_ms: AtomicU64,
    last_ping_ms: AtomicU64,
    bytes_in: WindowedCounter,
    frames_in: WindowedCounter,
    rows_inserted: WindowedCounter,
    rows_deleted: WindowedCounter,
}

impl UpstreamMetrics {
    pub fn set_connected(&self) {
        self.connected.store(true, Ordering::Relaxed);
        self.connected_since_ms
            .store(now_millis(), Ordering::Relaxed);
    }

    pub fn set_disconnected(&self) {
        self.connected.store(false, Ordering::Relaxed);
        self.connected_since_ms.store(0, Ordering::Relaxed);
    }

    pub fn record_frame(&self, bytes: u64) {
        self.frames_in.record(1);
        self.bytes_in.record(bytes);
        self.last_frame_ms.store(now_millis(), Ordering::Relaxed);
    }

    pub fn record_ping(&self) {
        self.last_ping_ms.store(now_millis(), Ordering::Relaxed);
    }

    pub fn record_rows(&self, inserted: u64, deleted: u64) {
        self.rows_inserted.record(inserted);
        self.rows_deleted.record(deleted);
    }

    pub fn snapshot(&self) -> UpstreamSnapshot {
        UpstreamSnapshot {
            connected: self.connected.load(Ordering::Relaxed),
            connected_since_ms: nonzero(self.connected_since_ms.load(Ordering::Relaxed)),
            last_frame_ms: nonzero(self.last_frame_ms.load(Ordering::Relaxed)),
            last_ping_ms: nonzero(self.last_ping_ms.load(Ordering::Relaxed)),
            bytes_in: self.bytes_in.windows(),
            frames_in: self.frames_in.windows(),
            rows_inserted: self.rows_inserted.windows(),
            rows_deleted: self.rows_deleted.windows(),
        }
    }
}

/// Aggregate downstream counters that survive client disconnect. The
/// dashboard shows these regardless of how many clients are currently
/// connected, so a hit-and-run subscriber's traffic is still visible.
#[derive(Default)]
pub struct DownstreamMetrics {
    clients_seen: AtomicU64,
    bytes_out: WindowedCounter,
    rows_inserted: WindowedCounter,
    rows_deleted: WindowedCounter,
    oneoff_queries: WindowedCounter,
}

impl DownstreamMetrics {
    fn record_outbound(&self, bytes: u64, inserted: u64, deleted: u64) {
        self.bytes_out.record(bytes);
        self.rows_inserted.record(inserted);
        self.rows_deleted.record(deleted);
    }

    fn record_oneoff(&self) {
        self.oneoff_queries.record(1);
    }

    fn snapshot(&self) -> DownstreamSnapshot {
        DownstreamSnapshot {
            clients_seen: self.clients_seen.load(Ordering::Relaxed),
            bytes_out: self.bytes_out.windows(),
            rows_inserted: self.rows_inserted.windows(),
            rows_deleted: self.rows_deleted.windows(),
            oneoff_queries: self.oneoff_queries.windows(),
        }
    }
}

pub struct ClientMetrics {
    pub addr: String,
    connected_since_ms: AtomicU64,
    pub subscriptions: AtomicUsize,
    bytes_out: WindowedCounter,
    rows_inserted: WindowedCounter,
    rows_deleted: WindowedCounter,
    oneoff_queries: WindowedCounter,
    downstream: Arc<DownstreamMetrics>,
}

impl ClientMetrics {
    fn new(addr: String, downstream: Arc<DownstreamMetrics>) -> Self {
        Self {
            addr,
            connected_since_ms: AtomicU64::new(now_millis()),
            subscriptions: AtomicUsize::default(),
            bytes_out: WindowedCounter::default(),
            rows_inserted: WindowedCounter::default(),
            rows_deleted: WindowedCounter::default(),
            oneoff_queries: WindowedCounter::default(),
            downstream,
        }
    }

    pub fn record_outbound(&self, bytes: u64, inserted: u64, deleted: u64) {
        self.bytes_out.record(bytes);
        self.rows_inserted.record(inserted);
        self.rows_deleted.record(deleted);
        self.downstream.record_outbound(bytes, inserted, deleted);
    }

    pub fn record_oneoff(&self) {
        self.oneoff_queries.record(1);
        self.downstream.record_oneoff();
    }

    pub fn set_subscriptions(&self, n: usize) {
        self.subscriptions.store(n, Ordering::Relaxed);
    }

    fn snapshot(&self, client_id: ClientId) -> ClientSnapshot {
        ClientSnapshot {
            client_id: client_id.0,
            addr: self.addr.clone(),
            connected_since_ms: self.connected_since_ms.load(Ordering::Relaxed),
            subscriptions: self.subscriptions.load(Ordering::Relaxed),
            bytes_out: self.bytes_out.windows(),
            rows_inserted: self.rows_inserted.windows(),
            rows_deleted: self.rows_deleted.windows(),
            oneoff_queries: self.oneoff_queries.windows(),
        }
    }
}

pub struct Metrics {
    pub upstream: UpstreamMetrics,
    pub downstream: Arc<DownstreamMetrics>,
    clients: DashMap<ClientId, Arc<ClientMetrics>>,
    system: SystemMetrics,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            upstream: UpstreamMetrics::default(),
            downstream: Arc::new(DownstreamMetrics::default()),
            clients: DashMap::new(),
            system: SystemMetrics::new(),
        })
    }

    pub fn register_client(&self, id: ClientId, addr: String) -> Arc<ClientMetrics> {
        self.downstream.clients_seen.fetch_add(1, Ordering::Relaxed);
        let m = Arc::new(ClientMetrics::new(addr, self.downstream.clone()));
        self.clients.insert(id, m.clone());
        m
    }

    pub fn deregister_client(&self, id: ClientId) {
        self.clients.remove(&id);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let mut clients: Vec<ClientSnapshot> = self
            .clients
            .iter()
            .map(|e| e.value().snapshot(*e.key()))
            .collect();
        clients.sort_by_key(|c| c.client_id);
        MetricsSnapshot {
            now_ms: now_millis(),
            upstream: self.upstream.snapshot(),
            downstream: self.downstream.snapshot(),
            system: self.system.snapshot(),
            n_clients: clients.len(),
            clients,
        }
    }
}

struct SystemMetrics {
    pid: Pid,
    sys: Mutex<System>,
}

impl SystemMetrics {
    fn new() -> Self {
        let pid = Pid::from_u32(std::process::id());
        let mut sys = System::new();
        sys.refresh_cpu_usage();
        sys.refresh_memory();
        sys.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
        Self {
            pid,
            sys: Mutex::new(sys),
        }
    }

    fn snapshot(&self) -> SystemSnapshot {
        let mut sys = self.sys.lock();
        sys.refresh_cpu_usage();
        sys.refresh_memory();
        sys.refresh_processes(ProcessesToUpdate::Some(&[self.pid]), true);

        let load = System::load_average();
        let (relay_memory_bytes, relay_cpu_percent) = sys
            .process(self.pid)
            .map(|p| (p.memory(), p.cpu_usage()))
            .unwrap_or((0, 0.0));

        SystemSnapshot {
            host_cpu_percent: sys.global_cpu_usage(),
            host_memory_total: sys.total_memory(),
            host_memory_used: sys.used_memory(),
            host_load_avg_1m: load.one,
            host_load_avg_5m: load.five,
            host_load_avg_15m: load.fifteen,
            relay_memory_bytes,
            relay_cpu_percent,
        }
    }
}

#[derive(serde::Serialize)]
pub struct UpstreamSnapshot {
    pub connected: bool,
    pub connected_since_ms: Option<u64>,
    pub last_frame_ms: Option<u64>,
    pub last_ping_ms: Option<u64>,
    pub bytes_in: WindowSnapshot,
    pub frames_in: WindowSnapshot,
    pub rows_inserted: WindowSnapshot,
    pub rows_deleted: WindowSnapshot,
}

#[derive(serde::Serialize)]
pub struct DownstreamSnapshot {
    pub clients_seen: u64,
    pub bytes_out: WindowSnapshot,
    pub rows_inserted: WindowSnapshot,
    pub rows_deleted: WindowSnapshot,
    pub oneoff_queries: WindowSnapshot,
}

#[derive(serde::Serialize)]
pub struct ClientSnapshot {
    pub client_id: u64,
    pub addr: String,
    pub connected_since_ms: u64,
    pub subscriptions: usize,
    pub bytes_out: WindowSnapshot,
    pub rows_inserted: WindowSnapshot,
    pub rows_deleted: WindowSnapshot,
    pub oneoff_queries: WindowSnapshot,
}

#[derive(serde::Serialize)]
pub struct SystemSnapshot {
    pub host_cpu_percent: f32,
    pub host_memory_total: u64,
    pub host_memory_used: u64,
    pub host_load_avg_1m: f64,
    pub host_load_avg_5m: f64,
    pub host_load_avg_15m: f64,
    pub relay_memory_bytes: u64,
    pub relay_cpu_percent: f32,
}

#[derive(serde::Serialize)]
pub struct MetricsSnapshot {
    pub now_ms: u64,
    pub upstream: UpstreamSnapshot,
    pub downstream: DownstreamSnapshot,
    pub system: SystemSnapshot,
    pub n_clients: usize,
    pub clients: Vec<ClientSnapshot>,
}

fn nonzero(v: u64) -> Option<u64> {
    (v != 0).then_some(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windowed_counter_sums_recent_records() {
        let w = WindowedCounter::default();
        w.record(5);
        w.record(7);
        let snap = w.windows();
        assert_eq!(snap.last_1m, 12);
        assert_eq!(snap.last_5m, 12);
        assert_eq!(snap.last_30m, 12);
    }

    #[test]
    fn windowed_counter_zero_record_is_a_noop() {
        let w = WindowedCounter::default();
        w.record(0);
        assert_eq!(w.sum_last(60), 0);
    }

    #[test]
    fn metrics_register_and_deregister_a_client() {
        let m = Metrics::new();
        let cm = m.register_client(ClientId(42), "1.2.3.4:5678".into());
        cm.record_outbound(100, 3, 1);
        cm.record_oneoff();
        cm.set_subscriptions(2);
        let snap = m.snapshot();
        assert_eq!(snap.n_clients, 1);
        let c = &snap.clients[0];
        assert_eq!(c.client_id, 42);
        assert_eq!(c.bytes_out.last_1m, 100);
        assert_eq!(c.rows_inserted.last_1m, 3);
        assert_eq!(c.rows_deleted.last_1m, 1);
        assert_eq!(c.oneoff_queries.last_1m, 1);
        assert_eq!(c.subscriptions, 2);

        m.deregister_client(ClientId(42));
        let snap = m.snapshot();
        assert_eq!(snap.n_clients, 0);
    }

    #[test]
    fn upstream_connection_state_round_trips() {
        let u = UpstreamMetrics::default();
        assert!(!u.snapshot().connected);
        u.set_connected();
        u.record_frame(120);
        u.record_ping();
        u.record_rows(4, 2);
        let s = u.snapshot();
        assert!(s.connected);
        assert!(s.connected_since_ms.is_some());
        assert!(s.last_frame_ms.is_some());
        assert!(s.last_ping_ms.is_some());
        assert_eq!(s.bytes_in.last_1m, 120);
        assert_eq!(s.frames_in.last_1m, 1);
        assert_eq!(s.rows_inserted.last_1m, 4);
        assert_eq!(s.rows_deleted.last_1m, 2);
        u.set_disconnected();
        assert!(!u.snapshot().connected);
    }

    #[test]
    fn snapshot_serializes_to_json() {
        let m = Metrics::new();
        m.upstream.set_connected();
        m.upstream.record_frame(10);
        m.register_client(ClientId(1), "addr".into())
            .record_outbound(50, 1, 0);
        let s = m.snapshot();
        let json = serde_json::to_string(&s).unwrap();
        assert!(json.contains("\"connected\":true"));
        assert!(json.contains("\"n_clients\":1"));
        assert!(json.contains("\"bytes_out\""));
        assert!(json.contains("\"downstream\""));
        assert!(json.contains("\"system\""));
        assert!(json.contains("\"host_cpu_percent\""));
        assert!(json.contains("\"relay_memory_bytes\""));
    }

    #[test]
    fn system_snapshot_reports_relay_process_memory() {
        let m = Metrics::new();
        let s = m.snapshot();
        assert!(
            s.system.relay_memory_bytes > 0,
            "relay process RSS should be reported"
        );
        assert!(s.system.host_memory_total >= s.system.host_memory_used);
    }

    #[test]
    fn downstream_totals_survive_client_disconnect() {
        let m = Metrics::new();
        let cm = m.register_client(ClientId(1), "1.2.3.4:1".into());
        cm.record_outbound(120, 4, 1);
        cm.record_oneoff();

        let cm2 = m.register_client(ClientId(2), "1.2.3.4:2".into());
        cm2.record_outbound(80, 2, 0);

        m.deregister_client(ClientId(1));
        m.deregister_client(ClientId(2));

        let s = m.snapshot();
        assert_eq!(s.n_clients, 0);
        assert_eq!(s.downstream.clients_seen, 2);
        assert_eq!(s.downstream.bytes_out.last_1m, 200);
        assert_eq!(s.downstream.rows_inserted.last_1m, 6);
        assert_eq!(s.downstream.rows_deleted.last_1m, 1);
        assert_eq!(s.downstream.oneoff_queries.last_1m, 1);
    }
}
