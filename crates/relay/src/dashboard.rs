// SPDX-License-Identifier: MIT

//! In-process dashboard. Tracks the link to upstream, the link to
//! local SpacetimeDB, and the publisher's last action. Per-table row
//! counts and on-disk sizes belong to the local SpacetimeDB process —
//! query it via SQL if you need those.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::State;
use axum::response::{Html, IntoResponse, Json};
use axum::routing::get;
use axum::Router;
use parking_lot::Mutex;
use serde::Serialize;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};

const WINDOW_BUCKETS: usize = 30;
const BUCKET_SECS: u64 = 60;

/// Sliding-window counter with per-minute buckets, 30 buckets total.
/// Reads return the sum of the last N buckets relative to the current
/// minute; reads of the partially-filled current bucket are included.
pub struct SlidingCounter {
    inner: Mutex<SlidingInner>,
}

struct SlidingInner {
    /// Buckets indexed by `epoch_minute % WINDOW_BUCKETS`.
    buckets: [u64; WINDOW_BUCKETS],
    /// Most recent epoch-minute we wrote to. Used to detect bucket
    /// rollover and zero out stale buckets between then and now.
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
        let now_minute = epoch_minute();
        let mut inner = self.inner.lock();
        self.advance_locked(&mut inner, now_minute);
        let idx = (now_minute as usize) % WINDOW_BUCKETS;
        inner.buckets[idx] = inner.buckets[idx].saturating_add(n);
    }

    pub fn last_minutes(&self, minutes: usize) -> u64 {
        let now_minute = epoch_minute();
        let mut inner = self.inner.lock();
        self.advance_locked(&mut inner, now_minute);
        let take = minutes.min(WINDOW_BUCKETS);
        let mut sum = 0u64;
        for i in 0..take {
            let m = now_minute.saturating_sub(i as u64);
            let idx = (m as usize) % WINDOW_BUCKETS;
            sum = sum.saturating_add(inner.buckets[idx]);
        }
        sum
    }

    /// Zero out any bucket that's older than the window. Called on
    /// every record + read, so the counter is always self-consistent
    /// even if no traffic has flowed in 30 minutes.
    fn advance_locked(&self, inner: &mut SlidingInner, now_minute: u64) {
        if inner.last_minute == 0 {
            inner.last_minute = now_minute;
            return;
        }
        if now_minute <= inner.last_minute {
            return;
        }
        let gap = (now_minute - inner.last_minute).min(WINDOW_BUCKETS as u64);
        for i in 1..=gap {
            let m = inner.last_minute + i;
            let idx = (m as usize) % WINDOW_BUCKETS;
            inner.buckets[idx] = 0;
        }
        inner.last_minute = now_minute;
    }
}

fn epoch_minute() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() / BUCKET_SECS)
        .unwrap_or(0)
}

fn epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Enum-style atomic for a connection's coarse state. We only need
/// three values, so a u8 is plenty.
#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub enum LinkState {
    /// Never connected since process start.
    Initial = 0,
    /// Connected and exchanging traffic.
    Up = 1,
    /// Was connected, currently down.
    Down = 2,
}

impl LinkState {
    fn from_u8(n: u8) -> Self {
        match n {
            1 => Self::Up,
            2 => Self::Down,
            _ => Self::Initial,
        }
    }
    fn label(self) -> &'static str {
        match self {
            Self::Initial => "initial",
            Self::Up => "up",
            Self::Down => "down",
        }
    }
}

pub struct LinkMetrics {
    state: AtomicU8,
    last_up_at: AtomicU64,
    last_down_at: AtomicU64,
    last_disconnect_reason: Mutex<Option<String>>,
    /// Total bytes / frames received (or sent, depending on direction)
    /// since process start.
    total_bytes: AtomicU64,
    total_units: AtomicU64,
    /// Per-window byte / unit counts for the last 30 minutes.
    bytes_window: SlidingCounter,
    units_window: SlidingCounter,
}

impl LinkMetrics {
    fn new() -> Self {
        Self {
            state: AtomicU8::new(LinkState::Initial as u8),
            last_up_at: AtomicU64::new(0),
            last_down_at: AtomicU64::new(0),
            last_disconnect_reason: Mutex::new(None),
            total_bytes: AtomicU64::new(0),
            total_units: AtomicU64::new(0),
            bytes_window: SlidingCounter::new(),
            units_window: SlidingCounter::new(),
        }
    }

    pub fn mark_up(&self) {
        self.state.store(LinkState::Up as u8, Ordering::Relaxed);
        self.last_up_at.store(epoch_secs(), Ordering::Relaxed);
    }

    pub fn mark_down(&self, reason: Option<String>) {
        self.state.store(LinkState::Down as u8, Ordering::Relaxed);
        self.last_down_at.store(epoch_secs(), Ordering::Relaxed);
        *self.last_disconnect_reason.lock() = reason;
    }

    pub fn record_traffic(&self, bytes: u64, units: u64) {
        self.total_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.total_units.fetch_add(units, Ordering::Relaxed);
        if bytes > 0 {
            self.bytes_window.record(bytes);
        }
        if units > 0 {
            self.units_window.record(units);
        }
    }
}

pub struct PublisherMetrics {
    fingerprint: Mutex<Option<String>>,
    last_published_at: AtomicU64,
    /// True if this process performed at least one (re)publish.
    republished_this_run: AtomicU8,
}

impl PublisherMetrics {
    fn new() -> Self {
        Self {
            fingerprint: Mutex::new(None),
            last_published_at: AtomicU64::new(0),
            republished_this_run: AtomicU8::new(0),
        }
    }

    pub fn record(&self, fingerprint: &str, republished: bool) {
        *self.fingerprint.lock() = Some(fingerprint.to_string());
        if republished {
            self.last_published_at.store(epoch_secs(), Ordering::Relaxed);
            self.republished_this_run.store(1, Ordering::Relaxed);
        }
    }
}

pub struct Metrics {
    pub upstream: LinkMetrics,
    pub local_stdb: LinkMetrics,
    pub publisher: PublisherMetrics,
    pub max_in_flight: AtomicU64,
    pub available_permits: AtomicU64,
    pub started_at: u64,
    pub upstream_database: String,
    pub mirror_database: String,
}

impl Metrics {
    pub fn new(
        upstream_database: String,
        mirror_database: String,
        max_in_flight: u64,
    ) -> Arc<Self> {
        Arc::new(Self {
            upstream: LinkMetrics::new(),
            local_stdb: LinkMetrics::new(),
            publisher: PublisherMetrics::new(),
            max_in_flight: AtomicU64::new(max_in_flight),
            available_permits: AtomicU64::new(max_in_flight),
            started_at: epoch_secs(),
            upstream_database,
            mirror_database,
        })
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            now: epoch_secs(),
            started_at: self.started_at,
            upstream_database: self.upstream_database.clone(),
            mirror_database: self.mirror_database.clone(),
            upstream: link_snapshot(&self.upstream),
            local_stdb: link_snapshot(&self.local_stdb),
            publisher: PublisherSnapshot {
                fingerprint: self.publisher.fingerprint.lock().clone(),
                last_published_at: nonzero(self.publisher.last_published_at.load(Ordering::Relaxed)),
                republished_this_run: self
                    .publisher
                    .republished_this_run
                    .load(Ordering::Relaxed)
                    != 0,
            },
            in_flight: InFlightSnapshot {
                max: self.max_in_flight.load(Ordering::Relaxed),
                available: self.available_permits.load(Ordering::Relaxed),
            },
        }
    }
}

fn link_snapshot(m: &LinkMetrics) -> LinkSnapshot {
    LinkSnapshot {
        state: LinkState::from_u8(m.state.load(Ordering::Relaxed)).label(),
        last_up_at: nonzero(m.last_up_at.load(Ordering::Relaxed)),
        last_down_at: nonzero(m.last_down_at.load(Ordering::Relaxed)),
        last_disconnect_reason: m.last_disconnect_reason.lock().clone(),
        total_bytes: m.total_bytes.load(Ordering::Relaxed),
        total_units: m.total_units.load(Ordering::Relaxed),
        bytes_1m: m.bytes_window.last_minutes(1),
        bytes_5m: m.bytes_window.last_minutes(5),
        bytes_30m: m.bytes_window.last_minutes(30),
        units_1m: m.units_window.last_minutes(1),
        units_5m: m.units_window.last_minutes(5),
        units_30m: m.units_window.last_minutes(30),
    }
}

fn nonzero(secs: u64) -> Option<u64> {
    if secs == 0 {
        None
    } else {
        Some(secs)
    }
}

#[derive(Serialize)]
pub struct MetricsSnapshot {
    pub now: u64,
    pub started_at: u64,
    pub upstream_database: String,
    pub mirror_database: String,
    pub upstream: LinkSnapshot,
    pub local_stdb: LinkSnapshot,
    pub publisher: PublisherSnapshot,
    pub in_flight: InFlightSnapshot,
}

#[derive(Serialize)]
pub struct LinkSnapshot {
    pub state: &'static str,
    pub last_up_at: Option<u64>,
    pub last_down_at: Option<u64>,
    pub last_disconnect_reason: Option<String>,
    pub total_bytes: u64,
    pub total_units: u64,
    pub bytes_1m: u64,
    pub bytes_5m: u64,
    pub bytes_30m: u64,
    pub units_1m: u64,
    pub units_5m: u64,
    pub units_30m: u64,
}

#[derive(Serialize)]
pub struct PublisherSnapshot {
    pub fingerprint: Option<String>,
    pub last_published_at: Option<u64>,
    pub republished_this_run: bool,
}

#[derive(Serialize)]
pub struct InFlightSnapshot {
    pub max: u64,
    pub available: u64,
}

pub async fn serve(bind: SocketAddr, metrics: Arc<Metrics>) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/", get(index))
        .route("/metrics", get(metrics_json))
        .with_state(metrics);
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .map_err(|e| anyhow::anyhow!("dashboard bind {bind}: {e}"))?;
    tracing::info!(target: "relay::dashboard", %bind, "dashboard listening");
    axum::serve(listener, app)
        .await
        .map_err(|e| anyhow::anyhow!("dashboard serve: {e}"))?;
    Ok(())
}

async fn metrics_json(State(metrics): State<Arc<Metrics>>) -> impl IntoResponse {
    Json(metrics.snapshot())
}

async fn index() -> impl IntoResponse {
    Html(INDEX_HTML)
}

const INDEX_HTML: &str = include_str!("dashboard.html");
