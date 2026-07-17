// SPDX-License-Identifier: MIT

//! In-process dashboard. Tracks the link to upstream, the link to
//! local SpacetimeDB, and the publisher's last action. Per-table row
//! counts and on-disk sizes belong to the local SpacetimeDB process —
//! query it via SQL if you need those.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::{Query, State};
use axum::response::{Html, IntoResponse, Json};
use axum::routing::get;
use axum::Router;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};

const WINDOW_BUCKETS: usize = 180; // 30 min × 6 buckets/min
const BUCKET_SECS: u64 = 10; // must evenly divide 60

/// Sliding-window counter with 10-second buckets, 180 buckets (30 min)
/// total. Reads are weighted across the current and aging buckets so
/// the value is continuous across rollover instead of snapping to zero.
pub struct SlidingCounter {
    inner: Mutex<SlidingInner>,
}

struct SlidingInner {
    /// Buckets indexed by `epoch_bucket % WINDOW_BUCKETS`.
    buckets: [u64; WINDOW_BUCKETS],
    /// Most recent epoch-bucket we wrote to. Used to detect bucket
    /// rollover and zero out stale buckets between then and now.
    last_bucket: u64,
}

impl SlidingCounter {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(SlidingInner {
                buckets: [0; WINDOW_BUCKETS],
                last_bucket: 0,
            }),
        }
    }

    pub fn record(&self, n: u64) {
        let now_bucket = epoch_bucket();
        let mut inner = self.inner.lock();
        self.advance_locked(&mut inner, now_bucket);
        let idx = (now_bucket as usize) % WINDOW_BUCKETS;
        inner.buckets[idx] = inner.buckets[idx].saturating_add(n);
    }

    pub fn last_minutes(&self, minutes: usize) -> u64 {
        let now_secs = epoch_secs();
        let now_bucket = now_secs / BUCKET_SECS;
        let elapsed = now_secs % BUCKET_SECS;
        let mut inner = self.inner.lock();
        self.advance_locked(&mut inner, now_bucket);
        windowed_sum(&inner.buckets, now_bucket, elapsed, minutes)
    }

    /// Zero out any bucket that's older than the window. Called on
    /// every record + read, so the counter is always self-consistent
    /// even if no traffic has flowed in 30 minutes.
    fn advance_locked(&self, inner: &mut SlidingInner, now_bucket: u64) {
        if inner.last_bucket == 0 {
            inner.last_bucket = now_bucket;
            return;
        }
        if now_bucket <= inner.last_bucket {
            return;
        }
        let gap = (now_bucket - inner.last_bucket).min(WINDOW_BUCKETS as u64);
        for i in 1..=gap {
            let m = inner.last_bucket + i;
            let idx = (m as usize) % WINDOW_BUCKETS;
            inner.buckets[idx] = 0;
        }
        inner.last_bucket = now_bucket;
    }
}

fn epoch_bucket() -> u64 {
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

/// Weighted sliding sum over the last `minutes` minutes.
///
/// `now_bucket` is the current epoch-bucket index; `elapsed` is the
/// seconds elapsed within it (0..BUCKET_SECS). The current bucket
/// contributes proportionally to `elapsed`, the bucket aging out of the
/// rear of the window contributes proportionally to the remaining slice,
/// and every bucket in between counts in full — so the result is
/// continuous across bucket rollover instead of snapping to zero.
///
/// Pure (no lock, no clock) so unit tests can exercise the rollover
/// boundary deterministically. Result is a count (units = bucket units).
fn windowed_sum(buckets: &[u64; WINDOW_BUCKETS], now_bucket: u64, elapsed: u64, minutes: usize) -> u64 {
    const BUCKETS_PER_MIN: usize = (60 / BUCKET_SECS) as usize; // 6
    let want = minutes.saturating_mul(BUCKETS_PER_MIN).min(WINDOW_BUCKETS);
    let now_slot = (now_bucket as usize) % WINDOW_BUCKETS;
    let mut acc: u128 = 0;
    // Bucket aging out of the rear of the window (skip if it aliases the
    // current slot — happens only for the full 30-minute read).
    if want > 0 {
        let aging = now_bucket.saturating_sub(want as u64);
        if (aging as usize) % WINDOW_BUCKETS != now_slot {
            let w = (BUCKET_SECS - elapsed) as u128;
            acc += (buckets[(aging as usize) % WINDOW_BUCKETS] as u128) * w;
        }
    }
    // Fully-covered middle buckets (full weight).
    for k in 1..want {
        let m = now_bucket.saturating_sub(k as u64);
        acc += (buckets[(m as usize) % WINDOW_BUCKETS] as u128) * BUCKET_SECS as u128;
    }
    // In-progress current bucket (partial weight).
    acc += (buckets[now_slot] as u128) * elapsed as u128;
    (acc / BUCKET_SECS as u128).min(u64::MAX as u128) as u64
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
    /// Lifetime count of times the local mirror module was detected
    /// dead (WASM panic) and a force-republish was triggered.
    /// Only meaningful on `local_stdb`; always 0 on `upstream`.
    pub module_death_count: AtomicU64,
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
            module_death_count: AtomicU64::new(0),
        }
    }

    pub fn mark_module_dead(&self) {
        self.module_death_count.fetch_add(1, Ordering::Relaxed);
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
            self.last_published_at
                .store(epoch_secs(), Ordering::Relaxed);
            self.republished_this_run.store(1, Ordering::Relaxed);
        }
    }
}

/// Bounded in-process log ring. The `EventLogLayer` (a
/// `tracing_subscriber::Layer`) pushes structured events here; the
/// dashboard's `/events` endpoint reads them. Capacity is per-process,
/// not per-target — events are evicted oldest-first when full.
pub struct EventRing {
    capacity: usize,
    inner: Mutex<EventRingInner>,
}

struct EventRingInner {
    /// Monotonic sequence assigned to every pushed event. Lets the
    /// dashboard poll `/events?since=N` and only fetch new lines.
    seq: u64,
    events: VecDeque<LogEvent>,
}

#[derive(Clone, Serialize)]
pub struct LogEvent {
    pub seq: u64,
    /// Milliseconds since UNIX epoch. Millisecond precision is enough
    /// to order events that fire within the same second.
    pub ts_ms: u64,
    pub level: &'static str,
    pub target: String,
    pub message: String,
    pub fields: Vec<(String, String)>,
}

impl EventRing {
    pub fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            capacity,
            inner: Mutex::new(EventRingInner {
                seq: 0,
                events: VecDeque::with_capacity(capacity),
            }),
        })
    }

    pub fn push(
        &self,
        level: &'static str,
        target: String,
        message: String,
        fields: Vec<(String, String)>,
    ) {
        let ts_ms = epoch_millis();
        let mut inner = self.inner.lock();
        inner.seq += 1;
        let event = LogEvent {
            seq: inner.seq,
            ts_ms,
            level,
            target,
            message,
            fields,
        };
        if inner.events.len() >= self.capacity {
            inner.events.pop_front();
        }
        inner.events.push_back(event);
    }

    /// Returns events with `seq > since`, capped at `max`. Used by the
    /// dashboard's polling tail.
    pub fn snapshot_since(&self, since: u64, max: usize) -> EventsSnapshot {
        let inner = self.inner.lock();
        let events: Vec<LogEvent> = inner
            .events
            .iter()
            .filter(|e| e.seq > since)
            .take(max)
            .cloned()
            .collect();
        EventsSnapshot {
            current_seq: inner.seq,
            events,
        }
    }
}

#[derive(Serialize)]
pub struct EventsSnapshot {
    pub current_seq: u64,
    pub events: Vec<LogEvent>,
}

fn epoch_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
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
    pub events: Arc<EventRing>,
    /// Optional handle to the frontend proxy. `None` when
    /// `--frontend-bind` is empty.
    pub frontend: Mutex<Option<FrontendHandles>>,
}

#[derive(Clone)]
pub struct FrontendHandles {
    pub metrics: Arc<relay_frontend::FrontendMetrics>,
    pub clients: relay_frontend::ActiveClients,
}

impl Metrics {
    pub fn new(
        upstream_database: String,
        mirror_database: String,
        max_in_flight: u64,
        events: Arc<EventRing>,
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
            events,
            frontend: Mutex::new(None),
        })
    }

    pub fn install_frontend(&self, handles: FrontendHandles) {
        *self.frontend.lock() = Some(handles);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let frontend = self
            .frontend
            .lock()
            .as_ref()
            .map(|h| relay_frontend::state::snapshot(&h.metrics, &h.clients));
        MetricsSnapshot {
            now: epoch_secs(),
            started_at: self.started_at,
            upstream_database: self.upstream_database.clone(),
            mirror_database: self.mirror_database.clone(),
            upstream: link_snapshot(&self.upstream),
            local_stdb: link_snapshot(&self.local_stdb),
            publisher: PublisherSnapshot {
                fingerprint: self.publisher.fingerprint.lock().clone(),
                last_published_at: nonzero(
                    self.publisher.last_published_at.load(Ordering::Relaxed),
                ),
                republished_this_run: self.publisher.republished_this_run.load(Ordering::Relaxed)
                    != 0,
            },
            in_flight: InFlightSnapshot {
                max: self.max_in_flight.load(Ordering::Relaxed),
                available: self.available_permits.load(Ordering::Relaxed),
            },
            frontend,
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
        module_death_count: m.module_death_count.load(Ordering::Relaxed),
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub frontend: Option<relay_frontend::FrontendSnapshot>,
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
    /// Lifetime count of module-fatal-error events; nonzero means
    /// at least one WASM panic was detected this run.
    #[serde(skip_serializing_if = "is_zero")]
    pub module_death_count: u64,
}

fn is_zero(n: &u64) -> bool {
    *n == 0
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
        .route("/events", get(events_json))
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

#[derive(Deserialize)]
pub struct EventsQuery {
    #[serde(default)]
    since: u64,
    #[serde(default = "default_max")]
    max: usize,
}

fn default_max() -> usize {
    200
}

async fn events_json(
    State(metrics): State<Arc<Metrics>>,
    Query(q): Query<EventsQuery>,
) -> impl IntoResponse {
    let max = q.max.min(1000);
    Json(metrics.events.snapshot_since(q.since, max))
}

async fn index() -> impl IntoResponse {
    Html(INDEX_HTML)
}

const INDEX_HTML: &str = include_str!("dashboard.html");

/// Custom `tracing_subscriber::Layer` that captures events with target
/// prefix `relay` and pushes them into the dashboard's event ring.
///
/// We capture every `relay::*` event regardless of `RUST_LOG` (the
/// fmt layer still respects it) so the dashboard can show debug-level
/// detail without restarting with verbose env. Pair this layer with a
/// per-layer `EnvFilter` like `EnvFilter::new("relay=debug")` so that
/// it only sees relay events even when other crates are noisy.
pub struct EventLogLayer {
    ring: Arc<EventRing>,
}

impl EventLogLayer {
    pub fn new(ring: Arc<EventRing>) -> Self {
        Self { ring }
    }
}

impl<S> tracing_subscriber::Layer<S> for EventLogLayer
where
    S: tracing::Subscriber,
{
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let metadata = event.metadata();
        let target = metadata.target();
        if !target.starts_with("relay") {
            return;
        }
        let mut visit = FieldCapture::default();
        event.record(&mut visit);
        self.ring.push(
            metadata.level().as_str(),
            target.to_string(),
            visit.message,
            visit.fields,
        );
    }
}

#[derive(Default)]
struct FieldCapture {
    message: String,
    fields: Vec<(String, String)>,
}

impl tracing::field::Visit for FieldCapture {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_string();
        } else {
            self.fields
                .push((field.name().to_string(), value.to_string()));
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        let formatted = format!("{value:?}");
        if field.name() == "message" {
            self.message = formatted;
        } else {
            self.fields.push((field.name().to_string(), formatted));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::Layer;

    #[test]
    fn sliding_counter_sums_recent_minutes() {
        // Both records land in the current bucket; at the bucket boundary
        // (elapsed == BUCKET_SECS) the full 17 is visible in every window.
        let mut buckets = [0u64; WINDOW_BUCKETS];
        let now_bucket = 1_000_000u64;
        buckets[now_bucket as usize % WINDOW_BUCKETS] = 17;
        assert_eq!(windowed_sum(&buckets, now_bucket, BUCKET_SECS, 1), 17);
        assert_eq!(windowed_sum(&buckets, now_bucket, BUCKET_SECS, 30), 17);
    }

    #[test]
    fn sliding_counter_weights_rollover() {
        // 30 sits in the bucket about to leave a 1-minute (6-bucket) window.
        // As the current bucket fills, the rear bucket ramps out: full at
        // elapsed=0, half at 5s, gone at 10s. Old code returned the current
        // bucket only (0 here) — the snap-to-zero bug.
        let now = 1_000_000u64;
        let rear = now - 6;
        let mut buckets = [0u64; WINDOW_BUCKETS];
        buckets[rear as usize % WINDOW_BUCKETS] = 30;
        assert_eq!(windowed_sum(&buckets, now, 0, 1), 30);
        assert_eq!(windowed_sum(&buckets, now, BUCKET_SECS / 2, 1), 15);
        assert_eq!(windowed_sum(&buckets, now, BUCKET_SECS, 1), 0);
    }

    #[test]
    fn ring_evicts_oldest_when_full() {
        let ring = EventRing::new(3);
        for i in 0..5 {
            ring.push("info", "relay::test".into(), format!("m{i}"), vec![]);
        }
        let snap = ring.snapshot_since(0, 100);
        assert_eq!(snap.current_seq, 5);
        assert_eq!(snap.events.len(), 3);
        assert_eq!(snap.events[0].message, "m2");
        assert_eq!(snap.events[2].message, "m4");
    }

    #[test]
    fn snapshot_since_returns_only_new_events() {
        let ring = EventRing::new(10);
        ring.push("info", "relay::test".into(), "first".into(), vec![]);
        let s1 = ring.snapshot_since(0, 100);
        assert_eq!(s1.current_seq, 1);
        ring.push("info", "relay::test".into(), "second".into(), vec![]);
        let s2 = ring.snapshot_since(s1.current_seq, 100);
        assert_eq!(s2.events.len(), 1);
        assert_eq!(s2.events[0].message, "second");
    }

    #[test]
    fn layer_captures_relay_targets_and_skips_others() {
        let ring = EventRing::new(50);
        let layer = EventLogLayer::new(ring.clone()).with_filter(
            tracing_subscriber::EnvFilter::new("relay=trace,other=trace"),
        );
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(target: "relay::test", field_a = "v1", "captured-message");
            tracing::info!(target: "other::test", "should-not-appear");
        });
        let snap = ring.snapshot_since(0, 100);
        assert_eq!(snap.events.len(), 1);
        let ev = &snap.events[0];
        assert_eq!(ev.target, "relay::test");
        assert_eq!(ev.message, "captured-message");
        assert_eq!(ev.level, "INFO");
        assert!(ev.fields.iter().any(|(k, v)| k == "field_a" && v == "v1"));
    }
}
