// SPDX-License-Identifier: MIT

//! Fleet `/health` aggregator.
//!
//! Two discovery modes:
//!
//! 1. **Public-mirror (preferred):** poll once
//!    `GET {mirrors_url}` (default `http://127.0.0.1:3030/v1/mirrors`)
//!    and map each mirror row into `sources[*]` with public port derived
//!    from the database name (`bitcraft-live-global` → 3000,
//!    `bitcraft-live-N` → `3000+N`). Legacy `relay-*.service` units for
//!    the same source name are skipped (mirror wins).
//! 2. **Legacy relay units only:** when `mirrors_url` is empty, walk
//!    `relay-*.service` files and poll each loopback `/metrics`.
//!
//! ```jsonc
//! {
//!   "sources": {
//!     "global": {
//!       "port": 3000,
//!       "database": "bitcraft-live-global",
//!       "schema_cached": true,
//!       "connectivity": "live",
//!       "tables_live": 12,
//!       "tables_total": 12,
//!       "transactions_processed": 12345,
//!       "transactions_per_sec": 42.0,
//!       "connected_since": "…"
//!     }
//!   },
//!   "schema_count": 1,
//!   "system": { "cpu": {...}, "memory": {...}, "network": {...} }
//! }
//! ```
//!
//! `transactions_per_sec` is computed in-process from a 60×1s rotating
//! bucket ring (mean of per-second deltas of `transactions_processed`).
//! Refreshing the dashboard never resets the rate.
//!
//! Failures are graceful but bounded: a failed `/v1/mirrors` (or
//! per-instance `/metrics`) poll keeps the prior snapshot verbatim for
//! [`MIRROR_POLL_GRACE_CYCLES`] cycles (the poll interval is 30s). Once
//! the grace is exceeded the prior rows are served with
//! `connectivity: "unreachable"` and a `last_success_unix` timestamp —
//! `/health` never keeps reporting `live` for an endpoint that is no
//! longer answering.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::{Mutex, RwLock};
use reqwest::Client;
use serde::Serialize;
use serde_json::{json, Map, Value};
use tokio::sync::Mutex as TokioMutex;

use crate::sys_metrics::SysState;

/// How often the sources poller refreshes the fleet map.
pub const SOURCES_POLL_INTERVAL: Duration = Duration::from_secs(30);
/// Per-instance `/metrics` fetch timeout.
pub const DEFAULT_FETCH_TIMEOUT: Duration = Duration::from_secs(4);
/// Tx/s sampler tick (one bucket per tick).
pub const TX_RATE_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);
/// Number of 1s buckets in the rotating tx/s window.
pub const TX_RATE_BUCKETS: usize = 60;
/// Consecutive failed `/v1/mirrors` polls before prior rows are marked
/// `unreachable`. 1 = the last good snapshot is served verbatim for one
/// failed cycle, then degraded — a sidecar that dies stops looking
/// `live` on `/health` within two poll cycles (~60s).
pub const MIRROR_POLL_GRACE_CYCLES: u32 = 1;
/// Same grace for legacy per-instance `/metrics` fetches before the
/// retained metrics are dropped and the row is marked `unreachable`.
pub const METRICS_POLL_GRACE_CYCLES: u32 = 1;

/// One row of the `sources` map. `metrics` is `None` when the last
/// legacy `/metrics` poll failed AND there was no prior snapshot.
/// Mirror-mode rows populate the connectivity / table / tx fields and
/// leave `metrics` unset.
///
/// When a source's status endpoint stays unreachable past the poll
/// grace, its row is served with `connectivity: "unreachable"` and the
/// last-known field values plus `last_success_unix` — never a stale
/// `"live"`.
#[derive(Clone, Serialize)]
pub struct SourceSnapshot {
    pub port: u16,
    pub database: String,
    pub schema_cached: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connectivity: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tables_live: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tables_total: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transactions_processed: Option<u64>,
    /// Mean tx/s over the last up-to-60 one-second buckets. Populated by
    /// the coordinator's rotating sampler; absent until two samples land.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transactions_per_sec: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connected_since: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disconnected_since: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_attempt_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_attempt_eta_secs: Option<u64>,
    /// When this source's status endpoint last answered successfully
    /// (unix seconds). Preserved through degraded cycles so consumers
    /// can tell how old the rest of the row is.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_success_unix: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metrics: Option<Value>,
}

/// Per-instance facts parsed from the systemd unit file.
#[derive(Clone, Debug)]
pub struct DiscoveredSource {
    /// Source name as shown to the UI. Derived from the unit stem via
    /// the deployment's [`NamingSpec`] (default: passthrough).
    pub name: String,
    /// Mirror database name (`--mirror-database`).
    pub database: String,
    /// Public frontend port (`--frontend-bind`).
    pub frontend_port: u16,
    /// Loopback dashboard port (`--dashboard-bind`).
    pub dashboard_port: u16,
}

/// How a unit stem is projected into the `sources[*]` key shown in
/// `/health`. Defaults to passthrough: the unit stem is used verbatim.
///
/// A deployment with a naming convention supplies a template:
///
/// - `template = "live-{stem}"`, `stem_prefix = "relay-region"`
///   → `relay-region14` becomes `live-14`.
/// - `template = "live-{stem}"`, `stem_prefix = "relay-"`
///   → `relay-region14` becomes `live-region14`.
///
/// `{stem}` is the unit stem after stripping `stem_prefix` (or the full
/// stem if no prefix matches). If the prefix is set but doesn't match,
/// the full stem is used (so a stray `relay-coordinator.service` won't
/// be mis-projected). `template` without a `{stem}` placeholder is
/// returned literally — usually a mistake, but allowed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NamingSpec {
    /// `format!`-style template with a single `{stem}` placeholder.
    /// Empty/`None` → use the (possibly prefix-stripped) stem verbatim.
    pub template: Option<String>,
    /// Prefix to strip from the unit stem before substitution. Empty/
    /// `None` → no stripping.
    pub stem_prefix: Option<String>,
}

impl NamingSpec {
    /// Passthrough naming — the unit stem is the source name as-is.
    pub fn passthrough() -> Self {
        Self::default()
    }

    /// Project a unit stem through this spec.
    pub fn project(&self, stem: &str) -> String {
        let trimmed = match &self.stem_prefix {
            Some(prefix) if !prefix.is_empty() && stem.starts_with(prefix) => {
                &stem[prefix.len()..]
            }
            _ => stem,
        };
        match &self.template {
            Some(tpl) => tpl.replace("{stem}", trimmed),
            None => trimmed.to_string(),
        }
    }
}

/// Shared state for the `/health` handler. Cheap to clone.
#[derive(Clone)]
pub struct HealthState {
    inner: Arc<Inner>,
}

/// Per-source rotating window of 1s `transactions_processed` deltas.
struct SourceTxRate {
    last_tx: Option<u64>,
    buckets: [u64; TX_RATE_BUCKETS],
    /// Next write index into `buckets`.
    head: usize,
    /// How many buckets hold a real sample (`0..TX_RATE_BUCKETS`).
    filled: usize,
}

impl Default for SourceTxRate {
    fn default() -> Self {
        Self {
            last_tx: None,
            buckets: [0; TX_RATE_BUCKETS],
            head: 0,
            filled: 0,
        }
    }
}

impl SourceTxRate {
    /// Record a cumulative `transactions_processed` counter. The first
    /// observation only primes `last_tx`; subsequent ticks push the
    /// delta into the ring (0 on counter reset / rewind).
    fn record(&mut self, tx: u64) {
        if let Some(prev) = self.last_tx {
            let delta = if tx >= prev { tx - prev } else { 0 };
            self.buckets[self.head] = delta;
            self.head = (self.head + 1) % TX_RATE_BUCKETS;
            if self.filled < TX_RATE_BUCKETS {
                self.filled += 1;
            }
        }
        self.last_tx = Some(tx);
    }

    /// Mean of the filled 1s buckets, or `None` before the first delta.
    fn rate(&self) -> Option<f64> {
        if self.filled == 0 {
            return None;
        }
        let sum: u64 = if self.filled < TX_RATE_BUCKETS {
            self.buckets[..self.filled].iter().sum()
        } else {
            self.buckets.iter().sum()
        };
        Some(sum as f64 / self.filled as f64)
    }
}

/// Fleet-wide tx/s rings keyed by `/health` source name.
#[derive(Default)]
struct TxRateTracker {
    sources: HashMap<String, SourceTxRate>,
}

impl TxRateTracker {
    fn record(&mut self, name: &str, tx: u64) {
        self.sources.entry(name.to_string()).or_default().record(tx);
    }

    fn rates(&self) -> HashMap<String, f64> {
        self.sources
            .iter()
            .filter_map(|(name, ring)| ring.rate().map(|r| (name.clone(), r)))
            .collect()
    }

    /// Drop rings for sources no longer present in the latest sample.
    fn retain(&mut self, live: &HashMap<String, u64>) {
        self.sources.retain(|name, _| live.contains_key(name));
    }
}

struct Inner {
    /// When set, poll this `/v1/mirrors` URL instead of legacy unit
    /// discovery. Empty/`None` → legacy `unit_dir` mode.
    mirrors_url: Option<String>,
    unit_dir: PathBuf,
    naming: NamingSpec,
    fetch_timeout: Duration,
    http: Client,
    sources: RwLock<BTreeMap<String, SourceSnapshot>>,
    /// 60×1s rotating `transactions_processed` deltas → tx/s.
    tx_rates: Mutex<TxRateTracker>,
    /// Consecutive failed `/v1/mirrors` polls per URL. Reset on success;
    /// past [`MIRROR_POLL_GRACE_CYCLES`] the prior rows for that URL are
    /// served degraded (`connectivity: "unreachable"`).
    mirror_fail_counts: Mutex<HashMap<String, u32>>,
    /// Consecutive failed legacy `/metrics` fetches per source name.
    /// Same reset/degrade rule with [`METRICS_POLL_GRACE_CYCLES`].
    metrics_fail_counts: Mutex<HashMap<String, u32>>,
    /// Guards concurrent `refresh_sources` calls — a single poll in
    /// flight at a time. Steady-state is one caller (the poller task);
    /// the lock exists so a manual `/health`-triggered refresh can't
    /// race the periodic one.
    refresh_lock: TokioMutex<()>,
    sys: SysState,
}

impl HealthState {
    pub fn new(unit_dir: impl Into<PathBuf>, sys: SysState) -> Self {
        Self::with_naming(unit_dir, sys, NamingSpec::passthrough())
    }

    /// Like [`HealthState::new`] but with a custom [`NamingSpec`] for
    /// projecting unit stems into source names (legacy unit mode only).
    pub fn with_naming(unit_dir: impl Into<PathBuf>, sys: SysState, naming: NamingSpec) -> Self {
        Self::with_options(None, unit_dir, sys, naming)
    }

    /// Preferred constructor: when `mirrors_url` is `Some`, the poller
    /// reads public-mirror status; otherwise falls back to systemd unit
    /// discovery under `unit_dir`.
    pub fn with_options(
        mirrors_url: Option<String>,
        unit_dir: impl Into<PathBuf>,
        sys: SysState,
        naming: NamingSpec,
    ) -> Self {
        let http = Client::builder()
            .timeout(DEFAULT_FETCH_TIMEOUT)
            .build()
            .expect("reqwest client build");
        let mirrors_url = mirrors_url.and_then(|u| {
            let t = u.trim().to_string();
            if t.is_empty() {
                None
            } else {
                Some(t)
            }
        });
        Self {
            inner: Arc::new(Inner {
                mirrors_url,
                unit_dir: unit_dir.into(),
                naming,
                fetch_timeout: DEFAULT_FETCH_TIMEOUT,
                http,
                sources: RwLock::new(BTreeMap::new()),
                tx_rates: Mutex::new(TxRateTracker::default()),
                mirror_fail_counts: Mutex::new(HashMap::new()),
                metrics_fail_counts: Mutex::new(HashMap::new()),
                refresh_lock: TokioMutex::new(()),
                sys,
            }),
        }
    }

    /// One discovery + poll pass. Idempotent; safe to call concurrently
    /// (the second caller waits on `refresh_lock`). On failure for any
    /// single instance that instance's prior snapshot is retained.
    ///
    /// When `mirrors_url` is set, public-mirror rows are merged with
    /// legacy unit discovery: mirror rows win for the same source name.
    pub async fn refresh_sources(&self) {
        let _guard = self.inner.refresh_lock.lock().await;
        if let Some(url) = self.inner.mirrors_url.clone() {
            self.refresh_hybrid(&url).await;
        } else {
            self.refresh_from_units().await;
        }
    }

    async fn refresh_hybrid(&self, mirrors_url: &str) {
        let mirror_map = self.fetch_mirror_snapshots(mirrors_url).await;
        let discovered = discover(&self.inner.unit_dir, &self.inner.naming);
        if discovered.is_empty() && mirror_map.is_empty() {
            self.inner.sources.write().clear();
            return;
        }

        let mut tasks = Vec::new();
        for src in &discovered {
            if mirror_map.contains_key(&src.name) {
                continue;
            }
            let http = self.inner.http.clone();
            let timeout = self.inner.fetch_timeout;
            let dash = src.dashboard_port;
            let db = src.database.clone();
            tasks.push((
                src.clone(),
                tokio::spawn(async move { (db, fetch_metrics(&http, timeout, dash).await) }),
            ));
        }

        let prior = self.inner.sources.read().clone();
        let mut next: BTreeMap<String, SourceSnapshot> = mirror_map;
        for (src, task) in tasks {
            let (db, fetched) = task.await.unwrap_or_else(|_| (src.database.clone(), None));
            let (metrics, connectivity, last_success_unix) =
                self.legacy_metrics_with_grace(&src.name, fetched, &prior);
            let schema_cached = match &metrics {
                Some(m) => m
                    .get("publisher")
                    .and_then(|p| p.get("fingerprint"))
                    .and_then(|f| f.as_str())
                    .map(|s| !s.is_empty())
                    .unwrap_or(false),
                None => false,
            };
            let metrics = metrics.map(|m| prepare_metrics(&m));
            next.insert(
                src.name.clone(),
                SourceSnapshot {
                    port: src.frontend_port,
                    database: db,
                    schema_cached,
                    connectivity,
                    tables_live: None,
                    tables_total: None,
                    transactions_processed: None,
                    transactions_per_sec: None,
                    connected_since: None,
                    disconnected_since: None,
                    next_attempt_at: None,
                    next_attempt_eta_secs: None,
                    last_success_unix,
                    metrics,
                },
            );
        }
        for src in &discovered {
            if let Some(snap) = next.get_mut(&src.name) {
                if snap.database.is_empty() {
                    snap.database = src.database.clone();
                }
            }
        }
        self.inner.sources.write().clone_from(&next);
        self.stamp_tx_rates();
    }

    async fn fetch_mirror_snapshots(&self, url: &str) -> BTreeMap<String, SourceSnapshot> {
        let urls: Vec<&str> = url
            .split(',')
            .map(str::trim)
            .filter(|u| !u.is_empty())
            .collect();
        if urls.is_empty() {
            return BTreeMap::new();
        }

        // Fresh rows from succeeding URLs always win; degraded rows
        // (their status endpoint is unreachable past the grace) only
        // fill names no live URL served, so one dead sidecar in the
        // comma-list can't shadow fresh data with stale rows.
        let mut next: BTreeMap<String, SourceSnapshot> = BTreeMap::new();
        let mut degraded: BTreeMap<String, SourceSnapshot> = BTreeMap::new();
        for u in urls {
            for (name, snap) in self.fetch_mirror_snapshots_one(u).await {
                if snap.connectivity.as_deref() == Some("unreachable") {
                    degraded.entry(name).or_insert(snap);
                } else {
                    next.insert(name, snap);
                }
            }
        }
        for (name, snap) in degraded {
            next.entry(name).or_insert(snap);
        }
        next
    }

    async fn fetch_mirror_snapshots_one(&self, url: &str) -> BTreeMap<String, SourceSnapshot> {
        let fetched = fetch_mirrors(&self.inner.http, self.inner.fetch_timeout, url).await;
        let Some(body) = fetched else {
            return self.degraded_mirror_rows(url, "poll failed");
        };
        let Some(arr) = body.get("mirrors").and_then(|v| v.as_array()) else {
            return self.degraded_mirror_rows(url, "missing mirrors[]");
        };
        self.inner
            .mirror_fail_counts
            .lock()
            .insert(url.to_string(), 0);

        let now = now_unix();
        let mut next: BTreeMap<String, SourceSnapshot> = BTreeMap::new();
        for m in arr {
            let database = m
                .get("database")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if database.is_empty() {
                continue;
            }
            let name = source_name_for_database(&database);
            let connectivity = m
                .get("connectivity")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let tables_live = m
                .get("tables_live")
                .and_then(|v| v.as_u64())
                .map(|n| n as u32);
            let tables_total = m
                .get("tables_total")
                .and_then(|v| v.as_u64())
                .map(|n| n as u32);
            let live = connectivity.as_deref() == Some("live")
                && tables_live.is_some()
                && tables_live == tables_total;
            next.insert(
                name,
                SourceSnapshot {
                    port: public_port_for_database(&database),
                    database,
                    schema_cached: live || tables_live.unwrap_or(0) > 0,
                    connectivity,
                    tables_live,
                    tables_total,
                    transactions_processed: m
                        .get("transactions_processed")
                        .and_then(|v| v.as_u64()),
                    transactions_per_sec: None,
                    connected_since: opt_string(m.get("connected_since")),
                    disconnected_since: opt_string(m.get("disconnected_since")),
                    next_attempt_at: opt_string(m.get("next_attempt_at")),
                    next_attempt_eta_secs: m.get("next_attempt_eta_secs").and_then(|v| v.as_u64()),
                    last_success_unix: Some(now),
                    metrics: None,
                },
            );
        }
        next
    }

    /// Serve the prior mirror rows for a `/v1/mirrors` URL that just
    /// failed to answer. Within [`MIRROR_POLL_GRACE_CYCLES`] the rows
    /// are returned verbatim (a transient blip never flips the fleet
    /// view); past it they come back with `connectivity: "unreachable"`
    /// and their `last_success_unix`, so `/health` stops reporting a
    /// dead fleet as `live`.
    fn degraded_mirror_rows(&self, url: &str, why: &str) -> BTreeMap<String, SourceSnapshot> {
        let fails = {
            let mut counts = self.inner.mirror_fail_counts.lock();
            let c = counts.entry(url.to_string()).or_insert(0);
            *c += 1;
            *c
        };
        let prior: BTreeMap<String, SourceSnapshot> = self
            .inner
            .sources
            .read()
            .iter()
            .filter(|(_, s)| s.metrics.is_none())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        if prior.is_empty() || fails <= MIRROR_POLL_GRACE_CYCLES {
            tracing::warn!(
                target: "relay_coordinator::health",
                %url,
                why,
                fails,
                "/v1/mirrors failed; keeping prior mirror sources (grace)"
            );
            return prior;
        }
        tracing::warn!(
            target: "relay_coordinator::health",
            %url,
            why,
            fails,
            count = prior.len(),
            "/v1/mirrors unreachable past grace; marking sources unreachable"
        );
        prior
            .into_iter()
            .map(|(k, mut v)| {
                v.connectivity = Some("unreachable".to_string());
                (k, v)
            })
            .collect()
    }

    async fn refresh_from_units(&self) {
        let discovered = discover(&self.inner.unit_dir, &self.inner.naming);
        if discovered.is_empty() {
            self.inner.sources.write().clear();
            return;
        }

        let mut tasks = Vec::with_capacity(discovered.len());
        for src in &discovered {
            let http = self.inner.http.clone();
            let timeout = self.inner.fetch_timeout;
            let dash = src.dashboard_port;
            let db = src.database.clone();
            tasks.push(tokio::spawn(async move {
                (db, fetch_metrics(&http, timeout, dash).await)
            }));
        }

        let prior = self.inner.sources.read().clone();
        let mut next: BTreeMap<String, SourceSnapshot> = BTreeMap::new();
        for (src, task) in discovered.iter().zip(tasks) {
            let (db, fetched) = task.await.unwrap_or_else(|_| (src.database.clone(), None));
            let (metrics, connectivity, last_success_unix) =
                self.legacy_metrics_with_grace(&src.name, fetched, &prior);
            let schema_cached = match &metrics {
                Some(m) => m
                    .get("publisher")
                    .and_then(|p| p.get("fingerprint"))
                    .and_then(|f| f.as_str())
                    .map(|s| !s.is_empty())
                    .unwrap_or(false),
                None => false,
            };
            let metrics = metrics.map(|m| prepare_metrics(&m));
            next.insert(
                src.name.clone(),
                SourceSnapshot {
                    port: src.frontend_port,
                    database: db,
                    schema_cached,
                    connectivity,
                    tables_live: None,
                    tables_total: None,
                    transactions_processed: None,
                    transactions_per_sec: None,
                    connected_since: None,
                    disconnected_since: None,
                    next_attempt_at: None,
                    next_attempt_eta_secs: None,
                    last_success_unix,
                    metrics,
                },
            );
        }
        for src in &discovered {
            if let Some(snap) = next.get_mut(&src.name) {
                if snap.database.is_empty() {
                    snap.database = src.database.clone();
                }
            }
        }
        self.inner.sources.write().clone_from(&next);
        self.stamp_tx_rates();
    }

    /// Merge a legacy `/metrics` fetch with the prior snapshot under the
    /// poll-grace rule. Success resets the per-source failure counter;
    /// the first [`METRICS_POLL_GRACE_CYCLES`] failures keep the prior
    /// metrics; beyond that the metrics are dropped and the row is
    /// marked `unreachable` instead of serving frozen numbers forever.
    ///
    /// Returns `(metrics, connectivity, last_success_unix)`.
    fn legacy_metrics_with_grace(
        &self,
        name: &str,
        fetched: Option<Value>,
        prior: &BTreeMap<String, SourceSnapshot>,
    ) -> (Option<Value>, Option<String>, Option<u64>) {
        let prior_snap = prior.get(name);
        match fetched {
            Some(m) => {
                self.inner
                    .metrics_fail_counts
                    .lock()
                    .insert(name.to_string(), 0);
                (Some(m), None, Some(now_unix()))
            }
            None => {
                let mut counts = self.inner.metrics_fail_counts.lock();
                let fails = counts.entry(name.to_string()).or_insert(0);
                *fails += 1;
                if *fails <= METRICS_POLL_GRACE_CYCLES {
                    (
                        prior_snap.and_then(|s| s.metrics.clone()),
                        None,
                        prior_snap.and_then(|s| s.last_success_unix),
                    )
                } else {
                    (
                        None,
                        Some("unreachable".to_string()),
                        prior_snap.and_then(|s| s.last_success_unix),
                    )
                }
            }
        }
    }

    /// Background task: poll every [`SOURCES_POLL_INTERVAL`], starting
    /// with one immediate poll so `/health` populates quickly after
    /// process start.
    pub async fn run_sources_poller(self, shutdown: impl std::future::Future<Output = ()>) {
        let mut shutdown = std::pin::pin!(shutdown);
        let mut tick = tokio::time::interval(SOURCES_POLL_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // First tick completes immediately → first poll happens now.
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => break,
                _ = tick.tick() => {
                    self.refresh_sources().await;
                }
            }
        }
    }

    /// Background task: sample mirror `transactions_processed` every
    /// [`TX_RATE_SAMPLE_INTERVAL`] into a 60-bucket rotating window and
    /// stamp `transactions_per_sec` onto `/health` sources.
    pub async fn run_tx_rate_sampler(self, shutdown: impl std::future::Future<Output = ()>) {
        let mut shutdown = std::pin::pin!(shutdown);
        let mut tick = tokio::time::interval(TX_RATE_SAMPLE_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => break,
                _ = tick.tick() => {
                    self.sample_tx_rates().await;
                }
            }
        }
    }

    async fn sample_tx_rates(&self) {
        let Some(mirrors_url) = self.inner.mirrors_url.clone() else {
            return;
        };
        let urls: Vec<&str> = mirrors_url
            .split(',')
            .map(str::trim)
            .filter(|u| !u.is_empty())
            .collect();
        if urls.is_empty() {
            return;
        }

        let mut observed: HashMap<String, u64> = HashMap::new();
        for url in urls {
            let Some(body) = fetch_mirrors(&self.inner.http, self.inner.fetch_timeout, url).await
            else {
                continue;
            };
            let Some(arr) = body.get("mirrors").and_then(|v| v.as_array()) else {
                continue;
            };
            for m in arr {
                let Some(database) = m.get("database").and_then(|v| v.as_str()) else {
                    continue;
                };
                let Some(tx) = m.get("transactions_processed").and_then(|v| v.as_u64()) else {
                    continue;
                };
                let name = source_name_for_database(database);
                observed.insert(name, tx);
            }
        }

        {
            let mut tracker = self.inner.tx_rates.lock();
            for (name, tx) in &observed {
                tracker.record(name, *tx);
            }
            tracker.retain(&observed);
        }

        // Keep lifetime counters fresh between the slower sources poller
        // ticks so Refresh always shows a current total + rate.
        {
            let mut sources = self.inner.sources.write();
            for (name, tx) in &observed {
                if let Some(snap) = sources.get_mut(name) {
                    snap.transactions_processed = Some(*tx);
                }
            }
        }
        self.stamp_tx_rates();
    }

    /// Copy current ring rates onto every source that has one.
    fn stamp_tx_rates(&self) {
        let rates = self.inner.tx_rates.lock().rates();
        let mut sources = self.inner.sources.write();
        for (name, snap) in sources.iter_mut() {
            snap.transactions_per_sec = rates.get(name).copied();
        }
    }

    /// Build the full `/health` JSON body. Cheap: clones the sources
    /// map under a read lock, then merges the system snapshot.
    pub fn snapshot_json(&self) -> Value {
        let sources = self.inner.sources.read().clone();
        let sys = self.inner.sys.snapshot();
        // schema_count: we don't have a host-wide table count anymore
        // (each relay has its own stdb now). Fall back to sources.len(),
        // which the dashboard already accepts as the default.
        json!({
            "sources": sources,
            "schema_count": sources.len(),
            "system": sys,
        })
    }
}

/// Fetch public-mirror `GET /v1/mirrors` JSON. Returns `None` on failure.
async fn fetch_mirrors(http: &Client, timeout: Duration, url: &str) -> Option<Value> {
    let resp = tokio::time::timeout(timeout, http.get(url).send()).await;
    let resp = match resp {
        Ok(Ok(r)) => r,
        _ => return None,
    };
    if !resp.status().is_success() {
        return None;
    }
    resp.json::<Value>().await.ok()
}

fn opt_string(v: Option<&Value>) -> Option<String> {
    v.and_then(|x| x.as_str()).map(|s| s.to_string())
}

/// Current unix time in seconds (0 only if the clock predates the
/// epoch — practically unreachable).
fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Offset from the main listen port to the isolated `GET /v1/mirrors`
/// sidecar. Must match spacetimedb-public-mirror `MIRROR_STATUS_PORT_OFFSET`.
pub const MIRROR_STATUS_PORT_OFFSET: u16 = 30;

/// Public frontend port for a mirrored database name.
///
/// `bitcraft-live-global` → 3000; `bitcraft-live-N` → 3000+N.
pub fn public_port_for_database(database: &str) -> u16 {
    if database == "bitcraft-live-global" || database.ends_with("-global") {
        return 3000;
    }
    if let Some(n) = database.strip_prefix("bitcraft-live-") {
        if let Ok(id) = n.parse::<u16>() {
            return 3000 + id;
        }
    }
    3000
}

/// Sidecar HTTP port for mirror readiness (`GET /v1/mirrors`).
pub fn mirror_status_port_for_database(database: &str) -> u16 {
    public_port_for_database(database)
        .checked_add(MIRROR_STATUS_PORT_OFFSET)
        .expect("mirror status port overflow")
}

/// `/health` sources key: global is shown as `"global"`, regions keep
/// the full `bitcraft-live-N` name (matches the dashboard fallback list).
pub fn source_name_for_database(database: &str) -> String {
    if database == "bitcraft-live-global" {
        "global".to_string()
    } else {
        database.to_string()
    }
}

/// Fetch one instance's `/metrics` JSON. Returns `None` on any failure
/// (transport, non-200, parse) — the caller keeps the prior snapshot.
async fn fetch_metrics(http: &Client, timeout: Duration, dashboard_port: u16) -> Option<Value> {
    let url = format!("http://127.0.0.1:{dashboard_port}/metrics");
    let resp = tokio::time::timeout(timeout, http.get(&url).send()).await;
    let resp = match resp {
        Ok(Ok(r)) => r,
        _ => return None,
    };
    if !resp.status().is_success() {
        return None;
    }
    resp.json::<Value>().await.ok()
}

/// Prepare the raw per-instance `/metrics` JSON for the public
/// fleet view: strip internal debug state, then inject the derived
/// uptime fields the page reads. Returns a fresh `Value` (does not
/// mutate input).
///
/// See module docs for what's stripped and how the uptime fields
/// derive.
fn prepare_metrics(metrics: &Value) -> Value {
    let mut out = metrics.clone();
    strip_internal_debug_fields(&mut out);
    inject_uptime_fields(&mut out);
    out
}

/// Remove internal debug state that's useful on the per-instance
/// loopback dashboard but shouldn't leave the host. Removes the entire
/// `frontend.clients` array — per-client detail (remote addresses,
/// per-connection counters, SQL strings) is operator-local state, not
/// part of the fleet health contract. Operates in place; tolerates any
/// shape.
fn strip_internal_debug_fields(metrics: &mut Value) {
    let Some(fe) = metrics.get_mut("frontend") else {
        return;
    };
    let Some(obj) = fe.as_object_mut() else {
        return;
    };
    obj.remove("clients");
}

/// Walk the metrics JSON and inject the derived uptime fields the page
/// reads. See module docs for the derivation rules.
fn inject_uptime_fields(metrics: &mut Value) {
    let Some(obj) = metrics.as_object_mut() else {
        return;
    };
    let now = obj.get("now").and_then(|v| v.as_u64()).unwrap_or(0);
    let started_at = obj.get("started_at").and_then(|v| v.as_u64()).unwrap_or(0);
    // process_uptime_seconds = now - started_at. Collapses to 0 if
    // either timestamp is missing/zero — saturating_sub alone would
    // return `now` when started_at is 0, which is not what we want.
    let process_uptime = if now == 0 || started_at == 0 {
        0
    } else {
        now.saturating_sub(started_at)
    };
    obj.insert("process_uptime_seconds".to_string(), json!(process_uptime));
    for link_key in ["upstream", "local_stdb"] {
        if let Some(link) = obj.get_mut(link_key).and_then(|v| v.as_object_mut()) {
            inject_link_uptime(link, now);
        }
    }
}

/// Derive `uptime_seconds` for one link (`upstream` or `local_stdb`).
/// Only meaningful when currently `state == "up"` and `last_up_at != 0`;
/// otherwise `null` (prevents a stale `last_up_at` masquerading as live
/// uptime while the link is actually down).
fn inject_link_uptime(link: &mut Map<String, Value>, now: u64) {
    let state = link.get("state").and_then(|v| v.as_str()).unwrap_or("");
    let last_up_at = link.get("last_up_at").and_then(|v| v.as_u64()).unwrap_or(0);
    let uptime = if state == "up" && last_up_at != 0 {
        json!(now.saturating_sub(last_up_at))
    } else {
        Value::Null
    };
    link.insert("uptime_seconds".to_string(), uptime);
}

/// Discover all mirror relay units in `unit_dir`, sorted by unit stem.
/// Recognises any `relay-<stem>.service` unit (where `<stem>` is non-empty
/// and the unit is not a known non-mirror relay utility — the coordinator,
/// fleet sequencer, staleness monitor, or shared stdb).
///
/// `naming` controls how the unit stem is projected into the source name
/// shown in `/health.sources` (default [`NamingSpec::passthrough`]).
pub fn discover(unit_dir: &Path, naming: &NamingSpec) -> Vec<DiscoveredSource> {
    let entries = match std::fs::read_dir(unit_dir) {
        Ok(e) => e,
        Err(_) => return Vec::new(),
    };
    let mut found: Vec<(String, DiscoveredSource)> = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let Some(stem) = name.strip_suffix(".service") else {
            continue;
        };
        // Recognise any `relay-*` unit that hosts a mirror. Skip the
        // known fleet-utility units (they carry no mirror to poll).
        if !is_mirror_unit(stem) {
            continue;
        }
        let body = match std::fs::read_to_string(&path) {
            Ok(b) => b,
            Err(_) => continue,
        };
        if let Some(src) = parse_unit(&body, stem, naming) {
            // Sort by the projected name so the dashboard sees a stable
            // ordering regardless of readdir sequence.
            found.push((src.name.clone(), src));
        }
    }
    found.sort_by(|a, b| a.0.cmp(&b.0));
    found.into_iter().map(|(_, s)| s).collect()
}

/// Does this unit stem host a relay mirror we should poll?
///
/// True for any `relay-<stem>.service` where `<stem>` is non-empty and
/// not one of the known fleet-utility units. The shared stdb unit
/// (`relay-stdb`) is included defensively even though `--stdb-spawn`
/// made it obsolete — a leftover unit shouldn't crash discovery.
fn is_mirror_unit(stem: &str) -> bool {
    // Strip the conventional `relay-` prefix; the remainder is the
    // per-deployment stem (`global`, `region14`, `bc14`, …).
    let Some(rest) = stem.strip_prefix("relay-") else {
        return false;
    };
    if rest.is_empty() {
        return false;
    }
    !matches!(
        stem,
        "relay-stdb"
            | "relay-coordinator"
            | "relay-fleet-sequencer"
            | "relay-fleet-start"
            | "relay-staleness-monitor"
    )
}

/// Parse a systemd unit file body into a [`DiscoveredSource`].
///
/// Looks for:
/// - `--frontend-bind 127.0.0.1:PORT` or `0.0.0.0:PORT` → frontend port
/// - `--dashboard-bind 127.0.0.1:PORT` → dashboard port
/// - `--database NAME` → upstream BitCraft database (used for the
///   `/health` source key via [`source_name_for_database`])
/// - `--mirror-database NAME` → local SpacetimeDB database clients
///   subscribe to on the relay frontend
///
/// When `--database` is present the source key matches the dashboard
/// contract (`global`, `bitcraft-live-N`). The `database` field always
/// comes from `--mirror-database`.
pub fn parse_unit(body: &str, unit_stem: &str, naming: &NamingSpec) -> Option<DiscoveredSource> {
    let frontend_port = parse_bind_port(body, "--frontend-bind")?;
    let dashboard_port = parse_bind_port(body, "--dashboard-bind")?;
    let mirror_database = parse_flag_value(body, "--mirror-database")?;
    let upstream_database = parse_flag_value(body, "--database");
    let name = match upstream_database {
        Some(upstream) => source_name_for_database(&upstream),
        None => naming.project(unit_stem),
    };
    Some(DiscoveredSource {
        name,
        database: mirror_database,
        frontend_port,
        dashboard_port,
    })
}

/// Parse `--<flag> 127.0.0.1:PORT` (or `0.0.0.0:PORT`) and return PORT.
/// Matches both space-separated and `=`-joined forms.
fn parse_bind_port(body: &str, flag: &str) -> Option<u16> {
    let pat_space = format!("{flag} ");
    let pat_eq = format!("{flag}=");
    for line in body.lines() {
        for raw in [pat_space.as_str(), pat_eq.as_str()] {
            if let Some(idx) = line.find(raw) {
                let rest = &line[idx + raw.len()..];
                let tok = rest.split_whitespace().next().unwrap_or(rest);
                // tok looks like "127.0.0.1:3009" — take the port after ':'.
                if let Some(port_str) = tok.rsplit(':').next() {
                    if let Ok(p) = port_str.parse::<u16>() {
                        return Some(p);
                    }
                }
            }
        }
    }
    None
}

/// Parse `--<flag> VALUE` (space or `=`). Returns the first match.
fn parse_flag_value(body: &str, flag: &str) -> Option<String> {
    let pat_space = format!("{flag} ");
    let pat_eq = format!("{flag}=");
    for line in body.lines() {
        for raw in [pat_space.as_str(), pat_eq.as_str()] {
            if let Some(idx) = line.find(raw) {
                let rest = &line[idx + raw.len()..];
                let tok = rest.split_whitespace().next().unwrap_or(rest);
                // Strip a trailing backslash (systemd line continuation).
                let cleaned = tok.trim_end_matches('\\');
                if !cleaned.is_empty() {
                    return Some(cleaned.to_string());
                }
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// Build a fake systemd unit body with the standard flag layout.
    fn unit_body(frontend: &str, dashboard: &str, mirror_db: &str) -> String {
        unit_body_with_upstream(frontend, dashboard, mirror_db, "upstream-db")
    }

    fn unit_body_with_upstream(
        frontend: &str,
        dashboard: &str,
        mirror_db: &str,
        upstream_db: &str,
    ) -> String {
        format!(
            "[Service]\n\
             ExecStart=/srv/relay/target/release/relay \\\n\
             --upstream wss://upstream.example.com \\\n\
             --database {upstream_db} \\\n\
             --mirror-database {mirror_db} \\\n\
             --frontend-bind {frontend} \\\n\
             --dashboard-bind {dashboard} \\\n\
             --stdb-spawn\n"
        )
    }

    fn unit_body_mirror_only(frontend: &str, dashboard: &str, mirror_db: &str) -> String {
        format!(
            "[Service]\n\
             ExecStart=/srv/relay/target/release/relay \\\n\
             --upstream wss://upstream.example.com \\\n\
             --mirror-database {mirror_db} \\\n\
             --frontend-bind {frontend} \\\n\
             --dashboard-bind {dashboard} \\\n\
             --stdb-spawn\n"
        )
    }

    #[test]
    fn parse_unit_file_extracts_ports_and_database() {
        // Default naming = passthrough when no `--database` is present.
        let body = unit_body_mirror_only("127.0.0.1:3014", "127.0.0.1:3114", "relay-mirror-region14");
        let src = parse_unit(&body, "relay-region14", &NamingSpec::passthrough()).expect("parsed");
        assert_eq!(src.name, "relay-region14");
        assert_eq!(src.database, "relay-mirror-region14");
        assert_eq!(src.frontend_port, 3014);
        assert_eq!(src.dashboard_port, 3114);
    }

    #[test]
    fn parse_unit_file_prefers_upstream_database_for_bitcraft_names() {
        let body = unit_body_with_upstream(
            "127.0.0.1:3014",
            "127.0.0.1:3114",
            "relay-mirror-bc14",
            "bitcraft-live-14",
        );
        let src = parse_unit(&body, "relay-bc14", &NamingSpec::passthrough()).expect("parsed");
        assert_eq!(src.name, "bitcraft-live-14");
        assert_eq!(src.database, "relay-mirror-bc14");

        let global = unit_body_with_upstream(
            "127.0.0.1:3000",
            "127.0.0.1:3100",
            "relay-mirror-bc-global",
            "bitcraft-live-global",
        );
        let src = parse_unit(&global, "relay-global", &NamingSpec::passthrough()).expect("parsed");
        assert_eq!(src.name, "global");
        assert_eq!(src.database, "relay-mirror-bc-global");
    }

    #[test]
    fn parse_unit_file_projects_name_via_naming_spec() {
        // A deployment convention: `relay-region14` → `live-14` when no
        // `--database` names the source.
        let naming = NamingSpec {
            template: Some("live-{stem}".into()),
            stem_prefix: Some("relay-region".into()),
        };
        let body = unit_body_mirror_only("127.0.0.1:3014", "127.0.0.1:3114", "relay-mirror-region14");
        let src = parse_unit(&body, "relay-region14", &naming).expect("parsed");
        assert_eq!(src.name, "live-14");
        assert_eq!(src.database, "relay-mirror-region14");
    }

    #[test]
    fn naming_spec_prefix_mismatch_falls_back_to_full_stem() {
        // If the configured prefix doesn't match, use the stem verbatim
        // inside the template rather than mis-projecting.
        let naming = NamingSpec {
            template: Some("live-{stem}".into()),
            stem_prefix: Some("relay-region".into()),
        };
        assert_eq!(naming.project("relay-other14"), "live-relay-other14");
    }

    #[test]
    fn naming_spec_template_without_placeholder_is_literal() {
        let naming = NamingSpec {
            template: Some("static".into()),
            stem_prefix: None,
        };
        assert_eq!(naming.project("relay-region14"), "static");
    }

    #[test]
    fn parse_unit_file_accepts_0000_frontend_bind() {
        // Legacy public-facing binds used 0.0.0.0. The parser must
        // still extract the port (only the host part differs).
        let body = unit_body_mirror_only("0.0.0.0:3000", "127.0.0.1:3100", "relay-mirror-global");
        let src = parse_unit(&body, "relay-global", &NamingSpec::passthrough()).expect("parsed");
        assert_eq!(src.name, "relay-global");
        assert_eq!(src.frontend_port, 3000);
        assert_eq!(src.dashboard_port, 3100);
    }

    #[test]
    fn parse_unit_file_accepts_equals_form() {
        // Some deployments use `--flag=value` instead of `--flag value`.
        let body = "[Service]\nExecStart=relay --mirror-database=relay-mirror-region7 \
             --frontend-bind=127.0.0.1:3007 --dashboard-bind=127.0.0.1:3107\n";
        let src = parse_unit(body, "relay-region7", &NamingSpec::passthrough()).expect("parsed");
        assert_eq!(src.database, "relay-mirror-region7");
        assert_eq!(src.frontend_port, 3007);
        assert_eq!(src.dashboard_port, 3107);
    }

    #[test]
    fn parse_unit_file_returns_none_when_flags_missing() {
        // Without --frontend-bind the unit isn't usable for /health.
        let body = "[Service]\nExecStart=relay --database upstream-db\n";
        assert!(parse_unit(body, "relay-region14", &NamingSpec::passthrough()).is_none());
    }

    #[test]
    fn discover_skips_non_mirror_units_and_sorts() {
        let dir = tempdir().expect("tempdir");
        let mk = |name: &str, body: &str| {
            fs::write(dir.path().join(format!("{name}.service")), body).unwrap();
        };
        mk(
            "relay-global",
            &unit_body_mirror_only("127.0.0.1:3000", "127.0.0.1:3100", "relay-mirror-global"),
        );
        mk(
            "relay-region14",
            &unit_body_mirror_only("127.0.0.1:3014", "127.0.0.1:3114", "relay-mirror-region14"),
        );
        mk(
            "relay-region7",
            &unit_body_mirror_only("127.0.0.1:3007", "127.0.0.1:3107", "relay-mirror-region7"),
        );
        // These should all be skipped:
        mk(
            "relay-stdb",
            "[Service]\nExecStart=spacetimedb-standalone\n",
        );
        mk(
            "relay-coordinator",
            "[Service]\nExecStart=relay-coordinator\n",
        );
        mk(
            "relay-fleet-sequencer",
            "[Service]\nExecStart=relay-fleet-start.sh\n",
        );
        mk(
            "relay-fleet-start",
            "[Service]\nExecStart=relay-fleet-start.sh\n",
        );
        mk(
            "relay-staleness-monitor",
            "[Service]\nExecStart=relay-staleness-monitor.sh\n",
        );
        // Non-relay-prefixed files ignored.
        mk("nginx", "[Service]\nExecStart=nginx\n");

        // Default passthrough naming: source names equal the unit stems.
        let found = discover(dir.path(), &NamingSpec::passthrough());
        let names: Vec<&str> = found.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(
            names,
            vec!["relay-global", "relay-region14", "relay-region7"]
        );
    }

    #[test]
    fn discover_applies_naming_spec_when_projecting() {
        // A deployment convention: `relay-region14` → `live-14`.
        let dir = tempdir().expect("tempdir");
        fs::write(
            dir.path().join("relay-region14.service"),
            unit_body_mirror_only("127.0.0.1:3014", "127.0.0.1:3114", "relay-mirror-region14"),
        )
        .unwrap();
        fs::write(
            dir.path().join("relay-global.service"),
            unit_body_mirror_only("127.0.0.1:3000", "127.0.0.1:3100", "relay-mirror-global"),
        )
        .unwrap();
        let naming = NamingSpec {
            template: Some("live-{stem}".into()),
            stem_prefix: Some("relay-region".into()),
        };
        let found = discover(dir.path(), &naming);
        let names: Vec<&str> = found.iter().map(|s| s.name.as_str()).collect();
        // relay-region14 → live-14; relay-global → the prefix matches so the
        // remainder after "relay-region" is "global"... no, it doesn't match.
        // "relay-global".starts_with("relay-region") is false, so the full
        // stem flows into the template as {stem}.
        assert_eq!(names, vec!["live-14", "live-relay-global"]);
    }

    #[test]
    fn derive_uptime_seconds_when_up() {
        // state="up", last_up_at=N, now=N+100 → uptime_seconds=100.
        let mut m = json!({
            "now": 1_000_100,
            "started_at": 1_000_000,
            "upstream": { "state": "up", "last_up_at": 1_000_000 },
            "local_stdb": { "state": "up", "last_up_at": 1_000_050 }
        });
        inject_uptime_fields(&mut m);
        assert_eq!(m["process_uptime_seconds"].as_u64(), Some(100));
        assert_eq!(m["upstream"]["uptime_seconds"].as_u64(), Some(100));
        assert_eq!(m["local_stdb"]["uptime_seconds"].as_u64(), Some(50));
    }

    #[test]
    fn derive_uptime_seconds_null_when_down() {
        // state="down" with a stale last_up_at must NOT report uptime.
        let mut m = json!({
            "now": 2_000_000,
            "started_at": 1_000_000,
            "upstream": { "state": "down", "last_up_at": 1_500_000 },
            "local_stdb": { "state": "initial" }
        });
        inject_uptime_fields(&mut m);
        assert_eq!(m["process_uptime_seconds"].as_u64(), Some(1_000_000));
        assert!(m["upstream"]["uptime_seconds"].is_null());
        assert!(m["local_stdb"]["uptime_seconds"].is_null());
    }

    #[test]
    fn derive_uptime_seconds_null_when_timestamp_missing() {
        // state="up" but last_up_at==0 (never set) → null, not 0.
        let mut m = json!({
            "now": 1_000,
            "started_at": 0,
            "upstream": { "state": "up", "last_up_at": 0 }
        });
        inject_uptime_fields(&mut m);
        assert!(m["upstream"]["uptime_seconds"].is_null());
        assert_eq!(m["process_uptime_seconds"].as_u64(), Some(0));
    }

    #[test]
    fn derive_uptime_handles_missing_link_objects() {
        // Older /metrics shape without local_stdb must not panic.
        let mut m = json!({
            "now": 1_000,
            "started_at": 500,
            "upstream": { "state": "up", "last_up_at": 900 }
        });
        inject_uptime_fields(&mut m);
        assert_eq!(m["upstream"]["uptime_seconds"].as_u64(), Some(100));
        assert!(m.get("local_stdb").is_none());
    }

    #[test]
    fn prepare_metrics_strips_clients_array() {
        // The per-client detail array is operator-local state; strip it
        // entirely from the public fleet /health response.
        let m = json!({
            "now": 1_000,
            "started_at": 900,
            "frontend": {
                "active_clients": 2,
                "clients": [
                    { "id": "a", "remote": "1.2.3.4:5", "subscriptions": ["SELECT * FROM secrets"] },
                    { "id": "b", "remote": "1.2.3.4:6", "subscriptions": ["SELECT * FROM t1", "SELECT * FROM t2"] }
                ]
            }
        });
        let out = prepare_metrics(&m);
        // clients array gone; aggregate counter preserved.
        assert!(out["frontend"].get("clients").is_none());
        assert_eq!(out["frontend"]["active_clients"].as_u64(), Some(2));
        // uptime derivation still ran end-to-end.
        assert_eq!(out["process_uptime_seconds"].as_u64(), Some(100));
    }

    #[test]
    fn prepare_metrics_tolerates_missing_frontend() {
        // An /metrics body without a frontend object (e.g. a relay that
        // has --frontend-bind disabled) must not panic.
        let m = json!({ "now": 100, "started_at": 50 });
        let out = prepare_metrics(&m);
        assert_eq!(out["process_uptime_seconds"].as_u64(), Some(50));
        assert!(out.get("frontend").is_none());
    }

    #[test]
    fn public_port_and_source_name_from_database() {
        assert_eq!(public_port_for_database("bitcraft-live-global"), 3000);
        assert_eq!(public_port_for_database("bitcraft-live-14"), 3014);
        assert_eq!(public_port_for_database("bitcraft-live-3"), 3003);
        assert_eq!(mirror_status_port_for_database("bitcraft-live-global"), 3030);
        assert_eq!(mirror_status_port_for_database("bitcraft-live-7"), 3037);
        assert_eq!(mirror_status_port_for_database("bitcraft-live-8"), 3038);
        assert_eq!(source_name_for_database("bitcraft-live-global"), "global");
        assert_eq!(source_name_for_database("bitcraft-live-14"), "bitcraft-live-14");
    }

    #[test]
    fn snapshot_json_shape_matches_index_html_contract() {
        // The page's required fields: top-level sources (object),
        // system.cpu.load_average.{one,five,fifteen},
        // system.memory.{total,free,available}_bytes, and
        // system.network.bytes_per_sec_{in,out}.
        let sys = SysState::new();
        let state = HealthState::new("/nonexistent", sys);
        let snap = state.snapshot_json();
        assert!(snap.get("sources").unwrap().is_object());
        assert_eq!(snap["schema_count"].as_u64(), Some(0));
        let cpu = &snap["system"]["cpu"];
        let la = &cpu["load_average"];
        for k in ["one", "five", "fifteen"] {
            assert!(la.get(k).is_some(), "load_average.{k} must be present");
        }
        let mem = &snap["system"]["memory"];
        for k in ["total_bytes", "free_bytes", "available_bytes"] {
            assert!(mem.get(k).is_some(), "memory.{k} must be present");
        }
        let net = &snap["system"]["network"];
        assert!(net.get("bytes_per_sec_in").is_some());
        assert!(net.get("bytes_per_sec_out").is_some());
        assert_eq!(net["window_seconds"].as_u64(), Some(300));
    }

    #[tokio::test]
    async fn refresh_sources_handles_missing_unit_dir() {
        // A bogus unit_dir must not panic; the map just ends up empty.
        let sys = SysState::new();
        let state = HealthState::new("/nonexistent/unit/dir", sys);
        state.refresh_sources().await;
        let snap = state.snapshot_json();
        // No sources discovered → empty sources object, schema_count 0.
        assert!(snap["sources"].as_object().unwrap().is_empty());
        assert_eq!(snap["schema_count"].as_u64(), Some(0));
    }

    #[test]
    fn tx_rate_ring_needs_two_samples() {
        let mut ring = SourceTxRate::default();
        ring.record(100);
        assert!(ring.rate().is_none());
        ring.record(110);
        assert_eq!(ring.rate(), Some(10.0));
    }

    #[test]
    fn tx_rate_ring_means_over_filled_buckets() {
        let mut ring = SourceTxRate::default();
        ring.record(0);
        ring.record(10); // +10
        ring.record(30); // +20
        ring.record(60); // +30
        assert_eq!(ring.rate(), Some(20.0));
    }

    #[test]
    fn tx_rate_ring_rotates_at_capacity() {
        let mut ring = SourceTxRate::default();
        ring.record(0);
        // Fill 60 buckets with +1 each → mean 1.0
        for i in 1..=TX_RATE_BUCKETS as u64 {
            ring.record(i);
        }
        assert_eq!(ring.filled, TX_RATE_BUCKETS);
        assert!((ring.rate().unwrap() - 1.0).abs() < 1e-9);
        // Next sample +100 should evict one +1 bucket.
        ring.record(TX_RATE_BUCKETS as u64 + 100);
        let expected = (59.0 + 100.0) / 60.0;
        assert!((ring.rate().unwrap() - expected).abs() < 1e-9);
    }

    #[test]
    fn tx_rate_ring_treats_counter_rewind_as_zero_delta() {
        let mut ring = SourceTxRate::default();
        ring.record(1_000);
        ring.record(50); // rewind after mirror restart
        assert_eq!(ring.rate(), Some(0.0));
    }

    /// Serve `body` as a canned 200/JSON response on an ephemeral
    /// loopback port. Returns the URL, the raw port, and the server
    /// task handle; abort the handle to make the port refuse
    /// connections.
    async fn spawn_json_server(body: String) -> (String, u16, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral");
        let port = listener.local_addr().unwrap().port();
        let handle = tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                let body = body.clone();
                tokio::spawn(async move {
                    // Drain (part of) the request head, then answer.
                    let mut buf = [0u8; 1024];
                    let _ = sock.read(&mut buf).await;
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                });
            }
        });
        (format!("http://127.0.0.1:{port}/v1/mirrors"), port, handle)
    }

    /// A loopback port with nothing listening (bind ephemeral, drop).
    async fn dead_port() -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral");
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        port
    }

    #[tokio::test]
    async fn mirror_poll_failure_marks_unreachable_after_grace() {
        let payload = json!({
            "mirrors": [
                {
                    "database": "bitcraft-live-global",
                    "connectivity": "live",
                    "tables_live": 12,
                    "tables_total": 12,
                    "transactions_processed": 100
                }
            ]
        })
        .to_string();
        let (url, _port, server) = spawn_json_server(payload).await;
        let state = HealthState::with_options(
            Some(url),
            "/nonexistent",
            SysState::new(),
            NamingSpec::passthrough(),
        );

        // Healthy: row is live, success timestamp stamped.
        state.refresh_sources().await;
        {
            let sources = state.inner.sources.read();
            let row = sources.get("global").expect("row present");
            assert_eq!(row.connectivity.as_deref(), Some("live"));
            assert!(row.last_success_unix.expect("success stamped") > 0);
        }

        server.abort();
        // First failed cycle: grace keeps the prior row verbatim.
        state.refresh_sources().await;
        {
            let sources = state.inner.sources.read();
            assert_eq!(
                sources.get("global").unwrap().connectivity.as_deref(),
                Some("live")
            );
        }
        // Past grace: the row must stop claiming live.
        state.refresh_sources().await;
        {
            let sources = state.inner.sources.read();
            let row = sources.get("global").unwrap();
            assert_eq!(row.connectivity.as_deref(), Some("unreachable"));
            // Last-known context is preserved, not blanked.
            assert_eq!(row.database, "bitcraft-live-global");
            assert_eq!(row.port, 3000);
            assert_eq!(row.tables_live, Some(12));
            assert_eq!(row.tables_total, Some(12));
            assert!(row.last_success_unix.unwrap() > 0);
        }
    }

    #[tokio::test]
    async fn degraded_url_does_not_shadow_fresh_rows() {
        let a_payload = json!({
            "mirrors": [
                {
                    "database": "bitcraft-live-global",
                    "connectivity": "live",
                    "tables_live": 12,
                    "tables_total": 12
                }
            ]
        })
        .to_string();
        let (url_a, _port_a, server_a) = spawn_json_server(a_payload).await;
        let url_b = format!("http://127.0.0.1:{}/v1/mirrors", dead_port().await);
        let state = HealthState::with_options(
            Some(format!("{url_a},{url_b}")),
            "/nonexistent",
            SysState::new(),
            NamingSpec::passthrough(),
        );

        // Cycle 1: A delivers; B fails for the first time (no prior
        // rows of its own to re-inject yet).
        state.refresh_sources().await;
        assert_eq!(
            state
                .inner
                .sources
                .read()
                .get("global")
                .unwrap()
                .connectivity
                .as_deref(),
            Some("live")
        );

        // Cycles 2+: B is past grace and degrades its copy of the prior
        // rows, but A keeps delivering fresh data — fresh must win.
        for _ in 0..3 {
            state.refresh_sources().await;
            assert_eq!(
                state
                    .inner
                    .sources
                    .read()
                    .get("global")
                    .unwrap()
                    .connectivity
                    .as_deref(),
                Some("live")
            );
        }

        // A dies too: one grace cycle, then nothing fresh remains and
        // the row degrades.
        server_a.abort();
        state.refresh_sources().await;
        assert_eq!(
            state
                .inner
                .sources
                .read()
                .get("global")
                .unwrap()
                .connectivity
                .as_deref(),
            Some("live")
        );
        state.refresh_sources().await;
        assert_eq!(
            state
                .inner
                .sources
                .read()
                .get("global")
                .unwrap()
                .connectivity
                .as_deref(),
            Some("unreachable")
        );
    }

    #[tokio::test]
    async fn legacy_metrics_failure_drops_after_grace() {
        let metrics = json!({
            "publisher": { "fingerprint": "abc123" },
            "now": 1_000,
            "started_at": 500
        })
        .to_string();
        let (_url, dash_port, server) = spawn_json_server(metrics).await;
        let dir = tempdir().expect("tempdir");
        fs::write(
            dir.path().join("relay-region14.service"),
            unit_body_mirror_only(
                "127.0.0.1:3014",
                &format!("127.0.0.1:{dash_port}"),
                "relay-mirror-region14",
            ),
        )
        .unwrap();
        let state =
            HealthState::with_options(None, dir.path(), SysState::new(), NamingSpec::passthrough());

        state.refresh_sources().await;
        {
            let sources = state.inner.sources.read();
            let row = sources.get("relay-region14").expect("row present");
            assert!(row.metrics.is_some());
            assert!(row.last_success_unix.is_some());
        }

        server.abort();
        // Grace cycle: prior metrics retained, no unreachable verdict.
        state.refresh_sources().await;
        {
            let sources = state.inner.sources.read();
            let row = sources.get("relay-region14").unwrap();
            assert!(row.metrics.is_some());
            assert_eq!(row.connectivity, None);
        }
        // Past grace: frozen metrics dropped, row marked unreachable.
        state.refresh_sources().await;
        {
            let sources = state.inner.sources.read();
            let row = sources.get("relay-region14").unwrap();
            assert!(row.metrics.is_none());
            assert_eq!(row.connectivity.as_deref(), Some("unreachable"));
            assert!(row.last_success_unix.is_some());
        }
    }
}
