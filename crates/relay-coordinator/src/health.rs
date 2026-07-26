// SPDX-License-Identifier: MIT

//! Fleet `/health` aggregator.
//!
//! Discovers the relay-* systemd units on the host, polls each
//! instance's loopback `/metrics` JSON dashboard, and folds the
//! results into the shape a dashboard page consumes:
//!
//! ```jsonc
//! {
//!   "sources": {
//!     "global":   { "port": 3000, "database": "...", "schema_cached": true, "metrics": {...} },
//!     "region14": { ... }
//!   },
//!   "schema_count": 2,
//!   "system": { "cpu": {...}, "memory": {...}, "network": {...} }
//! }
//! ```
//!
//! The source name shown in `sources` is **deployment-configured**, not
//! hardcoded: by default the systemd unit stem passes through verbatim
//! (`relay-region14` → `relay-region14`). A deployment can supply a
//! `NamingSpec` (e.g. `template = "live-{stem}", stem_prefix = "relay-region"`)
//! to project `relay-region14` → `live-14` for its dashboard. See
//! [`NamingSpec`].
//!
//! `sources[*].metrics` is the **raw relay `/metrics` body** plus three
//! derived fields the page reads but the relay doesn't emit:
//!
//! - `process_uptime_seconds = now - started_at`
//! - `upstream.uptime_seconds = now - last_up_at` (only when state=="up"
//!   and `last_up_at != 0`; otherwise `null` so a stale timestamp can't
//!   masquerade as live uptime)
//! - `local_stdb.uptime_seconds` — same rule on the local_stdb link
//!
//! Failures are graceful: a single instance's `/metrics` timing out
//! doesn't blank the whole fleet — that source keeps its prior snapshot
//! for one cycle (so a flaky curl doesn't flap a row).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use reqwest::Client;
use serde::Serialize;
use serde_json::{json, Map, Value};
use tokio::sync::Mutex as TokioMutex;

use crate::sys_metrics::SysState;

/// How often the sources poller refreshes the fleet map.
pub const SOURCES_POLL_INTERVAL: Duration = Duration::from_secs(30);
/// Per-instance `/metrics` fetch timeout.
pub const DEFAULT_FETCH_TIMEOUT: Duration = Duration::from_secs(4);

/// One row of the `sources` map. `metrics` is `None` when the last
/// poll failed AND there was no prior snapshot to fall back to.
#[derive(Clone, Serialize)]
pub struct SourceSnapshot {
    pub port: u16,
    pub database: String,
    pub schema_cached: bool,
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

struct Inner {
    unit_dir: PathBuf,
    naming: NamingSpec,
    fetch_timeout: Duration,
    http: Client,
    sources: RwLock<BTreeMap<String, SourceSnapshot>>,
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
    /// projecting unit stems into source names.
    pub fn with_naming(
        unit_dir: impl Into<PathBuf>,
        sys: SysState,
        naming: NamingSpec,
    ) -> Self {
        let http = Client::builder()
            .timeout(DEFAULT_FETCH_TIMEOUT)
            .build()
            .expect("reqwest client build");
        Self {
            inner: Arc::new(Inner {
                unit_dir: unit_dir.into(),
                naming,
                fetch_timeout: DEFAULT_FETCH_TIMEOUT,
                http,
                sources: RwLock::new(BTreeMap::new()),
                refresh_lock: TokioMutex::new(()),
                sys,
            }),
        }
    }

    /// One discovery + poll pass. Idempotent; safe to call concurrently
    /// (the second caller waits on `refresh_lock`). On failure for any
    /// single instance that instance's prior snapshot is retained.
    pub async fn refresh_sources(&self) {
        // Serialize refreshes — the poller task is the only steady-state
        // caller, but a manual trigger shouldn't race it.
        let _guard = self.inner.refresh_lock.lock().await;

        let discovered = discover(&self.inner.unit_dir, &self.inner.naming);
        if discovered.is_empty() {
            // Nothing to poll; clear the map so a config wipe shows up.
            self.inner.sources.write().clear();
            return;
        }

        // Poll every instance concurrently; each is independent and
        // capped at fetch_timeout, so the worst-case wall-time is one
        // timeout regardless of fleet size.
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

        // Collect new snapshots; keep prior data for instances whose
        // poll failed this cycle (transient curl timeouts shouldn't
        // blank a row). Drop instances that disappeared from discovery.
        let prior = self.inner.sources.read().clone();
        let mut next: BTreeMap<String, SourceSnapshot> = BTreeMap::new();
        for (src, task) in discovered.iter().zip(tasks) {
            let (db, fetched) = task.await.unwrap_or_else(|_| (src.database.clone(), None));
            let metrics = fetched.or_else(|| prior.get(&src.name).and_then(|s| s.metrics.clone()));
            let schema_cached = match &metrics {
                Some(m) => m
                    .get("publisher")
                    .and_then(|p| p.get("fingerprint"))
                    .and_then(|f| f.as_str())
                    .map(|s| !s.is_empty())
                    .unwrap_or(false),
                None => false,
            };
            // If we derived uptime fields this cycle, embed them into a
            // fresh copy so the page sees consistent `uptime_seconds`.
            let metrics = metrics.map(|m| derive_uptime_fields(&m));
            next.insert(
                src.name.clone(),
                SourceSnapshot {
                    port: src.frontend_port,
                    database: db,
                    schema_cached,
                    metrics,
                },
            );
        }
        // The task closure consumed `src.database`'s clone for logging;
        // re-anchor the database name from discovery in case the fetch
        // path lost it (it can't, but be defensive).
        for src in &discovered {
            if let Some(snap) = next.get_mut(&src.name) {
                if snap.database.is_empty() {
                    snap.database = src.database.clone();
                }
            }
        }
        self.inner.sources.write().clone_from(&next);
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

/// Walk the raw `/metrics` JSON and inject the derived uptime fields
/// the page reads. Returns a fresh `Value` (does not mutate input).
///
/// See module docs for the derivation rules.
fn derive_uptime_fields(metrics: &Value) -> Value {
    let mut out = metrics.clone();
    let Some(obj) = out.as_object_mut() else {
        return out;
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
    out
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
/// - `--mirror-database NAME` or `--mirror-database=NAME` → database
///
/// The source name comes from `naming.project(unit_stem)`.
pub fn parse_unit(body: &str, unit_stem: &str, naming: &NamingSpec) -> Option<DiscoveredSource> {
    let frontend_port = parse_bind_port(body, "--frontend-bind")?;
    let dashboard_port = parse_bind_port(body, "--dashboard-bind")?;
    let database = parse_flag_value(body, "--mirror-database")?;
    let name = naming.project(unit_stem);
    Some(DiscoveredSource {
        name,
        database,
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

    /// Build a fake systemd unit body with the standard flag layout.
    fn unit_body(frontend: &str, dashboard: &str, mirror_db: &str) -> String {
        format!(
            "[Service]\n\
             ExecStart=/srv/relay/target/release/relay \\\n\
             --upstream wss://upstream.example.com \\\n\
             --database upstream-db \\\n\
             --mirror-database {mirror_db} \\\n\
             --frontend-bind {frontend} \\\n\
             --dashboard-bind {dashboard} \\\n\
             --stdb-spawn\n"
        )
    }

    #[test]
    fn parse_unit_file_extracts_ports_and_database() {
        // Default naming = passthrough: the unit stem is the source name.
        let body = unit_body("127.0.0.1:3014", "127.0.0.1:3114", "relay-mirror-region14");
        let src = parse_unit(&body, "relay-region14", &NamingSpec::passthrough()).expect("parsed");
        assert_eq!(src.name, "relay-region14");
        assert_eq!(src.database, "relay-mirror-region14");
        assert_eq!(src.frontend_port, 3014);
        assert_eq!(src.dashboard_port, 3114);
    }

    #[test]
    fn parse_unit_file_projects_name_via_naming_spec() {
        // A deployment convention: `relay-region14` → `live-14`.
        let naming = NamingSpec {
            template: Some("live-{stem}".into()),
            stem_prefix: Some("relay-region".into()),
        };
        let body = unit_body("127.0.0.1:3014", "127.0.0.1:3114", "relay-mirror-region14");
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
        let body = unit_body("0.0.0.0:3000", "127.0.0.1:3100", "relay-mirror-global");
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
            &unit_body("127.0.0.1:3000", "127.0.0.1:3100", "relay-mirror-global"),
        );
        mk(
            "relay-region14",
            &unit_body("127.0.0.1:3014", "127.0.0.1:3114", "relay-mirror-region14"),
        );
        mk(
            "relay-region7",
            &unit_body("127.0.0.1:3007", "127.0.0.1:3107", "relay-mirror-region7"),
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
            unit_body("127.0.0.1:3014", "127.0.0.1:3114", "relay-mirror-region14"),
        )
        .unwrap();
        fs::write(
            dir.path().join("relay-global.service"),
            unit_body("127.0.0.1:3000", "127.0.0.1:3100", "relay-mirror-global"),
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
        let m = json!({
            "now": 1_000_100,
            "started_at": 1_000_000,
            "upstream": { "state": "up", "last_up_at": 1_000_000 },
            "local_stdb": { "state": "up", "last_up_at": 1_000_050 }
        });
        let out = derive_uptime_fields(&m);
        assert_eq!(out["process_uptime_seconds"].as_u64(), Some(100));
        assert_eq!(out["upstream"]["uptime_seconds"].as_u64(), Some(100));
        assert_eq!(out["local_stdb"]["uptime_seconds"].as_u64(), Some(50));
    }

    #[test]
    fn derive_uptime_seconds_null_when_down() {
        // state="down" with a stale last_up_at must NOT report uptime.
        let m = json!({
            "now": 2_000_000,
            "started_at": 1_000_000,
            "upstream": { "state": "down", "last_up_at": 1_500_000 },
            "local_stdb": { "state": "initial" }
        });
        let out = derive_uptime_fields(&m);
        assert_eq!(out["process_uptime_seconds"].as_u64(), Some(1_000_000));
        assert!(out["upstream"]["uptime_seconds"].is_null());
        assert!(out["local_stdb"]["uptime_seconds"].is_null());
    }

    #[test]
    fn derive_uptime_seconds_null_when_timestamp_missing() {
        // state="up" but last_up_at==0 (never set) → null, not 0.
        let m = json!({
            "now": 1_000,
            "started_at": 0,
            "upstream": { "state": "up", "last_up_at": 0 }
        });
        let out = derive_uptime_fields(&m);
        assert!(out["upstream"]["uptime_seconds"].is_null());
        assert_eq!(out["process_uptime_seconds"].as_u64(), Some(0));
    }

    #[test]
    fn derive_uptime_handles_missing_link_objects() {
        // Older /metrics shape without local_stdb must not panic.
        let m = json!({
            "now": 1_000,
            "started_at": 500,
            "upstream": { "state": "up", "last_up_at": 900 }
        });
        let out = derive_uptime_fields(&m);
        assert_eq!(out["upstream"]["uptime_seconds"].as_u64(), Some(100));
        assert!(out.get("local_stdb").is_none());
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
}
