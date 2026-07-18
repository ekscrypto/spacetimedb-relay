// SPDX-License-Identifier: MIT

//! Host-level metrics for the `/health` endpoint: CPU load average and a
//! rolling network throughput rate.
//!
//! Two independent samplers feed [`SysState`]:
//! - Load average + CPU count: read on each tick from `sysinfo` (the
//!   kernel keeps the EWMA; we just read it).
//! - Network rate: a 5-minute sliding-window mean of per-interface byte
//!   deltas, sampled every `SAMPLE_INTERVAL_SECS` (15s). Loopback and
//!   virtual bridges are excluded so local relay↔stdb traffic doesn't
//!   inflate the number.
//!
//! This mirrors what the retired BitCraft-Relay's `sys_metrics` module
//! surfaced. Only Linux is deployed; the code compiles on macOS for
//! dev but network counters may be empty there.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use serde::Serialize;
use sysinfo::{Networks, System};

/// Sample interval for the network-rate ring buffer (15s).
pub const SAMPLE_INTERVAL_SECS: u64 = 15;
/// Window length the ring buffer covers (300s = 5 min).
pub const WINDOW_SECS: u64 = 300;
/// How many buckets fit in the window (WINDOW / SAMPLE).
pub const WINDOW_BUCKETS: usize = (WINDOW_SECS / SAMPLE_INTERVAL_SECS) as usize;

/// Latest observed host CPU + network snapshot, served verbatim under
/// `/health.system`. All fields are optional-ish: the page degrades
/// gracefully if any is zero/missing.
#[derive(Clone, Default, Serialize)]
pub struct SysSnapshot {
    pub cpu: CpuSnapshot,
    pub network: NetSnapshot,
}

#[derive(Clone, Default, Serialize)]
pub struct CpuSnapshot {
    pub load_average: LoadAverage,
    pub num_cpus: usize,
}

#[derive(Clone, Copy, Default, Serialize)]
pub struct LoadAverage {
    pub one: f64,
    pub five: f64,
    pub fifteen: f64,
}

#[derive(Clone, Default, Serialize)]
pub struct NetSnapshot {
    pub bytes_per_sec_in: u64,
    pub bytes_per_sec_out: u64,
    pub samples: usize,
    pub window_seconds: u64,
    pub sample_interval_seconds: u64,
}

/// State shared between the sampler task and the `/health` handler.
/// Cheap to clone (one `Arc`).
#[derive(Clone)]
pub struct SysState {
    inner: Arc<Inner>,
}

struct Inner {
    /// Most recent snapshot, swapped in by the sampler. Read by every
    /// `/health` request — `parking_lot::Mutex` keeps the critical
    /// section tiny (just the clone).
    latest: Mutex<SysSnapshot>,
}

impl SysState {
    pub fn new() -> Self {
        // Initialise with the configured window constants so the
        // `/health.system.network` payload advertises its own design
        // (window_seconds, sample_interval_seconds) from process start,
        // before the first real sample lands (~15s later). The byte
        // counts stay 0; index.html renders "average" rather than
        // "5-min avg" only when window_seconds == 0.
        let initial = SysSnapshot {
            cpu: CpuSnapshot::default(),
            network: NetSnapshot {
                bytes_per_sec_in: 0,
                bytes_per_sec_out: 0,
                samples: 0,
                window_seconds: WINDOW_SECS,
                sample_interval_seconds: SAMPLE_INTERVAL_SECS,
            },
        };
        Self {
            inner: Arc::new(Inner {
                latest: Mutex::new(initial),
            }),
        }
    }

    /// Latest snapshot (clone). Returns a default if the sampler hasn't
    /// run yet — `/health` is served from process start, the first
    /// sample lands within `SAMPLE_INTERVAL_SECS`.
    pub fn snapshot(&self) -> SysSnapshot {
        self.inner.latest.lock().clone()
    }

    /// Run the sampler loop until `shutdown` resolves. Samples load
    /// average + CPU count + NIC byte deltas every
    /// [`SAMPLE_INTERVAL_SECS`] seconds. Cheap to run; a single
    /// task is enough.
    pub async fn run(self, shutdown: impl std::future::Future<Output = ()>) {
        let mut sys = System::new();
        let mut nets = Networks::new();
        // Previous sample's per-interface byte counters; differenced
        // against the current sample to compute the rate.
        let mut prev_bytes: HashMap<String, (u64, u64)> = HashMap::new();
        // Ring buffer of per-second (in, out) rates. Vec-as-ring:
        // push_back, drain front when over capacity. WINDOW_BUCKETS
        // is small (20) so the shift cost is negligible.
        let mut ring: Vec<(f64, f64)> = Vec::with_capacity(WINDOW_BUCKETS);

        let mut shutdown = std::pin::pin!(shutdown);
        let mut tick = tokio::time::interval(Duration::from_secs(SAMPLE_INTERVAL_SECS));
        // The first `tick().await` completes immediately — that's the
        // "sample right now at startup" behaviour we want.
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => break,
                _ = tick.tick() => {
                    sys.refresh_cpu_all();
                    nets.refresh(true);

                    // Sum byte deltas across all non-loopback interfaces.
                    let (mut d_in, mut d_out) = (0u64, 0u64);
                    for (name, data) in nets.list() {
                        // Skip loopback (lo on Linux, lo0 on macOS) so
                        // local relay↔stdb traffic doesn't inflate the
                        // rate. Also skip virtual/docker bridges — they
                        // churn without representing real traffic.
                        if is_loopback_or_virtual(name) {
                            continue;
                        }
                        let received = data.total_received();
                        let transmitted = data.total_transmitted();
                        if let Some(&(pr, pt)) = prev_bytes.get(name.as_str()) {
                            d_in = d_in.saturating_add(received.saturating_sub(pr));
                            d_out = d_out.saturating_add(transmitted.saturating_sub(pt));
                        }
                        prev_bytes.insert(name.clone(), (received, transmitted));
                    }

                    // Per-second rate for this sample.
                    let rate_in = d_in as f64 / SAMPLE_INTERVAL_SECS.max(1) as f64;
                    let rate_out = d_out as f64 / SAMPLE_INTERVAL_SECS.max(1) as f64;
                    ring.push((rate_in, rate_out));
                    if ring.len() > WINDOW_BUCKETS {
                        ring.remove(0);
                    }

                    // Sliding-window mean over the populated samples.
                    // `samples` reflects how many buckets we actually
                    // have (less than WINDOW_BUCKETS during warmup).
                    let (sum_in, sum_out) =
                        ring.iter().fold((0.0_f64, 0.0_f64), |(si, so), (ri, ro)| {
                            (si + ri, so + ro)
                        });
                    let n = ring.len().max(1) as f64;
                    let net = NetSnapshot {
                        bytes_per_sec_in: (sum_in / n) as u64,
                        bytes_per_sec_out: (sum_out / n) as u64,
                        samples: ring.len(),
                        window_seconds: WINDOW_SECS,
                        sample_interval_seconds: SAMPLE_INTERVAL_SECS,
                    };

                    let la = System::load_average();
                    let cpu = CpuSnapshot {
                        load_average: LoadAverage {
                            one: la.one,
                            five: la.five,
                            fifteen: la.fifteen,
                        },
                        num_cpus: sys.cpus().len(),
                    };
                    let mut latest = self.inner.latest.lock();
                    latest.cpu = cpu;
                    latest.network = net;
                }
            }
        }
    }
}

/// Loopback on Linux is `lo`, on macOS `lo0`. Virtual bridges (`docker*`,
/// `br-*`, `veth*`, `vnet*`) churn without representing real host
/// traffic — skip them too so the rate stays meaningful.
fn is_loopback_or_virtual(name: &str) -> bool {
    matches!(name, "lo" | "lo0")
        || name.starts_with("docker")
        || name.starts_with("br-")
        || name.starts_with("veth")
        || name.starts_with("vnet")
        || name.starts_with("virbr")
}

impl Default for SysState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_defaults_are_zero_when_unsampled() {
        let s = SysState::new();
        let snap = s.snapshot();
        assert_eq!(snap.cpu.num_cpus, 0);
        assert_eq!(snap.cpu.load_average.one, 0.0);
        assert_eq!(snap.network.bytes_per_sec_in, 0);
        assert_eq!(snap.network.window_seconds, WINDOW_SECS);
        assert_eq!(snap.network.sample_interval_seconds, SAMPLE_INTERVAL_SECS);
    }

    #[test]
    fn ring_mean_averages_samples() {
        // Two samples: 100 B/s in and 300 B/s in → mean 200.
        let ring: Vec<(f64, f64)> = vec![(100.0, 0.0), (300.0, 0.0)];
        let (si, so) = ring
            .iter()
            .fold((0.0, 0.0), |(a, b), (r, t)| (a + r, b + t));
        let n = ring.len() as f64;
        assert_eq!((si / n) as u64, 200);
        assert_eq!((so / n) as u64, 0);
    }

    #[test]
    fn ring_mean_is_zero_with_no_samples() {
        // Warmup state: empty ring, mean = 0/1 = 0 (max(1) avoids div-by-zero).
        let ring: Vec<(f64, f64)> = Vec::new();
        let (si, so) = ring
            .iter()
            .fold((0.0, 0.0), |(a, b), (r, t)| (a + r, b + t));
        let n = ring.len().max(1) as f64;
        assert_eq!((si / n) as u64, 0);
        assert_eq!((so / n) as u64, 0);
    }

    #[test]
    fn loopback_and_virtual_interfaces_are_excluded() {
        assert!(is_loopback_or_virtual("lo"));
        assert!(is_loopback_or_virtual("lo0"));
        assert!(is_loopback_or_virtual("docker0"));
        assert!(is_loopback_or_virtual("br-abcdef123456"));
        assert!(is_loopback_or_virtual("veth1234567"));
        assert!(!is_loopback_or_virtual("eth0"));
        assert!(!is_loopback_or_virtual("en0"));
        assert!(!is_loopback_or_virtual("ens3"));
    }
}
