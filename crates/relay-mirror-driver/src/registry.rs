// SPDX-License-Identifier: MIT

//! Shared map keyed by `CallReducer` request_id, recording the
//! `UpstreamReducerMeta` the relay-mirror-driver attached to each
//! `relay_apply_<table>` call.
//!
//! The relay-frontend proxy reads this map when it sees a v1
//! `TransactionUpdateLight` from the local SpacetimeDB: TUL only
//! carries `request_id + DatabaseUpdate`, so the proxy joins it
//! against the meta we stashed at send-time to synthesise the full
//! v1 `TransactionUpdate` a downstream v1 client expects.
//!
//! Memory grows with in-flight calls; a periodic eviction sweep
//! removes entries older than [`MetaRegistry::DEFAULT_MAX_AGE`].

use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use relay_protocol::UpstreamReducerMeta;

#[derive(Clone, Debug)]
pub struct MetaEntry {
    /// `None` for `relay_apply_<table>(None, …)` calls — typically the
    /// initial subscribe-applied row apply where we have no upstream
    /// reducer to attribute. The proxy treats those as "pass the TUL
    /// through verbatim" rather than synthesising a fake TU.
    pub meta: Option<UpstreamReducerMeta>,
    pub inserted_at: Instant,
}

#[derive(Default)]
pub struct MetaRegistry {
    inner: DashMap<u32, MetaEntry>,
}

impl MetaRegistry {
    /// Drop entries older than this when [`Self::sweep`] is called.
    /// Picked to comfortably cover the local-stdb roundtrip even on a
    /// loaded host while bounding worst-case memory at
    /// `peak_calls_per_minute * sizeof(MetaEntry)` — a few MB at most.
    pub const DEFAULT_MAX_AGE: Duration = Duration::from_secs(60);

    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: DashMap::new(),
        })
    }

    /// Stash the meta we attached to `relay_apply_<table>(meta, …)`
    /// before sending the `CallReducer` to local stdb.
    pub fn record(&self, request_id: u32, meta: Option<UpstreamReducerMeta>) {
        self.inner.insert(
            request_id,
            MetaEntry {
                meta,
                inserted_at: Instant::now(),
            },
        );
    }

    /// Look up the meta for a TUL whose `request_id` echoes a
    /// previously-sent CallReducer's request_id. Returns:
    /// * `Some(Some(meta))` — synthesise full TU with this meta.
    /// * `Some(None)` — caller had no upstream meta; pass through.
    /// * `None` — request_id we don't recognise (race or a non-relay
    ///   writer); pass through.
    pub fn get(&self, request_id: u32) -> Option<Option<UpstreamReducerMeta>> {
        self.inner.get(&request_id).map(|e| e.meta.clone())
    }

    /// Remove the entry for `request_id`, e.g. once the corresponding
    /// `ReducerResult` is observed. Doesn't error if the key is absent.
    pub fn forget(&self, request_id: u32) {
        self.inner.remove(&request_id);
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Remove entries older than `max_age`. Cheap O(n) scan; call this
    /// from a low-frequency timer (e.g. every 10 s) — one-shot
    /// cleanup is sufficient since we don't stop the world.
    pub fn sweep(&self, max_age: Duration) -> usize {
        let cutoff = Instant::now().checked_sub(max_age);
        let Some(cutoff) = cutoff else {
            return 0;
        };
        let mut removed = 0usize;
        self.inner.retain(|_, entry| {
            if entry.inserted_at < cutoff {
                removed += 1;
                false
            } else {
                true
            }
        });
        removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(name: &str) -> UpstreamReducerMeta {
        UpstreamReducerMeta {
            reducer_name: name.into(),
            caller_identity: relay_protocol::lib::Identity::ZERO,
            caller_connection_id: relay_protocol::lib::ConnectionId::ZERO,
            timestamp: relay_protocol::lib::Timestamp::UNIX_EPOCH,
            request_id: 0,
            args: vec![],
        }
    }

    #[test]
    fn record_get_round_trip() {
        let r = MetaRegistry::new();
        r.record(7, Some(meta("send_message")));
        let got = r.get(7).unwrap().unwrap();
        assert_eq!(got.reducer_name, "send_message");
    }

    #[test]
    fn get_returns_none_for_unknown() {
        let r = MetaRegistry::new();
        assert!(r.get(99).is_none());
    }

    #[test]
    fn record_none_is_distinguishable_from_unknown() {
        let r = MetaRegistry::new();
        r.record(1, None);
        assert!(matches!(r.get(1), Some(None)));
        assert!(r.get(2).is_none());
    }

    #[test]
    fn forget_removes_entry() {
        let r = MetaRegistry::new();
        r.record(3, Some(meta("x")));
        r.forget(3);
        assert!(r.get(3).is_none());
    }

    #[test]
    fn sweep_evicts_old_entries() {
        let r = MetaRegistry::new();
        r.record(1, Some(meta("a")));
        std::thread::sleep(Duration::from_millis(20));
        r.record(2, Some(meta("b")));
        let evicted = r.sweep(Duration::from_millis(10));
        assert!(evicted >= 1);
        assert!(r.get(1).is_none());
        assert!(r.get(2).is_some());
    }
}
