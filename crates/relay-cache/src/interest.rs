// SPDX-License-Identifier: MIT

//! Interest hub for live inventory WebSocket streams.
//!
//! Keys are `(Topic, entity_id)`. Shard apply paths call [`InterestHub::notify`]
//! after mutating rows; WS tasks hold a [`watch::Receiver`] and rebuild
//! snapshots when the generation advances. Idle keys cost nothing beyond
//! the DashMap entry while at least one receiver is alive.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::watch;

/// Inventory stream topic — mirrors mats' three HTTP sources.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Topic {
    PlayerInventory,
    PlayerHousing,
    ClaimInventory,
}

impl Topic {
    pub fn as_str(self) -> &'static str {
        match self {
            Topic::PlayerInventory => "player_inventory",
            Topic::PlayerHousing => "player_housing",
            Topic::ClaimInventory => "claim_inventory",
        }
    }
}

type Key = (Topic, u64);

/// Shared notify hub wired into every shard and the HTTP/WS fleet.
pub struct InterestHub {
    map: DashMap<Key, watch::Sender<u64>>,
    /// Active WS subscriptions (receiver leases).
    active_streams: AtomicU64,
    /// Lifetime notify calls that reached at least one receiver.
    lifetime_notifies: AtomicU64,
    /// Lifetime coalesced snapshot pushes from WS tasks.
    lifetime_pushes: AtomicU64,
}

impl InterestHub {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            map: DashMap::new(),
            active_streams: AtomicU64::new(0),
            lifetime_notifies: AtomicU64::new(0),
            lifetime_pushes: AtomicU64::new(0),
        })
    }

    /// Fast path: no WS clients → shards skip touch collection.
    pub fn has_subscribers(&self) -> bool {
        self.active_streams.load(Ordering::Relaxed) > 0
    }

    pub fn active_streams(&self) -> u64 {
        self.active_streams.load(Ordering::Relaxed)
    }

    pub fn lifetime_notifies(&self) -> u64 {
        self.lifetime_notifies.load(Ordering::Relaxed)
    }

    pub fn lifetime_pushes(&self) -> u64 {
        self.lifetime_pushes.load(Ordering::Relaxed)
    }

    pub fn record_push(&self) {
        self.lifetime_pushes.fetch_add(1, Ordering::Relaxed);
    }

    /// Subscribe to generation bumps for `(topic, entity_id)`.
    /// Drop the returned [`Subscription`] to decrement the active count
    /// and remove the map entry when the last receiver goes away.
    pub fn subscribe(self: &Arc<Self>, topic: Topic, entity_id: u64) -> Subscription {
        let key = (topic, entity_id);
        let rx = {
            let entry = self.map.entry(key).or_insert_with(|| {
                let (tx, _) = watch::channel(0u64);
                tx
            });
            entry.subscribe()
        };
        self.active_streams.fetch_add(1, Ordering::Relaxed);
        Subscription {
            hub: Arc::clone(self),
            topic,
            entity_id,
            rx,
        }
    }

    /// Bump generation for a key. No-op when nobody is listening.
    pub fn notify(&self, topic: Topic, entity_id: u64) {
        let key = (topic, entity_id);
        let Some(tx) = self.map.get(&key) else {
            return;
        };
        if tx.receiver_count() == 0 {
            return;
        }
        tx.send_modify(|g| *g = g.wrapping_add(1));
        self.lifetime_notifies.fetch_add(1, Ordering::Relaxed);
    }

    fn unsubscribe(&self, topic: Topic, entity_id: u64) {
        self.active_streams.fetch_sub(1, Ordering::Relaxed);
        let key = (topic, entity_id);
        self.map.remove_if(&key, |_, tx| tx.receiver_count() == 0);
    }
}

/// RAII lease on one hub subscription.
pub struct Subscription {
    hub: Arc<InterestHub>,
    topic: Topic,
    entity_id: u64,
    rx: watch::Receiver<u64>,
}

impl Subscription {
    pub fn receiver(&mut self) -> &mut watch::Receiver<u64> {
        &mut self.rx
    }

    pub fn clone_receiver(&self) -> watch::Receiver<u64> {
        self.rx.clone()
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.hub.unsubscribe(self.topic, self.entity_id);
    }
}

/// Deduped touch set collected during one TransactionUpdate apply.
#[derive(Default)]
pub struct TouchBatch {
    player_inv: Vec<u64>,
    player_housing: Vec<u64>,
    claim_inv: Vec<u64>,
}

impl TouchBatch {
    pub fn player_inv(&mut self, id: u64) {
        if id != 0 {
            self.player_inv.push(id);
        }
    }

    pub fn player_housing(&mut self, id: u64) {
        if id != 0 {
            self.player_housing.push(id);
        }
    }

    pub fn claim_inv(&mut self, id: u64) {
        if id != 0 {
            self.claim_inv.push(id);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.player_inv.is_empty() && self.player_housing.is_empty() && self.claim_inv.is_empty()
    }

    /// Sort+dedup then notify the hub.
    pub fn flush(mut self, hub: &InterestHub) {
        if self.is_empty() {
            return;
        }
        dedup_ids(&mut self.player_inv);
        dedup_ids(&mut self.player_housing);
        dedup_ids(&mut self.claim_inv);
        for id in self.player_inv {
            hub.notify(Topic::PlayerInventory, id);
        }
        for id in self.player_housing {
            hub.notify(Topic::PlayerHousing, id);
        }
        for id in self.claim_inv {
            hub.notify(Topic::ClaimInventory, id);
        }
    }
}

fn dedup_ids(ids: &mut Vec<u64>) {
    ids.sort_unstable();
    ids.dedup();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn notify_wakes_subscriber() {
        let hub = InterestHub::new();
        let mut sub = hub.subscribe(Topic::PlayerInventory, 42);
        assert_eq!(hub.active_streams(), 1);
        assert!(hub.has_subscribers());

        hub.notify(Topic::PlayerInventory, 42);
        sub.receiver()
            .changed()
            .await
            .expect("generation should advance");
        assert_eq!(*sub.receiver().borrow(), 1);
        assert_eq!(hub.lifetime_notifies(), 1);

        drop(sub);
        assert_eq!(hub.active_streams(), 0);
        assert!(!hub.has_subscribers());
        // Stale notify after unsubscribe is a no-op.
        hub.notify(Topic::PlayerInventory, 42);
        assert_eq!(hub.lifetime_notifies(), 1);
    }

    #[test]
    fn touch_batch_dedups_on_flush() {
        let hub = InterestHub::new();
        let _sub = hub.subscribe(Topic::ClaimInventory, 7);
        let mut batch = TouchBatch::default();
        batch.claim_inv(7);
        batch.claim_inv(7);
        batch.claim_inv(9); // no subscriber
        batch.flush(&hub);
        assert_eq!(hub.lifetime_notifies(), 1);
    }
}
