// SPDX-License-Identifier: MIT

//! Per-client subscription registry.
//!
//! `ClientId` is a relay-internal identifier handed out by
//! `ServerHandle::register` when a downstream WebSocket connects.
//! `QuerySetId` mirrors the upstream `QueryId` (per-client query set).
//!
//! The registry is the engine's source of truth for "who is listening
//! to what". Two indices live side-by-side:
//!
//! 1. **`clients`** — `ClientId -> { QuerySetId -> CompiledQuery }`,
//!    used when the connection task wants to drop one query or the
//!    whole client.
//! 2. **`by_table`** — `&str -> Vec<(ClientId, QuerySetId)>`, used on
//!    every upstream `TransactionUpdate` to find the small set of
//!    interested clients without scanning the full client list.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::query::CompiledQuery;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ClientId(pub u64);

impl ClientId {
    pub fn next(counter: &AtomicU64) -> Self {
        Self(counter.fetch_add(1, Ordering::Relaxed))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct QuerySetId(pub u32);

#[derive(Default)]
pub(crate) struct Registry {
    inner: RwLock<RegistryInner>,
}

#[derive(Default)]
struct RegistryInner {
    clients: HashMap<ClientId, HashMap<QuerySetId, Arc<CompiledQuery>>>,
    by_table: HashMap<Arc<str>, Vec<(ClientId, QuerySetId)>>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, client: ClientId, qset: QuerySetId, query: Arc<CompiledQuery>) {
        let mut g = self.inner.write();
        let table = query.table.clone();
        let prior = g
            .clients
            .entry(client)
            .or_default()
            .insert(qset, query.clone());
        if let Some(prior) = prior {
            remove_from_table_index(&mut g.by_table, &prior.table, client, qset);
        }
        g.by_table
            .entry(table)
            .or_default()
            .push((client, qset));
    }

    pub fn remove_qset(&self, client: ClientId, qset: QuerySetId) {
        let mut g = self.inner.write();
        let removed = g
            .clients
            .get_mut(&client)
            .and_then(|m| m.remove(&qset));
        if let Some(q) = removed {
            remove_from_table_index(&mut g.by_table, &q.table, client, qset);
        }
    }

    pub fn drop_client(&self, client: ClientId) {
        let mut g = self.inner.write();
        let Some(qsets) = g.clients.remove(&client) else {
            return;
        };
        for (qset, q) in qsets {
            remove_from_table_index(&mut g.by_table, &q.table, client, qset);
        }
    }

    /// Walk every (client, qset, query) registered for a table.
    /// The closure runs under the read lock; keep it cheap.
    pub fn for_table<F: FnMut(ClientId, QuerySetId, &CompiledQuery)>(
        &self,
        table: &str,
        mut f: F,
    ) {
        let g = self.inner.read();
        let Some(entries) = g.by_table.get(table) else {
            return;
        };
        for (client, qset) in entries {
            if let Some(q) = g.clients.get(client).and_then(|m| m.get(qset)) {
                f(*client, *qset, q);
            }
        }
    }

    pub fn n_clients(&self) -> usize {
        self.inner.read().clients.len()
    }
}

fn remove_from_table_index(
    by_table: &mut HashMap<Arc<str>, Vec<(ClientId, QuerySetId)>>,
    table: &Arc<str>,
    client: ClientId,
    qset: QuerySetId,
) {
    if let Some(vec) = by_table.get_mut(table) {
        vec.retain(|(c, q)| !(*c == client && *q == qset));
        if vec.is_empty() {
            by_table.remove(table);
        }
    }
}
