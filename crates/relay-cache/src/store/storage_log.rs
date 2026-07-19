// SPDX-License-Identifier: MIT

//! Columnar store for `storage_log_state` with secondary indexes for
//! forensics queries (by storage unit, player, and item).

use hashbrown::{HashMap, HashSet};

use crate::decode::StorageLogRow;
use crate::store::Pocket;

/// Upstream `ActionLogData` discriminant.
pub const ACTION_RESERVED: u8 = 0;
pub const ACTION_WITHDRAW: u8 = 1;
pub const ACTION_DEPOSIT: u8 = 2;

pub struct StorageLogSoA {
    pub id: Vec<u64>,
    pub storage_entity_id: Vec<u64>,
    pub player_entity_id: Vec<u64>,
    pub player_username: Vec<Box<str>>,
    pub action: Vec<u8>,
    pub item_id: Vec<i32>,
    pub item_type: Vec<u8>,
    pub quantity: Vec<i32>,
    pub timestamp_micros: Vec<i64>,
    pub days_since_epoch: Vec<i32>,
    free_slots: Vec<u32>,
    pk: HashMap<u64, u32>,
    by_storage: HashMap<u64, Vec<u32>>,
    by_player: HashMap<u64, Vec<u32>>,
    /// `(item_id, item_type)` → slots.
    by_item: HashMap<(i32, u8), Vec<u32>>,
}

impl StorageLogSoA {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            id: Vec::with_capacity(cap),
            storage_entity_id: Vec::with_capacity(cap),
            player_entity_id: Vec::with_capacity(cap),
            player_username: Vec::with_capacity(cap),
            action: Vec::with_capacity(cap),
            item_id: Vec::with_capacity(cap),
            item_type: Vec::with_capacity(cap),
            quantity: Vec::with_capacity(cap),
            timestamp_micros: Vec::with_capacity(cap),
            days_since_epoch: Vec::with_capacity(cap),
            free_slots: Vec::new(),
            pk: HashMap::with_capacity(cap),
            by_storage: HashMap::with_capacity(cap / 8),
            by_player: HashMap::with_capacity(cap / 8),
            by_item: HashMap::with_capacity(cap / 4),
        }
    }

    pub fn len(&self) -> usize {
        self.pk.len()
    }

    pub fn by_storage(&self, storage: u64) -> &[u32] {
        self.by_storage
            .get(&storage)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub fn by_player(&self, player: u64) -> &[u32] {
        self.by_player
            .get(&player)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    /// Logs for `item_id` (optional `item_type` filter) whose storage is in
    /// `storage_ids`.
    pub fn by_item_in_storages(
        &self,
        item_id: i32,
        item_type: Option<u8>,
        storage_ids: &HashSet<u64>,
    ) -> Vec<u32> {
        let mut out = Vec::new();
        for slot in self.item_slots(item_id, item_type) {
            let i = slot as usize;
            if storage_ids.contains(&self.storage_entity_id[i]) {
                out.push(slot);
            }
        }
        out
    }

    /// Logs for `item_id` by a given player (optional `item_type` filter).
    pub fn by_item_and_player(
        &self,
        item_id: i32,
        item_type: Option<u8>,
        player: u64,
    ) -> Vec<u32> {
        let mut out = Vec::new();
        for slot in self.item_slots(item_id, item_type) {
            let i = slot as usize;
            if self.player_entity_id[i] == player {
                out.push(slot);
            }
        }
        out
    }

    fn item_slots(&self, item_id: i32, item_type: Option<u8>) -> Vec<u32> {
        match item_type {
            Some(t) => self
                .by_item
                .get(&(item_id, t))
                .cloned()
                .unwrap_or_default(),
            None => {
                let mut out = Vec::new();
                if let Some(v) = self.by_item.get(&(item_id, Pocket::ITEM)) {
                    out.extend_from_slice(v);
                }
                if let Some(v) = self.by_item.get(&(item_id, Pocket::CARGO)) {
                    out.extend_from_slice(v);
                }
                out
            }
        }
    }

    pub fn upsert(&mut self, row: StorageLogRow) {
        if let Some(&slot) = self.pk.get(&row.id) {
            let i = slot as usize;
            let old_storage = self.storage_entity_id[i];
            let old_player = self.player_entity_id[i];
            let old_item = (self.item_id[i], self.item_type[i]);
            self.write_at(slot, &row);
            let new_item = (row.item_id, row.item_type);
            if old_storage != row.storage_entity_id {
                reindex_u64(&mut self.by_storage, slot, old_storage, row.storage_entity_id);
            }
            if old_player != row.player_entity_id {
                reindex_u64(&mut self.by_player, slot, old_player, row.player_entity_id);
            }
            if old_item != new_item {
                self.reindex_item(slot, old_item, new_item);
            }
            return;
        }
        let slot = self.alloc_slot();
        self.write_at(slot, &row);
        self.pk.insert(row.id, slot);
        self.by_storage
            .entry(row.storage_entity_id)
            .or_default()
            .push(slot);
        self.by_player
            .entry(row.player_entity_id)
            .or_default()
            .push(slot);
        self.by_item
            .entry((row.item_id, row.item_type))
            .or_default()
            .push(slot);
    }

    pub fn delete(&mut self, id: u64) {
        let Some(slot) = self.pk.remove(&id) else {
            return;
        };
        let i = slot as usize;
        let storage = self.storage_entity_id[i];
        let player = self.player_entity_id[i];
        let item = (self.item_id[i], self.item_type[i]);
        remove_from_map(&mut self.by_storage, storage, slot);
        remove_from_map(&mut self.by_player, player, slot);
        if let Some(vec) = self.by_item.get_mut(&item) {
            remove_one(vec, slot);
            if vec.is_empty() {
                self.by_item.remove(&item);
            }
        }
        self.id[i] = 0;
        self.player_username[i] = Box::from("");
        self.free_slots.push(slot);
    }

    fn alloc_slot(&mut self) -> u32 {
        if let Some(slot) = self.free_slots.pop() {
            slot
        } else {
            let slot = self.id.len() as u32;
            self.id.push(0);
            self.storage_entity_id.push(0);
            self.player_entity_id.push(0);
            self.player_username.push(Box::from(""));
            self.action.push(0);
            self.item_id.push(0);
            self.item_type.push(0);
            self.quantity.push(0);
            self.timestamp_micros.push(0);
            self.days_since_epoch.push(0);
            slot
        }
    }

    fn write_at(&mut self, slot: u32, row: &StorageLogRow) {
        let i = slot as usize;
        self.id[i] = row.id;
        self.storage_entity_id[i] = row.storage_entity_id;
        self.player_entity_id[i] = row.player_entity_id;
        self.player_username[i] = Box::from(row.player_username.as_str());
        self.action[i] = row.action;
        self.item_id[i] = row.item_id;
        self.item_type[i] = row.item_type;
        self.quantity[i] = row.quantity;
        self.timestamp_micros[i] = row.timestamp_micros;
        self.days_since_epoch[i] = row.days_since_epoch;
    }

    fn reindex_item(&mut self, slot: u32, old: (i32, u8), new: (i32, u8)) {
        if let Some(vec) = self.by_item.get_mut(&old) {
            remove_one(vec, slot);
            if vec.is_empty() {
                self.by_item.remove(&old);
            }
        }
        self.by_item.entry(new).or_default().push(slot);
    }
}

fn reindex_u64(map: &mut HashMap<u64, Vec<u32>>, slot: u32, old_key: u64, new_key: u64) {
    remove_from_map(map, old_key, slot);
    map.entry(new_key).or_default().push(slot);
}

fn remove_from_map(map: &mut HashMap<u64, Vec<u32>>, key: u64, slot: u32) {
    if let Some(vec) = map.get_mut(&key) {
        remove_one(vec, slot);
        if vec.is_empty() {
            map.remove(&key);
        }
    }
}

fn remove_one(vec: &mut Vec<u32>, val: u32) {
    if let Some(idx) = vec.iter().position(|&v| v == val) {
        vec.swap_remove(idx);
    }
}

pub fn action_label(action: u8) -> &'static str {
    match action {
        ACTION_RESERVED => "reserved",
        ACTION_WITHDRAW => "withdraw",
        ACTION_DEPOSIT => "deposit",
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(
        id: u64,
        storage: u64,
        player: u64,
        action: u8,
        item_id: i32,
        item_type: u8,
    ) -> StorageLogRow {
        StorageLogRow {
            id,
            storage_entity_id: storage,
            player_entity_id: player,
            player_username: "Test".into(),
            action,
            item_id,
            item_type,
            quantity: 1,
            timestamp_micros: id as i64,
            days_since_epoch: 1,
        }
    }

    #[test]
    fn indexes_and_delete() {
        let mut s = StorageLogSoA::with_capacity(4);
        s.upsert(row(1, 10, 100, ACTION_DEPOSIT, 5, Pocket::ITEM));
        s.upsert(row(2, 10, 101, ACTION_WITHDRAW, 5, Pocket::ITEM));
        s.upsert(row(3, 11, 100, ACTION_DEPOSIT, 5, Pocket::CARGO));
        assert_eq!(s.by_storage(10).len(), 2);
        assert_eq!(s.by_player(100).len(), 2);
        assert_eq!(s.by_item_and_player(5, Some(Pocket::ITEM), 100).len(), 1);
        let mut storages = HashSet::new();
        storages.insert(10);
        assert_eq!(s.by_item_in_storages(5, None, &storages).len(), 2);
        s.delete(1);
        assert_eq!(s.by_storage(10).len(), 1);
        assert_eq!(s.len(), 2);
    }
}
