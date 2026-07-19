// SPDX-License-Identifier: MIT

//! Columnar store for `inventory_state`.
//!
//! Schema: `entity_id (U64, PK), pockets (Array<Pocket>), inventory_index
//! (I32), cargo_index (I32), owner_entity_id (U64),
//! player_owner_entity_id (U64)`.
//!
//! **Nested storage**: each source row carries its `pockets` array intact
//! as `Box<[Pocket]>`, not flattened. Flattening would lose the source-row
//! identity needed to apply upstream `TransactionUpdate`s, which are keyed
//! on `entity_id` and replace the whole row (including pockets) at once.
//! Nested storage turns an update into a single slot's `Box<[Pocket]>`
//! swap; flattened storage would force a diff-and-rewrite per pocket.
//!
//! ~1.2M source rows per region × 13 regions ≈ ~15M total. The Q3 query
//! walks `by_owner[building.entity_id]` and folds pockets by
//! `(item_id, item_type)`.

use hashbrown::HashMap;

use crate::decode::InventoryRow;

/// One pocket of an inventory. `Copy` so swapping a `Box<[Pocket]>` is a
/// single allocation; the array reallocs only when the pocket count
/// actually changes (rare — buildings don't usually gain slots).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Pocket {
    pub volume: i32,
    pub has_contents: bool,
    pub item_id: i32,
    pub quantity: i32,
    /// 0 = Item, 1 = Cargo. Mirrors the upstream `sum { Item, Cargo }`
    /// discriminant without the `String` overhead the sync crate pays.
    pub item_type: u8,
    pub has_durability: bool,
    pub durability: i32,
}

impl Pocket {
    pub const ITEM: u8 = 0;
    pub const CARGO: u8 = 1;
}

pub struct InventorySoA {
    pub entity_id: Vec<u64>,
    pub pockets: Vec<Box<[Pocket]>>,
    pub inventory_index: Vec<i32>,
    pub cargo_index: Vec<i32>,
    pub owner_entity_id: Vec<u64>,
    pub player_owner_entity_id: Vec<u64>,
    free_slots: Vec<u32>,
    pk: HashMap<u64, u32>,
    by_owner: HashMap<u64, Vec<u32>>,
    by_player_owner: HashMap<u64, Vec<u32>>,
}

impl InventorySoA {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            entity_id: Vec::with_capacity(cap),
            pockets: Vec::with_capacity(cap),
            inventory_index: Vec::with_capacity(cap),
            cargo_index: Vec::with_capacity(cap),
            owner_entity_id: Vec::with_capacity(cap),
            player_owner_entity_id: Vec::with_capacity(cap),
            free_slots: Vec::new(),
            pk: HashMap::with_capacity(cap),
            by_owner: HashMap::with_capacity(cap),
            by_player_owner: HashMap::with_capacity(cap),
        }
    }

    pub fn len(&self) -> usize {
        self.pk.len()
    }

    #[allow(dead_code)] // exercised in unit tests; available for future queries
    pub fn find(&self, entity_id: u64) -> Option<u32> {
        self.pk.get(&entity_id).copied()
    }

    /// All inventory source rows whose `owner_entity_id == owner`. Used by
    /// Q3 to walk a building's inventories.
    pub fn by_owner(&self, owner: u64) -> &[u32] {
        self.by_owner.get(&owner).map(Vec::as_slice).unwrap_or(&[])
    }

    /// Inventories whose `player_owner_entity_id == player` (banks, caches,
    /// deployables). Body bags use `owner_entity_id == player` instead.
    pub fn by_player_owner(&self, player: u64) -> &[u32] {
        self.by_player_owner
            .get(&player)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    /// Insert or replace a row. On replace, the `by_owner` /
    /// `by_player_owner` indexes are updated iff those keys changed.
    /// Takes ownership so the pockets `Box<[Pocket]>` moves in without
    /// cloning.
    pub fn upsert(&mut self, row: InventoryRow) {
        if let Some(&slot) = self.pk.get(&row.entity_id) {
            let i = slot as usize;
            let old_owner = self.owner_entity_id[i];
            let old_player = self.player_owner_entity_id[i];
            let new_owner = row.owner_entity_id;
            let new_player = row.player_owner_entity_id;
            self.write_at(slot, row);
            if old_owner != new_owner {
                self.reindex_by_owner(slot, old_owner, new_owner);
            }
            if old_player != new_player {
                self.reindex_by_player_owner(slot, old_player, new_player);
            }
            return;
        }
        let entity_id = row.entity_id;
        let owner = row.owner_entity_id;
        let player = row.player_owner_entity_id;
        let slot = self.alloc_slot();
        self.write_at(slot, row);
        self.pk.insert(entity_id, slot);
        self.by_owner.entry(owner).or_default().push(slot);
        if player != 0 {
            self.by_player_owner.entry(player).or_default().push(slot);
        }
    }

    pub fn delete(&mut self, entity_id: u64) {
        let Some(slot) = self.pk.remove(&entity_id) else {
            return;
        };
        let i = slot as usize;
        let owner = self.owner_entity_id[i];
        let player = self.player_owner_entity_id[i];
        if let Some(vec) = self.by_owner.get_mut(&owner) {
            remove_one(vec, slot);
            if vec.is_empty() {
                self.by_owner.remove(&owner);
            }
        }
        if player != 0 {
            if let Some(vec) = self.by_player_owner.get_mut(&player) {
                remove_one(vec, slot);
                if vec.is_empty() {
                    self.by_player_owner.remove(&player);
                }
            }
        }
        self.entity_id[i] = 0;
        self.pockets[i] = Box::from([]);
        self.free_slots.push(slot);
    }

    fn alloc_slot(&mut self) -> u32 {
        if let Some(slot) = self.free_slots.pop() {
            slot
        } else {
            let slot = self.entity_id.len() as u32;
            self.entity_id.push(0);
            self.pockets.push(Box::from([]));
            self.inventory_index.push(0);
            self.cargo_index.push(0);
            self.owner_entity_id.push(0);
            self.player_owner_entity_id.push(0);
            slot
        }
    }

    fn write_at(&mut self, slot: u32, row: InventoryRow) {
        let i = slot as usize;
        self.entity_id[i] = row.entity_id;
        self.pockets[i] = row.pockets;
        self.inventory_index[i] = row.inventory_index;
        self.cargo_index[i] = row.cargo_index;
        self.owner_entity_id[i] = row.owner_entity_id;
        self.player_owner_entity_id[i] = row.player_owner_entity_id;
    }

    fn reindex_by_owner(&mut self, slot: u32, old_owner: u64, new_owner: u64) {
        if let Some(vec) = self.by_owner.get_mut(&old_owner) {
            remove_one(vec, slot);
            if vec.is_empty() {
                self.by_owner.remove(&old_owner);
            }
        }
        self.by_owner.entry(new_owner).or_default().push(slot);
    }

    fn reindex_by_player_owner(&mut self, slot: u32, old_player: u64, new_player: u64) {
        if old_player != 0 {
            if let Some(vec) = self.by_player_owner.get_mut(&old_player) {
                remove_one(vec, slot);
                if vec.is_empty() {
                    self.by_player_owner.remove(&old_player);
                }
            }
        }
        if new_player != 0 {
            self.by_player_owner
                .entry(new_player)
                .or_default()
                .push(slot);
        }
    }
}

fn remove_one(vec: &mut Vec<u32>, val: u32) {
    if let Some(idx) = vec.iter().position(|&v| v == val) {
        vec.swap_remove(idx);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pocket(item_id: i32, qty: i32, item_type: u8) -> Pocket {
        Pocket {
            volume: 100,
            has_contents: true,
            item_id,
            quantity: qty,
            item_type,
            has_durability: false,
            durability: 0,
        }
    }

    fn row(entity_id: u64, owner: u64, pockets: &[Pocket]) -> InventoryRow {
        InventoryRow {
            entity_id,
            pockets: pockets.into(),
            inventory_index: 0,
            cargo_index: 0,
            owner_entity_id: owner,
            player_owner_entity_id: 0,
        }
    }

    #[test]
    fn upsert_insert_and_pk_lookup() {
        let mut s = InventorySoA::with_capacity(4);
        let p = [pocket(1020003, 50, Pocket::ITEM)];
        s.upsert(row(100, 1000, &p));
        let slot = s.find(100).unwrap();
        assert_eq!(s.owner_entity_id[slot as usize], 1000);
        assert_eq!(s.pockets[slot as usize].len(), 1);
        assert_eq!(s.pockets[slot as usize][0].item_id, 1020003);
    }

    #[test]
    fn by_owner_returns_all_inventories_of_owner() {
        let mut s = InventorySoA::with_capacity(8);
        s.upsert(row(100, 1000, &[pocket(1, 10, Pocket::ITEM)]));
        s.upsert(row(101, 1000, &[pocket(2, 20, Pocket::CARGO)]));
        s.upsert(row(102, 2000, &[pocket(3, 30, Pocket::ITEM)]));
        let slots = s.by_owner(1000);
        assert_eq!(slots.len(), 2);
        assert_eq!(s.by_owner(2000).len(), 1);
        assert!(s.by_owner(9999).is_empty());
    }

    #[test]
    fn upsert_overwrite_swaps_pockets_in_place() {
        let mut s = InventorySoA::with_capacity(4);
        s.upsert(row(100, 1000, &[pocket(1, 10, Pocket::ITEM)]));
        // Update with a different pockets array — owner unchanged so no
        // secondary index churn.
        s.upsert(row(
            100,
            1000,
            &[pocket(1, 15, Pocket::ITEM), pocket(2, 20, Pocket::CARGO)],
        ));
        assert_eq!(s.len(), 1);
        let slot = s.find(100).unwrap();
        assert_eq!(s.pockets[slot as usize].len(), 2);
        assert_eq!(s.pockets[slot as usize][0].quantity, 15);
        assert_eq!(s.pockets[slot as usize][1].item_type, Pocket::CARGO);
        // by_owner untouched.
        assert_eq!(s.by_owner(1000).len(), 1);
    }

    #[test]
    fn upsert_overwrite_moves_by_owner_when_owner_changes() {
        let mut s = InventorySoA::with_capacity(4);
        s.upsert(row(100, 1000, &[pocket(1, 10, Pocket::ITEM)]));
        s.upsert(row(100, 5000, &[pocket(1, 10, Pocket::ITEM)]));
        assert_eq!(s.len(), 1);
        assert!(s.by_owner(1000).is_empty());
        assert_eq!(s.by_owner(5000).len(), 1);
    }

    #[test]
    fn delete_removes_from_all_indexes() {
        let mut s = InventorySoA::with_capacity(8);
        s.upsert(row(100, 1000, &[pocket(1, 10, Pocket::ITEM)]));
        s.upsert(row(101, 1000, &[pocket(2, 20, Pocket::CARGO)]));
        s.delete(100);
        assert!(s.find(100).is_none());
        assert_eq!(s.by_owner(1000).len(), 1);
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn delete_last_in_owner_bucket_removes_the_key() {
        let mut s = InventorySoA::with_capacity(4);
        s.upsert(row(100, 1000, &[pocket(1, 10, Pocket::ITEM)]));
        s.delete(100);
        assert!(s.by_owner(1000).is_empty());
        s.upsert(row(101, 1000, &[pocket(2, 20, Pocket::CARGO)]));
        assert_eq!(s.by_owner(1000).len(), 1);
    }

    #[test]
    fn delete_missing_is_noop() {
        let mut s = InventorySoA::with_capacity(4);
        s.upsert(row(100, 1000, &[pocket(1, 10, Pocket::ITEM)]));
        s.delete(999);
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn by_player_owner_tracks_player_key() {
        let mut s = InventorySoA::with_capacity(4);
        let mut a = row(100, 1000, &[pocket(1, 10, Pocket::ITEM)]);
        a.player_owner_entity_id = 7;
        s.upsert(a);
        assert_eq!(s.by_player_owner(7).len(), 1);
        let mut b = row(100, 1000, &[pocket(1, 10, Pocket::ITEM)]);
        b.player_owner_entity_id = 8;
        s.upsert(b);
        assert!(s.by_player_owner(7).is_empty());
        assert_eq!(s.by_player_owner(8).len(), 1);
    }

    #[test]
    fn empty_pockets_row_is_storable() {
        // Upstream sometimes sends source rows with all-empty pockets; we
        // keep the row (it's still a live entity) but its pocket array is
        // zero-length.
        let mut s = InventorySoA::with_capacity(4);
        s.upsert(row(100, 1000, &[]));
        let slot = s.find(100).unwrap();
        assert_eq!(s.pockets[slot as usize].len(), 0);
    }
}
