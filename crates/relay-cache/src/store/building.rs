// SPDX-License-Identifier: MIT

//! Columnar store for `building_state`.
//!
//! Shape: `entity_id (U64, PK), claim_entity_id (U64),
//! building_description_id (I32)`. Catalog name and player nickname live
//! in separate tables (`building_desc`, `building_nickname_state`).
//!
//! ~74K rows per region × 13 regions ≈ ~960K total. Two secondary indexes
//! drive the inventory-reconstruction and building-type-filter joins:
//!
//! - `by_claim`: `claim_entity_id → Vec<slot>` — the "list buildings in
//!   claim X" entry point for the Q3 inventory rollup.
//! - `by_desc`: `building_description_id → Vec<slot>` — the "buildings of
//!   description D" filter used by future queries that combine with
//!   `location_state` (when that lands).
//!
//! Both secondary indexes are non-unique; a `Vec<u32>` per key. Deletes do
//! a linear `swap_remove` of the slot from the vec — at a few hundred
//! buildings per claim this is sub-microsecond.

use hashbrown::HashMap;

use crate::decode::BuildingRow;

pub struct BuildingSoA {
    pub entity_id: Vec<u64>,
    pub claim_entity_id: Vec<u64>,
    pub building_description_id: Vec<i32>,
    free_slots: Vec<u32>,
    pk: HashMap<u64, u32>,
    by_claim: HashMap<u64, Vec<u32>>,
    by_desc: HashMap<i32, Vec<u32>>,
}

impl BuildingSoA {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            entity_id: Vec::with_capacity(cap),
            claim_entity_id: Vec::with_capacity(cap),
            building_description_id: Vec::with_capacity(cap),
            free_slots: Vec::new(),
            pk: HashMap::with_capacity(cap),
            by_claim: HashMap::with_capacity(cap),
            by_desc: HashMap::with_capacity(cap / 10),
        }
    }

    pub fn len(&self) -> usize {
        self.pk.len()
    }

    #[allow(dead_code)] // exercised in unit tests; available for future queries
    pub fn find(&self, entity_id: u64) -> Option<u32> {
        self.pk.get(&entity_id).copied()
    }

    /// Buildings whose `claim_entity_id == claim`. Returns borrowed slots
    /// slice to avoid cloning when the caller just wants to iterate.
    pub fn by_claim(&self, claim: u64) -> &[u32] {
        self.by_claim.get(&claim).map(Vec::as_slice).unwrap_or(&[])
    }

    /// Buildings whose `building_description_id == desc`.
    #[allow(dead_code)] // exercised in unit tests; available for future queries
    pub fn by_desc(&self, desc: i32) -> &[u32] {
        self.by_desc.get(&desc).map(Vec::as_slice).unwrap_or(&[])
    }

    /// Insert or replace a row. On replace, secondary indexes are updated
    /// iff the secondary key changed.
    pub fn upsert(&mut self, row: BuildingRow) {
        if let Some(&slot) = self.pk.get(&row.entity_id) {
            let i = slot as usize;
            let old_claim = self.claim_entity_id[i];
            let old_desc = self.building_description_id[i];
            self.write_at(slot, &row);
            if old_claim != row.claim_entity_id {
                self.reindex_by_claim(slot, old_claim, row.claim_entity_id);
            }
            if old_desc != row.building_description_id {
                self.reindex_by_desc(slot, old_desc, row.building_description_id);
            }
            return;
        }
        let slot = self.alloc_slot();
        self.write_at(slot, &row);
        self.pk.insert(row.entity_id, slot);
        self.by_claim
            .entry(row.claim_entity_id)
            .or_default()
            .push(slot);
        self.by_desc
            .entry(row.building_description_id)
            .or_default()
            .push(slot);
    }

    pub fn delete(&mut self, entity_id: u64) {
        let Some(slot) = self.pk.remove(&entity_id) else {
            return;
        };
        let i = slot as usize;
        let claim = self.claim_entity_id[i];
        let desc = self.building_description_id[i];
        if let Some(vec) = self.by_claim.get_mut(&claim) {
            remove_one(vec, slot);
            if vec.is_empty() {
                self.by_claim.remove(&claim);
            }
        }
        if let Some(vec) = self.by_desc.get_mut(&desc) {
            remove_one(vec, slot);
            if vec.is_empty() {
                self.by_desc.remove(&desc);
            }
        }
        self.entity_id[i] = 0;
        self.free_slots.push(slot);
    }

    fn alloc_slot(&mut self) -> u32 {
        if let Some(slot) = self.free_slots.pop() {
            slot
        } else {
            let slot = self.entity_id.len() as u32;
            self.entity_id.push(0);
            self.claim_entity_id.push(0);
            self.building_description_id.push(0);
            slot
        }
    }

    fn write_at(&mut self, slot: u32, row: &BuildingRow) {
        let i = slot as usize;
        self.entity_id[i] = row.entity_id;
        self.claim_entity_id[i] = row.claim_entity_id;
        self.building_description_id[i] = row.building_description_id;
    }

    fn reindex_by_claim(&mut self, slot: u32, old_claim: u64, new_claim: u64) {
        if let Some(vec) = self.by_claim.get_mut(&old_claim) {
            remove_one(vec, slot);
            if vec.is_empty() {
                self.by_claim.remove(&old_claim);
            }
        }
        self.by_claim.entry(new_claim).or_default().push(slot);
    }

    fn reindex_by_desc(&mut self, slot: u32, old_desc: i32, new_desc: i32) {
        if let Some(vec) = self.by_desc.get_mut(&old_desc) {
            remove_one(vec, slot);
            if vec.is_empty() {
                self.by_desc.remove(&old_desc);
            }
        }
        self.by_desc.entry(new_desc).or_default().push(slot);
    }
}

/// Remove the first occurrence of `val` from `vec` via swap_remove. O(n)
/// but cheap for the small per-claim / per-desc vecs we maintain.
fn remove_one(vec: &mut Vec<u32>, val: u32) {
    if let Some(idx) = vec.iter().position(|&v| v == val) {
        vec.swap_remove(idx);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(entity_id: u64, claim: u64, desc: i32) -> BuildingRow {
        BuildingRow {
            entity_id,
            claim_entity_id: claim,
            building_description_id: desc,
        }
    }

    #[test]
    fn upsert_insert_and_pk_lookup() {
        let mut s = BuildingSoA::with_capacity(4);
        s.upsert(row(100, 1000, 1007));
        let slot = s.find(100).unwrap();
        assert_eq!(s.claim_entity_id[slot as usize], 1000);
        assert_eq!(s.building_description_id[slot as usize], 1007);
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn by_claim_returns_all_buildings_in_claim() {
        let mut s = BuildingSoA::with_capacity(8);
        s.upsert(row(10, 1000, 1007));
        s.upsert(row(11, 1000, 2007));
        s.upsert(row(12, 2000, 1007));
        let slots = s.by_claim(1000).to_vec();
        assert_eq!(slots.len(), 2);
        let mut ids: Vec<u64> = slots.iter().map(|&sl| s.entity_id[sl as usize]).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![10u64, 11]);
        assert_eq!(s.by_claim(2000).len(), 1);
        assert!(s.by_claim(9999).is_empty());
    }

    #[test]
    fn by_desc_indexes_all_descriptions() {
        let mut s = BuildingSoA::with_capacity(8);
        s.upsert(row(10, 1000, 1007));
        s.upsert(row(11, 1000, 2007));
        s.upsert(row(12, 2000, 1007));
        assert_eq!(s.by_desc(1007).len(), 2);
        assert_eq!(s.by_desc(2007).len(), 1);
        assert!(s.by_desc(9999).is_empty());
    }

    #[test]
    fn upsert_overwrite_no_secondary_move_when_keys_unchanged() {
        let mut s = BuildingSoA::with_capacity(4);
        s.upsert(row(10, 1000, 1007));
        s.upsert(row(10, 1000, 1007));
        assert_eq!(s.len(), 1);
        assert_eq!(s.by_claim(1000).len(), 1);
        assert_eq!(s.by_desc(1007).len(), 1);
    }

    #[test]
    fn upsert_overwrite_moves_secondary_when_claim_changes() {
        let mut s = BuildingSoA::with_capacity(4);
        s.upsert(row(10, 1000, 1007));
        s.upsert(row(10, 5000, 1007));
        assert_eq!(s.len(), 1);
        assert!(s.by_claim(1000).is_empty());
        assert_eq!(s.by_claim(5000).len(), 1);
        assert_eq!(s.by_desc(1007).len(), 1);
    }

    #[test]
    fn upsert_overwrite_moves_secondary_when_desc_changes() {
        let mut s = BuildingSoA::with_capacity(4);
        s.upsert(row(10, 1000, 1007));
        s.upsert(row(10, 1000, 9001));
        assert_eq!(s.len(), 1);
        assert!(s.by_desc(1007).is_empty());
        assert_eq!(s.by_desc(9001).len(), 1);
    }

    #[test]
    fn delete_removes_from_all_indexes() {
        let mut s = BuildingSoA::with_capacity(8);
        s.upsert(row(10, 1000, 1007));
        s.upsert(row(11, 1000, 2007));
        s.delete(10);
        assert!(s.find(10).is_none());
        assert_eq!(s.by_claim(1000).len(), 1);
        assert!(s.by_desc(1007).is_empty());
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn delete_missing_is_noop() {
        let mut s = BuildingSoA::with_capacity(4);
        s.upsert(row(10, 1000, 1007));
        s.delete(999);
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn delete_last_in_claim_bucket_removes_the_key() {
        let mut s = BuildingSoA::with_capacity(4);
        s.upsert(row(10, 1000, 1007));
        s.delete(10);
        assert!(s.by_claim(1000).is_empty());
        s.upsert(row(11, 1000, 1007));
        assert_eq!(s.by_claim(1000).len(), 1);
    }
}
