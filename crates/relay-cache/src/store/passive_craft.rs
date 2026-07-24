// SPDX-License-Identifier: MIT

//! Columnar store for `passive_craft_state` with by-owner and by-building
//! secondary indexes.

use hashbrown::HashMap;

use crate::decode::{PassiveCraftRow, PassiveCraftStatus};

pub struct PassiveCraftSoA {
    pub entity_id: Vec<u64>,
    pub owner_entity_id: Vec<u64>,
    pub recipe_id: Vec<i32>,
    pub building_entity_id: Vec<u64>,
    pub status: Vec<PassiveCraftStatus>,
    free_slots: Vec<u32>,
    pk: HashMap<u64, u32>,
    by_owner: HashMap<u64, Vec<u32>>,
    by_building: HashMap<u64, Vec<u32>>,
}

impl PassiveCraftSoA {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            entity_id: Vec::with_capacity(cap),
            owner_entity_id: Vec::with_capacity(cap),
            recipe_id: Vec::with_capacity(cap),
            building_entity_id: Vec::with_capacity(cap),
            status: Vec::with_capacity(cap),
            free_slots: Vec::new(),
            pk: HashMap::with_capacity(cap),
            by_owner: HashMap::with_capacity(cap),
            by_building: HashMap::with_capacity(cap),
        }
    }

    pub fn len(&self) -> usize {
        self.pk.len()
    }

    pub fn find(&self, entity_id: u64) -> Option<u32> {
        self.pk.get(&entity_id).copied()
    }

    pub fn by_owner(&self, owner: u64) -> &[u32] {
        self.by_owner.get(&owner).map(Vec::as_slice).unwrap_or(&[])
    }

    pub fn by_building(&self, building: u64) -> &[u32] {
        self.by_building
            .get(&building)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub fn upsert(&mut self, row: PassiveCraftRow) {
        if let Some(&slot) = self.pk.get(&row.entity_id) {
            let i = slot as usize;
            let old_owner = self.owner_entity_id[i];
            let old_building = self.building_entity_id[i];
            self.write_at(slot, &row);
            if old_owner != row.owner_entity_id {
                reindex(&mut self.by_owner, slot, old_owner, row.owner_entity_id);
            }
            if old_building != row.building_entity_id {
                reindex(
                    &mut self.by_building,
                    slot,
                    old_building,
                    row.building_entity_id,
                );
            }
            return;
        }
        let slot = self.alloc_slot();
        self.write_at(slot, &row);
        self.pk.insert(row.entity_id, slot);
        self.by_owner
            .entry(row.owner_entity_id)
            .or_default()
            .push(slot);
        self.by_building
            .entry(row.building_entity_id)
            .or_default()
            .push(slot);
    }

    pub fn delete(&mut self, entity_id: u64) {
        let Some(slot) = self.pk.remove(&entity_id) else {
            return;
        };
        let i = slot as usize;
        remove_from_index(&mut self.by_owner, self.owner_entity_id[i], slot);
        remove_from_index(&mut self.by_building, self.building_entity_id[i], slot);
        self.entity_id[i] = 0;
        self.free_slots.push(slot);
    }

    fn alloc_slot(&mut self) -> u32 {
        if let Some(slot) = self.free_slots.pop() {
            slot
        } else {
            let slot = self.entity_id.len() as u32;
            self.entity_id.push(0);
            self.owner_entity_id.push(0);
            self.recipe_id.push(0);
            self.building_entity_id.push(0);
            self.status.push(PassiveCraftStatus::Queued);
            slot
        }
    }

    fn write_at(&mut self, slot: u32, row: &PassiveCraftRow) {
        let i = slot as usize;
        self.entity_id[i] = row.entity_id;
        self.owner_entity_id[i] = row.owner_entity_id;
        self.recipe_id[i] = row.recipe_id;
        self.building_entity_id[i] = row.building_entity_id;
        self.status[i] = row.status;
    }
}

fn reindex(index: &mut HashMap<u64, Vec<u32>>, slot: u32, old_key: u64, new_key: u64) {
    remove_from_index(index, old_key, slot);
    index.entry(new_key).or_default().push(slot);
}

fn remove_from_index(index: &mut HashMap<u64, Vec<u32>>, key: u64, slot: u32) {
    if let Some(vec) = index.get_mut(&key) {
        remove_one(vec, slot);
        if vec.is_empty() {
            index.remove(&key);
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

    fn row(id: u64, building: u64, owner: u64) -> PassiveCraftRow {
        PassiveCraftRow {
            entity_id: id,
            owner_entity_id: owner,
            recipe_id: 1,
            building_entity_id: building,
            status: PassiveCraftStatus::Processing,
        }
    }

    #[test]
    fn secondary_indexes() {
        let mut s = PassiveCraftSoA::with_capacity(4);
        s.upsert(row(1, 10, 100));
        s.upsert(row(2, 10, 101));
        assert_eq!(s.by_building(10).len(), 2);
        s.delete(1);
        assert_eq!(s.by_building(10).len(), 1);
        assert!(s.by_owner(100).is_empty());
    }
}
