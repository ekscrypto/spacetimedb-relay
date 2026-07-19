// SPDX-License-Identifier: MIT

//! Columnar store for Hexite Deposit rows from `resource_state`.
//!
//! Only Hexite Deposit / Depleted Hexite Deposit resource_ids are
//! subscribed; coords come from a follow-up `location_state` PK
//! subscribe per entity (overworld `x`/`z`).

use hashbrown::HashMap;

use crate::decode::{is_hexite_resource_id, ResourceRow, DEPLETED_HEXITE_DEPOSIT_RESOURCE_ID};

pub struct ResourceSoA {
    pub entity_id: Vec<u64>,
    pub resource_id: Vec<i32>,
    pub location_x: Vec<i32>,
    pub location_z: Vec<i32>,
    pub has_location: Vec<bool>,
    free_slots: Vec<u32>,
    pk: HashMap<u64, u32>,
}

impl ResourceSoA {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            entity_id: Vec::with_capacity(cap),
            resource_id: Vec::with_capacity(cap),
            location_x: Vec::with_capacity(cap),
            location_z: Vec::with_capacity(cap),
            has_location: Vec::with_capacity(cap),
            free_slots: Vec::new(),
            pk: HashMap::with_capacity(cap),
        }
    }

    pub fn len(&self) -> usize {
        self.pk.len()
    }

    pub fn iter_slots(&self) -> impl Iterator<Item = u32> + '_ {
        self.pk.values().copied()
    }

    pub fn is_active(&self, slot: u32) -> bool {
        self.resource_id[slot as usize] != DEPLETED_HEXITE_DEPOSIT_RESOURCE_ID
    }

    pub fn upsert(&mut self, row: ResourceRow) {
        if !is_hexite_resource_id(row.resource_id) {
            self.delete(row.entity_id);
            return;
        }
        if let Some(&slot) = self.pk.get(&row.entity_id) {
            self.resource_id[slot as usize] = row.resource_id;
            return;
        }
        let slot = self.alloc_slot();
        let i = slot as usize;
        self.entity_id[i] = row.entity_id;
        self.resource_id[i] = row.resource_id;
        self.location_x[i] = 0;
        self.location_z[i] = 0;
        self.has_location[i] = false;
        self.pk.insert(row.entity_id, slot);
    }

    pub fn delete(&mut self, entity_id: u64) {
        let Some(slot) = self.pk.remove(&entity_id) else {
            return;
        };
        let i = slot as usize;
        self.entity_id[i] = 0;
        self.resource_id[i] = 0;
        self.location_x[i] = 0;
        self.location_z[i] = 0;
        self.has_location[i] = false;
        self.free_slots.push(slot);
    }

    /// Apply a `location_state` row when this entity is a known hexite deposit.
    /// Returns true when the location was applied.
    pub fn set_location(&mut self, entity_id: u64, x: i32, z: i32) -> bool {
        let Some(&slot) = self.pk.get(&entity_id) else {
            return false;
        };
        let i = slot as usize;
        self.location_x[i] = x;
        self.location_z[i] = z;
        self.has_location[i] = true;
        true
    }

    pub fn clear_location(&mut self, entity_id: u64) {
        if let Some(&slot) = self.pk.get(&entity_id) {
            let i = slot as usize;
            self.location_x[i] = 0;
            self.location_z[i] = 0;
            self.has_location[i] = false;
        }
    }

    fn alloc_slot(&mut self) -> u32 {
        if let Some(slot) = self.free_slots.pop() {
            slot
        } else {
            let slot = self.entity_id.len() as u32;
            self.entity_id.push(0);
            self.resource_id.push(0);
            self.location_x.push(0);
            self.location_z.push(0);
            self.has_location.push(false);
            slot
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode::HEXITE_DEPOSIT_RESOURCE_ID;

    #[test]
    fn upsert_location_and_deplete() {
        let mut s = ResourceSoA::with_capacity(2);
        s.upsert(ResourceRow {
            entity_id: 10,
            resource_id: HEXITE_DEPOSIT_RESOURCE_ID,
        });
        assert!(s.set_location(10, 8174, 6158));
        let slot = s.iter_slots().next().unwrap();
        assert_eq!(s.entity_id[slot as usize], 10);
        assert!(s.has_location[slot as usize]);
        assert_eq!(s.location_x[slot as usize], 8174);
        assert_eq!(s.location_z[slot as usize], 6158);
        assert!(s.is_active(slot));

        s.upsert(ResourceRow {
            entity_id: 10,
            resource_id: DEPLETED_HEXITE_DEPOSIT_RESOURCE_ID,
        });
        assert!(!s.is_active(slot));
        // Location ignored for unknown entities.
        assert!(!s.set_location(99, 1, 2));
    }
}
