// SPDX-License-Identifier: MIT

//! Columnar store for `claim_local_state` (1:1 with claim entity_id).

use hashbrown::HashMap;

use crate::decode::ClaimLocalRow;

pub struct ClaimLocalSoA {
    pub entity_id: Vec<u64>,
    pub supplies: Vec<i32>,
    pub building_maintenance: Vec<f32>,
    pub num_tiles: Vec<i32>,
    pub treasury: Vec<u32>,
    pub supplies_purchase_threshold: Vec<u32>,
    pub supplies_purchase_price: Vec<f32>,
    pub location_x: Vec<i32>,
    pub location_z: Vec<i32>,
    pub location_dimension: Vec<u32>,
    pub has_location: Vec<bool>,
    free_slots: Vec<u32>,
    pk: HashMap<u64, u32>,
}

impl ClaimLocalSoA {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            entity_id: Vec::with_capacity(cap),
            supplies: Vec::with_capacity(cap),
            building_maintenance: Vec::with_capacity(cap),
            num_tiles: Vec::with_capacity(cap),
            treasury: Vec::with_capacity(cap),
            supplies_purchase_threshold: Vec::with_capacity(cap),
            supplies_purchase_price: Vec::with_capacity(cap),
            location_x: Vec::with_capacity(cap),
            location_z: Vec::with_capacity(cap),
            location_dimension: Vec::with_capacity(cap),
            has_location: Vec::with_capacity(cap),
            free_slots: Vec::new(),
            pk: HashMap::with_capacity(cap),
        }
    }

    pub fn len(&self) -> usize {
        self.pk.len()
    }

    pub fn find(&self, entity_id: u64) -> Option<u32> {
        self.pk.get(&entity_id).copied()
    }

    pub fn upsert(&mut self, row: ClaimLocalRow) {
        if let Some(&slot) = self.pk.get(&row.entity_id) {
            self.write_at(slot, &row);
            return;
        }
        let slot = self.alloc_slot();
        self.write_at(slot, &row);
        self.pk.insert(row.entity_id, slot);
    }

    pub fn delete(&mut self, entity_id: u64) {
        let Some(slot) = self.pk.remove(&entity_id) else {
            return;
        };
        let i = slot as usize;
        self.entity_id[i] = 0;
        self.free_slots.push(slot);
    }

    fn alloc_slot(&mut self) -> u32 {
        if let Some(slot) = self.free_slots.pop() {
            slot
        } else {
            let slot = self.entity_id.len() as u32;
            self.entity_id.push(0);
            self.supplies.push(0);
            self.building_maintenance.push(0.0);
            self.num_tiles.push(0);
            self.treasury.push(0);
            self.supplies_purchase_threshold.push(0);
            self.supplies_purchase_price.push(0.0);
            self.location_x.push(0);
            self.location_z.push(0);
            self.location_dimension.push(0);
            self.has_location.push(false);
            slot
        }
    }

    fn write_at(&mut self, slot: u32, row: &ClaimLocalRow) {
        let i = slot as usize;
        self.entity_id[i] = row.entity_id;
        self.supplies[i] = row.supplies;
        self.building_maintenance[i] = row.building_maintenance;
        self.num_tiles[i] = row.num_tiles;
        self.treasury[i] = row.treasury;
        self.supplies_purchase_threshold[i] = row.supplies_purchase_threshold;
        self.supplies_purchase_price[i] = row.supplies_purchase_price;
        self.location_x[i] = row.location_x;
        self.location_z[i] = row.location_z;
        self.location_dimension[i] = row.location_dimension;
        self.has_location[i] = row.has_location;
    }
}
