// SPDX-License-Identifier: MIT

//! Stores for `player_housing_state` and `player_housing_desc`.

use hashbrown::HashMap;

use crate::decode::{PlayerHousingDescRow, PlayerHousingRow};

pub struct PlayerHousingSoA {
    pub entity_id: Vec<u64>,
    pub entrance_building_entity_id: Vec<u64>,
    pub network_entity_id: Vec<u64>,
    pub rank: Vec<i32>,
    pub is_empty: Vec<bool>,
    free_slots: Vec<u32>,
    pk: HashMap<u64, u32>,
    by_network: HashMap<u64, u32>,
}

impl PlayerHousingSoA {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            entity_id: Vec::with_capacity(cap),
            entrance_building_entity_id: Vec::with_capacity(cap),
            network_entity_id: Vec::with_capacity(cap),
            rank: Vec::with_capacity(cap),
            is_empty: Vec::with_capacity(cap),
            free_slots: Vec::new(),
            pk: HashMap::with_capacity(cap),
            by_network: HashMap::with_capacity(cap),
        }
    }

    pub fn len(&self) -> usize {
        self.pk.len()
    }

    #[allow(dead_code)] // PK lookup for future queries / debugging
    pub fn find(&self, entity_id: u64) -> Option<u32> {
        self.pk.get(&entity_id).copied()
    }

    pub fn by_network(&self, network_entity_id: u64) -> Option<u32> {
        self.by_network.get(&network_entity_id).copied()
    }

    pub fn upsert(&mut self, row: PlayerHousingRow) {
        if let Some(&slot) = self.pk.get(&row.entity_id) {
            let i = slot as usize;
            let old_net = self.network_entity_id[i];
            self.write_at(slot, &row);
            if old_net != row.network_entity_id {
                if self.by_network.get(&old_net).copied() == Some(slot) {
                    self.by_network.remove(&old_net);
                }
                self.by_network.insert(row.network_entity_id, slot);
            }
            return;
        }
        let slot = self.alloc_slot();
        self.write_at(slot, &row);
        self.pk.insert(row.entity_id, slot);
        self.by_network.insert(row.network_entity_id, slot);
    }

    pub fn delete(&mut self, entity_id: u64) {
        let Some(slot) = self.pk.remove(&entity_id) else {
            return;
        };
        let i = slot as usize;
        let net = self.network_entity_id[i];
        if self.by_network.get(&net).copied() == Some(slot) {
            self.by_network.remove(&net);
        }
        self.entity_id[i] = 0;
        self.network_entity_id[i] = 0;
        self.free_slots.push(slot);
    }

    fn alloc_slot(&mut self) -> u32 {
        if let Some(slot) = self.free_slots.pop() {
            slot
        } else {
            let slot = self.entity_id.len() as u32;
            self.entity_id.push(0);
            self.entrance_building_entity_id.push(0);
            self.network_entity_id.push(0);
            self.rank.push(0);
            self.is_empty.push(false);
            slot
        }
    }

    fn write_at(&mut self, slot: u32, row: &PlayerHousingRow) {
        let i = slot as usize;
        self.entity_id[i] = row.entity_id;
        self.entrance_building_entity_id[i] = row.entrance_building_entity_id;
        self.network_entity_id[i] = row.network_entity_id;
        self.rank[i] = row.rank;
        self.is_empty[i] = row.is_empty;
    }
}

/// Catalog: rank → house display name.
pub struct PlayerHousingDescStore {
    by_rank: HashMap<i32, Box<str>>,
}

impl PlayerHousingDescStore {
    pub fn new() -> Self {
        Self {
            by_rank: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.by_rank.len()
    }

    pub fn name_for_rank(&self, rank: i32) -> Option<&str> {
        self.by_rank.get(&rank).map(|s| s.as_ref())
    }

    pub fn upsert(&mut self, row: PlayerHousingDescRow) {
        self.by_rank
            .insert(row.rank, Box::from(row.name.as_str()));
    }

    pub fn delete_rank(&mut self, rank: i32) {
        self.by_rank.remove(&rank);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn by_network_and_rank_name() {
        let mut s = PlayerHousingSoA::with_capacity(2);
        s.upsert(PlayerHousingRow {
            entity_id: 100,
            entrance_building_entity_id: 50,
            network_entity_id: 77,
            rank: 1,
            is_empty: false,
        });
        let slot = s.by_network(77).unwrap();
        assert_eq!(s.entity_id[slot as usize], 100);

        let mut d = PlayerHousingDescStore::new();
        d.upsert(PlayerHousingDescRow {
            secondary_knowledge_id: 9,
            rank: 1,
            name: "Player Housing Catacombs".into(),
        });
        assert_eq!(d.name_for_rank(1), Some("Player Housing Catacombs"));
    }
}
