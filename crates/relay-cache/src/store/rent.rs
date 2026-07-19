// SPDX-License-Identifier: MIT

//! Columnar store for `rent_state` with player whitelist secondary index.

use hashbrown::HashMap;

use crate::decode::RentRow;

pub struct RentSoA {
    pub entity_id: Vec<u64>,
    pub dimension_network_id: Vec<u64>,
    pub claim_entity_id: Vec<u64>,
    pub white_list: Vec<Box<[u64]>>,
    pub active: Vec<bool>,
    free_slots: Vec<u32>,
    pk: HashMap<u64, u32>,
    /// Player entity_id → rent slots whose white_list contains that player.
    by_player: HashMap<u64, Vec<u32>>,
}

impl RentSoA {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            entity_id: Vec::with_capacity(cap),
            dimension_network_id: Vec::with_capacity(cap),
            claim_entity_id: Vec::with_capacity(cap),
            white_list: Vec::with_capacity(cap),
            active: Vec::with_capacity(cap),
            free_slots: Vec::new(),
            pk: HashMap::with_capacity(cap),
            by_player: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.pk.len()
    }

    #[allow(dead_code)] // PK lookup for future queries / debugging
    pub fn find(&self, entity_id: u64) -> Option<u32> {
        self.pk.get(&entity_id).copied()
    }

    /// Rent slots that list `player` on their whitelist.
    pub fn by_player(&self, player: u64) -> &[u32] {
        self.by_player
            .get(&player)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub fn upsert(&mut self, row: RentRow) {
        if let Some(&slot) = self.pk.get(&row.entity_id) {
            let i = slot as usize;
            let old_list = std::mem::replace(&mut self.white_list[i], Box::from([]));
            let new_list = row.white_list.clone();
            self.write_at(slot, row);
            self.reindex_players(slot, &old_list, &new_list);
            return;
        }
        let entity_id = row.entity_id;
        let list = row.white_list.clone();
        let slot = self.alloc_slot();
        self.write_at(slot, row);
        self.pk.insert(entity_id, slot);
        for &player in list.iter() {
            self.by_player.entry(player).or_default().push(slot);
        }
    }

    pub fn delete(&mut self, entity_id: u64) {
        let Some(slot) = self.pk.remove(&entity_id) else {
            return;
        };
        let i = slot as usize;
        let list = self.white_list[i].clone();
        for &player in list.iter() {
            if let Some(vec) = self.by_player.get_mut(&player) {
                remove_one(vec, slot);
                if vec.is_empty() {
                    self.by_player.remove(&player);
                }
            }
        }
        self.entity_id[i] = 0;
        self.white_list[i] = Box::from([]);
        self.free_slots.push(slot);
    }

    fn alloc_slot(&mut self) -> u32 {
        if let Some(slot) = self.free_slots.pop() {
            slot
        } else {
            let slot = self.entity_id.len() as u32;
            self.entity_id.push(0);
            self.dimension_network_id.push(0);
            self.claim_entity_id.push(0);
            self.white_list.push(Box::from([]));
            self.active.push(false);
            slot
        }
    }

    fn write_at(&mut self, slot: u32, row: RentRow) {
        let i = slot as usize;
        self.entity_id[i] = row.entity_id;
        self.dimension_network_id[i] = row.dimension_network_id;
        self.claim_entity_id[i] = row.claim_entity_id;
        self.white_list[i] = row.white_list;
        self.active[i] = row.active;
    }

    fn reindex_players(&mut self, slot: u32, old: &[u64], new: &[u64]) {
        for &player in old {
            if !new.contains(&player) {
                if let Some(vec) = self.by_player.get_mut(&player) {
                    remove_one(vec, slot);
                    if vec.is_empty() {
                        self.by_player.remove(&player);
                    }
                }
            }
        }
        for &player in new {
            if !old.contains(&player) {
                self.by_player.entry(player).or_default().push(slot);
            }
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

    fn row(id: u64, network: u64, players: &[u64]) -> RentRow {
        RentRow {
            entity_id: id,
            dimension_network_id: network,
            claim_entity_id: 1,
            white_list: players.into(),
            active: true,
        }
    }

    #[test]
    fn by_player_tracks_whitelist() {
        let mut s = RentSoA::with_capacity(4);
        s.upsert(row(10, 100, &[1, 2]));
        s.upsert(row(11, 101, &[2]));
        assert_eq!(s.by_player(1).len(), 1);
        assert_eq!(s.by_player(2).len(), 2);
        s.upsert(row(10, 100, &[1]));
        assert_eq!(s.by_player(2).len(), 1);
        s.delete(11);
        assert!(s.by_player(2).is_empty());
    }
}
