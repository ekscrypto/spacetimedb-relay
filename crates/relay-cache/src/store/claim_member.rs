// SPDX-License-Identifier: MIT

//! Columnar store for `claim_member_state` with by-claim secondary index.

use hashbrown::HashMap;

use crate::decode::ClaimMemberRow;

pub struct ClaimMemberSoA {
    pub entity_id: Vec<u64>,
    pub claim_entity_id: Vec<u64>,
    pub player_entity_id: Vec<u64>,
    pub user_name: Vec<Box<str>>,
    pub inventory_permission: Vec<bool>,
    pub build_permission: Vec<bool>,
    pub officer_permission: Vec<bool>,
    pub co_owner_permission: Vec<bool>,
    free_slots: Vec<u32>,
    pk: HashMap<u64, u32>,
    by_claim: HashMap<u64, Vec<u32>>,
}

impl ClaimMemberSoA {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            entity_id: Vec::with_capacity(cap),
            claim_entity_id: Vec::with_capacity(cap),
            player_entity_id: Vec::with_capacity(cap),
            user_name: Vec::with_capacity(cap),
            inventory_permission: Vec::with_capacity(cap),
            build_permission: Vec::with_capacity(cap),
            officer_permission: Vec::with_capacity(cap),
            co_owner_permission: Vec::with_capacity(cap),
            free_slots: Vec::new(),
            pk: HashMap::with_capacity(cap),
            by_claim: HashMap::with_capacity(cap),
        }
    }

    pub fn len(&self) -> usize {
        self.pk.len()
    }

    pub fn by_claim(&self, claim: u64) -> &[u32] {
        self.by_claim.get(&claim).map(Vec::as_slice).unwrap_or(&[])
    }

    pub fn upsert(&mut self, row: ClaimMemberRow) {
        if let Some(&slot) = self.pk.get(&row.entity_id) {
            let i = slot as usize;
            let old_claim = self.claim_entity_id[i];
            self.write_at(slot, &row);
            if old_claim != row.claim_entity_id {
                self.reindex_by_claim(slot, old_claim, row.claim_entity_id);
            }
            return;
        }
        let claim = row.claim_entity_id;
        let slot = self.alloc_slot();
        self.write_at(slot, &row);
        self.pk.insert(row.entity_id, slot);
        self.by_claim.entry(claim).or_default().push(slot);
    }

    pub fn delete(&mut self, entity_id: u64) {
        let Some(slot) = self.pk.remove(&entity_id) else {
            return;
        };
        let i = slot as usize;
        let claim = self.claim_entity_id[i];
        if let Some(vec) = self.by_claim.get_mut(&claim) {
            remove_one(vec, slot);
            if vec.is_empty() {
                self.by_claim.remove(&claim);
            }
        }
        self.entity_id[i] = 0;
        self.user_name[i] = Box::from("");
        self.free_slots.push(slot);
    }

    fn alloc_slot(&mut self) -> u32 {
        if let Some(slot) = self.free_slots.pop() {
            slot
        } else {
            let slot = self.entity_id.len() as u32;
            self.entity_id.push(0);
            self.claim_entity_id.push(0);
            self.player_entity_id.push(0);
            self.user_name.push(Box::from(""));
            self.inventory_permission.push(false);
            self.build_permission.push(false);
            self.officer_permission.push(false);
            self.co_owner_permission.push(false);
            slot
        }
    }

    fn write_at(&mut self, slot: u32, row: &ClaimMemberRow) {
        let i = slot as usize;
        self.entity_id[i] = row.entity_id;
        self.claim_entity_id[i] = row.claim_entity_id;
        self.player_entity_id[i] = row.player_entity_id;
        self.user_name[i] = Box::from(row.user_name.as_str());
        self.inventory_permission[i] = row.inventory_permission;
        self.build_permission[i] = row.build_permission;
        self.officer_permission[i] = row.officer_permission;
        self.co_owner_permission[i] = row.co_owner_permission;
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
}

fn remove_one(vec: &mut Vec<u32>, val: u32) {
    if let Some(idx) = vec.iter().position(|&v| v == val) {
        vec.swap_remove(idx);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(id: u64, claim: u64, player: u64, name: &str) -> ClaimMemberRow {
        ClaimMemberRow {
            entity_id: id,
            claim_entity_id: claim,
            player_entity_id: player,
            user_name: name.into(),
            inventory_permission: true,
            build_permission: false,
            officer_permission: false,
            co_owner_permission: false,
        }
    }

    #[test]
    fn by_claim_index() {
        let mut s = ClaimMemberSoA::with_capacity(4);
        s.upsert(row(1, 100, 10, "A"));
        s.upsert(row(2, 100, 11, "B"));
        s.upsert(row(3, 200, 12, "C"));
        assert_eq!(s.by_claim(100).len(), 2);
        assert_eq!(s.by_claim(200).len(), 1);
        s.upsert(row(1, 200, 10, "A"));
        assert_eq!(s.by_claim(100).len(), 1);
        assert_eq!(s.by_claim(200).len(), 2);
        s.delete(2);
        assert!(s.by_claim(100).is_empty());
    }
}
