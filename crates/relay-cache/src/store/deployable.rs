// SPDX-License-Identifier: MIT

//! Lookup store for `deployable_state_v2` + thin catalog for `deployable_desc`.
//!
//! Schema relationship for player-owned storage:
//!   `deployable_state_v2.owner_id`        → player entity
//!   `inventory_state.owner_entity_id`     → `deployable_state_v2.entity_id`
//!
//! Personal Cache / Cart / Mount / Boat inventories often leave
//! `inventory_state.player_owner_entity_id = 0`, so player inventory
//! queries must walk `by_owner(player)` on this store and then
//! `inventory.by_owner(deployable_entity_id)`.
//!
//! Prefer v2 over legacy `deployable_state`: newer boats (Skiff II+,
//! Clipper, …) are written only to v2.

use hashbrown::HashMap;

use crate::decode::{DeployableDescRow, DeployableKind, DeployableRow};

pub struct DeployableSoA {
    pub entity_id: Vec<u64>,
    pub owner_id: Vec<u64>,
    pub claim_entity_id: Vec<u64>,
    pub deployable_description_id: Vec<i32>,
    pub nickname: Vec<Box<str>>,
    free_slots: Vec<u32>,
    pk: HashMap<u64, u32>,
    by_owner: HashMap<u64, Vec<u32>>,
}

impl DeployableSoA {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            entity_id: Vec::with_capacity(cap),
            owner_id: Vec::with_capacity(cap),
            claim_entity_id: Vec::with_capacity(cap),
            deployable_description_id: Vec::with_capacity(cap),
            nickname: Vec::with_capacity(cap),
            free_slots: Vec::new(),
            pk: HashMap::with_capacity(cap),
            by_owner: HashMap::with_capacity(cap),
        }
    }

    pub fn len(&self) -> usize {
        self.pk.len()
    }

    pub fn find(&self, entity_id: u64) -> Option<u32> {
        self.pk.get(&entity_id).copied()
    }

    /// Deployables whose `owner_id == owner` (player entity).
    pub fn by_owner(&self, owner: u64) -> &[u32] {
        self.by_owner.get(&owner).map(Vec::as_slice).unwrap_or(&[])
    }

    pub fn upsert(&mut self, row: DeployableRow) {
        if let Some(&slot) = self.pk.get(&row.entity_id) {
            let old_owner = self.owner_id[slot as usize];
            let new_owner = row.owner_id;
            self.write_at(slot, &row);
            if old_owner != new_owner {
                self.reindex_by_owner(slot, old_owner, new_owner);
            }
            return;
        }
        let owner = row.owner_id;
        let slot = self.alloc_slot();
        self.write_at(slot, &row);
        self.pk.insert(row.entity_id, slot);
        if owner != 0 {
            self.by_owner.entry(owner).or_default().push(slot);
        }
    }

    pub fn delete(&mut self, entity_id: u64) {
        let Some(slot) = self.pk.remove(&entity_id) else {
            return;
        };
        let i = slot as usize;
        let owner = self.owner_id[i];
        if owner != 0 {
            if let Some(vec) = self.by_owner.get_mut(&owner) {
                vec.retain(|&s| s != slot);
                if vec.is_empty() {
                    self.by_owner.remove(&owner);
                }
            }
        }
        self.entity_id[i] = 0;
        self.owner_id[i] = 0;
        self.claim_entity_id[i] = 0;
        self.deployable_description_id[i] = 0;
        self.nickname[i] = Box::from("");
        self.free_slots.push(slot);
    }

    fn alloc_slot(&mut self) -> u32 {
        if let Some(slot) = self.free_slots.pop() {
            slot
        } else {
            let slot = self.entity_id.len() as u32;
            self.entity_id.push(0);
            self.owner_id.push(0);
            self.claim_entity_id.push(0);
            self.deployable_description_id.push(0);
            self.nickname.push(Box::from(""));
            slot
        }
    }

    fn write_at(&mut self, slot: u32, row: &DeployableRow) {
        let i = slot as usize;
        self.entity_id[i] = row.entity_id;
        self.owner_id[i] = row.owner_id;
        self.claim_entity_id[i] = row.claim_entity_id;
        self.deployable_description_id[i] = row.deployable_description_id;
        self.nickname[i] = Box::from(row.nickname.as_str());
    }

    fn reindex_by_owner(&mut self, slot: u32, old_owner: u64, new_owner: u64) {
        if old_owner != 0 {
            if let Some(vec) = self.by_owner.get_mut(&old_owner) {
                vec.retain(|&s| s != slot);
                if vec.is_empty() {
                    self.by_owner.remove(&old_owner);
                }
            }
        }
        if new_owner != 0 {
            self.by_owner.entry(new_owner).or_default().push(slot);
        }
    }
}

struct DescEntry {
    name: Box<str>,
    kind: DeployableKind,
}

pub struct DeployableDescStore {
    by_id: HashMap<i32, DescEntry>,
}

impl DeployableDescStore {
    pub fn new() -> Self {
        Self {
            by_id: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    pub fn get(&self, id: i32) -> Option<(&str, DeployableKind)> {
        self.by_id.get(&id).map(|e| (e.name.as_ref(), e.kind))
    }

    pub fn upsert(&mut self, row: DeployableDescRow) {
        self.by_id.insert(
            row.id,
            DescEntry {
                name: Box::from(row.name.as_str()),
                kind: row.kind,
            },
        );
    }

    pub fn delete(&mut self, id: i32) {
        self.by_id.remove(&id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deployable_pk_and_desc() {
        let mut s = DeployableSoA::with_capacity(2);
        s.upsert(DeployableRow {
            entity_id: 10,
            owner_id: 1,
            claim_entity_id: 2,
            deployable_description_id: 5,
            nickname: "Bird".into(),
        });
        assert_eq!(
            s.find(10).map(|slot| s.nickname[slot as usize].as_ref()),
            Some("Bird")
        );
        assert_eq!(s.by_owner(1).len(), 1);

        let mut d = DeployableDescStore::new();
        d.upsert(DeployableDescRow {
            id: 5,
            name: "Bird".into(),
            kind: DeployableKind::Mount,
        });
        assert_eq!(d.get(5), Some(("Bird", DeployableKind::Mount)));
    }

    #[test]
    fn by_owner_tracks_player_and_reindexes() {
        let mut s = DeployableSoA::with_capacity(2);
        s.upsert(DeployableRow {
            entity_id: 10,
            owner_id: 7,
            claim_entity_id: 0,
            deployable_description_id: 1,
            nickname: String::new(),
        });
        s.upsert(DeployableRow {
            entity_id: 11,
            owner_id: 7,
            claim_entity_id: 0,
            deployable_description_id: 2,
            nickname: String::new(),
        });
        assert_eq!(s.by_owner(7).len(), 2);

        s.upsert(DeployableRow {
            entity_id: 10,
            owner_id: 8,
            claim_entity_id: 0,
            deployable_description_id: 1,
            nickname: String::new(),
        });
        assert_eq!(s.by_owner(7).len(), 1);
        assert_eq!(s.by_owner(8).len(), 1);

        s.delete(11);
        assert!(s.by_owner(7).is_empty());
        assert_eq!(s.by_owner(8).len(), 1);
    }
}
