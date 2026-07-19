// SPDX-License-Identifier: MIT

//! Lookup store for `deployable_state` + thin catalog for `deployable_desc`.

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
        }
    }

    pub fn len(&self) -> usize {
        self.pk.len()
    }

    pub fn find(&self, entity_id: u64) -> Option<u32> {
        self.pk.get(&entity_id).copied()
    }

    pub fn upsert(&mut self, row: DeployableRow) {
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

        let mut d = DeployableDescStore::new();
        d.upsert(DeployableDescRow {
            id: 5,
            name: "Bird".into(),
            kind: DeployableKind::Mount,
        });
        assert_eq!(d.get(5), Some(("Bird", DeployableKind::Mount)));
    }
}
