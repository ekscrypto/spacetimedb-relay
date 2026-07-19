// SPDX-License-Identifier: MIT

//! Store for `experience_state` (player → skill XP stacks).

use hashbrown::HashMap;

use crate::decode::ExperienceRow;

pub struct ExperienceSoA {
    by_entity: HashMap<u64, Box<[(i32, i32)]>>,
}

impl ExperienceSoA {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            by_entity: HashMap::with_capacity(cap),
        }
    }

    pub fn len(&self) -> usize {
        self.by_entity.len()
    }

    pub fn get(&self, entity_id: u64) -> Option<&[(i32, i32)]> {
        self.by_entity.get(&entity_id).map(|b| b.as_ref())
    }

    pub fn upsert(&mut self, row: ExperienceRow) {
        self.by_entity.insert(row.entity_id, row.stacks);
    }

    pub fn delete(&mut self, entity_id: u64) {
        self.by_entity.remove(&entity_id);
    }
}
