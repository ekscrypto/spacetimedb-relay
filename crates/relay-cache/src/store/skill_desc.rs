// SPDX-License-Identifier: MIT

//! Catalog store for `skill_desc`.

use hashbrown::HashMap;

use crate::decode::SkillDescRow;

pub struct SkillDescStore {
    by_id: HashMap<i32, SkillDescEntry>,
}

pub struct SkillDescEntry {
    pub name: Box<str>,
    #[allow(dead_code)]
    pub title: Box<str>,
    #[allow(dead_code)]
    pub max_level: i32,
}

impl SkillDescStore {
    pub fn new() -> Self {
        Self {
            by_id: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    #[allow(dead_code)]
    pub fn get(&self, id: i32) -> Option<&SkillDescEntry> {
        self.by_id.get(&id)
    }

    pub fn name(&self, id: i32) -> Option<&str> {
        self.by_id.get(&id).map(|e| e.name.as_ref())
    }

    pub fn upsert(&mut self, row: SkillDescRow) {
        self.by_id.insert(
            row.id,
            SkillDescEntry {
                name: Box::from(row.name.as_str()),
                title: Box::from(row.title.as_str()),
                max_level: row.max_level,
            },
        );
    }

    pub fn delete(&mut self, id: i32) {
        self.by_id.remove(&id);
    }
}
