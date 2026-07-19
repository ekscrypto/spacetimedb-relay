// SPDX-License-Identifier: MIT

//! Lookup store for `building_nickname_state` (entity_id → nickname).

use hashbrown::HashMap;

use crate::decode::BuildingNicknameRow;

pub struct BuildingNicknameStore {
    by_entity: HashMap<u64, Box<str>>,
}

impl BuildingNicknameStore {
    pub fn new() -> Self {
        Self {
            by_entity: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.by_entity.len()
    }

    pub fn get(&self, entity_id: u64) -> Option<&str> {
        self.by_entity.get(&entity_id).map(|s| s.as_ref())
    }

    pub fn upsert(&mut self, row: BuildingNicknameRow) {
        self.by_entity
            .insert(row.entity_id, Box::from(row.nickname.as_str()));
    }

    pub fn delete(&mut self, entity_id: u64) {
        self.by_entity.remove(&entity_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(entity_id: u64, nickname: &str) -> BuildingNicknameRow {
        BuildingNicknameRow {
            entity_id,
            nickname: nickname.into(),
        }
    }

    #[test]
    fn upsert_get_overwrite_delete() {
        let mut s = BuildingNicknameStore::new();
        s.upsert(row(100, "Bob's Chest"));
        assert_eq!(s.get(100), Some("Bob's Chest"));
        assert_eq!(s.len(), 1);
        s.upsert(row(100, "Alice's Chest"));
        assert_eq!(s.get(100), Some("Alice's Chest"));
        s.delete(100);
        assert!(s.get(100).is_none());
        assert_eq!(s.len(), 0);
    }

    #[test]
    fn delete_missing_is_noop() {
        let mut s = BuildingNicknameStore::new();
        s.upsert(row(1, "a"));
        s.delete(999);
        assert_eq!(s.len(), 1);
    }
}
