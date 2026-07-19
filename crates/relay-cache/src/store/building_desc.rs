// SPDX-License-Identifier: MIT

//! Lookup store for `building_desc` (catalog id → name + storage flag).

use hashbrown::HashMap;

use crate::decode::BuildingDescRow;

struct Entry {
    name: Box<str>,
    is_storage: bool,
    /// Town Bank / bank-category buildings — personal storage; omitted
    /// from the claim inventory rollup (matches BitJita's policy).
    is_bank: bool,
}

pub struct BuildingDescStore {
    by_id: HashMap<i32, Entry>,
}

fn name_is_bank(name: &str) -> bool {
    name.to_ascii_lowercase().contains("bank")
}

impl BuildingDescStore {
    pub fn new() -> Self {
        Self {
            by_id: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    pub fn get(&self, id: i32) -> Option<&str> {
        self.by_id.get(&id).map(|e| e.name.as_ref())
    }

    /// Claim-inventory inclusion: storage-capable and not a bank.
    /// Missing ids are treated as excluded.
    pub fn include_in_claim_inventory(&self, id: i32) -> bool {
        self.by_id
            .get(&id)
            .is_some_and(|e| e.is_storage && !e.is_bank)
    }

    pub fn upsert(&mut self, row: BuildingDescRow) {
        self.by_id.insert(
            row.id,
            Entry {
                is_bank: name_is_bank(&row.name),
                name: Box::from(row.name.as_str()),
                is_storage: row.is_storage,
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

    fn row(id: i32, name: &str, is_storage: bool) -> BuildingDescRow {
        BuildingDescRow {
            id,
            name: name.into(),
            is_storage,
        }
    }

    #[test]
    fn upsert_get_overwrite_delete() {
        let mut s = BuildingDescStore::new();
        s.upsert(row(1007, "Storage Hut", true));
        assert_eq!(s.get(1007), Some("Storage Hut"));
        assert!(s.include_in_claim_inventory(1007));
        assert_eq!(s.len(), 1);
        s.upsert(row(1007, "Storage Chest", true));
        assert_eq!(s.get(1007), Some("Storage Chest"));
        assert_eq!(s.len(), 1);
        s.delete(1007);
        assert!(s.get(1007).is_none());
        assert!(!s.include_in_claim_inventory(1007));
        assert_eq!(s.len(), 0);
    }

    #[test]
    fn town_bank_excluded_despite_storage_slots() {
        let mut s = BuildingDescStore::new();
        s.upsert(row(985246037, "Town Bank", true));
        assert_eq!(s.get(985246037), Some("Town Bank"));
        assert!(!s.include_in_claim_inventory(985246037));
    }

    #[test]
    fn non_storage_and_missing() {
        let mut s = BuildingDescStore::new();
        s.upsert(row(405, "Settlement Totem", false));
        assert_eq!(s.get(405), Some("Settlement Totem"));
        assert!(!s.include_in_claim_inventory(405));
        assert!(!s.include_in_claim_inventory(999));
    }

    #[test]
    fn delete_missing_is_noop() {
        let mut s = BuildingDescStore::new();
        s.upsert(row(1, "a", false));
        s.delete(999);
        assert_eq!(s.len(), 1);
    }
}
