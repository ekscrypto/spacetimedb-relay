// SPDX-License-Identifier: MIT

//! Lookup store for `dimension_network_state`
//! (`entrance_dimension_id` / `entity_id` → entrance building + claim + rent).

use hashbrown::HashMap;

use crate::decode::DimensionNetworkRow;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkEntry {
    pub entity_id: u64,
    pub building_id: u64,
    pub claim_entity_id: u64,
    pub rent_entity_id: u64,
    pub entrance_dimension_id: u32,
    pub is_collapsed: bool,
}

pub struct DimensionNetworkStore {
    by_entrance_dim: HashMap<u32, NetworkEntry>,
    by_entity_id: HashMap<u64, NetworkEntry>,
}

impl DimensionNetworkStore {
    pub fn new() -> Self {
        Self {
            by_entrance_dim: HashMap::new(),
            by_entity_id: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.by_entity_id.len()
    }

    pub fn by_entrance_dim(&self, dimension: u32) -> Option<&NetworkEntry> {
        self.by_entrance_dim.get(&dimension)
    }

    pub fn by_entity_id(&self, entity_id: u64) -> Option<&NetworkEntry> {
        self.by_entity_id.get(&entity_id)
    }

    pub fn upsert(&mut self, row: DimensionNetworkRow) {
        if let Some(old) = self.by_entity_id.remove(&row.entity_id) {
            if self
                .by_entrance_dim
                .get(&old.entrance_dimension_id)
                .is_some_and(|e| e.entity_id == row.entity_id)
            {
                self.by_entrance_dim.remove(&old.entrance_dimension_id);
            }
        }
        let entry = NetworkEntry {
            entity_id: row.entity_id,
            building_id: row.building_id,
            claim_entity_id: row.claim_entity_id,
            rent_entity_id: row.rent_entity_id,
            entrance_dimension_id: row.entrance_dimension_id,
            is_collapsed: row.is_collapsed,
        };
        self.by_entrance_dim
            .insert(row.entrance_dimension_id, entry.clone());
        self.by_entity_id.insert(row.entity_id, entry);
    }

    pub fn delete_by_entity(&mut self, entity_id: u64) {
        let Some(old) = self.by_entity_id.remove(&entity_id) else {
            return;
        };
        if self
            .by_entrance_dim
            .get(&old.entrance_dimension_id)
            .is_some_and(|e| e.entity_id == entity_id)
        {
            self.by_entrance_dim.remove(&old.entrance_dimension_id);
        }
    }

    /// Legacy helper used by older call sites that only know the entrance dim.
    #[allow(dead_code)]
    pub fn delete(&mut self, entrance_dimension_id: u32) {
        let Some(old) = self.by_entrance_dim.remove(&entrance_dimension_id) else {
            return;
        };
        self.by_entity_id.remove(&old.entity_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(entity: u64, dim: u32, building: u64, claim: u64, rent: u64) -> DimensionNetworkRow {
        DimensionNetworkRow {
            entity_id: entity,
            building_id: building,
            claim_entity_id: claim,
            rent_entity_id: rent,
            entrance_dimension_id: dim,
            is_collapsed: false,
        }
    }

    #[test]
    fn upsert_get_overwrite_delete() {
        let mut s = DimensionNetworkStore::new();
        s.upsert(row(9, 1649, 100, 200, 3));
        let e = s.by_entrance_dim(1649).unwrap();
        assert_eq!(e.building_id, 100);
        assert_eq!(e.entity_id, 9);
        assert_eq!(s.by_entity_id(9).unwrap().rent_entity_id, 3);
        assert_eq!(s.len(), 1);
        s.upsert(row(9, 1649, 101, 200, 3));
        assert_eq!(s.by_entrance_dim(1649).unwrap().building_id, 101);
        s.delete_by_entity(9);
        assert!(s.by_entrance_dim(1649).is_none());
        assert_eq!(s.len(), 0);
    }

    #[test]
    fn delete_missing_is_noop() {
        let mut s = DimensionNetworkStore::new();
        s.upsert(row(1, 1, 1, 1, 1));
        s.delete(999);
        assert_eq!(s.len(), 1);
    }
}
