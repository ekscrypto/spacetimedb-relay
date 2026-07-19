// SPDX-License-Identifier: MIT

//! Lookup store for `dimension_network_state`
//! (`entrance_dimension_id` → entrance building + claim).

use hashbrown::HashMap;

use crate::decode::DimensionNetworkRow;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkEntry {
    pub building_id: u64,
    pub claim_entity_id: u64,
    pub is_collapsed: bool,
}

pub struct DimensionNetworkStore {
    by_entrance_dim: HashMap<u32, NetworkEntry>,
}

impl DimensionNetworkStore {
    pub fn new() -> Self {
        Self {
            by_entrance_dim: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.by_entrance_dim.len()
    }

    pub fn by_entrance_dim(&self, dimension: u32) -> Option<&NetworkEntry> {
        self.by_entrance_dim.get(&dimension)
    }

    pub fn upsert(&mut self, row: DimensionNetworkRow) {
        self.by_entrance_dim.insert(
            row.entrance_dimension_id,
            NetworkEntry {
                building_id: row.building_id,
                claim_entity_id: row.claim_entity_id,
                is_collapsed: row.is_collapsed,
            },
        );
    }

    pub fn delete(&mut self, entrance_dimension_id: u32) {
        self.by_entrance_dim.remove(&entrance_dimension_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(dim: u32, building: u64, claim: u64) -> DimensionNetworkRow {
        DimensionNetworkRow {
            building_id: building,
            claim_entity_id: claim,
            entrance_dimension_id: dim,
            is_collapsed: false,
        }
    }

    #[test]
    fn upsert_get_overwrite_delete() {
        let mut s = DimensionNetworkStore::new();
        s.upsert(row(1649, 100, 200));
        let e = s.by_entrance_dim(1649).unwrap();
        assert_eq!(e.building_id, 100);
        assert_eq!(e.claim_entity_id, 200);
        assert_eq!(s.len(), 1);
        s.upsert(row(1649, 101, 200));
        assert_eq!(s.by_entrance_dim(1649).unwrap().building_id, 101);
        s.delete(1649);
        assert!(s.by_entrance_dim(1649).is_none());
        assert_eq!(s.len(), 0);
    }

    #[test]
    fn delete_missing_is_noop() {
        let mut s = DimensionNetworkStore::new();
        s.upsert(row(1, 1, 1));
        s.delete(999);
        assert_eq!(s.len(), 1);
    }
}
