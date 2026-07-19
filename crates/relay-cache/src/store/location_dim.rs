// SPDX-License-Identifier: MIT

//! Interior-only entity → dimension map from `location_state`.
//!
//! We subscribe with `WHERE dimension != 1` so overworld rows never land
//! here. Buildings absent from this map are treated as overworld (1).

use hashbrown::HashMap;

use crate::decode::{LocationDimRow, OVERWORLD_DIMENSION};

pub struct LocationDimStore {
    by_entity: HashMap<u64, u32>,
}

impl LocationDimStore {
    pub fn new() -> Self {
        Self {
            by_entity: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.by_entity.len()
    }

    /// Dimension for `entity_id`, or overworld when unknown.
    pub fn get_or_overworld(&self, entity_id: u64) -> u32 {
        self.by_entity
            .get(&entity_id)
            .copied()
            .unwrap_or(OVERWORLD_DIMENSION)
    }

    pub fn upsert(&mut self, row: LocationDimRow) {
        if row.dimension == OVERWORLD_DIMENSION {
            self.by_entity.remove(&row.entity_id);
            return;
        }
        self.by_entity.insert(row.entity_id, row.dimension);
    }

    pub fn delete(&mut self, entity_id: u64) {
        self.by_entity.remove(&entity_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(entity_id: u64, dimension: u32) -> LocationDimRow {
        LocationDimRow {
            entity_id,
            dimension,
        }
    }

    #[test]
    fn missing_defaults_to_overworld() {
        let s = LocationDimStore::new();
        assert_eq!(s.get_or_overworld(42), OVERWORLD_DIMENSION);
    }

    #[test]
    fn upsert_interior_and_delete() {
        let mut s = LocationDimStore::new();
        s.upsert(row(100, 1649));
        assert_eq!(s.get_or_overworld(100), 1649);
        assert_eq!(s.len(), 1);
        s.delete(100);
        assert_eq!(s.get_or_overworld(100), OVERWORLD_DIMENSION);
        assert_eq!(s.len(), 0);
    }

    #[test]
    fn upsert_overworld_clears_entry() {
        let mut s = LocationDimStore::new();
        s.upsert(row(100, 1649));
        s.upsert(row(100, OVERWORLD_DIMENSION));
        assert_eq!(s.get_or_overworld(100), OVERWORLD_DIMENSION);
        assert_eq!(s.len(), 0);
    }
}
