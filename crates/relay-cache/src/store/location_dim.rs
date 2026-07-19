// SPDX-License-Identifier: MIT

//! Interior-only entity → dimension map from `location_state`.
//!
//! We subscribe with `WHERE dimension != 1` so overworld rows never land
//! here. Buildings absent from this map are treated as overworld (1).
//!
//! Secondary `by_dimension` supports player-housing inventory: interior
//! storages often have `claim_entity_id = 0`, so they must be found by
//! dimension rather than claim.

use hashbrown::HashMap;

use crate::decode::{LocationDimRow, OVERWORLD_DIMENSION};

pub struct LocationDimStore {
    by_entity: HashMap<u64, u32>,
    by_dimension: HashMap<u32, Vec<u64>>,
}

impl LocationDimStore {
    pub fn new() -> Self {
        Self {
            by_entity: HashMap::new(),
            by_dimension: HashMap::new(),
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

    /// All entity ids currently mapped to `dimension` (interior only).
    pub fn entities_in(&self, dimension: u32) -> &[u64] {
        self.by_dimension
            .get(&dimension)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub fn upsert(&mut self, row: LocationDimRow) {
        if row.dimension == OVERWORLD_DIMENSION {
            self.remove_entity(row.entity_id);
            return;
        }
        if let Some(old_dim) = self.by_entity.insert(row.entity_id, row.dimension) {
            if old_dim != row.dimension {
                remove_from_dim(&mut self.by_dimension, old_dim, row.entity_id);
                self.by_dimension
                    .entry(row.dimension)
                    .or_default()
                    .push(row.entity_id);
            }
            return;
        }
        self.by_dimension
            .entry(row.dimension)
            .or_default()
            .push(row.entity_id);
    }

    pub fn delete(&mut self, entity_id: u64) {
        self.remove_entity(entity_id);
    }

    fn remove_entity(&mut self, entity_id: u64) {
        let Some(old_dim) = self.by_entity.remove(&entity_id) else {
            return;
        };
        remove_from_dim(&mut self.by_dimension, old_dim, entity_id);
    }
}

fn remove_from_dim(map: &mut HashMap<u32, Vec<u64>>, dim: u32, entity_id: u64) {
    if let Some(vec) = map.get_mut(&dim) {
        if let Some(idx) = vec.iter().position(|&e| e == entity_id) {
            vec.swap_remove(idx);
        }
        if vec.is_empty() {
            map.remove(&dim);
        }
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
        assert!(s.entities_in(1649).is_empty());
    }

    #[test]
    fn upsert_interior_and_delete() {
        let mut s = LocationDimStore::new();
        s.upsert(row(100, 1649));
        assert_eq!(s.get_or_overworld(100), 1649);
        assert_eq!(s.entities_in(1649), &[100]);
        assert_eq!(s.len(), 1);
        s.delete(100);
        assert_eq!(s.get_or_overworld(100), OVERWORLD_DIMENSION);
        assert!(s.entities_in(1649).is_empty());
        assert_eq!(s.len(), 0);
    }

    #[test]
    fn upsert_overworld_clears_entry() {
        let mut s = LocationDimStore::new();
        s.upsert(row(100, 1649));
        s.upsert(row(100, OVERWORLD_DIMENSION));
        assert_eq!(s.get_or_overworld(100), OVERWORLD_DIMENSION);
        assert!(s.entities_in(1649).is_empty());
        assert_eq!(s.len(), 0);
    }

    #[test]
    fn move_between_dimensions_reindexes() {
        let mut s = LocationDimStore::new();
        s.upsert(row(100, 1649));
        s.upsert(row(100, 2155));
        assert!(s.entities_in(1649).is_empty());
        assert_eq!(s.entities_in(2155), &[100]);
        assert_eq!(s.get_or_overworld(100), 2155);
    }
}
