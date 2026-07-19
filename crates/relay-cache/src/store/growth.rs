// SPDX-License-Identifier: MIT

//! Lookup store for `growth_state` (public growth / respawn countdowns).
//!
//! Hexite Deposits use this instead of `resource_spawn_timer`: depleted
//! entities carry `end_timestamp` while a growth recipe runs
//! (Depleted → Hexite over 6–8 days).

use hashbrown::HashMap;

use crate::decode::GrowthRow;

pub struct GrowthStore {
    /// entity_id → end_timestamp micros since unix epoch.
    by_entity: HashMap<u64, i64>,
}

impl GrowthStore {
    pub fn new() -> Self {
        Self {
            by_entity: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.by_entity.len()
    }

    pub fn end_timestamp_micros(&self, entity_id: u64) -> Option<i64> {
        self.by_entity.get(&entity_id).copied()
    }

    pub fn upsert(&mut self, row: GrowthRow) {
        self.by_entity
            .insert(row.entity_id, row.end_timestamp_micros);
    }

    pub fn delete(&mut self, entity_id: u64) {
        self.by_entity.remove(&entity_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upsert_get_delete() {
        let mut s = GrowthStore::new();
        s.upsert(GrowthRow {
            entity_id: 10,
            end_timestamp_micros: 1_000_000,
            growth_recipe_id: 42,
        });
        assert_eq!(s.end_timestamp_micros(10), Some(1_000_000));
        s.delete(10);
        assert_eq!(s.end_timestamp_micros(10), None);
    }
}
