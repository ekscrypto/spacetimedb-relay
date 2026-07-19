// SPDX-License-Identifier: MIT

//! Stores for `claim_tech_state` and `claim_tech_desc`.

use hashbrown::HashMap;

use crate::decode::{ClaimTechDescRow, ClaimTechStateRow};

pub struct ClaimTechStateStore {
    by_entity: HashMap<u64, ClaimTechStateEntry>,
}

pub struct ClaimTechStateEntry {
    pub learned: Box<[i32]>,
    pub researching: i32,
    pub start_timestamp_micros: i64,
}

impl ClaimTechStateStore {
    pub fn new() -> Self {
        Self {
            by_entity: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.by_entity.len()
    }

    pub fn get(&self, entity_id: u64) -> Option<&ClaimTechStateEntry> {
        self.by_entity.get(&entity_id)
    }

    pub fn upsert(&mut self, row: ClaimTechStateRow) {
        self.by_entity.insert(
            row.entity_id,
            ClaimTechStateEntry {
                learned: row.learned,
                researching: row.researching,
                start_timestamp_micros: row.start_timestamp_micros,
            },
        );
    }

    pub fn delete(&mut self, entity_id: u64) {
        self.by_entity.remove(&entity_id);
    }
}

pub struct ClaimTechDescStore {
    by_id: HashMap<i32, ClaimTechDescEntry>,
}

#[derive(Clone)]
pub struct ClaimTechDescEntry {
    pub id: i32,
    pub name: Box<str>,
    pub description: Box<str>,
    pub tier: i32,
    pub tech_type: Box<str>,
    pub supplies_cost: i32,
    pub research_time: i32,
    pub requirements: Box<[i32]>,
    pub members: i32,
    pub area: i32,
    pub unlocks_techs: Box<[i32]>,
}

impl ClaimTechDescStore {
    pub fn new() -> Self {
        Self {
            by_id: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    pub fn get(&self, id: i32) -> Option<&ClaimTechDescEntry> {
        self.by_id.get(&id)
    }

    pub fn upsert(&mut self, row: ClaimTechDescRow) {
        self.by_id.insert(
            row.id,
            ClaimTechDescEntry {
                id: row.id,
                name: Box::from(row.name.as_str()),
                description: Box::from(row.description.as_str()),
                tier: row.tier,
                tech_type: Box::from(row.tech_type.as_str()),
                supplies_cost: row.supplies_cost,
                research_time: row.research_time,
                requirements: row.requirements,
                members: row.members,
                area: row.area,
                unlocks_techs: row.unlocks_techs,
            },
        );
    }

    pub fn delete(&mut self, id: i32) {
        self.by_id.remove(&id);
    }
}

/// Highest N in `2..=10` where a learned tech's name or description is
/// exactly `Tier {N}` (matches bitcraft-httpd / BitJita claim_tier).
pub fn claim_tier_from_descs<'a>(
    learned: &[i32],
    descs: impl Fn(i32) -> Option<&'a ClaimTechDescEntry>,
) -> Option<i32> {
    (2..=10).rev().find(|&tier| {
        let label = format!("Tier {tier}");
        learned.iter().any(|&id| {
            descs(id).is_some_and(|d| d.name.as_ref() == label || d.description.as_ref() == label)
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claim_tier_picks_highest() {
        let mut store = ClaimTechDescStore::new();
        store.upsert(ClaimTechDescRow {
            id: 200,
            name: "Tier 2".into(),
            description: "Unlocks…".into(),
            tier: 2,
            tech_type: "tier_upgrade".into(),
            supplies_cost: 0,
            research_time: 0,
            requirements: Box::from([]),
            members: 0,
            area: 0,
            unlocks_techs: Box::from([]),
        });
        store.upsert(ClaimTechDescRow {
            id: 700,
            name: "Tier 7".into(),
            description: "Unlocks…".into(),
            tier: 7,
            tech_type: "tier_upgrade".into(),
            supplies_cost: 0,
            research_time: 0,
            requirements: Box::from([]),
            members: 0,
            area: 0,
            unlocks_techs: Box::from([]),
        });
        assert_eq!(
            claim_tier_from_descs(&[200, 700], |id| store.get(id)),
            Some(7)
        );
        assert_eq!(claim_tier_from_descs(&[200], |id| store.get(id)), Some(2));
        assert_eq!(claim_tier_from_descs(&[], |id| store.get(id)), None);
    }
}
