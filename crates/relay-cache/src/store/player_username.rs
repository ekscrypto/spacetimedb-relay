// SPDX-License-Identifier: MIT

//! Columnar store for `player_username_state`.

use hashbrown::HashMap;

use crate::decode::PlayerUsernameRow;
use crate::store::claim::contains_ascii_ci;

pub struct PlayerUsernameSoA {
    pub entity_id: Vec<u64>,
    pub username: Vec<Box<str>>,
    free_slots: Vec<u32>,
    pk: HashMap<u64, u32>,
}

impl PlayerUsernameSoA {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            entity_id: Vec::with_capacity(cap),
            username: Vec::with_capacity(cap),
            free_slots: Vec::new(),
            pk: HashMap::with_capacity(cap),
        }
    }

    pub fn len(&self) -> usize {
        self.pk.len()
    }

    pub fn find(&self, entity_id: u64) -> Option<u32> {
        self.pk.get(&entity_id).copied()
    }

    pub fn upsert(&mut self, row: PlayerUsernameRow) {
        if let Some(&slot) = self.pk.get(&row.entity_id) {
            self.write_at(slot, &row);
            return;
        }
        let slot = self.alloc_slot();
        self.write_at(slot, &row);
        self.pk.insert(row.entity_id, slot);
    }

    pub fn delete(&mut self, entity_id: u64) {
        let Some(slot) = self.pk.remove(&entity_id) else {
            return;
        };
        self.entity_id[slot as usize] = 0;
        self.username[slot as usize] = Box::from("");
        self.free_slots.push(slot);
    }

    /// Case-insensitive substring search on username. O(rows).
    pub fn search_name(&self, needle: &str) -> Vec<u32> {
        if needle.is_empty() {
            return Vec::new();
        }
        let mut needle_buf = vec![0u8; needle.len()];
        for (i, b) in needle.as_bytes().iter().enumerate() {
            needle_buf[i] = b.to_ascii_lowercase();
        }
        let needle_lower = std::str::from_utf8(&needle_buf).unwrap_or(needle);
        let mut hits = Vec::new();
        for &slot in self.pk.values() {
            if contains_ascii_ci(&self.username[slot as usize], needle_lower) {
                hits.push(slot);
            }
        }
        hits
    }

    fn alloc_slot(&mut self) -> u32 {
        if let Some(slot) = self.free_slots.pop() {
            slot
        } else {
            let slot = self.entity_id.len() as u32;
            self.entity_id.push(0);
            self.username.push(Box::from(""));
            slot
        }
    }

    fn write_at(&mut self, slot: u32, row: &PlayerUsernameRow) {
        let i = slot as usize;
        self.entity_id[i] = row.entity_id;
        self.username[i] = Box::from(row.username.as_str());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(id: u64, name: &str) -> PlayerUsernameRow {
        PlayerUsernameRow {
            entity_id: id,
            username: name.into(),
        }
    }

    #[test]
    fn search_and_pk() {
        let mut s = PlayerUsernameSoA::with_capacity(4);
        s.upsert(row(1, "Maplesugar"));
        s.upsert(row(2, "mapleleaf"));
        s.upsert(row(3, "Other"));
        let mut hits = s.search_name("maple");
        hits.sort();
        assert_eq!(hits.len(), 2);
        assert_eq!(s.find(1).map(|slot| s.username[slot as usize].as_ref()), Some("Maplesugar"));
    }
}
