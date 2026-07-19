// SPDX-License-Identifier: MIT

//! Columnar store for `claim_state`.
//!
//! Shape: `entity_id (U64, PK), owner_player_entity_id (U64),
//! owner_building_entity_id (U64), name (String), neutral (Bool)`.
//!
//! Tiny table (~10K rows per region × 13 regions ≈ 130K total), so the
//! hot query — case-insensitive substring search on `name` — is a linear
//! scan over the PK index. Not worth a trigram or sorted-names index at
//! this scale; the scan is sub-millisecond even on the full fleet.
//!
//! ## Index strategy
//!
//! `hashbrown::HashMap<u64, u32>` for the PK: key stored alongside the
//! slot, no closure gymnastics to recover the key from the column. The
//! secondary indexes on `building_state` and `inventory_state` use
//! `HashMap<key, Vec<u32>>` for the multi-valued case (multiple rows per
//! claim / owner).

use hashbrown::HashMap;

use crate::decode::ClaimRow;

pub struct ClaimSoA {
    pub entity_id: Vec<u64>,
    pub owner_player_entity_id: Vec<u64>,
    pub owner_building_entity_id: Vec<u64>,
    pub name: Vec<Box<str>>,
    pub neutral: Vec<bool>,
    free_slots: Vec<u32>,
    /// PK index: `entity_id → slot`. O(1) lookup.
    pk: HashMap<u64, u32>,
}

impl ClaimSoA {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            entity_id: Vec::with_capacity(cap),
            owner_player_entity_id: Vec::with_capacity(cap),
            owner_building_entity_id: Vec::with_capacity(cap),
            name: Vec::with_capacity(cap),
            neutral: Vec::with_capacity(cap),
            free_slots: Vec::new(),
            pk: HashMap::with_capacity(cap),
        }
    }

    pub fn len(&self) -> usize {
        self.pk.len()
    }

    /// PK lookup. O(1).
    pub fn find(&self, entity_id: u64) -> Option<u32> {
        self.pk.get(&entity_id).copied()
    }

    /// Insert or replace a row. If a row with the same `entity_id` already
    /// exists, overwrite its columns in place; otherwise allocate a new
    /// slot (reusing the free-list if non-empty).
    pub fn upsert(&mut self, row: ClaimRow) {
        if let Some(&slot) = self.pk.get(&row.entity_id) {
            self.write_at(slot, &row);
            return;
        }
        let slot = self.alloc_slot();
        self.write_at(slot, &row);
        self.pk.insert(row.entity_id, slot);
    }

    /// Delete the row with this `entity_id`, if present. Frees its slot.
    pub fn delete(&mut self, entity_id: u64) {
        let Some(slot) = self.pk.remove(&entity_id) else {
            return;
        };
        self.entity_id[slot as usize] = 0;
        self.free_slots.push(slot);
    }

    /// Case-insensitive substring search across the name column.
    /// Returns slots of matching rows. O(rows).
    pub fn search_name(&self, needle: &str) -> Vec<u32> {
        if needle.is_empty() {
            return Vec::new();
        }
        let mut needle_buf = vec![0u8; needle.len()];
        for (i, b) in needle.as_bytes().iter().enumerate() {
            needle_buf[i] = b.to_ascii_lowercase();
        }
        let needle_lower = std::str::from_utf8(&needle_buf).unwrap_or(needle);

        // Walk the PK index so we skip freed slots without a tombstone.
        let mut hits = Vec::new();
        for &slot in self.pk.values() {
            if contains_ascii_ci(&self.name[slot as usize], needle_lower) {
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
            self.owner_player_entity_id.push(0);
            self.owner_building_entity_id.push(0);
            self.name.push(Box::from(""));
            self.neutral.push(false);
            slot
        }
    }

    fn write_at(&mut self, slot: u32, row: &ClaimRow) {
        let i = slot as usize;
        self.entity_id[i] = row.entity_id;
        self.owner_player_entity_id[i] = row.owner_player_entity_id;
        self.owner_building_entity_id[i] = row.owner_building_entity_id;
        self.name[i] = Box::from(row.name.as_str());
        self.neutral[i] = row.neutral;
    }
}

/// `haystack.contains(needle)` ignoring ASCII case. The needle is already
/// ASCII-lowercased by the caller to avoid a per-row allocation.
fn contains_ascii_ci(haystack: &str, needle_lower: &str) -> bool {
    let h = haystack.as_bytes();
    let n = needle_lower.as_bytes();
    if n.is_empty() {
        return true;
    }
    if h.len() < n.len() {
        return false;
    }
    for window in h.windows(n.len()) {
        if window
            .iter()
            .zip(n.iter())
            .all(|(a, b)| a.to_ascii_lowercase() == *b)
        {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(id: u64, name: &str) -> ClaimRow {
        ClaimRow {
            entity_id: id,
            owner_player_entity_id: 0,
            owner_building_entity_id: 0,
            name: name.into(),
            neutral: false,
        }
    }

    #[test]
    fn upsert_insert_and_lookup() {
        let mut s = ClaimSoA::with_capacity(4);
        s.upsert(row(100, "UMB Concordia"));
        let slot = s.find(100).expect("just inserted");
        assert_eq!(s.entity_id[slot as usize], 100);
        assert_eq!(&*s.name[slot as usize], "UMB Concordia");
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn upsert_overwrite_in_place() {
        let mut s = ClaimSoA::with_capacity(4);
        s.upsert(row(100, "old name"));
        s.upsert(row(100, "new name"));
        assert_eq!(s.len(), 1);
        let slot = s.find(100).unwrap();
        assert_eq!(&*s.name[slot as usize], "new name");
    }

    #[test]
    fn delete_frees_slot_for_reuse() {
        let mut s = ClaimSoA::with_capacity(4);
        s.upsert(row(1, "a"));
        s.upsert(row(2, "b"));
        let len_before = s.entity_id.len();
        s.delete(1);
        assert!(s.find(1).is_none());
        assert_eq!(s.len(), 1);
        s.upsert(row(3, "c"));
        assert_eq!(s.entity_id.len(), len_before);
        assert!(s.find(3).is_some());
    }

    #[test]
    fn delete_missing_is_noop() {
        let mut s = ClaimSoA::with_capacity(4);
        s.upsert(row(1, "a"));
        s.delete(999);
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn search_name_case_insensitive_substring() {
        let mut s = ClaimSoA::with_capacity(8);
        s.upsert(row(1, "UMB Concordia"));
        s.upsert(row(2, "umb cohort"));
        s.upsert(row(3, "Beta Outpost"));
        s.upsert(row(4, "Gamma"));

        let mut hits = s.search_name("umb");
        let mut names: Vec<&str> = hits
            .drain(..)
            .map(|slot| s.name[slot as usize].as_ref())
            .collect();
        names.sort();
        assert_eq!(names, ["UMB Concordia", "umb cohort"]);
    }

    #[test]
    fn search_name_empty_needle_returns_nothing() {
        let mut s = ClaimSoA::with_capacity(4);
        s.upsert(row(1, "anything"));
        assert!(s.search_name("").is_empty());
    }

    #[test]
    fn contains_ascii_ci_basic() {
        // Needle must already be ASCII-lowercased (search_name does that).
        assert!(contains_ascii_ci("UMB Concordia", "umb"));
        assert!(contains_ascii_ci("UMB Concordia", "concordia"));
        assert!(contains_ascii_ci("UMB Concordia", "ord"));
        assert!(!contains_ascii_ci("UMB", "xyz"));
        assert!(contains_ascii_ci("abc", ""));
        assert!(!contains_ascii_ci("ab", "abc"));
    }

    #[test]
    fn distinct_pks_get_distinct_slots() {
        let mut s = ClaimSoA::with_capacity(4);
        s.upsert(row(0xDEAD_BEEF, "first"));
        s.upsert(row(0xCAFE_F00D, "second"));
        let slot_a = s.find(0xDEAD_BEEF).expect("first id present");
        let slot_b = s.find(0xCAFE_F00D).expect("second id present");
        assert_ne!(slot_a, slot_b);
        assert_eq!(&*s.name[slot_a as usize], "first");
        assert_eq!(&*s.name[slot_b as usize], "second");
    }
}
