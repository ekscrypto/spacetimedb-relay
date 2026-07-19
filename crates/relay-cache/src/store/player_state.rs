// SPDX-License-Identifier: MIT

//! Columnar store for `player_state` (login / session timestamps).

use hashbrown::HashMap;

use crate::decode::PlayerStateRow;

pub struct PlayerStateSoA {
    pub entity_id: Vec<u64>,
    /// Unix seconds — BitCraft `sign_in_timestamp` (= BitJita lastLogin).
    pub sign_in_timestamp: Vec<i32>,
    pub session_start_timestamp: Vec<i32>,
    pub signed_in: Vec<bool>,
    free_slots: Vec<u32>,
    pk: HashMap<u64, u32>,
}

impl PlayerStateSoA {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            entity_id: Vec::with_capacity(cap),
            sign_in_timestamp: Vec::with_capacity(cap),
            session_start_timestamp: Vec::with_capacity(cap),
            signed_in: Vec::with_capacity(cap),
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

    /// Last-login unix seconds when known.
    pub fn last_login_timestamp(&self, entity_id: u64) -> Option<i64> {
        let slot = self.find(entity_id)?;
        let ts = self.sign_in_timestamp[slot as usize];
        if ts == 0 {
            None
        } else {
            Some(i64::from(ts))
        }
    }

    pub fn upsert(&mut self, row: PlayerStateRow) {
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
        self.free_slots.push(slot);
    }

    fn alloc_slot(&mut self) -> u32 {
        if let Some(slot) = self.free_slots.pop() {
            slot
        } else {
            let slot = self.entity_id.len() as u32;
            self.entity_id.push(0);
            self.sign_in_timestamp.push(0);
            self.session_start_timestamp.push(0);
            self.signed_in.push(false);
            slot
        }
    }

    fn write_at(&mut self, slot: u32, row: &PlayerStateRow) {
        let i = slot as usize;
        self.entity_id[i] = row.entity_id;
        self.sign_in_timestamp[i] = row.sign_in_timestamp;
        self.session_start_timestamp[i] = row.session_start_timestamp;
        self.signed_in[i] = row.signed_in;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn last_login_skips_zero() {
        let mut s = PlayerStateSoA::with_capacity(2);
        s.upsert(PlayerStateRow {
            entity_id: 1,
            sign_in_timestamp: 0,
            session_start_timestamp: 0,
            signed_in: false,
        });
        assert_eq!(s.last_login_timestamp(1), None);
        s.upsert(PlayerStateRow {
            entity_id: 1,
            sign_in_timestamp: 1_784_465_493,
            session_start_timestamp: 1_784_468_165,
            signed_in: true,
        });
        assert_eq!(s.last_login_timestamp(1), Some(1_784_465_493));
    }
}
