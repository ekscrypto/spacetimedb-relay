// SPDX-License-Identifier: MIT

//! Columnar store for `mobile_entity_state` (last-active proxy).
//!
//! BitCraft zeros `player_state.sign_in_timestamp` on logout, but
//! `mobile_entity_state.timestamp` (u64 unix **milliseconds**) stays on
//! the row and tracks the last position/movement update — including for
//! signed-out players. That is the public stand-in for the private
//! `player_timestamp_state` table the game client uses.

use hashbrown::HashMap;

use crate::decode::MobileEntityRow;

pub struct MobileEntitySoA {
    pub entity_id: Vec<u64>,
    /// Unix milliseconds from upstream `mobile_entity_state.timestamp`.
    pub timestamp_ms: Vec<u64>,
    free_slots: Vec<u32>,
    pk: HashMap<u64, u32>,
}

impl MobileEntitySoA {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            entity_id: Vec::with_capacity(cap),
            timestamp_ms: Vec::with_capacity(cap),
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

    /// Last-active unix seconds when known (ms rounded down).
    pub fn last_active_timestamp(&self, entity_id: u64) -> Option<i64> {
        let slot = self.find(entity_id)?;
        let ms = self.timestamp_ms[slot as usize];
        if ms == 0 {
            return None;
        }
        i64::try_from(ms / 1000).ok()
    }

    pub fn upsert(&mut self, row: MobileEntityRow) {
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
        self.timestamp_ms[slot as usize] = 0;
        self.free_slots.push(slot);
    }

    fn alloc_slot(&mut self) -> u32 {
        if let Some(slot) = self.free_slots.pop() {
            slot
        } else {
            let slot = self.entity_id.len() as u32;
            self.entity_id.push(0);
            self.timestamp_ms.push(0);
            slot
        }
    }

    fn write_at(&mut self, slot: u32, row: &MobileEntityRow) {
        let i = slot as usize;
        self.entity_id[i] = row.entity_id;
        self.timestamp_ms[i] = row.timestamp_ms;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn last_active_converts_ms_and_skips_zero() {
        let mut s = MobileEntitySoA::with_capacity(2);
        s.upsert(MobileEntityRow {
            entity_id: 1,
            timestamp_ms: 0,
        });
        assert_eq!(s.last_active_timestamp(1), None);
        s.upsert(MobileEntityRow {
            entity_id: 1,
            timestamp_ms: 1_784_779_859_028,
        });
        assert_eq!(s.last_active_timestamp(1), Some(1_784_779_859));
        assert_eq!(s.last_active_timestamp(99), None);
    }
}
