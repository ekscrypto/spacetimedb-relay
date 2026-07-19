// SPDX-License-Identifier: MIT

//! Lookup table for `claim_tile_cost` (tile-count brackets → cost/tile).

use crate::decode::ClaimTileCostRow;

/// Sorted ascending by `tile_count`. Bracket lookup: highest
/// `tile_count <= num_tiles`.
pub struct ClaimTileCostStore {
    rows: Vec<(i32, f32)>,
}

impl ClaimTileCostStore {
    pub fn new() -> Self {
        Self { rows: Vec::new() }
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn upsert(&mut self, row: ClaimTileCostRow) {
        if let Some(existing) = self
            .rows
            .iter_mut()
            .find(|(tc, _)| *tc == row.tile_count)
        {
            existing.1 = row.cost_per_tile;
            return;
        }
        self.rows.push((row.tile_count, row.cost_per_tile));
        self.rows.sort_by_key(|(tc, _)| *tc);
    }

    pub fn delete(&mut self, tile_count: i32) {
        self.rows.retain(|(tc, _)| *tc != tile_count);
    }

    /// Cost per tile for a claim with `num_tiles` tiles.
    pub fn cost_per_tile(&self, num_tiles: i32) -> Option<f32> {
        self.rows
            .iter()
            .rev()
            .find(|(tc, _)| *tc <= num_tiles)
            .map(|(_, c)| *c)
    }
}

/// `upkeep = num_tiles * cost_per_tile + building_maintenance`.
pub fn upkeep_cost(num_tiles: i32, cost_per_tile: f32, building_maintenance: f32) -> f64 {
    f64::from(num_tiles) * f64::from(cost_per_tile) + f64::from(building_maintenance)
}

/// Unix ms when supplies run out at constant upkeep. `None` if upkeep ≤ 0.
pub fn supplies_run_out_ms(now_ms: i64, supplies: i32, upkeep: f64) -> Option<i64> {
    if upkeep <= 0.0 {
        return None;
    }
    let hours = f64::from(supplies) / upkeep;
    let delta_ms = (hours * 3_600_000.0).round() as i64;
    Some(now_ms.saturating_add(delta_ms))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bracket_and_upkeep() {
        let mut s = ClaimTileCostStore::new();
        s.upsert(ClaimTileCostRow {
            tile_count: 1,
            cost_per_tile: 0.01,
        });
        s.upsert(ClaimTileCostRow {
            tile_count: 6001,
            cost_per_tile: 0.035,
        });
        s.upsert(ClaimTileCostRow {
            tile_count: 8001,
            cost_per_tile: 0.04,
        });
        assert_eq!(s.cost_per_tile(6460), Some(0.035));
        let u = upkeep_cost(6460, 0.035, 0.0);
        assert!((u - 226.1).abs() < 1e-6, "got {u}");
        assert!(supplies_run_out_ms(1_000, 2261, u).is_some());
        assert!(supplies_run_out_ms(1_000, 100, 0.0).is_none());
    }
}
