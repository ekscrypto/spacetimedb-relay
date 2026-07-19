// SPDX-License-Identifier: MIT

//! XP → skill-level thresholds, vendored from BitCraft's static
//! `experience/levels.json` (same table bitcraft-httpd / BitJita use).

use std::sync::OnceLock;

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
struct XpLevelThreshold {
    level: i64,
    xp: i64,
}

fn levels() -> &'static [XpLevelThreshold] {
    static LEVELS: OnceLock<Vec<XpLevelThreshold>> = OnceLock::new();
    LEVELS
        .get_or_init(|| {
            let mut v: Vec<XpLevelThreshold> =
                serde_json::from_str(include_str!("../data/xp_levels.json"))
                    .expect("xp_levels.json must parse");
            v.sort_by_key(|t| t.xp);
            v
        })
        .as_slice()
}

/// Convert raw XP to skill level. Empty table → 0.
pub fn xp_to_level(raw_xp: i64) -> i64 {
    let levels = levels();
    if levels.is_empty() {
        return 0;
    }
    match levels.partition_point(|t| t.xp <= raw_xp) {
        0 => 0,
        i => levels[i - 1].level,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xp_to_level_thresholds() {
        assert_eq!(xp_to_level(0), 1);
        assert_eq!(xp_to_level(519), 1);
        assert_eq!(xp_to_level(520), 2);
        assert_eq!(xp_to_level(1100), 3);
        assert!(xp_to_level(i64::MAX) >= 100);
    }
}
