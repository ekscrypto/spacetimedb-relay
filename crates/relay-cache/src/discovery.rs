// SPDX-License-Identifier: MIT

//! One-shot discovery of regional relay frontends from systemd unit files.
//!
//! Wraps `relay_coordinator::health::discover` so we get the same
//! `--frontend-bind` / `--mirror-database` parsing the coordinator and
//! `fleet-status.sh` already rely on. Filters out `global` (cross-region
//! reference data, out of scope for the three claim/inventory queries).

use std::path::Path;

use anyhow::Result;

/// One region frontend on the local host.
#[derive(Debug, Clone)]
pub struct DiscoveredRegion {
    pub region: u32,
    pub database: String,
    pub frontend_port: u16,
}

/// Walk `unit_dir` for `relay-bc<N>.service` units and return the
/// regional mirrors. Skips `relay-global` and any source whose region
/// number cannot be parsed (with a warn log).
pub fn discover_regions(unit_dir: &Path) -> Result<Vec<DiscoveredRegion>> {
    let sources = relay_coordinator::health::discover(unit_dir);
    let mut out = Vec::with_capacity(sources.len());
    for src in sources {
        if src.name == "global" {
            continue;
        }
        let Some(region) = parse_region_number(&src.name) else {
            tracing::warn!(
                target: "relay_cache::discovery",
                name = %src.name,
                "skipping source: cannot parse region number"
            );
            continue;
        };
        out.push(DiscoveredRegion {
            region,
            database: src.database,
            frontend_port: src.frontend_port,
        });
    }
    Ok(out)
}

/// `"bitcraft-live-14"` → `Some(14)`.
fn parse_region_number(name: &str) -> Option<u32> {
    name.strip_prefix("bitcraft-live-")?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_region_from_source_name() {
        assert_eq!(parse_region_number("bitcraft-live-14"), Some(14));
        assert_eq!(parse_region_number("bitcraft-live-3"), Some(3));
        assert_eq!(parse_region_number("global"), None);
        assert_eq!(parse_region_number("bitcraft-live-"), None);
        assert_eq!(parse_region_number("relay-bc14"), None);
    }
}
