// SPDX-License-Identifier: MIT

//! Per-region columnar store. One `RegionStore` lives behind each shard's
//! `Arc<RwLock<RegionStore>>`. Read paths acquire a read lock, project to
//! response DTOs, and release; the shard's WS task holds the write lock
//! briefly to apply each `TransactionUpdate`.

pub mod building;
pub mod building_desc;
pub mod building_nickname;
pub mod claim;
pub mod inventory;

pub use building::BuildingSoA;
pub use building_desc::BuildingDescStore;
pub use building_nickname::BuildingNicknameStore;
pub use claim::ClaimSoA;
pub use inventory::{InventorySoA, Pocket};

/// One region's worth of in-memory state. The `ready` flag is `false`
/// during initial `SubscribeApplied` load and after a disconnect; the
/// HTTP layer treats not-ready shards as contributing nothing to fan-out
/// queries (and reports them in `/healthz`).
pub struct RegionStore {
    pub region: u32,
    pub ready: bool,
    pub claim: ClaimSoA,
    pub building: BuildingSoA,
    pub inventory: InventorySoA,
    pub building_desc: BuildingDescStore,
    pub building_nickname: BuildingNicknameStore,
}

impl RegionStore {
    pub fn empty(region: u32) -> Self {
        Self {
            region,
            ready: false,
            claim: ClaimSoA::with_capacity(0),
            building: BuildingSoA::with_capacity(0),
            inventory: InventorySoA::with_capacity(0),
            building_desc: BuildingDescStore::new(),
            building_nickname: BuildingNicknameStore::new(),
        }
    }
}
