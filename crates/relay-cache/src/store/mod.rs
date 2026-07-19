// SPDX-License-Identifier: MIT

//! Per-region columnar store. One `RegionStore` lives behind each shard's
//! `Arc<RwLock<RegionStore>>`. Read paths acquire a read lock, project to
//! response DTOs, and release; the shard's WS task holds the write lock
//! briefly to apply each `TransactionUpdate`.

pub mod building;
pub mod building_desc;
pub mod building_nickname;
pub mod claim;
pub mod deployable;
pub mod dimension_network;
pub mod inventory;
pub mod location_dim;
pub mod player_housing;
pub mod player_username;
pub mod rent;

pub use building::BuildingSoA;
pub use building_desc::BuildingDescStore;
pub use building_nickname::BuildingNicknameStore;
pub use claim::ClaimSoA;
pub use deployable::{DeployableDescStore, DeployableSoA};
pub use dimension_network::DimensionNetworkStore;
pub use inventory::{InventorySoA, Pocket};
pub use location_dim::LocationDimStore;
pub use player_housing::{PlayerHousingDescStore, PlayerHousingSoA};
pub use player_username::PlayerUsernameSoA;
pub use rent::RentSoA;

/// One region's worth of in-memory state. The `ready` flag is `false`
/// during initial `SubscribeApplied` load and after a disconnect; the
/// HTTP layer treats not-ready shards as contributing nothing to fan-out
/// queries (and reports them in `/cache-health`).
pub struct RegionStore {
    pub region: u32,
    pub ready: bool,
    pub claim: ClaimSoA,
    pub building: BuildingSoA,
    pub inventory: InventorySoA,
    pub building_desc: BuildingDescStore,
    pub building_nickname: BuildingNicknameStore,
    pub location_dim: LocationDimStore,
    pub dimension_network: DimensionNetworkStore,
    pub player_username: PlayerUsernameSoA,
    pub deployable: DeployableSoA,
    pub deployable_desc: DeployableDescStore,
    pub player_housing: PlayerHousingSoA,
    pub player_housing_desc: PlayerHousingDescStore,
    pub rent: RentSoA,
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
            location_dim: LocationDimStore::new(),
            dimension_network: DimensionNetworkStore::new(),
            player_username: PlayerUsernameSoA::with_capacity(0),
            deployable: DeployableSoA::with_capacity(0),
            deployable_desc: DeployableDescStore::new(),
            player_housing: PlayerHousingSoA::with_capacity(0),
            player_housing_desc: PlayerHousingDescStore::new(),
            rent: RentSoA::with_capacity(0),
        }
    }
}
