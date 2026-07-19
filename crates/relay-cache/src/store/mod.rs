// SPDX-License-Identifier: MIT

//! Per-region columnar store. One `RegionStore` lives behind each shard's
//! `Arc<RwLock<RegionStore>>`. Read paths acquire a read lock, project to
//! response DTOs, and release; the shard's WS task holds the write lock
//! briefly to apply each `TransactionUpdate`.

pub mod building;
pub mod building_desc;
pub mod building_nickname;
pub mod claim;
pub mod claim_local;
pub mod claim_member;
pub mod claim_tech;
pub mod claim_tile_cost;
pub mod deployable;
pub mod dimension_network;
pub mod experience;
pub mod inventory;
pub mod location_dim;
pub mod player_housing;
pub mod player_state;
pub mod player_username;
pub mod rent;
pub mod skill_desc;

pub use building::BuildingSoA;
pub use building_desc::BuildingDescStore;
pub use building_nickname::BuildingNicknameStore;
pub use claim::ClaimSoA;
pub use claim_local::ClaimLocalSoA;
pub use claim_member::ClaimMemberSoA;
pub use claim_tech::{ClaimTechDescStore, ClaimTechStateStore};
pub use claim_tile_cost::ClaimTileCostStore;
pub use deployable::{DeployableDescStore, DeployableSoA};
pub use dimension_network::DimensionNetworkStore;
pub use experience::ExperienceSoA;
pub use inventory::{InventorySoA, Pocket};
pub use location_dim::LocationDimStore;
pub use player_housing::{PlayerHousingDescStore, PlayerHousingSoA};
pub use player_state::PlayerStateSoA;
pub use player_username::PlayerUsernameSoA;
pub use rent::RentSoA;
pub use skill_desc::SkillDescStore;

/// One region's worth of in-memory state. The `ready` flag is `false`
/// during initial `SubscribeApplied` load and after a disconnect; the
/// HTTP layer treats not-ready shards as contributing nothing to fan-out
/// queries (and reports them in `/cache-health`).
pub struct RegionStore {
    pub region: u32,
    pub ready: bool,
    pub claim: ClaimSoA,
    pub claim_local: ClaimLocalSoA,
    pub claim_member: ClaimMemberSoA,
    pub claim_tech_state: ClaimTechStateStore,
    pub claim_tech_desc: ClaimTechDescStore,
    pub claim_tile_cost: ClaimTileCostStore,
    pub building: BuildingSoA,
    pub inventory: InventorySoA,
    pub building_desc: BuildingDescStore,
    pub building_nickname: BuildingNicknameStore,
    pub location_dim: LocationDimStore,
    pub dimension_network: DimensionNetworkStore,
    pub player_username: PlayerUsernameSoA,
    pub player_state: PlayerStateSoA,
    pub deployable: DeployableSoA,
    pub deployable_desc: DeployableDescStore,
    pub player_housing: PlayerHousingSoA,
    pub player_housing_desc: PlayerHousingDescStore,
    pub rent: RentSoA,
    pub experience: ExperienceSoA,
    pub skill_desc: SkillDescStore,
}

impl RegionStore {
    pub fn empty(region: u32) -> Self {
        Self {
            region,
            ready: false,
            claim: ClaimSoA::with_capacity(0),
            claim_local: ClaimLocalSoA::with_capacity(0),
            claim_member: ClaimMemberSoA::with_capacity(0),
            claim_tech_state: ClaimTechStateStore::new(),
            claim_tech_desc: ClaimTechDescStore::new(),
            claim_tile_cost: ClaimTileCostStore::new(),
            building: BuildingSoA::with_capacity(0),
            inventory: InventorySoA::with_capacity(0),
            building_desc: BuildingDescStore::new(),
            building_nickname: BuildingNicknameStore::new(),
            location_dim: LocationDimStore::new(),
            dimension_network: DimensionNetworkStore::new(),
            player_username: PlayerUsernameSoA::with_capacity(0),
            player_state: PlayerStateSoA::with_capacity(0),
            deployable: DeployableSoA::with_capacity(0),
            deployable_desc: DeployableDescStore::new(),
            player_housing: PlayerHousingSoA::with_capacity(0),
            player_housing_desc: PlayerHousingDescStore::new(),
            rent: RentSoA::with_capacity(0),
            experience: ExperienceSoA::with_capacity(0),
            skill_desc: SkillDescStore::new(),
        }
    }
}
