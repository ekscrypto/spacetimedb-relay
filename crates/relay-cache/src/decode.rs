// SPDX-License-Identifier: MIT

//! Projection from raw BSATN row bytes → typed row structs.
//!
//! The actual BSATN walk lives in `relay_protocol::bsatn::decode_row`,
//! which is schema-driven and produces `Vec<Cell>` per row. We resolve
//! column indices by name once at shard init and then index into `cells`
//! — no per-row name lookup, no JSON in the hot path (except for the
//! nested `pockets` array, which the decoder renders as `Cell::Jsonb`
//! because it's a sum-typed product array; we walk that JSON exactly
//! once per insert to build a typed `Box<[Pocket]>`).
//!
//! ## Cell → Rust mapping
//!
//! The relay-protocol decoder maps U64 to `Cell::Bytea` (8 LE bytes) to
//! avoid a NUMERIC dependency on the Postgres side. We convert back to
//! `u64` via `from_le_bytes`. Documented invariant: BitCraft entity IDs
//! are well below `i64::MAX` (verified by `bitcraft-relay-sync` and
//! enforced by the relay), so the `Bytea` roundtrip preserves full u64
//! precision without loss.

use anyhow::{anyhow, bail, Result};
use relay_protocol::bsatn::Cell;
use relay_protocol::{bsatn, MirroredField, MirroredSchema, MirroredType};
use serde_json::Value;

use crate::store::Pocket;

pub const CLAIM_TABLE: &str = "claim_state";
pub const CLAIM_LOCAL_TABLE: &str = "claim_local_state";
pub const CLAIM_MEMBER_TABLE: &str = "claim_member_state";
pub const CLAIM_TECH_STATE_TABLE: &str = "claim_tech_state";
pub const CLAIM_TECH_DESC_TABLE: &str = "claim_tech_desc";
pub const CLAIM_TILE_COST_TABLE: &str = "claim_tile_cost";
pub const BUILDING_TABLE: &str = "building_state";
pub const INVENTORY_TABLE: &str = "inventory_state";
pub const BUILDING_DESC_TABLE: &str = "building_desc";
pub const BUILDING_NICKNAME_TABLE: &str = "building_nickname_state";
pub const LOCATION_TABLE: &str = "location_state";
pub const DIMENSION_NETWORK_TABLE: &str = "dimension_network_state";
pub const PLAYER_USERNAME_TABLE: &str = "player_username_state";
pub const PLAYER_STATE_TABLE: &str = "player_state";
pub const DEPLOYABLE_TABLE: &str = "deployable_state";
pub const DEPLOYABLE_DESC_TABLE: &str = "deployable_desc";
pub const PLAYER_HOUSING_TABLE: &str = "player_housing_state";
pub const PLAYER_HOUSING_DESC_TABLE: &str = "player_housing_desc";
pub const RENT_TABLE: &str = "rent_state";
pub const EXPERIENCE_TABLE: &str = "experience_state";
pub const SKILL_DESC_TABLE: &str = "skill_desc";
pub const PROGRESSIVE_ACTION_TABLE: &str = "progressive_action_state";
pub const PASSIVE_CRAFT_TABLE: &str = "passive_craft_state";
pub const CRAFTING_RECIPE_DESC_TABLE: &str = "crafting_recipe_desc";
pub const RESOURCE_TABLE: &str = "resource_state";
pub const GROWTH_TABLE: &str = "growth_state";
pub const STORAGE_LOG_TABLE: &str = "storage_log_state";

/// Hexite Deposit (`resource_desc.id`). Live / harvestable form.
pub const HEXITE_DEPOSIT_RESOURCE_ID: i32 = 348497955;
/// Depleted Hexite Deposit — same entity, growing back via `growth_state`.
pub const DEPLETED_HEXITE_DEPOSIT_RESOURCE_ID: i32 = 854132798;

pub fn is_hexite_resource_id(resource_id: i32) -> bool {
    resource_id == HEXITE_DEPOSIT_RESOURCE_ID || resource_id == DEPLETED_HEXITE_DEPOSIT_RESOURCE_ID
}

/// Overworld dimension id used when a building has no interior location row.
pub const OVERWORLD_DIMENSION: u32 = 1;

/// Resolved column indices for `claim_state`, looked up once per shard.
#[derive(Clone, Copy)]
pub struct ClaimCols {
    pub entity_id: usize,
    pub owner_player_entity_id: usize,
    pub owner_building_entity_id: usize,
    pub name: usize,
    pub neutral: usize,
}

#[derive(Clone, Copy)]
pub struct ClaimLocalCols {
    pub entity_id: usize,
    pub supplies: usize,
    pub building_maintenance: usize,
    pub num_tiles: usize,
    pub location: usize,
    pub treasury: usize,
    pub supplies_purchase_threshold: usize,
    pub supplies_purchase_price: usize,
}

#[derive(Clone, Copy)]
pub struct ClaimMemberCols {
    pub entity_id: usize,
    pub claim_entity_id: usize,
    pub player_entity_id: usize,
    pub user_name: usize,
    pub inventory_permission: usize,
    pub build_permission: usize,
    pub officer_permission: usize,
    pub co_owner_permission: usize,
}

#[derive(Clone, Copy)]
pub struct ClaimTechStateCols {
    pub entity_id: usize,
    pub learned: usize,
    pub researching: usize,
    pub start_timestamp: usize,
}

#[derive(Clone, Copy)]
pub struct ClaimTechDescCols {
    pub id: usize,
    pub name: usize,
    pub description: usize,
    pub tier: usize,
    pub tech_type: usize,
    pub supplies_cost: usize,
    pub research_time: usize,
    pub requirements: usize,
    pub members: usize,
    pub area: usize,
    pub unlocks_techs: usize,
}

#[derive(Clone, Copy)]
pub struct ClaimTileCostCols {
    pub tile_count: usize,
    pub cost_per_tile: usize,
}

#[derive(Clone, Copy)]
pub struct ExperienceCols {
    pub entity_id: usize,
    pub experience_stacks: usize,
}

#[derive(Clone, Copy)]
pub struct SkillDescCols {
    pub id: usize,
    pub name: usize,
    pub title: usize,
    pub max_level: usize,
}

#[derive(Clone, Copy)]
pub struct ProgressiveActionCols {
    pub entity_id: usize,
    pub building_entity_id: usize,
    pub progress: usize,
    pub recipe_id: usize,
    pub craft_count: usize,
    pub owner_entity_id: usize,
}

#[derive(Clone, Copy)]
pub struct PassiveCraftCols {
    pub entity_id: usize,
    pub owner_entity_id: usize,
    pub recipe_id: usize,
    pub building_entity_id: usize,
    pub status: usize,
}

#[derive(Clone, Copy)]
pub struct CraftingRecipeDescCols {
    pub id: usize,
    pub crafted_item_stacks: usize,
    pub actions_required: usize,
}

/// Resolved column indices for `building_state`.
#[derive(Clone, Copy)]
pub struct BuildingCols {
    pub entity_id: usize,
    pub claim_entity_id: usize,
    pub building_description_id: usize,
}

/// Resolved column indices for `inventory_state`.
#[derive(Clone, Copy)]
pub struct InventoryCols {
    pub entity_id: usize,
    pub pockets: usize,
    pub inventory_index: usize,
    pub cargo_index: usize,
    pub owner_entity_id: usize,
    pub player_owner_entity_id: usize,
}

/// Resolved column indices for `building_desc` (catalog).
#[derive(Clone, Copy)]
pub struct BuildingDescCols {
    pub id: usize,
    pub name: usize,
    pub functions: usize,
}

/// Resolved column indices for `building_nickname_state`.
#[derive(Clone, Copy)]
pub struct BuildingNicknameCols {
    pub entity_id: usize,
    pub nickname: usize,
}

/// Resolved column indices for `location_state`.
#[derive(Clone, Copy)]
pub struct LocationCols {
    pub entity_id: usize,
    pub x: usize,
    pub z: usize,
    pub dimension: usize,
}

#[derive(Clone, Copy)]
pub struct ResourceCols {
    pub entity_id: usize,
    pub resource_id: usize,
}

#[derive(Clone, Copy)]
pub struct GrowthCols {
    pub entity_id: usize,
    pub end_timestamp: usize,
    pub growth_recipe_id: usize,
}

#[derive(Clone, Copy)]
pub struct StorageLogCols {
    pub id: usize,
    pub object_entity_id: usize,
    pub subject_entity_id: usize,
    pub subject_name: usize,
    pub data: usize,
    pub timestamp: usize,
    pub days_since_epoch: usize,
}

/// Resolved column indices for `dimension_network_state`.
#[derive(Clone, Copy)]
pub struct DimensionNetworkCols {
    pub entity_id: usize,
    pub building_id: usize,
    pub claim_entity_id: usize,
    pub rent_entity_id: usize,
    pub entrance_dimension_id: usize,
    pub is_collapsed: usize,
}

#[derive(Clone, Copy)]
pub struct PlayerUsernameCols {
    pub entity_id: usize,
    pub username: usize,
}

#[derive(Clone, Copy)]
pub struct PlayerStateCols {
    pub entity_id: usize,
    pub sign_in_timestamp: usize,
    pub session_start_timestamp: usize,
    pub signed_in: usize,
}

#[derive(Clone, Copy)]
pub struct DeployableCols {
    pub entity_id: usize,
    pub owner_id: usize,
    pub claim_entity_id: usize,
    pub deployable_description_id: usize,
    pub nickname: usize,
}

#[derive(Clone, Copy)]
pub struct DeployableDescCols {
    pub id: usize,
    pub name: usize,
    pub deployable_type: usize,
}

#[derive(Clone, Copy)]
pub struct PlayerHousingCols {
    pub entity_id: usize,
    pub entrance_building_entity_id: usize,
    pub network_entity_id: usize,
    pub rank: usize,
    pub is_empty: usize,
}

#[derive(Clone, Copy)]
pub struct PlayerHousingDescCols {
    pub secondary_knowledge_id: usize,
    pub rank: usize,
    pub name: usize,
}

#[derive(Clone, Copy)]
pub struct RentCols {
    pub entity_id: usize,
    pub dimension_network_id: usize,
    pub claim_entity_id: usize,
    pub white_list: usize,
    pub active: usize,
}

/// Per-shard bundle of column indices. Built once at shard init from the
/// shared schema.
pub struct ColMaps {
    pub claim: ClaimCols,
    pub claim_local: ClaimLocalCols,
    pub claim_member: ClaimMemberCols,
    pub claim_tech_state: ClaimTechStateCols,
    pub claim_tech_desc: ClaimTechDescCols,
    pub claim_tile_cost: ClaimTileCostCols,
    pub building: BuildingCols,
    pub inventory: InventoryCols,
    pub building_desc: BuildingDescCols,
    pub building_nickname: BuildingNicknameCols,
    pub location: LocationCols,
    pub dimension_network: DimensionNetworkCols,
    pub player_username: PlayerUsernameCols,
    pub player_state: PlayerStateCols,
    pub deployable: DeployableCols,
    pub deployable_desc: DeployableDescCols,
    pub player_housing: PlayerHousingCols,
    pub player_housing_desc: PlayerHousingDescCols,
    pub rent: RentCols,
    pub experience: ExperienceCols,
    pub skill_desc: SkillDescCols,
    pub progressive_action: ProgressiveActionCols,
    pub passive_craft: PassiveCraftCols,
    pub crafting_recipe_desc: CraftingRecipeDescCols,
    pub resource: ResourceCols,
    pub growth: GrowthCols,
    pub storage_log: StorageLogCols,
}

/// Resolve column indices for the tables we hold. Errors if any expected
/// column is missing — a sign of upstream schema drift.
pub fn resolve_cols(schema: &MirroredSchema) -> Result<ColMaps> {
    Ok(ColMaps {
        claim: resolve_claim_cols(schema)?,
        claim_local: resolve_claim_local_cols(schema)?,
        claim_member: resolve_claim_member_cols(schema)?,
        claim_tech_state: resolve_claim_tech_state_cols(schema)?,
        claim_tech_desc: resolve_claim_tech_desc_cols(schema)?,
        claim_tile_cost: resolve_claim_tile_cost_cols(schema)?,
        building: resolve_building_cols(schema)?,
        inventory: resolve_inventory_cols(schema)?,
        building_desc: resolve_building_desc_cols(schema)?,
        building_nickname: resolve_building_nickname_cols(schema)?,
        location: resolve_location_cols(schema)?,
        dimension_network: resolve_dimension_network_cols(schema)?,
        player_username: resolve_player_username_cols(schema)?,
        player_state: resolve_player_state_cols(schema)?,
        deployable: resolve_deployable_cols(schema)?,
        deployable_desc: resolve_deployable_desc_cols(schema)?,
        player_housing: resolve_player_housing_cols(schema)?,
        player_housing_desc: resolve_player_housing_desc_cols(schema)?,
        rent: resolve_rent_cols(schema)?,
        experience: resolve_experience_cols(schema)?,
        skill_desc: resolve_skill_desc_cols(schema)?,
        progressive_action: resolve_progressive_action_cols(schema)?,
        passive_craft: resolve_passive_craft_cols(schema)?,
        crafting_recipe_desc: resolve_crafting_recipe_desc_cols(schema)?,
        resource: resolve_resource_cols(schema)?,
        growth: resolve_growth_cols(schema)?,
        storage_log: resolve_storage_log_cols(schema)?,
    })
}

fn fields_of<'a>(schema: &'a MirroredSchema, table: &str) -> Result<&'a [MirroredField]> {
    let tbl = schema
        .tables
        .iter()
        .find(|t| t.name == table)
        .ok_or_else(|| anyhow!("schema has no table `{table}`"))?;
    let ty = schema
        .typespace
        .get(tbl.product_type_ref as usize)
        .ok_or_else(|| anyhow!("typespace has no type at ref {}", tbl.product_type_ref))?;
    let resolved = schema.resolve(ty);
    match resolved {
        MirroredType::Product(f) => Ok(f),
        _ => bail!("table {table} product_type_ref did not resolve to a Product"),
    }
}

fn find_field(fields: &[MirroredField], name: &str, table: &str) -> Result<usize> {
    fields
        .iter()
        .position(|f| f.name.as_deref() == Some(name))
        .ok_or_else(|| anyhow!("table `{table}` has no column `{name}`"))
}

fn resolve_claim_cols(schema: &MirroredSchema) -> Result<ClaimCols> {
    let f = fields_of(schema, CLAIM_TABLE)?;
    Ok(ClaimCols {
        entity_id: find_field(f, "entity_id", CLAIM_TABLE)?,
        owner_player_entity_id: find_field(f, "owner_player_entity_id", CLAIM_TABLE)?,
        owner_building_entity_id: find_field(f, "owner_building_entity_id", CLAIM_TABLE)?,
        name: find_field(f, "name", CLAIM_TABLE)?,
        neutral: find_field(f, "neutral", CLAIM_TABLE)?,
    })
}

fn resolve_claim_local_cols(schema: &MirroredSchema) -> Result<ClaimLocalCols> {
    let f = fields_of(schema, CLAIM_LOCAL_TABLE)?;
    Ok(ClaimLocalCols {
        entity_id: find_field(f, "entity_id", CLAIM_LOCAL_TABLE)?,
        supplies: find_field(f, "supplies", CLAIM_LOCAL_TABLE)?,
        building_maintenance: find_field(f, "building_maintenance", CLAIM_LOCAL_TABLE)?,
        num_tiles: find_field(f, "num_tiles", CLAIM_LOCAL_TABLE)?,
        location: find_field(f, "location", CLAIM_LOCAL_TABLE)?,
        treasury: find_field(f, "treasury", CLAIM_LOCAL_TABLE)?,
        supplies_purchase_threshold: find_field(
            f,
            "supplies_purchase_threshold",
            CLAIM_LOCAL_TABLE,
        )?,
        supplies_purchase_price: find_field(f, "supplies_purchase_price", CLAIM_LOCAL_TABLE)?,
    })
}

fn resolve_claim_member_cols(schema: &MirroredSchema) -> Result<ClaimMemberCols> {
    let f = fields_of(schema, CLAIM_MEMBER_TABLE)?;
    Ok(ClaimMemberCols {
        entity_id: find_field(f, "entity_id", CLAIM_MEMBER_TABLE)?,
        claim_entity_id: find_field(f, "claim_entity_id", CLAIM_MEMBER_TABLE)?,
        player_entity_id: find_field(f, "player_entity_id", CLAIM_MEMBER_TABLE)?,
        user_name: find_field(f, "user_name", CLAIM_MEMBER_TABLE)?,
        inventory_permission: find_field(f, "inventory_permission", CLAIM_MEMBER_TABLE)?,
        build_permission: find_field(f, "build_permission", CLAIM_MEMBER_TABLE)?,
        officer_permission: find_field(f, "officer_permission", CLAIM_MEMBER_TABLE)?,
        co_owner_permission: find_field(f, "co_owner_permission", CLAIM_MEMBER_TABLE)?,
    })
}

fn resolve_claim_tech_state_cols(schema: &MirroredSchema) -> Result<ClaimTechStateCols> {
    let f = fields_of(schema, CLAIM_TECH_STATE_TABLE)?;
    Ok(ClaimTechStateCols {
        entity_id: find_field(f, "entity_id", CLAIM_TECH_STATE_TABLE)?,
        learned: find_field(f, "learned", CLAIM_TECH_STATE_TABLE)?,
        researching: find_field(f, "researching", CLAIM_TECH_STATE_TABLE)?,
        start_timestamp: find_field(f, "start_timestamp", CLAIM_TECH_STATE_TABLE)?,
    })
}

fn resolve_claim_tech_desc_cols(schema: &MirroredSchema) -> Result<ClaimTechDescCols> {
    let f = fields_of(schema, CLAIM_TECH_DESC_TABLE)?;
    Ok(ClaimTechDescCols {
        id: find_field(f, "id", CLAIM_TECH_DESC_TABLE)?,
        name: find_field(f, "name", CLAIM_TECH_DESC_TABLE)?,
        description: find_field(f, "description", CLAIM_TECH_DESC_TABLE)?,
        tier: find_field(f, "tier", CLAIM_TECH_DESC_TABLE)?,
        tech_type: find_field(f, "tech_type", CLAIM_TECH_DESC_TABLE)?,
        supplies_cost: find_field(f, "supplies_cost", CLAIM_TECH_DESC_TABLE)?,
        research_time: find_field(f, "research_time", CLAIM_TECH_DESC_TABLE)?,
        requirements: find_field(f, "requirements", CLAIM_TECH_DESC_TABLE)?,
        members: find_field(f, "members", CLAIM_TECH_DESC_TABLE)?,
        area: find_field(f, "area", CLAIM_TECH_DESC_TABLE)?,
        unlocks_techs: find_field(f, "unlocks_techs", CLAIM_TECH_DESC_TABLE)?,
    })
}

fn resolve_claim_tile_cost_cols(schema: &MirroredSchema) -> Result<ClaimTileCostCols> {
    let f = fields_of(schema, CLAIM_TILE_COST_TABLE)?;
    Ok(ClaimTileCostCols {
        tile_count: find_field(f, "tile_count", CLAIM_TILE_COST_TABLE)?,
        cost_per_tile: find_field(f, "cost_per_tile", CLAIM_TILE_COST_TABLE)?,
    })
}

fn resolve_experience_cols(schema: &MirroredSchema) -> Result<ExperienceCols> {
    let f = fields_of(schema, EXPERIENCE_TABLE)?;
    Ok(ExperienceCols {
        entity_id: find_field(f, "entity_id", EXPERIENCE_TABLE)?,
        experience_stacks: find_field(f, "experience_stacks", EXPERIENCE_TABLE)?,
    })
}

fn resolve_skill_desc_cols(schema: &MirroredSchema) -> Result<SkillDescCols> {
    let f = fields_of(schema, SKILL_DESC_TABLE)?;
    Ok(SkillDescCols {
        id: find_field(f, "id", SKILL_DESC_TABLE)?,
        name: find_field(f, "name", SKILL_DESC_TABLE)?,
        title: find_field(f, "title", SKILL_DESC_TABLE)?,
        max_level: find_field(f, "max_level", SKILL_DESC_TABLE)?,
    })
}

fn resolve_progressive_action_cols(schema: &MirroredSchema) -> Result<ProgressiveActionCols> {
    let f = fields_of(schema, PROGRESSIVE_ACTION_TABLE)?;
    Ok(ProgressiveActionCols {
        entity_id: find_field(f, "entity_id", PROGRESSIVE_ACTION_TABLE)?,
        building_entity_id: find_field(f, "building_entity_id", PROGRESSIVE_ACTION_TABLE)?,
        progress: find_field(f, "progress", PROGRESSIVE_ACTION_TABLE)?,
        recipe_id: find_field(f, "recipe_id", PROGRESSIVE_ACTION_TABLE)?,
        craft_count: find_field(f, "craft_count", PROGRESSIVE_ACTION_TABLE)?,
        owner_entity_id: find_field(f, "owner_entity_id", PROGRESSIVE_ACTION_TABLE)?,
    })
}

fn resolve_passive_craft_cols(schema: &MirroredSchema) -> Result<PassiveCraftCols> {
    let f = fields_of(schema, PASSIVE_CRAFT_TABLE)?;
    Ok(PassiveCraftCols {
        entity_id: find_field(f, "entity_id", PASSIVE_CRAFT_TABLE)?,
        owner_entity_id: find_field(f, "owner_entity_id", PASSIVE_CRAFT_TABLE)?,
        recipe_id: find_field(f, "recipe_id", PASSIVE_CRAFT_TABLE)?,
        building_entity_id: find_field(f, "building_entity_id", PASSIVE_CRAFT_TABLE)?,
        status: find_field(f, "status", PASSIVE_CRAFT_TABLE)?,
    })
}

fn resolve_crafting_recipe_desc_cols(schema: &MirroredSchema) -> Result<CraftingRecipeDescCols> {
    let f = fields_of(schema, CRAFTING_RECIPE_DESC_TABLE)?;
    Ok(CraftingRecipeDescCols {
        id: find_field(f, "id", CRAFTING_RECIPE_DESC_TABLE)?,
        crafted_item_stacks: find_field(f, "crafted_item_stacks", CRAFTING_RECIPE_DESC_TABLE)?,
        actions_required: find_field(f, "actions_required", CRAFTING_RECIPE_DESC_TABLE)?,
    })
}

fn resolve_building_cols(schema: &MirroredSchema) -> Result<BuildingCols> {
    let f = fields_of(schema, BUILDING_TABLE)?;
    Ok(BuildingCols {
        entity_id: find_field(f, "entity_id", BUILDING_TABLE)?,
        claim_entity_id: find_field(f, "claim_entity_id", BUILDING_TABLE)?,
        building_description_id: find_field(f, "building_description_id", BUILDING_TABLE)?,
    })
}

fn resolve_building_desc_cols(schema: &MirroredSchema) -> Result<BuildingDescCols> {
    let f = fields_of(schema, BUILDING_DESC_TABLE)?;
    Ok(BuildingDescCols {
        id: find_field(f, "id", BUILDING_DESC_TABLE)?,
        name: find_field(f, "name", BUILDING_DESC_TABLE)?,
        functions: find_field(f, "functions", BUILDING_DESC_TABLE)?,
    })
}

fn resolve_building_nickname_cols(schema: &MirroredSchema) -> Result<BuildingNicknameCols> {
    let f = fields_of(schema, BUILDING_NICKNAME_TABLE)?;
    Ok(BuildingNicknameCols {
        entity_id: find_field(f, "entity_id", BUILDING_NICKNAME_TABLE)?,
        nickname: find_field(f, "nickname", BUILDING_NICKNAME_TABLE)?,
    })
}

fn resolve_inventory_cols(schema: &MirroredSchema) -> Result<InventoryCols> {
    let f = fields_of(schema, INVENTORY_TABLE)?;
    Ok(InventoryCols {
        entity_id: find_field(f, "entity_id", INVENTORY_TABLE)?,
        pockets: find_field(f, "pockets", INVENTORY_TABLE)?,
        inventory_index: find_field(f, "inventory_index", INVENTORY_TABLE)?,
        cargo_index: find_field(f, "cargo_index", INVENTORY_TABLE)?,
        owner_entity_id: find_field(f, "owner_entity_id", INVENTORY_TABLE)?,
        player_owner_entity_id: find_field(f, "player_owner_entity_id", INVENTORY_TABLE)?,
    })
}

fn resolve_location_cols(schema: &MirroredSchema) -> Result<LocationCols> {
    let f = fields_of(schema, LOCATION_TABLE)?;
    Ok(LocationCols {
        entity_id: find_field(f, "entity_id", LOCATION_TABLE)?,
        x: find_field(f, "x", LOCATION_TABLE)?,
        z: find_field(f, "z", LOCATION_TABLE)?,
        dimension: find_field(f, "dimension", LOCATION_TABLE)?,
    })
}

fn resolve_resource_cols(schema: &MirroredSchema) -> Result<ResourceCols> {
    let f = fields_of(schema, RESOURCE_TABLE)?;
    Ok(ResourceCols {
        entity_id: find_field(f, "entity_id", RESOURCE_TABLE)?,
        resource_id: find_field(f, "resource_id", RESOURCE_TABLE)?,
    })
}

fn resolve_growth_cols(schema: &MirroredSchema) -> Result<GrowthCols> {
    let f = fields_of(schema, GROWTH_TABLE)?;
    Ok(GrowthCols {
        entity_id: find_field(f, "entity_id", GROWTH_TABLE)?,
        end_timestamp: find_field(f, "end_timestamp", GROWTH_TABLE)?,
        growth_recipe_id: find_field(f, "growth_recipe_id", GROWTH_TABLE)?,
    })
}

fn resolve_storage_log_cols(schema: &MirroredSchema) -> Result<StorageLogCols> {
    let f = fields_of(schema, STORAGE_LOG_TABLE)?;
    Ok(StorageLogCols {
        id: find_field(f, "id", STORAGE_LOG_TABLE)?,
        object_entity_id: find_field(f, "object_entity_id", STORAGE_LOG_TABLE)?,
        subject_entity_id: find_field(f, "subject_entity_id", STORAGE_LOG_TABLE)?,
        subject_name: find_field(f, "subject_name", STORAGE_LOG_TABLE)?,
        data: find_field(f, "data", STORAGE_LOG_TABLE)?,
        timestamp: find_field(f, "timestamp", STORAGE_LOG_TABLE)?,
        days_since_epoch: find_field(f, "days_since_epoch", STORAGE_LOG_TABLE)?,
    })
}

fn resolve_dimension_network_cols(schema: &MirroredSchema) -> Result<DimensionNetworkCols> {
    let f = fields_of(schema, DIMENSION_NETWORK_TABLE)?;
    Ok(DimensionNetworkCols {
        entity_id: find_field(f, "entity_id", DIMENSION_NETWORK_TABLE)?,
        building_id: find_field(f, "building_id", DIMENSION_NETWORK_TABLE)?,
        claim_entity_id: find_field(f, "claim_entity_id", DIMENSION_NETWORK_TABLE)?,
        rent_entity_id: find_field(f, "rent_entity_id", DIMENSION_NETWORK_TABLE)?,
        entrance_dimension_id: find_field(f, "entrance_dimension_id", DIMENSION_NETWORK_TABLE)?,
        is_collapsed: find_field(f, "is_collapsed", DIMENSION_NETWORK_TABLE)?,
    })
}

fn resolve_player_username_cols(schema: &MirroredSchema) -> Result<PlayerUsernameCols> {
    let f = fields_of(schema, PLAYER_USERNAME_TABLE)?;
    Ok(PlayerUsernameCols {
        entity_id: find_field(f, "entity_id", PLAYER_USERNAME_TABLE)?,
        username: find_field(f, "username", PLAYER_USERNAME_TABLE)?,
    })
}

fn resolve_player_state_cols(schema: &MirroredSchema) -> Result<PlayerStateCols> {
    let f = fields_of(schema, PLAYER_STATE_TABLE)?;
    Ok(PlayerStateCols {
        entity_id: find_field(f, "entity_id", PLAYER_STATE_TABLE)?,
        sign_in_timestamp: find_field(f, "sign_in_timestamp", PLAYER_STATE_TABLE)?,
        session_start_timestamp: find_field(f, "session_start_timestamp", PLAYER_STATE_TABLE)?,
        signed_in: find_field(f, "signed_in", PLAYER_STATE_TABLE)?,
    })
}

fn resolve_deployable_cols(schema: &MirroredSchema) -> Result<DeployableCols> {
    let f = fields_of(schema, DEPLOYABLE_TABLE)?;
    Ok(DeployableCols {
        entity_id: find_field(f, "entity_id", DEPLOYABLE_TABLE)?,
        owner_id: find_field(f, "owner_id", DEPLOYABLE_TABLE)?,
        claim_entity_id: find_field(f, "claim_entity_id", DEPLOYABLE_TABLE)?,
        deployable_description_id: find_field(f, "deployable_description_id", DEPLOYABLE_TABLE)?,
        nickname: find_field(f, "nickname", DEPLOYABLE_TABLE)?,
    })
}

fn resolve_deployable_desc_cols(schema: &MirroredSchema) -> Result<DeployableDescCols> {
    let f = fields_of(schema, DEPLOYABLE_DESC_TABLE)?;
    Ok(DeployableDescCols {
        id: find_field(f, "id", DEPLOYABLE_DESC_TABLE)?,
        name: find_field(f, "name", DEPLOYABLE_DESC_TABLE)?,
        deployable_type: find_field(f, "deployable_type", DEPLOYABLE_DESC_TABLE)?,
    })
}

fn resolve_player_housing_cols(schema: &MirroredSchema) -> Result<PlayerHousingCols> {
    let f = fields_of(schema, PLAYER_HOUSING_TABLE)?;
    Ok(PlayerHousingCols {
        entity_id: find_field(f, "entity_id", PLAYER_HOUSING_TABLE)?,
        entrance_building_entity_id: find_field(
            f,
            "entrance_building_entity_id",
            PLAYER_HOUSING_TABLE,
        )?,
        network_entity_id: find_field(f, "network_entity_id", PLAYER_HOUSING_TABLE)?,
        rank: find_field(f, "rank", PLAYER_HOUSING_TABLE)?,
        is_empty: find_field(f, "is_empty", PLAYER_HOUSING_TABLE)?,
    })
}

fn resolve_player_housing_desc_cols(schema: &MirroredSchema) -> Result<PlayerHousingDescCols> {
    let f = fields_of(schema, PLAYER_HOUSING_DESC_TABLE)?;
    Ok(PlayerHousingDescCols {
        secondary_knowledge_id: find_field(f, "secondary_knowledge_id", PLAYER_HOUSING_DESC_TABLE)?,
        rank: find_field(f, "rank", PLAYER_HOUSING_DESC_TABLE)?,
        name: find_field(f, "name", PLAYER_HOUSING_DESC_TABLE)?,
    })
}

fn resolve_rent_cols(schema: &MirroredSchema) -> Result<RentCols> {
    let f = fields_of(schema, RENT_TABLE)?;
    Ok(RentCols {
        entity_id: find_field(f, "entity_id", RENT_TABLE)?,
        dimension_network_id: find_field(f, "dimension_network_id", RENT_TABLE)?,
        claim_entity_id: find_field(f, "claim_entity_id", RENT_TABLE)?,
        white_list: find_field(f, "white_list", RENT_TABLE)?,
        active: find_field(f, "active", RENT_TABLE)?,
    })
}

// --- Typed row structs (mirrors bitcraft-relay-sync::decode shapes) ---

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimRow {
    pub entity_id: u64,
    pub owner_player_entity_id: u64,
    pub owner_building_entity_id: u64,
    pub name: String,
    pub neutral: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildingRow {
    pub entity_id: u64,
    pub claim_entity_id: u64,
    pub building_description_id: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildingDescRow {
    pub id: i32,
    pub name: String,
    /// True when any function entry has `storage_slots > 0` or
    /// `cargo_slots > 0` — the BitCraft signal for a storage-capable
    /// building type (chests, banks, cargo stockpiles, etc.).
    pub is_storage: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildingNicknameRow {
    pub entity_id: u64,
    pub nickname: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InventoryRow {
    pub entity_id: u64,
    pub pockets: Box<[Pocket]>,
    pub inventory_index: i32,
    pub cargo_index: i32,
    pub owner_entity_id: u64,
    pub player_owner_entity_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocationDimRow {
    pub entity_id: u64,
    pub dimension: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocationRow {
    pub entity_id: u64,
    pub x: i32,
    pub z: i32,
    pub dimension: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceRow {
    pub entity_id: u64,
    pub resource_id: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrowthRow {
    pub entity_id: u64,
    pub end_timestamp_micros: i64,
    pub growth_recipe_id: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageLogRow {
    pub id: u64,
    pub storage_entity_id: u64,
    pub player_entity_id: u64,
    pub player_username: String,
    /// `ACTION_RESERVED` / `ACTION_WITHDRAW` / `ACTION_DEPOSIT`.
    pub action: u8,
    pub item_id: i32,
    pub item_type: u8,
    pub quantity: i32,
    pub timestamp_micros: i64,
    pub days_since_epoch: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DimensionNetworkRow {
    pub entity_id: u64,
    pub building_id: u64,
    pub claim_entity_id: u64,
    pub rent_entity_id: u64,
    pub entrance_dimension_id: u32,
    pub is_collapsed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlayerUsernameRow {
    pub entity_id: u64,
    pub username: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlayerStateRow {
    pub entity_id: u64,
    pub sign_in_timestamp: i32,
    pub session_start_timestamp: i32,
    pub signed_in: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeployableKind {
    Cart,
    Cache,
    Mount,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeployableRow {
    pub entity_id: u64,
    pub owner_id: u64,
    pub claim_entity_id: u64,
    pub deployable_description_id: i32,
    pub nickname: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeployableDescRow {
    pub id: i32,
    pub name: String,
    pub kind: DeployableKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlayerHousingRow {
    pub entity_id: u64,
    pub entrance_building_entity_id: u64,
    pub network_entity_id: u64,
    pub rank: i32,
    pub is_empty: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlayerHousingDescRow {
    pub secondary_knowledge_id: i32,
    pub rank: i32,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RentRow {
    pub entity_id: u64,
    pub dimension_network_id: u64,
    pub claim_entity_id: u64,
    pub white_list: Box<[u64]>,
    pub active: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClaimLocalRow {
    pub entity_id: u64,
    pub supplies: i32,
    pub building_maintenance: f32,
    pub num_tiles: i32,
    pub treasury: u32,
    pub supplies_purchase_threshold: u32,
    pub supplies_purchase_price: f32,
    pub location_x: i32,
    pub location_z: i32,
    pub location_dimension: u32,
    pub has_location: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimMemberRow {
    pub entity_id: u64,
    pub claim_entity_id: u64,
    pub player_entity_id: u64,
    pub user_name: String,
    pub inventory_permission: bool,
    pub build_permission: bool,
    pub officer_permission: bool,
    pub co_owner_permission: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimTechStateRow {
    pub entity_id: u64,
    pub learned: Box<[i32]>,
    pub researching: i32,
    pub start_timestamp_micros: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimTechDescRow {
    pub id: i32,
    pub name: String,
    pub description: String,
    pub tier: i32,
    pub tech_type: String,
    pub supplies_cost: i32,
    pub research_time: i32,
    pub requirements: Box<[i32]>,
    pub members: i32,
    pub area: i32,
    pub unlocks_techs: Box<[i32]>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClaimTileCostRow {
    pub tile_count: i32,
    pub cost_per_tile: f32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExperienceRow {
    pub entity_id: u64,
    /// `(skill_id, xp quantity)` stacks.
    pub stacks: Box<[(i32, i32)]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillDescRow {
    pub id: i32,
    pub name: String,
    pub title: String,
    pub max_level: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PassiveCraftStatus {
    Queued,
    Processing,
    Complete,
}

impl PassiveCraftStatus {
    pub fn is_complete(self) -> bool {
        matches!(self, Self::Complete)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProgressiveActionRow {
    pub entity_id: u64,
    pub building_entity_id: u64,
    pub progress: i32,
    pub recipe_id: i32,
    pub craft_count: i32,
    pub owner_entity_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PassiveCraftRow {
    pub entity_id: u64,
    pub owner_entity_id: u64,
    pub recipe_id: i32,
    pub building_entity_id: u64,
    pub status: PassiveCraftStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CraftedItemStack {
    pub item_id: i32,
    pub quantity: i32,
    /// `Pocket::ITEM` or `Pocket::CARGO`.
    pub item_type: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CraftingRecipeDescRow {
    pub id: i32,
    pub actions_required: i32,
    pub crafted_item: Box<[CraftedItemStack]>,
}

// --- Decoders ---

/// Decode one BSATN row using the pre-resolved claim fields. The fields
/// slice comes from `fields_of(schema, CLAIM_TABLE)` resolved once per
/// shard (alongside `ClaimCols`) — we don't re-resolve per row.
pub fn decode_claim_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: ClaimCols,
    schema: &MirroredSchema,
) -> Result<ClaimRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(ClaimRow {
        entity_id: cell_u64(&cells[cols.entity_id], "claim.entity_id")?,
        owner_player_entity_id: cell_u64(
            &cells[cols.owner_player_entity_id],
            "claim.owner_player_entity_id",
        )?,
        owner_building_entity_id: cell_u64(
            &cells[cols.owner_building_entity_id],
            "claim.owner_building_entity_id",
        )?,
        name: cell_string(&cells[cols.name], "claim.name")?,
        neutral: cell_bool(&cells[cols.neutral], "claim.neutral")?,
    })
}

pub fn decode_building_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: BuildingCols,
    schema: &MirroredSchema,
) -> Result<BuildingRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(BuildingRow {
        entity_id: cell_u64(&cells[cols.entity_id], "building.entity_id")?,
        claim_entity_id: cell_u64(&cells[cols.claim_entity_id], "building.claim_entity_id")?,
        building_description_id: cell_i32(
            &cells[cols.building_description_id],
            "building.building_description_id",
        )?,
    })
}

pub fn decode_building_desc_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: BuildingDescCols,
    schema: &MirroredSchema,
) -> Result<BuildingDescRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(BuildingDescRow {
        id: cell_i32(&cells[cols.id], "building_desc.id")?,
        name: cell_string(&cells[cols.name], "building_desc.name")?,
        is_storage: functions_is_storage(&cells[cols.functions])?,
    })
}

/// `building_desc.functions` is an array of products; the BSATN decoder
/// renders it as `Cell::Jsonb`. A type is storage-capable when any entry
/// advertises item or cargo pockets.
fn functions_is_storage(cell: &Cell) -> Result<bool> {
    let json = cell_json(cell)?;
    let Value::Array(arr) = json else {
        bail!("building_desc.functions is not a JSON array: {json}");
    };
    for entry in arr {
        let Value::Object(obj) = entry else {
            continue;
        };
        let storage = obj
            .get("storage_slots")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        let cargo = obj.get("cargo_slots").and_then(Value::as_i64).unwrap_or(0);
        if storage > 0 || cargo > 0 {
            return Ok(true);
        }
    }
    Ok(false)
}

pub fn decode_building_nickname_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: BuildingNicknameCols,
    schema: &MirroredSchema,
) -> Result<BuildingNicknameRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(BuildingNicknameRow {
        entity_id: cell_u64(&cells[cols.entity_id], "building_nickname.entity_id")?,
        nickname: cell_string(&cells[cols.nickname], "building_nickname.nickname")?,
    })
}

pub fn decode_inventory_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: InventoryCols,
    schema: &MirroredSchema,
) -> Result<InventoryRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(InventoryRow {
        entity_id: cell_u64(&cells[cols.entity_id], "inventory.entity_id")?,
        pockets: decode_pockets(&cells[cols.pockets])?,
        inventory_index: cell_i32(&cells[cols.inventory_index], "inventory.inventory_index")?,
        cargo_index: cell_i32(&cells[cols.cargo_index], "inventory.cargo_index")?,
        owner_entity_id: cell_u64(&cells[cols.owner_entity_id], "inventory.owner_entity_id")?,
        player_owner_entity_id: cell_u64(
            &cells[cols.player_owner_entity_id],
            "inventory.player_owner_entity_id",
        )?,
    })
}

pub fn decode_location_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: LocationCols,
    schema: &MirroredSchema,
) -> Result<LocationRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(LocationRow {
        entity_id: cell_u64(&cells[cols.entity_id], "location.entity_id")?,
        x: cell_i32(&cells[cols.x], "location.x")?,
        z: cell_i32(&cells[cols.z], "location.z")?,
        dimension: cell_u32(&cells[cols.dimension], "location.dimension")?,
    })
}

pub fn decode_resource_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: ResourceCols,
    schema: &MirroredSchema,
) -> Result<ResourceRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(ResourceRow {
        entity_id: cell_u64(&cells[cols.entity_id], "resource.entity_id")?,
        resource_id: cell_i32(&cells[cols.resource_id], "resource.resource_id")?,
    })
}

pub fn decode_growth_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: GrowthCols,
    schema: &MirroredSchema,
) -> Result<GrowthRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(GrowthRow {
        entity_id: cell_u64(&cells[cols.entity_id], "growth.entity_id")?,
        end_timestamp_micros: decode_timestamp_micros(
            &cells[cols.end_timestamp],
            "growth.end_timestamp",
        )?,
        growth_recipe_id: cell_i32(&cells[cols.growth_recipe_id], "growth.growth_recipe_id")?,
    })
}

pub fn decode_storage_log_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: StorageLogCols,
    schema: &MirroredSchema,
) -> Result<StorageLogRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    let (action, item_id, item_type, quantity) = decode_action_log_data(&cells[cols.data])?;
    Ok(StorageLogRow {
        id: cell_u64(&cells[cols.id], "storage_log.id")?,
        storage_entity_id: cell_u64(&cells[cols.object_entity_id], "storage_log.object_entity_id")?,
        player_entity_id: cell_u64(
            &cells[cols.subject_entity_id],
            "storage_log.subject_entity_id",
        )?,
        player_username: cell_string(&cells[cols.subject_name], "storage_log.subject_name")?,
        action,
        item_id,
        item_type,
        quantity,
        timestamp_micros: decode_timestamp_micros(
            &cells[cols.timestamp],
            "storage_log.timestamp",
        )?,
        days_since_epoch: cell_i32(
            &cells[cols.days_since_epoch],
            "storage_log.days_since_epoch",
        )?,
    })
}

/// `ActionLogData` sum → (action, item_id, item_type, quantity).
/// Reserved uses `item1`'s stack fields.
fn decode_action_log_data(cell: &Cell) -> Result<(u8, i32, u8, i32)> {
    use crate::store::storage_log::{ACTION_DEPOSIT, ACTION_RESERVED, ACTION_WITHDRAW};

    let json = cell_json(cell)?;
    let Value::Object(obj) = json else {
        bail!("storage_log.data: expected object, got {json}");
    };
    if let Some(stack) = obj.get("DepositItem") {
        let (item_id, item_type, quantity) = decode_item_stack(stack, "DepositItem")?;
        return Ok((ACTION_DEPOSIT, item_id, item_type, quantity));
    }
    if let Some(stack) = obj.get("WithdrawItem") {
        let (item_id, item_type, quantity) = decode_item_stack(stack, "WithdrawItem")?;
        return Ok((ACTION_WITHDRAW, item_id, item_type, quantity));
    }
    if let Some(reserved) = obj.get("Reserved") {
        let Value::Object(r) = reserved else {
            bail!("storage_log.data.Reserved: expected object, got {reserved}");
        };
        let stack = r
            .get("item1")
            .ok_or_else(|| anyhow!("storage_log.data.Reserved: missing item1"))?;
        let (item_id, item_type, quantity) = decode_item_stack(stack, "Reserved.item1")?;
        return Ok((ACTION_RESERVED, item_id, item_type, quantity));
    }
    bail!("storage_log.data: unknown ActionLogData variant {obj:?}")
}

fn decode_item_stack(v: &Value, ctx: &str) -> Result<(i32, u8, i32)> {
    let Value::Object(obj) = v else {
        bail!("{ctx}: expected ItemStack object, got {v}");
    };
    let item_id = json_i32(obj.get("item_id"), &format!("{ctx}.item_id"))?;
    let quantity = json_i32(obj.get("quantity"), &format!("{ctx}.quantity"))?;
    let item_type = match obj.get("item_type") {
        Some(Value::Object(t)) if t.contains_key("Item") => Pocket::ITEM,
        Some(Value::Object(t)) if t.contains_key("Cargo") => Pocket::CARGO,
        other => bail!("{ctx}.item_type unexpected: {other:?}"),
    };
    Ok((item_id, item_type, quantity))
}

pub fn decode_dimension_network_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: DimensionNetworkCols,
    schema: &MirroredSchema,
) -> Result<DimensionNetworkRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(DimensionNetworkRow {
        entity_id: cell_u64(&cells[cols.entity_id], "dimension_network.entity_id")?,
        building_id: cell_u64(&cells[cols.building_id], "dimension_network.building_id")?,
        claim_entity_id: cell_u64(
            &cells[cols.claim_entity_id],
            "dimension_network.claim_entity_id",
        )?,
        rent_entity_id: cell_u64(
            &cells[cols.rent_entity_id],
            "dimension_network.rent_entity_id",
        )?,
        entrance_dimension_id: cell_u32(
            &cells[cols.entrance_dimension_id],
            "dimension_network.entrance_dimension_id",
        )?,
        is_collapsed: cell_bool(&cells[cols.is_collapsed], "dimension_network.is_collapsed")?,
    })
}

pub fn decode_player_username_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: PlayerUsernameCols,
    schema: &MirroredSchema,
) -> Result<PlayerUsernameRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(PlayerUsernameRow {
        entity_id: cell_u64(&cells[cols.entity_id], "player_username.entity_id")?,
        username: cell_string(&cells[cols.username], "player_username.username")?,
    })
}

pub fn decode_player_state_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: PlayerStateCols,
    schema: &MirroredSchema,
) -> Result<PlayerStateRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(PlayerStateRow {
        entity_id: cell_u64(&cells[cols.entity_id], "player_state.entity_id")?,
        sign_in_timestamp: cell_i32(
            &cells[cols.sign_in_timestamp],
            "player_state.sign_in_timestamp",
        )?,
        session_start_timestamp: cell_i32(
            &cells[cols.session_start_timestamp],
            "player_state.session_start_timestamp",
        )?,
        signed_in: cell_bool(&cells[cols.signed_in], "player_state.signed_in")?,
    })
}

pub fn decode_deployable_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: DeployableCols,
    schema: &MirroredSchema,
) -> Result<DeployableRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(DeployableRow {
        entity_id: cell_u64(&cells[cols.entity_id], "deployable.entity_id")?,
        owner_id: cell_u64(&cells[cols.owner_id], "deployable.owner_id")?,
        claim_entity_id: cell_u64(&cells[cols.claim_entity_id], "deployable.claim_entity_id")?,
        deployable_description_id: cell_i32(
            &cells[cols.deployable_description_id],
            "deployable.deployable_description_id",
        )?,
        nickname: cell_string(&cells[cols.nickname], "deployable.nickname")?,
    })
}

pub fn decode_deployable_desc_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: DeployableDescCols,
    schema: &MirroredSchema,
) -> Result<DeployableDescRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(DeployableDescRow {
        id: cell_i32(&cells[cols.id], "deployable_desc.id")?,
        name: cell_string(&cells[cols.name], "deployable_desc.name")?,
        kind: deployable_kind_from_cell(&cells[cols.deployable_type])?,
    })
}

fn deployable_kind_from_cell(cell: &Cell) -> Result<DeployableKind> {
    let json = cell_json(cell)?;
    let Value::Object(obj) = json else {
        bail!("deployable_type is not an object: {json}");
    };
    if obj.contains_key("Cart") {
        Ok(DeployableKind::Cart)
    } else if obj.contains_key("Cache") {
        Ok(DeployableKind::Cache)
    } else if obj.contains_key("Mount") {
        Ok(DeployableKind::Mount)
    } else {
        Ok(DeployableKind::Other)
    }
}

pub fn decode_player_housing_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: PlayerHousingCols,
    schema: &MirroredSchema,
) -> Result<PlayerHousingRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(PlayerHousingRow {
        entity_id: cell_u64(&cells[cols.entity_id], "player_housing.entity_id")?,
        entrance_building_entity_id: cell_u64(
            &cells[cols.entrance_building_entity_id],
            "player_housing.entrance_building_entity_id",
        )?,
        network_entity_id: cell_u64(
            &cells[cols.network_entity_id],
            "player_housing.network_entity_id",
        )?,
        rank: cell_i32(&cells[cols.rank], "player_housing.rank")?,
        is_empty: cell_bool(&cells[cols.is_empty], "player_housing.is_empty")?,
    })
}

pub fn decode_player_housing_desc_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: PlayerHousingDescCols,
    schema: &MirroredSchema,
) -> Result<PlayerHousingDescRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(PlayerHousingDescRow {
        secondary_knowledge_id: cell_i32(
            &cells[cols.secondary_knowledge_id],
            "player_housing_desc.secondary_knowledge_id",
        )?,
        rank: cell_i32(&cells[cols.rank], "player_housing_desc.rank")?,
        name: cell_string(&cells[cols.name], "player_housing_desc.name")?,
    })
}

pub fn decode_rent_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: RentCols,
    schema: &MirroredSchema,
) -> Result<RentRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(RentRow {
        entity_id: cell_u64(&cells[cols.entity_id], "rent.entity_id")?,
        dimension_network_id: cell_u64(
            &cells[cols.dimension_network_id],
            "rent.dimension_network_id",
        )?,
        claim_entity_id: cell_u64(&cells[cols.claim_entity_id], "rent.claim_entity_id")?,
        white_list: decode_u64_array(&cells[cols.white_list], "rent.white_list")?,
        active: cell_bool(&cells[cols.active], "rent.active")?,
    })
}

pub fn decode_claim_local_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: ClaimLocalCols,
    schema: &MirroredSchema,
) -> Result<ClaimLocalRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    let (has_location, location_x, location_z, location_dimension) =
        decode_optional_location(&cells[cols.location])?;
    Ok(ClaimLocalRow {
        entity_id: cell_u64(&cells[cols.entity_id], "claim_local.entity_id")?,
        supplies: cell_i32(&cells[cols.supplies], "claim_local.supplies")?,
        building_maintenance: cell_f32(
            &cells[cols.building_maintenance],
            "claim_local.building_maintenance",
        )?,
        num_tiles: cell_i32(&cells[cols.num_tiles], "claim_local.num_tiles")?,
        treasury: cell_u32(&cells[cols.treasury], "claim_local.treasury")?,
        supplies_purchase_threshold: cell_u32(
            &cells[cols.supplies_purchase_threshold],
            "claim_local.supplies_purchase_threshold",
        )?,
        supplies_purchase_price: cell_f32(
            &cells[cols.supplies_purchase_price],
            "claim_local.supplies_purchase_price",
        )?,
        location_x,
        location_z,
        location_dimension,
        has_location,
    })
}

pub fn decode_claim_member_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: ClaimMemberCols,
    schema: &MirroredSchema,
) -> Result<ClaimMemberRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(ClaimMemberRow {
        entity_id: cell_u64(&cells[cols.entity_id], "claim_member.entity_id")?,
        claim_entity_id: cell_u64(&cells[cols.claim_entity_id], "claim_member.claim_entity_id")?,
        player_entity_id: cell_u64(
            &cells[cols.player_entity_id],
            "claim_member.player_entity_id",
        )?,
        user_name: cell_string(&cells[cols.user_name], "claim_member.user_name")?,
        inventory_permission: cell_bool(
            &cells[cols.inventory_permission],
            "claim_member.inventory_permission",
        )?,
        build_permission: cell_bool(
            &cells[cols.build_permission],
            "claim_member.build_permission",
        )?,
        officer_permission: cell_bool(
            &cells[cols.officer_permission],
            "claim_member.officer_permission",
        )?,
        co_owner_permission: cell_bool(
            &cells[cols.co_owner_permission],
            "claim_member.co_owner_permission",
        )?,
    })
}

pub fn decode_claim_tech_state_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: ClaimTechStateCols,
    schema: &MirroredSchema,
) -> Result<ClaimTechStateRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(ClaimTechStateRow {
        entity_id: cell_u64(&cells[cols.entity_id], "claim_tech_state.entity_id")?,
        learned: decode_i32_array(&cells[cols.learned], "claim_tech_state.learned")?,
        researching: cell_i32(&cells[cols.researching], "claim_tech_state.researching")?,
        start_timestamp_micros: decode_timestamp_micros(
            &cells[cols.start_timestamp],
            "claim_tech_state.start_timestamp",
        )?,
    })
}

pub fn decode_claim_tech_desc_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: ClaimTechDescCols,
    schema: &MirroredSchema,
) -> Result<ClaimTechDescRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(ClaimTechDescRow {
        id: cell_i32(&cells[cols.id], "claim_tech_desc.id")?,
        name: cell_string(&cells[cols.name], "claim_tech_desc.name")?,
        description: cell_string(&cells[cols.description], "claim_tech_desc.description")?,
        tier: cell_i32(&cells[cols.tier], "claim_tech_desc.tier")?,
        tech_type: sum_variant_snake(&cells[cols.tech_type], "claim_tech_desc.tech_type")?,
        supplies_cost: cell_i32(&cells[cols.supplies_cost], "claim_tech_desc.supplies_cost")?,
        research_time: cell_i32(&cells[cols.research_time], "claim_tech_desc.research_time")?,
        requirements: decode_i32_array(&cells[cols.requirements], "claim_tech_desc.requirements")?,
        members: cell_i32(&cells[cols.members], "claim_tech_desc.members")?,
        area: cell_i32(&cells[cols.area], "claim_tech_desc.area")?,
        unlocks_techs: decode_i32_array(
            &cells[cols.unlocks_techs],
            "claim_tech_desc.unlocks_techs",
        )?,
    })
}

pub fn decode_claim_tile_cost_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: ClaimTileCostCols,
    schema: &MirroredSchema,
) -> Result<ClaimTileCostRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(ClaimTileCostRow {
        tile_count: cell_i32(&cells[cols.tile_count], "claim_tile_cost.tile_count")?,
        cost_per_tile: cell_f32(&cells[cols.cost_per_tile], "claim_tile_cost.cost_per_tile")?,
    })
}

pub fn decode_experience_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: ExperienceCols,
    schema: &MirroredSchema,
) -> Result<ExperienceRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(ExperienceRow {
        entity_id: cell_u64(&cells[cols.entity_id], "experience.entity_id")?,
        stacks: decode_experience_stacks(&cells[cols.experience_stacks])?,
    })
}

pub fn decode_skill_desc_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: SkillDescCols,
    schema: &MirroredSchema,
) -> Result<SkillDescRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(SkillDescRow {
        id: cell_i32(&cells[cols.id], "skill_desc.id")?,
        name: cell_string(&cells[cols.name], "skill_desc.name")?,
        title: cell_string(&cells[cols.title], "skill_desc.title")?,
        max_level: cell_i32(&cells[cols.max_level], "skill_desc.max_level")?,
    })
}

pub fn decode_progressive_action_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: ProgressiveActionCols,
    schema: &MirroredSchema,
) -> Result<ProgressiveActionRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(ProgressiveActionRow {
        entity_id: cell_u64(&cells[cols.entity_id], "progressive_action.entity_id")?,
        building_entity_id: cell_u64(
            &cells[cols.building_entity_id],
            "progressive_action.building_entity_id",
        )?,
        progress: cell_i32(&cells[cols.progress], "progressive_action.progress")?,
        recipe_id: cell_i32(&cells[cols.recipe_id], "progressive_action.recipe_id")?,
        craft_count: cell_i32(&cells[cols.craft_count], "progressive_action.craft_count")?,
        owner_entity_id: cell_u64(
            &cells[cols.owner_entity_id],
            "progressive_action.owner_entity_id",
        )?,
    })
}

pub fn decode_passive_craft_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: PassiveCraftCols,
    schema: &MirroredSchema,
) -> Result<PassiveCraftRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(PassiveCraftRow {
        entity_id: cell_u64(&cells[cols.entity_id], "passive_craft.entity_id")?,
        owner_entity_id: cell_u64(
            &cells[cols.owner_entity_id],
            "passive_craft.owner_entity_id",
        )?,
        recipe_id: cell_i32(&cells[cols.recipe_id], "passive_craft.recipe_id")?,
        building_entity_id: cell_u64(
            &cells[cols.building_entity_id],
            "passive_craft.building_entity_id",
        )?,
        status: decode_passive_craft_status(&cells[cols.status])?,
    })
}

pub fn decode_crafting_recipe_desc_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: CraftingRecipeDescCols,
    schema: &MirroredSchema,
) -> Result<CraftingRecipeDescRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(CraftingRecipeDescRow {
        id: cell_i32(&cells[cols.id], "crafting_recipe_desc.id")?,
        actions_required: cell_i32(
            &cells[cols.actions_required],
            "crafting_recipe_desc.actions_required",
        )?,
        crafted_item: decode_crafted_item_stacks(&cells[cols.crafted_item_stacks])?,
    })
}

fn decode_passive_craft_status(cell: &Cell) -> Result<PassiveCraftStatus> {
    let json = cell_json(cell)?;
    let Value::Object(obj) = json else {
        bail!("passive_craft.status: expected object, got {json}");
    };
    let key = obj
        .keys()
        .next()
        .ok_or_else(|| anyhow!("passive_craft.status: empty sum object"))?;
    match key.as_str() {
        "Queued" => Ok(PassiveCraftStatus::Queued),
        "Processing" => Ok(PassiveCraftStatus::Processing),
        "Complete" => Ok(PassiveCraftStatus::Complete),
        other => bail!("passive_craft.status: unknown variant {other}"),
    }
}

fn decode_crafted_item_stacks(cell: &Cell) -> Result<Box<[CraftedItemStack]>> {
    let json = cell_json(cell)?;
    let Value::Array(arr) = json else {
        bail!("crafted_item_stacks is not a JSON array: {json}");
    };
    let mut out = Vec::with_capacity(arr.len());
    for (i, entry) in arr.iter().enumerate() {
        let Value::Object(obj) = entry else {
            bail!("crafted_item_stacks[{i}] is not an object: {entry}");
        };
        let item_id = json_i32(
            obj.get("item_id"),
            &format!("crafted_item_stacks[{i}].item_id"),
        )?;
        let quantity = json_i32(
            obj.get("quantity"),
            &format!("crafted_item_stacks[{i}].quantity"),
        )?;
        let item_type = match obj.get("item_type") {
            Some(Value::Object(t)) if t.contains_key("Item") => Pocket::ITEM,
            Some(Value::Object(t)) if t.contains_key("Cargo") => Pocket::CARGO,
            other => bail!("crafted_item_stacks[{i}].item_type unexpected: {other:?}"),
        };
        out.push(CraftedItemStack {
            item_id,
            quantity,
            item_type,
        });
    }
    Ok(out.into())
}

/// `Array<U64>` is rendered as JSON array of hex-encoded LE byte strings.
fn decode_u64_array(cell: &Cell, ctx: &str) -> Result<Box<[u64]>> {
    let json = cell_json(cell)?;
    let Value::Array(arr) = json else {
        bail!("{ctx}: expected JSON array, got {json}");
    };
    let mut out = Vec::with_capacity(arr.len());
    for (i, v) in arr.iter().enumerate() {
        let s = v
            .as_str()
            .ok_or_else(|| anyhow!("{ctx}[{i}]: expected hex string, got {v}"))?;
        let bytes = hex::decode(s).map_err(|e| anyhow!("{ctx}[{i}]: hex decode: {e}"))?;
        if bytes.len() != 8 {
            bail!("{ctx}[{i}]: expected 8 bytes, got {}", bytes.len());
        }
        let mut arr8 = [0u8; 8];
        arr8.copy_from_slice(&bytes);
        out.push(u64::from_le_bytes(arr8));
    }
    Ok(out.into())
}

/// Walk a `pockets` JSON array (as produced by `relay_protocol::bsatn`'s
/// fallback for nested sum-product arrays) into a typed `Box<[Pocket]>`.
/// The JSON only exists transiently during decode — the stored row carries
/// the typed version.
///
/// The decoder renders the upstream `pockets: Array<Pocket>` shape
/// (where `Pocket = Product{ volume: I32, contents: Option<Contents>,
/// locked: Bool }` and `Contents = Product{ item_id: I32, quantity: I32,
/// item_type: Sum{Item, Cargo}, durability: Option<I32> }`) as a JSON
/// array of objects:
///
/// ```json
/// [
///   {"volume": 100, "contents": {"some": {"item_id": 1, "quantity": 50, "item_type": {"Item": {}}, "durability": {"some": 100}}}, "locked": false},
///   {"volume": 0,   "contents": {"none": {}}, "locked": false}
/// ]
/// ```
fn decode_pockets(cell: &Cell) -> Result<Box<[Pocket]>> {
    let json = cell_json(cell)?;
    let Value::Array(arr) = json else {
        bail!("pockets is not a JSON array: {json}");
    };
    let mut out = Vec::with_capacity(arr.len());
    for pocket_val in arr {
        let Value::Object(obj) = pocket_val else {
            bail!("pocket is not an object: {pocket_val}");
        };
        let volume = json_i32(obj.get("volume"), "pocket.volume")?;
        let (has_contents, item_id, quantity, item_type, has_durability, durability) =
            match obj.get("contents") {
                // `relay_protocol::bsatn` renders Option<T> as a one-key
                // object: `{"some": payload}` or `{"none": {}}`.
                Some(Value::Object(c)) if c.contains_key("some") => {
                    let inner = &c["some"];
                    let Value::Object(contents) = inner else {
                        bail!("contents.some is not an object: {inner}");
                    };
                    let item_id = json_i32(contents.get("item_id"), "contents.item_id")?;
                    let quantity = json_i32(contents.get("quantity"), "contents.quantity")?;
                    let item_type = match contents.get("item_type") {
                        Some(Value::Object(t)) if t.contains_key("Item") => Pocket::ITEM,
                        Some(Value::Object(t)) if t.contains_key("Cargo") => Pocket::CARGO,
                        other => bail!("contents.item_type unexpected: {other:?}"),
                    };
                    let (has_durability, durability) = match contents.get("durability") {
                        Some(Value::Object(d)) if d.contains_key("some") => {
                            let v = d["some"].as_i64().and_then(|n| i32::try_from(n).ok());
                            (true, v.unwrap_or(0))
                        }
                        _ => (false, 0),
                    };
                    (
                        true,
                        item_id,
                        quantity,
                        item_type,
                        has_durability,
                        durability,
                    )
                }
                _ => (false, 0, 0, Pocket::ITEM, false, 0),
            };
        out.push(Pocket {
            volume,
            has_contents,
            item_id,
            quantity,
            item_type,
            has_durability,
            durability,
        });
    }
    Ok(out.into())
}

fn json_i32(v: Option<&Value>, ctx: &str) -> Result<i32> {
    v.and_then(Value::as_i64)
        .and_then(|n| i32::try_from(n).ok())
        .ok_or_else(|| anyhow!("{ctx}: missing or not i32"))
}

// --- Cell accessors ---

/// Pull the inner JSON value out of `Cell::Jsonb`. Errors on any other variant.
fn cell_json(cell: &Cell) -> Result<&Value> {
    match cell {
        Cell::Jsonb(v) => Ok(v),
        _ => bail!("expected Jsonb, got {cell:?}"),
    }
}

fn cell_f32(cell: &Cell, ctx: &str) -> Result<f32> {
    match cell {
        Cell::Real(Some(n)) => Ok(*n),
        Cell::Real(None) => bail!("{ctx}: Real is NULL"),
        Cell::DoublePrecision(Some(n)) => Ok(*n as f32),
        other => bail!("{ctx}: expected Real, got {other:?}"),
    }
}

/// `Option<{x,z,dimension}>` rendered as `{"some":{...}}` / `{"none":{}}`.
fn decode_optional_location(cell: &Cell) -> Result<(bool, i32, i32, u32)> {
    let json = cell_json(cell)?;
    let Value::Object(obj) = json else {
        bail!("claim_local.location is not an object: {json}");
    };
    if let Some(inner) = obj.get("some") {
        let Value::Object(loc) = inner else {
            bail!("claim_local.location.some is not an object: {inner}");
        };
        let x = json_i32(loc.get("x"), "location.x")?;
        let z = json_i32(loc.get("z"), "location.z")?;
        let dimension = loc
            .get("dimension")
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow!("location.dimension missing"))?;
        let dimension =
            u32::try_from(dimension).map_err(|_| anyhow!("location.dimension overflow"))?;
        return Ok((true, x, z, dimension));
    }
    Ok((false, 0, 0, 0))
}

fn decode_i32_array(cell: &Cell, ctx: &str) -> Result<Box<[i32]>> {
    let json = cell_json(cell)?;
    let Value::Array(arr) = json else {
        bail!("{ctx}: expected JSON array, got {json}");
    };
    let mut out = Vec::with_capacity(arr.len());
    for (i, v) in arr.iter().enumerate() {
        let n = v
            .as_i64()
            .ok_or_else(|| anyhow!("{ctx}[{i}]: expected i64, got {v}"))?;
        out.push(i32::try_from(n).map_err(|_| anyhow!("{ctx}[{i}]: i32 overflow"))?);
    }
    Ok(out.into())
}

fn decode_timestamp_micros(cell: &Cell, ctx: &str) -> Result<i64> {
    match cell {
        Cell::Bigint(Some(n)) => Ok(*n),
        Cell::Jsonb(Value::Object(obj)) => {
            if let Some(v) = obj.get("__timestamp_micros_since_unix_epoch__") {
                return v
                    .as_i64()
                    .ok_or_else(|| anyhow!("{ctx}: timestamp not i64"));
            }
            bail!("{ctx}: unexpected timestamp object {obj:?}")
        }
        other => bail!("{ctx}: expected timestamp, got {other:?}"),
    }
}

/// Sum unit variants decode as `{"VariantName":{}}` → snake_case label.
fn sum_variant_snake(cell: &Cell, ctx: &str) -> Result<String> {
    let json = cell_json(cell)?;
    let Value::Object(obj) = json else {
        bail!("{ctx}: expected object, got {json}");
    };
    let key = obj
        .keys()
        .next()
        .ok_or_else(|| anyhow!("{ctx}: empty sum object"))?;
    Ok(pascal_to_snake(key))
}

fn pascal_to_snake(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 4);
    for (i, ch) in s.chars().enumerate() {
        if ch.is_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.extend(ch.to_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

fn decode_experience_stacks(cell: &Cell) -> Result<Box<[(i32, i32)]>> {
    let json = cell_json(cell)?;
    let Value::Array(arr) = json else {
        bail!("experience_stacks is not a JSON array: {json}");
    };
    let mut out = Vec::with_capacity(arr.len());
    for entry in arr {
        let Value::Object(obj) = entry else {
            bail!("experience stack is not an object: {entry}");
        };
        let skill_id = json_i32(obj.get("skill_id"), "stack.skill_id")?;
        let quantity = json_i32(obj.get("quantity"), "stack.quantity")?;
        out.push((skill_id, quantity));
    }
    Ok(out.into())
}

/// Read a `Cell::Bytea(8 bytes LE)` as `u64`. The relay-protocol decoder
/// intentionally maps U64 to Bytea to avoid NUMERIC; we recover the u64.
fn cell_u64(cell: &Cell, ctx: &str) -> Result<u64> {
    let bytes = match cell {
        Cell::Bytea(Some(b)) => b,
        Cell::Bytea(None) => bail!("{ctx}: Bytea is NULL"),
        _ => bail!("{ctx}: expected Bytea, got {cell:?}"),
    };
    if bytes.len() != 8 {
        bail!("{ctx}: expected 8-byte Bytea, got {} bytes", bytes.len());
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(bytes);
    Ok(u64::from_le_bytes(arr))
}

fn cell_i32(cell: &Cell, ctx: &str) -> Result<i32> {
    match cell {
        Cell::Integer(Some(n)) => Ok(*n),
        _ => bail!("{ctx}: expected Integer, got {cell:?}"),
    }
}

/// U32 is mapped to `Cell::Bigint` by relay-protocol.
fn cell_u32(cell: &Cell, ctx: &str) -> Result<u32> {
    match cell {
        Cell::Bigint(Some(n)) => {
            u32::try_from(*n).map_err(|_| anyhow!("{ctx}: Bigint {n} out of u32 range"))
        }
        _ => bail!("{ctx}: expected Bigint, got {cell:?}"),
    }
}

fn cell_string(cell: &Cell, ctx: &str) -> Result<String> {
    match cell {
        Cell::Text(Some(s)) => Ok(s.clone()),
        _ => bail!("{ctx}: expected Text, got {cell:?}"),
    }
}

fn cell_bool(cell: &Cell, ctx: &str) -> Result<bool> {
    match cell {
        Cell::Bool(Some(b)) => Ok(*b),
        _ => bail!("{ctx}: expected Bool, got {cell:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn decode_pockets_walks_mixed_array() {
        // Matches the shape produced by relay_protocol::bsatn::decode_json
        // for `Array<Pocket>` where Pocket has `contents: Option<...>`:
        // each option renders as `{"some": payload}` or `{"none": {}}`,
        // and the Item/Cargo sum renders as `{"Item": {}}` / `{"Cargo": {}}`.
        let cell = Cell::Jsonb(json!([
            {"volume": 100,  "contents": {"some": {"item_id": 1020003, "quantity": 50, "item_type": {"Item": {}},  "durability": {"some": 100}}}, "locked": false},
            {"volume": 0,    "contents": {"none": {}},                                                                                              "locked": false},
            {"volume": 6000, "contents": {"some": {"item_id": 5001,    "quantity": 1,  "item_type": {"Cargo": {}}, "durability": {"none": {}}}},  "locked": false}
        ]));
        let pockets = decode_pockets(&cell).unwrap();
        assert_eq!(pockets.len(), 3);
        assert!(pockets[0].has_contents);
        assert_eq!(pockets[0].item_id, 1020003);
        assert_eq!(pockets[0].quantity, 50);
        assert_eq!(pockets[0].item_type, Pocket::ITEM);
        assert!(pockets[0].has_durability);
        assert_eq!(pockets[0].durability, 100);

        assert!(!pockets[1].has_contents);

        assert!(pockets[2].has_contents);
        assert_eq!(pockets[2].item_id, 5001);
        assert_eq!(pockets[2].item_type, Pocket::CARGO);
        assert!(!pockets[2].has_durability);
    }

    #[test]
    fn decode_pockets_handles_empty_array() {
        let cell = Cell::Jsonb(json!([]));
        let pockets = decode_pockets(&cell).unwrap();
        assert!(pockets.is_empty());
    }

    #[test]
    fn decode_pockets_rejects_non_array() {
        let cell = Cell::Jsonb(json!({"not": "an array"}));
        assert!(decode_pockets(&cell).is_err());
    }

    #[test]
    fn decode_pockets_rejects_unknown_item_type() {
        let cell = Cell::Jsonb(json!([
            {"volume": 1, "contents": {"some": {"item_id": 1, "quantity": 1, "item_type": {"Quest": {}}, "durability": {"none": {}}}}, "locked": false}
        ]));
        let err = decode_pockets(&cell).unwrap_err();
        assert!(err.to_string().contains("item_type"));
    }

    #[test]
    fn decode_crafted_item_stacks_walks_item_and_cargo() {
        let cell = Cell::Jsonb(json!([
            {"item_id": 11006, "quantity": 10, "item_type": {"Item": {}}, "durability": {"none": {}}},
            {"item_id": 5001, "quantity": 1, "item_type": {"Cargo": {}}, "durability": {"some": 0}}
        ]));
        let stacks = decode_crafted_item_stacks(&cell).unwrap();
        assert_eq!(stacks.len(), 2);
        assert_eq!(stacks[0].item_id, 11006);
        assert_eq!(stacks[0].quantity, 10);
        assert_eq!(stacks[0].item_type, Pocket::ITEM);
        assert_eq!(stacks[1].item_id, 5001);
        assert_eq!(stacks[1].item_type, Pocket::CARGO);
    }

    #[test]
    fn functions_is_storage_detects_slots() {
        let storage = Cell::Jsonb(json!([
            {"function_type": 3, "storage_slots": 18, "cargo_slots": 0}
        ]));
        assert!(functions_is_storage(&storage).unwrap());

        let cargo = Cell::Jsonb(json!([
            {"function_type": 4, "storage_slots": 0, "cargo_slots": 8}
        ]));
        assert!(functions_is_storage(&cargo).unwrap());

        let totem = Cell::Jsonb(json!([
            {"function_type": 28, "storage_slots": 0, "cargo_slots": 0}
        ]));
        assert!(!functions_is_storage(&totem).unwrap());

        let empty = Cell::Jsonb(json!([]));
        assert!(!functions_is_storage(&empty).unwrap());
    }
}
