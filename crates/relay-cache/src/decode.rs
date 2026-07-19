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
pub const BUILDING_TABLE: &str = "building_state";
pub const INVENTORY_TABLE: &str = "inventory_state";
pub const BUILDING_DESC_TABLE: &str = "building_desc";
pub const BUILDING_NICKNAME_TABLE: &str = "building_nickname_state";
pub const LOCATION_TABLE: &str = "location_state";
pub const DIMENSION_NETWORK_TABLE: &str = "dimension_network_state";
pub const PLAYER_USERNAME_TABLE: &str = "player_username_state";
pub const DEPLOYABLE_TABLE: &str = "deployable_state";
pub const DEPLOYABLE_DESC_TABLE: &str = "deployable_desc";
pub const PLAYER_HOUSING_TABLE: &str = "player_housing_state";
pub const PLAYER_HOUSING_DESC_TABLE: &str = "player_housing_desc";
pub const RENT_TABLE: &str = "rent_state";

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

/// Resolved column indices for `location_state` (we only keep entity + dimension).
#[derive(Clone, Copy)]
pub struct LocationCols {
    pub entity_id: usize,
    pub dimension: usize,
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
    pub building: BuildingCols,
    pub inventory: InventoryCols,
    pub building_desc: BuildingDescCols,
    pub building_nickname: BuildingNicknameCols,
    pub location: LocationCols,
    pub dimension_network: DimensionNetworkCols,
    pub player_username: PlayerUsernameCols,
    pub deployable: DeployableCols,
    pub deployable_desc: DeployableDescCols,
    pub player_housing: PlayerHousingCols,
    pub player_housing_desc: PlayerHousingDescCols,
    pub rent: RentCols,
}

/// Resolve column indices for the tables we hold. Errors if any expected
/// column is missing — a sign of upstream schema drift.
pub fn resolve_cols(schema: &MirroredSchema) -> Result<ColMaps> {
    Ok(ColMaps {
        claim: resolve_claim_cols(schema)?,
        building: resolve_building_cols(schema)?,
        inventory: resolve_inventory_cols(schema)?,
        building_desc: resolve_building_desc_cols(schema)?,
        building_nickname: resolve_building_nickname_cols(schema)?,
        location: resolve_location_cols(schema)?,
        dimension_network: resolve_dimension_network_cols(schema)?,
        player_username: resolve_player_username_cols(schema)?,
        deployable: resolve_deployable_cols(schema)?,
        deployable_desc: resolve_deployable_desc_cols(schema)?,
        player_housing: resolve_player_housing_cols(schema)?,
        player_housing_desc: resolve_player_housing_desc_cols(schema)?,
        rent: resolve_rent_cols(schema)?,
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
        dimension: find_field(f, "dimension", LOCATION_TABLE)?,
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
        secondary_knowledge_id: find_field(
            f,
            "secondary_knowledge_id",
            PLAYER_HOUSING_DESC_TABLE,
        )?,
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

pub fn decode_location_dim_with_fields(
    row: &[u8],
    fields: &[MirroredField],
    cols: LocationCols,
    schema: &MirroredSchema,
) -> Result<LocationDimRow> {
    let cells = bsatn::decode_row(row, fields, schema).map_err(|e| anyhow!("bsatn: {e}"))?;
    Ok(LocationDimRow {
        entity_id: cell_u64(&cells[cols.entity_id], "location.entity_id")?,
        dimension: cell_u32(&cells[cols.dimension], "location.dimension")?,
    })
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
