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

/// Per-shard bundle of column indices. Built once at shard init from the
/// shared schema.
pub struct ColMaps {
    pub claim: ClaimCols,
    pub building: BuildingCols,
    pub inventory: InventoryCols,
    pub building_desc: BuildingDescCols,
    pub building_nickname: BuildingNicknameCols,
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
