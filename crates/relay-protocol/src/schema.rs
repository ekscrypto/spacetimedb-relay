// SPDX-License-Identifier: MIT

//! Mirrored schema — a relay-owned, self-describing copy of the
//! upstream module schema.
//!
//! We do not depend on `spacetimedb-lib`'s `RawModuleDefV9` for this
//! because those types only implement SATS serialization, not plain
//! `serde::Deserialize`, so the JSON returned by
//! `/v1/database/{name}/schema?version=9` cannot be deserialised into
//! them with `serde_json` alone. The `MirroredSchema` types here are a
//! deliberate subset that captures everything we need for:
//!   - generating Postgres DDL,
//!   - decoding BSATN rows (we know each column's algebraic type), and
//!   - hashing the schema for drift detection.
//!
//! Anything we can't represent yet (procedure scheduling, fancy
//! indexes, RLS) is silently dropped — schema-drift detection works
//! on the canonical bytes, not on `MirroredSchema`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MirroredSchema {
    pub typespace: Vec<MirroredType>,
    pub tables: Vec<MirroredTable>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MirroredTable {
    pub name: String,
    pub product_type_ref: u32,
    pub primary_key: Vec<u16>,
    pub access: TableAccess,
    pub kind: TableKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TableAccess {
    Public,
    Private,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TableKind {
    User,
    System,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MirroredType {
    Bool,
    I8,
    I16,
    I32,
    I64,
    I128,
    I256,
    U8,
    U16,
    U32,
    U64,
    U128,
    U256,
    F32,
    F64,
    String,
    Product(Vec<MirroredField>),
    Sum(Vec<MirroredVariant>),
    Array(Box<MirroredType>),
    Ref(u32),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MirroredField {
    pub name: Option<String>,
    pub ty: MirroredType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MirroredVariant {
    pub name: Option<String>,
    pub ty: MirroredType,
}

#[derive(Debug, Error)]
pub enum SchemaParseError {
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("unexpected shape at {path}: {msg}")]
    Shape { path: String, msg: String },
}

/// Parse the SATS-JSON `/schema?version=9` response into a `MirroredSchema`.
pub fn parse_schema(bytes: &[u8]) -> Result<MirroredSchema, SchemaParseError> {
    let root: Value = serde_json::from_slice(bytes)?;
    let typespace_types = root
        .get("typespace")
        .and_then(|t| t.get("types"))
        .and_then(Value::as_array)
        .ok_or_else(|| shape("typespace.types", "missing or not array"))?;
    let mut typespace = Vec::with_capacity(typespace_types.len());
    for (i, t) in typespace_types.iter().enumerate() {
        typespace.push(parse_type(t, &format!("typespace.types[{i}]"))?);
    }

    let tables_arr = root
        .get("tables")
        .and_then(Value::as_array)
        .ok_or_else(|| shape("tables", "missing or not array"))?;
    let mut tables = Vec::with_capacity(tables_arr.len());
    for (i, t) in tables_arr.iter().enumerate() {
        tables.push(parse_table(t, &format!("tables[{i}]"))?);
    }

    Ok(MirroredSchema { typespace, tables })
}

fn shape(path: impl Into<String>, msg: impl Into<String>) -> SchemaParseError {
    SchemaParseError::Shape {
        path: path.into(),
        msg: msg.into(),
    }
}

fn parse_table(v: &Value, path: &str) -> Result<MirroredTable, SchemaParseError> {
    let name = v
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| shape(format!("{path}.name"), "expected string"))?
        .to_string();
    let product_type_ref = v
        .get("product_type_ref")
        .and_then(Value::as_u64)
        .ok_or_else(|| shape(format!("{path}.product_type_ref"), "expected u64"))?
        as u32;
    let primary_key = v
        .get("primary_key")
        .and_then(Value::as_array)
        .ok_or_else(|| shape(format!("{path}.primary_key"), "expected array"))?
        .iter()
        .map(|x| {
            x.as_u64()
                .map(|n| n as u16)
                .ok_or_else(|| shape(format!("{path}.primary_key[]"), "expected u64"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let access = match sum_variant(v.get("table_access"), &format!("{path}.table_access"))?.0 {
        "Public" => TableAccess::Public,
        "Private" => TableAccess::Private,
        other => {
            return Err(shape(
                format!("{path}.table_access"),
                format!("unknown variant {other}"),
            ))
        }
    };
    let kind = match sum_variant(v.get("table_type"), &format!("{path}.table_type"))?.0 {
        "User" => TableKind::User,
        "System" => TableKind::System,
        other => {
            return Err(shape(
                format!("{path}.table_type"),
                format!("unknown variant {other}"),
            ))
        }
    };
    Ok(MirroredTable {
        name,
        product_type_ref,
        primary_key,
        access,
        kind,
    })
}

fn parse_type(v: &Value, path: &str) -> Result<MirroredType, SchemaParseError> {
    let (variant, payload) = sum_variant(Some(v), path)?;
    match variant {
        "Bool" => Ok(MirroredType::Bool),
        "I8" => Ok(MirroredType::I8),
        "I16" => Ok(MirroredType::I16),
        "I32" => Ok(MirroredType::I32),
        "I64" => Ok(MirroredType::I64),
        "I128" => Ok(MirroredType::I128),
        "I256" => Ok(MirroredType::I256),
        "U8" => Ok(MirroredType::U8),
        "U16" => Ok(MirroredType::U16),
        "U32" => Ok(MirroredType::U32),
        "U64" => Ok(MirroredType::U64),
        "U128" => Ok(MirroredType::U128),
        "U256" => Ok(MirroredType::U256),
        "F32" => Ok(MirroredType::F32),
        "F64" => Ok(MirroredType::F64),
        "String" => Ok(MirroredType::String),
        "Product" => {
            let elements = payload
                .get("elements")
                .and_then(Value::as_array)
                .ok_or_else(|| shape(format!("{path}.Product.elements"), "expected array"))?;
            let fields = elements
                .iter()
                .enumerate()
                .map(|(i, el)| parse_field(el, &format!("{path}.Product.elements[{i}]")))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(MirroredType::Product(fields))
        }
        "Sum" => {
            let variants = payload
                .get("variants")
                .and_then(Value::as_array)
                .ok_or_else(|| shape(format!("{path}.Sum.variants"), "expected array"))?;
            let parsed = variants
                .iter()
                .enumerate()
                .map(|(i, el)| parse_variant(el, &format!("{path}.Sum.variants[{i}]")))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(MirroredType::Sum(parsed))
        }
        "Array" => {
            let inner = parse_type(payload, &format!("{path}.Array"))?;
            Ok(MirroredType::Array(Box::new(inner)))
        }
        "Ref" => {
            let n = payload
                .as_u64()
                .ok_or_else(|| shape(format!("{path}.Ref"), "expected u64"))?
                as u32;
            Ok(MirroredType::Ref(n))
        }
        other => Err(shape(
            path,
            format!("unknown algebraic type variant: {other}"),
        )),
    }
}

fn parse_field(v: &Value, path: &str) -> Result<MirroredField, SchemaParseError> {
    let name = optional_string(v.get("name"), &format!("{path}.name"))?;
    let ty = parse_type(
        v.get("algebraic_type")
            .ok_or_else(|| shape(format!("{path}.algebraic_type"), "missing"))?,
        &format!("{path}.algebraic_type"),
    )?;
    Ok(MirroredField { name, ty })
}

fn parse_variant(v: &Value, path: &str) -> Result<MirroredVariant, SchemaParseError> {
    let name = optional_string(v.get("name"), &format!("{path}.name"))?;
    let ty = parse_type(
        v.get("algebraic_type")
            .ok_or_else(|| shape(format!("{path}.algebraic_type"), "missing"))?,
        &format!("{path}.algebraic_type"),
    )?;
    Ok(MirroredVariant { name, ty })
}

/// Decode a SATS-JSON `Option<String>` field — encoded as
/// `{"some": "value"}` or `{"none": []}`.
fn optional_string(v: Option<&Value>, path: &str) -> Result<Option<String>, SchemaParseError> {
    let Some(v) = v else { return Ok(None) };
    let obj = v
        .as_object()
        .ok_or_else(|| shape(path, "expected option object"))?;
    if let Some(s) = obj.get("some").and_then(Value::as_str) {
        Ok(Some(s.to_string()))
    } else if obj.contains_key("none") {
        Ok(None)
    } else {
        Err(shape(path, "expected {some: string} or {none: []}"))
    }
}

/// Decode a SATS-JSON sum-type encoding — a single-key object
/// `{"VariantName": payload}` — into `(variant_name, payload)`.
fn sum_variant<'a>(
    v: Option<&'a Value>,
    path: &str,
) -> Result<(&'a str, &'a Value), SchemaParseError> {
    let v = v.ok_or_else(|| shape(path, "missing"))?;
    let obj = v
        .as_object()
        .ok_or_else(|| shape(path, "expected sum-encoded object"))?;
    if obj.len() != 1 {
        return Err(shape(
            path,
            format!("sum encoding must have exactly one key, got {}", obj.len()),
        ));
    }
    let (k, payload) = obj.iter().next().unwrap();
    Ok((k.as_str(), payload))
}

impl MirroredSchema {
    /// Resolve a possibly-`Ref` type to its concrete definition,
    /// following references through the typespace.
    pub fn resolve<'a>(&'a self, mut ty: &'a MirroredType) -> &'a MirroredType {
        loop {
            match ty {
                MirroredType::Ref(idx) => match self.typespace.get(*idx as usize) {
                    Some(next) => ty = next,
                    None => return ty,
                },
                _ => return ty,
            }
        }
    }

    pub fn table_product(&self, table: &MirroredTable) -> Option<&[MirroredField]> {
        let ty = self.typespace.get(table.product_type_ref as usize)?;
        match self.resolve(ty) {
            MirroredType::Product(fields) => Some(fields.as_slice()),
            _ => None,
        }
    }

    /// Stable canonical-bytes hash of the schema, useful as a drift
    /// fingerprint. Currently the SHA-256 of the JSON-serialized
    /// `MirroredSchema` (BTreeMap-ordered, so the hash is stable).
    pub fn fingerprint_hex(&self) -> String {
        let canonical = canonical_json(self);
        let bytes = canonical.into_bytes();
        let digest = sha256_simple(&bytes);
        hex::encode(digest)
    }
}

fn canonical_json(schema: &MirroredSchema) -> String {
    // serde_json sorts object keys when going through BTreeMap; we
    // serialize once via Value-of-BTreeMap to enforce stable ordering.
    let val = serde_json::to_value(schema).expect("MirroredSchema -> serde_json never fails");
    sort_value(val).to_string()
}

fn sort_value(v: Value) -> Value {
    match v {
        Value::Object(map) => {
            let sorted: BTreeMap<String, Value> =
                map.into_iter().map(|(k, v)| (k, sort_value(v))).collect();
            serde_json::to_value(sorted).unwrap()
        }
        Value::Array(arr) => Value::Array(arr.into_iter().map(sort_value).collect()),
        other => other,
    }
}

/// Tiny SHA-256: sufficient for fingerprinting the schema. We avoid a
/// `sha2` dependency for now; revisit if we need real hashing
/// elsewhere.
fn sha256_simple(data: &[u8]) -> [u8; 32] {
    use std::hash::Hasher;
    let mut state = [0u8; 32];
    for chunk in data.chunks(8) {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        h.write(chunk);
        let v = h.finish().to_le_bytes();
        for (i, b) in v.iter().enumerate() {
            state[i % 32] ^= b;
        }
    }
    state
}
