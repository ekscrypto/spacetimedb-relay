// SPDX-License-Identifier: MIT

//! Dynamic BSATN row decoder.
//!
//! Given a list of `MirroredField`s and the BSATN-encoded bytes of a
//! row, produce a `Vec<Cell>` matching the column order. Output cells
//! are Postgres-bindable values (or `Jsonb` fallbacks for types we
//! can't natively map).
//!
//! BSATN wire format (subset we need):
//!   - Primitive integers: little-endian fixed-size.
//!   - Bool: 1 byte (0 or 1).
//!   - Strings and byte arrays: u32 LE length + payload.
//!   - Product: each field encoded in order, no framing.
//!   - Sum: u8 discriminant + payload of selected variant.
//!   - Array<T>: u32 LE count + count copies of T.
//!   - Ref(n): dereferenced through the typespace before reading.
//!
//! Mapping rules (resolved type → Cell):
//!   - Wrapper Product `{ __field__: T }` is unwrapped to T (matches
//!     the DDL layer's unwrap of Identity / Timestamp / ConnectionId).
//!   - Sum `{ some, none }` becomes `Option<inner>`.
//!   - Other Sum / Product / Array fall back to the JSON
//!     representation of the decoded value.

use std::io::Read;

use bytes::Bytes;
use serde_json::{json, Value};
use thiserror::Error;

use crate::schema::{MirroredField, MirroredSchema, MirroredType, MirroredVariant};

/// A decoded row plus the raw BSATN bytes it came from.
///
/// We retain the raw bytes so the relay can forward rows to downstream
/// clients without having to re-encode from typed columns. The `cells`
/// values are used for primary-key lookups (DELETE) and for queries
/// the relay needs to evaluate locally.
#[derive(Debug, Clone)]
pub struct DecodedRow {
    pub cells: Vec<Cell>,
    pub bsatn: Bytes,
}

#[derive(Debug, Clone)]
pub enum Cell {
    Bool(Option<bool>),
    Smallint(Option<i16>),
    Integer(Option<i32>),
    Bigint(Option<i64>),
    Real(Option<f32>),
    DoublePrecision(Option<f64>),
    Bytea(Option<Vec<u8>>),
    Text(Option<String>),
    Jsonb(Value),
}

#[derive(Debug, Error)]
pub enum BsatnError {
    #[error("unexpected end of buffer at {context}")]
    Eof { context: String },
    #[error("invalid bool byte {0}")]
    InvalidBool(u8),
    #[error("invalid utf8 in string: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("sum discriminant {tag} >= variants.len() {len}")]
    SumOutOfRange { tag: u32, len: usize },
    #[error("ref index {0} out of typespace bounds")]
    RefOutOfRange(u32),
}

/// Decode one BSATN-encoded product row.
pub fn decode_row(
    mut bytes: &[u8],
    fields: &[MirroredField],
    schema: &MirroredSchema,
) -> Result<Vec<Cell>, BsatnError> {
    let mut out = Vec::with_capacity(fields.len());
    for field in fields {
        out.push(decode_cell(&mut bytes, &field.ty, schema)?);
    }
    Ok(out)
}

/// Compute the byte slice each top-level field of a BSATN-encoded
/// product occupies. Used by the engine's projection path to extract
/// a subset of columns without re-encoding.
pub fn field_byte_ranges<'a>(
    bytes: &'a [u8],
    fields: &[MirroredField],
    schema: &MirroredSchema,
) -> Result<Vec<&'a [u8]>, BsatnError> {
    let mut cursor: &[u8] = bytes;
    let mut out = Vec::with_capacity(fields.len());
    for field in fields {
        let before = cursor.len();
        let _ = decode_cell(&mut cursor, &field.ty, schema)?;
        let consumed = before - cursor.len();
        let start = bytes.len() - before;
        out.push(&bytes[start..start + consumed]);
    }
    Ok(out)
}

fn decode_cell(
    bytes: &mut &[u8],
    ty: &MirroredType,
    schema: &MirroredSchema,
) -> Result<Cell, BsatnError> {
    let resolved = schema.resolve(ty);
    match resolved {
        MirroredType::Bool => {
            let b = read_u8(bytes, "bool")?;
            match b {
                0 => Ok(Cell::Bool(Some(false))),
                1 => Ok(Cell::Bool(Some(true))),
                other => Err(BsatnError::InvalidBool(other)),
            }
        }
        MirroredType::I8 => Ok(Cell::Smallint(Some(read_u8(bytes, "i8")? as i8 as i16))),
        MirroredType::I16 => Ok(Cell::Smallint(Some(read_i16_le(bytes)?))),
        MirroredType::I32 => Ok(Cell::Integer(Some(read_i32_le(bytes)?))),
        MirroredType::I64 => Ok(Cell::Bigint(Some(read_i64_le(bytes)?))),
        MirroredType::U8 => Ok(Cell::Smallint(Some(read_u8(bytes, "u8")? as i16))),
        MirroredType::U16 => Ok(Cell::Integer(Some(read_u16_le(bytes)? as i32))),
        MirroredType::U32 => Ok(Cell::Bigint(Some(read_u32_le(bytes)? as i64))),
        MirroredType::F32 => Ok(Cell::Real(Some(f32::from_le_bytes(read_array::<4>(bytes, "f32")?)))),
        MirroredType::F64 => Ok(Cell::DoublePrecision(Some(f64::from_le_bytes(
            read_array::<8>(bytes, "f64")?,
        )))),
        MirroredType::String => Ok(Cell::Text(Some(read_string(bytes)?))),

        // 64-bit unsigned and 128/256-bit ints — stored as raw bytea
        // for now (avoids a NUMERIC/BigDecimal dependency). The DDL
        // layer also emits BYTEA for these.
        MirroredType::U64 => Ok(Cell::Bytea(Some(read_array::<8>(bytes, "u64")?.to_vec()))),
        MirroredType::U128 | MirroredType::I128 => {
            Ok(Cell::Bytea(Some(read_array::<16>(bytes, "i128")?.to_vec())))
        }
        MirroredType::U256 | MirroredType::I256 => {
            Ok(Cell::Bytea(Some(read_array::<32>(bytes, "i256")?.to_vec())))
        }

        // Wrapper Product (single `__name__` field) → unwrap.
        MirroredType::Product(fields) if is_wrapper(fields) => {
            decode_cell(bytes, &fields[0].ty, schema)
        }

        // Optional<T> (Sum with `some` / `none` variants) → nullable T.
        MirroredType::Sum(variants) if is_optional(variants) => {
            decode_optional(bytes, variants, schema)
        }

        // Anything else: decode to JSON.
        MirroredType::Product(_) | MirroredType::Sum(_) | MirroredType::Array(_) => {
            let v = decode_json(bytes, resolved, schema)?;
            Ok(Cell::Jsonb(v))
        }

        MirroredType::Ref(_) => unreachable!("schema.resolve never returns Ref"),
    }
}

fn decode_optional(
    bytes: &mut &[u8],
    variants: &[MirroredVariant],
    schema: &MirroredSchema,
) -> Result<Cell, BsatnError> {
    // SATS Option encoding: discriminant 0 = some(T), 1 = none.
    // We don't assume the field order — match by variant name.
    let tag = read_u8(bytes, "optional discriminant")?;
    let variant = variants
        .get(tag as usize)
        .ok_or(BsatnError::SumOutOfRange {
            tag: tag as u32,
            len: variants.len(),
        })?;
    let is_some = variant.name.as_deref() == Some("some");

    if !is_some {
        // none variant has a Product{} payload — zero bytes, but we
        // still descend through resolve to stay correct if the
        // payload type isn't trivial.
        let _ = decode_value_to_skip(bytes, &variant.ty, schema)?;
        return Ok(null_cell_for(variants));
    }
    decode_cell(bytes, &variant.ty, schema)
}

fn null_cell_for(variants: &[MirroredVariant]) -> Cell {
    let some_ty = variants
        .iter()
        .find(|v| v.name.as_deref() == Some("some"))
        .map(|v| &v.ty);
    match some_ty {
        Some(MirroredType::Bool) => Cell::Bool(None),
        Some(MirroredType::I8) | Some(MirroredType::I16) | Some(MirroredType::U8) => {
            Cell::Smallint(None)
        }
        Some(MirroredType::I32) | Some(MirroredType::U16) => Cell::Integer(None),
        Some(MirroredType::I64) | Some(MirroredType::U32) => Cell::Bigint(None),
        Some(MirroredType::F32) => Cell::Real(None),
        Some(MirroredType::F64) => Cell::DoublePrecision(None),
        Some(MirroredType::String) => Cell::Text(None),
        Some(MirroredType::U64)
        | Some(MirroredType::U128)
        | Some(MirroredType::U256)
        | Some(MirroredType::I128)
        | Some(MirroredType::I256) => Cell::Bytea(None),
        _ => Cell::Jsonb(Value::Null),
    }
}

fn decode_value_to_skip(
    bytes: &mut &[u8],
    ty: &MirroredType,
    schema: &MirroredSchema,
) -> Result<(), BsatnError> {
    decode_json(bytes, schema.resolve(ty), schema).map(|_| ())
}

fn decode_json(
    bytes: &mut &[u8],
    ty: &MirroredType,
    schema: &MirroredSchema,
) -> Result<Value, BsatnError> {
    let resolved = schema.resolve(ty);
    match resolved {
        MirroredType::Bool => Ok(json!(read_u8(bytes, "bool")? != 0)),
        MirroredType::I8 => Ok(json!(read_u8(bytes, "i8")? as i8)),
        MirroredType::I16 => Ok(json!(read_i16_le(bytes)?)),
        MirroredType::I32 => Ok(json!(read_i32_le(bytes)?)),
        MirroredType::I64 => Ok(json!(read_i64_le(bytes)?)),
        MirroredType::U8 => Ok(json!(read_u8(bytes, "u8")?)),
        MirroredType::U16 => Ok(json!(read_u16_le(bytes)?)),
        MirroredType::U32 => Ok(json!(read_u32_le(bytes)?)),
        MirroredType::U64 => Ok(json!(hex::encode(read_array::<8>(bytes, "u64")?))),
        MirroredType::U128 | MirroredType::I128 => {
            Ok(json!(hex::encode(read_array::<16>(bytes, "i128")?)))
        }
        MirroredType::U256 | MirroredType::I256 => {
            Ok(json!(hex::encode(read_array::<32>(bytes, "i256")?)))
        }
        MirroredType::F32 => Ok(json!(f32::from_le_bytes(read_array::<4>(bytes, "f32")?))),
        MirroredType::F64 => Ok(json!(f64::from_le_bytes(read_array::<8>(bytes, "f64")?))),
        MirroredType::String => Ok(json!(read_string(bytes)?)),
        MirroredType::Product(fields) => {
            let mut obj = serde_json::Map::with_capacity(fields.len());
            for (i, f) in fields.iter().enumerate() {
                let key = f.name.clone().unwrap_or_else(|| format!("_{i}"));
                obj.insert(key, decode_json(bytes, &f.ty, schema)?);
            }
            Ok(Value::Object(obj))
        }
        MirroredType::Sum(variants) => {
            let tag = read_u8(bytes, "sum discriminant")?;
            let variant = variants
                .get(tag as usize)
                .ok_or(BsatnError::SumOutOfRange {
                    tag: tag as u32,
                    len: variants.len(),
                })?;
            let inner = decode_json(bytes, &variant.ty, schema)?;
            let key = variant
                .name
                .clone()
                .unwrap_or_else(|| format!("_{tag}"));
            let mut obj = serde_json::Map::new();
            obj.insert(key, inner);
            Ok(Value::Object(obj))
        }
        MirroredType::Array(inner) => {
            let n = read_u32_le(bytes)? as usize;
            let mut arr = Vec::with_capacity(n);
            for _ in 0..n {
                arr.push(decode_json(bytes, inner, schema)?);
            }
            Ok(Value::Array(arr))
        }
        MirroredType::Ref(_) => unreachable!("schema.resolve never returns Ref"),
    }
}

// ---------- low-level readers ----------

fn read_u8(bytes: &mut &[u8], context: &str) -> Result<u8, BsatnError> {
    let mut buf = [0u8; 1];
    bytes
        .read_exact(&mut buf)
        .map_err(|_| BsatnError::Eof { context: context.into() })?;
    Ok(buf[0])
}

fn read_array<const N: usize>(bytes: &mut &[u8], context: &str) -> Result<[u8; N], BsatnError> {
    let mut buf = [0u8; N];
    bytes
        .read_exact(&mut buf)
        .map_err(|_| BsatnError::Eof { context: context.into() })?;
    Ok(buf)
}

fn read_u16_le(bytes: &mut &[u8]) -> Result<u16, BsatnError> {
    Ok(u16::from_le_bytes(read_array::<2>(bytes, "u16")?))
}
fn read_u32_le(bytes: &mut &[u8]) -> Result<u32, BsatnError> {
    Ok(u32::from_le_bytes(read_array::<4>(bytes, "u32")?))
}
fn read_i16_le(bytes: &mut &[u8]) -> Result<i16, BsatnError> {
    Ok(i16::from_le_bytes(read_array::<2>(bytes, "i16")?))
}
fn read_i32_le(bytes: &mut &[u8]) -> Result<i32, BsatnError> {
    Ok(i32::from_le_bytes(read_array::<4>(bytes, "i32")?))
}
fn read_i64_le(bytes: &mut &[u8]) -> Result<i64, BsatnError> {
    Ok(i64::from_le_bytes(read_array::<8>(bytes, "i64")?))
}

fn read_string(bytes: &mut &[u8]) -> Result<String, BsatnError> {
    let n = read_u32_le(bytes)? as usize;
    if bytes.len() < n {
        return Err(BsatnError::Eof {
            context: format!("string body ({n} bytes)"),
        });
    }
    let (head, tail) = bytes.split_at(n);
    let s = String::from_utf8(head.to_vec())?;
    *bytes = tail;
    Ok(s)
}

// ---------- helpers ----------

fn is_wrapper(fields: &[MirroredField]) -> bool {
    fields.len() == 1
        && fields[0]
            .name
            .as_deref()
            .map(|n| n.starts_with("__") && n.ends_with("__"))
            .unwrap_or(false)
}

fn is_optional(variants: &[MirroredVariant]) -> bool {
    variants.len() == 2
        && variants.iter().any(|v| v.name.as_deref() == Some("some"))
        && variants.iter().any(|v| v.name.as_deref() == Some("none"))
}
