// SPDX-License-Identifier: MIT

//! Column projection — `SELECT col_a, col_b FROM …`.
//!
//! Reuses the original BSATN bytes per kept field. Walking the row to
//! find each field's byte range still requires going through the SATS
//! decoder once (so we know how much to skip for variable-length and
//! sum types), but no field is allocated twice and no Cell is encoded
//! back to bytes.

use bytes::{BufMut, Bytes, BytesMut};

use relay_protocol::{field_byte_ranges, BsatnError, MirroredField, MirroredSchema};

use crate::query::{CompiledQuery, Projection};

/// Apply `query.projection` to a single BSATN-encoded row.
/// `Projection::Star` is a zero-copy pass-through.
pub fn project_row(
    schema: &MirroredSchema,
    query: &CompiledQuery,
    fields: &[MirroredField],
    bytes: &Bytes,
) -> Result<Bytes, BsatnError> {
    let cols = match &query.projection {
        Projection::Star => return Ok(bytes.clone()),
        Projection::Cols(c) => c,
    };
    let ranges = field_byte_ranges(bytes, fields, schema)?;
    let total: usize = cols
        .iter()
        .map(|&i| ranges.get(i as usize).map(|s| s.len()).unwrap_or(0))
        .sum();
    let mut out = BytesMut::with_capacity(total);
    for &i in cols {
        if let Some(slice) = ranges.get(i as usize) {
            out.put_slice(slice);
        }
    }
    Ok(out.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use relay_protocol::{
        decode_row, MirroredField, MirroredSchema, MirroredType, MirroredVariant,
    };

    fn schema() -> MirroredSchema {
        let product = MirroredType::Product(vec![
            MirroredField {
                name: Some("a".into()),
                ty: MirroredType::I32,
            },
            MirroredField {
                name: Some("b".into()),
                ty: MirroredType::String,
            },
            MirroredField {
                name: Some("c".into()),
                ty: MirroredType::Bool,
            },
            MirroredField {
                name: Some("d".into()),
                ty: MirroredType::Sum(vec![
                    MirroredVariant {
                        name: Some("some".into()),
                        ty: MirroredType::I32,
                    },
                    MirroredVariant {
                        name: Some("none".into()),
                        ty: MirroredType::Product(vec![]),
                    },
                ]),
            },
        ]);
        MirroredSchema {
            typespace: vec![product],
            tables: vec![relay_protocol::MirroredTable {
                name: "t".into(),
                product_type_ref: 0,
                primary_key: vec![],
                access: relay_protocol::TableAccess::Public,
                kind: relay_protocol::TableKind::User,
            }],
        }
    }

    fn encode_row() -> Bytes {
        let mut out = BytesMut::new();
        // a: I32 (LE) — 7
        out.put_slice(&7i32.to_le_bytes());
        // b: String "hi" — 4-byte length + payload
        let body = b"hi";
        out.put_slice(&(body.len() as u32).to_le_bytes());
        out.put_slice(body);
        // c: Bool — 1 byte
        out.put_slice(&[1]);
        // d: Sum some(42) — discriminant 0 + I32 payload
        out.put_slice(&[0]);
        out.put_slice(&42i32.to_le_bytes());
        out.freeze()
    }

    #[test]
    fn star_returns_input() {
        let s = schema();
        let q = CompiledQuery {
            original_sql: String::new(),
            table: Arc::from("t"),
            table_idx: 0,
            projection: Projection::Star,
            predicate: None,
        };
        let row = encode_row();
        let out = project_row(&s, &q, &[], &row).unwrap();
        assert_eq!(out.as_ref(), row.as_ref());
    }

    #[test]
    fn cols_in_original_order() {
        let s = schema();
        let fields = match &s.typespace[0] {
            MirroredType::Product(f) => f.clone(),
            _ => unreachable!(),
        };
        let q = CompiledQuery {
            original_sql: String::new(),
            table: Arc::from("t"),
            table_idx: 0,
            projection: Projection::Cols(vec![0, 2]), // a, c
            predicate: None,
        };
        let row = encode_row();
        let out = project_row(&s, &q, &fields, &row).unwrap();
        // Decoding the projected row with a 2-field schema should
        // yield exactly `Integer(7)`, `Bool(true)`.
        let projected_fields = vec![fields[0].clone(), fields[2].clone()];
        let cells = decode_row(&out, &projected_fields, &s).unwrap();
        assert_eq!(cells.len(), 2);
        match (&cells[0], &cells[1]) {
            (relay_protocol::Cell::Integer(Some(7)), relay_protocol::Cell::Bool(Some(true))) => {}
            other => panic!("unexpected cells {other:?}"),
        }
    }

    #[test]
    fn cols_reordered() {
        let s = schema();
        let fields = match &s.typespace[0] {
            MirroredType::Product(f) => f.clone(),
            _ => unreachable!(),
        };
        // Request columns out of original order: c, a.
        let q = CompiledQuery {
            original_sql: String::new(),
            table: Arc::from("t"),
            table_idx: 0,
            projection: Projection::Cols(vec![2, 0]),
            predicate: None,
        };
        let row = encode_row();
        let out = project_row(&s, &q, &fields, &row).unwrap();
        let projected_fields = vec![fields[2].clone(), fields[0].clone()];
        let cells = decode_row(&out, &projected_fields, &s).unwrap();
        match (&cells[0], &cells[1]) {
            (relay_protocol::Cell::Bool(Some(true)), relay_protocol::Cell::Integer(Some(7))) => {}
            other => panic!("unexpected cells {other:?}"),
        }
    }

    #[test]
    fn variable_length_field_skip() {
        // Make sure projecting past a String + Sum still produces
        // valid bytes for the trailing field.
        let s = schema();
        let fields = match &s.typespace[0] {
            MirroredType::Product(f) => f.clone(),
            _ => unreachable!(),
        };
        let q = CompiledQuery {
            original_sql: String::new(),
            table: Arc::from("t"),
            table_idx: 0,
            projection: Projection::Cols(vec![3]), // d: optional i32 = some(42)
            predicate: None,
        };
        let row = encode_row();
        let out = project_row(&s, &q, &fields, &row).unwrap();
        let projected = vec![fields[3].clone()];
        let cells = decode_row(&out, &projected, &s).unwrap();
        match &cells[0] {
            relay_protocol::Cell::Integer(Some(42)) => {}
            other => panic!("unexpected cell {other:?}"),
        }
    }
}
