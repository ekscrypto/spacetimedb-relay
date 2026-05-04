// SPDX-License-Identifier: MIT

//! Translate `MirroredSchema` into Postgres DDL.
//!
//! Mapping rules:
//!   - Primitive types map to native Postgres types where possible.
//!   - Identity-style wrappers (single-field Product whose name
//!     follows the `__name__` convention) are unwrapped — e.g.
//!     `Product { __identity__: U256 }` becomes `BYTEA` (32 bytes).
//!   - SpacetimeDB Optionals (Sum with two variants `some`/`none`)
//!     become a nullable column of the `some` inner type.
//!   - Anything we can't represent yet (general Sum, nested Product,
//!     Array) falls back to JSONB so we don't lose data.
//!
//! A `_bsatn` BYTEA column is added to every mirror table and stores
//! the raw row bytes verbatim. This lets the relay forward rows
//! downstream without having to re-encode from typed columns.

use std::fmt::Write as _;

use relay_protocol::{MirroredField, MirroredSchema, MirroredTable, MirroredType};

use crate::StorageError;

/// Postgres column name holding the raw BSATN bytes of each row.
/// Used for fast pass-through to downstream clients without
/// re-encoding from typed columns.
pub const BSATN_COLUMN: &str = "_bsatn";

/// Bumped whenever the DDL produced by `map_type` changes in a way
/// that's incompatible with previously-created mirror tables. Composed
/// into the schema fingerprint so a version bump forces drop-and-recreate.
pub const MIRROR_DDL_VERSION: u32 = 2;

#[derive(Debug, Clone)]
pub struct TableSpec {
    pub upstream_name: String,
    pub postgres_name: String,
    pub columns: Vec<ColumnSpec>,
    pub primary_key_columns: Vec<String>,
    /// Column indices of the primary-key columns within `columns`,
    /// in the same order as `primary_key_columns`. Used to extract
    /// the PK from a decoded row.
    pub primary_key_indices: Vec<usize>,
}

#[derive(Debug, Clone)]
pub struct ColumnSpec {
    pub upstream_name: String,
    pub postgres_name: String,
    pub sql_type: String,
    pub nullable: bool,
}

pub fn build_table_specs(
    schema: &MirroredSchema,
    upstream_database: &str,
) -> Result<Vec<TableSpec>, StorageError> {
    let prefix = database_prefix(upstream_database)?;
    let mut specs = Vec::new();
    for table in &schema.tables {
        if !matches!(table.kind, relay_protocol::TableKind::User) {
            continue;
        }
        if !matches!(table.access, relay_protocol::TableAccess::Public) {
            continue;
        }
        specs.push(build_table_spec(schema, table, &prefix)?);
    }
    Ok(specs)
}

fn build_table_spec(
    schema: &MirroredSchema,
    table: &MirroredTable,
    prefix: &str,
) -> Result<TableSpec, StorageError> {
    let upstream_name = table.name.clone();
    let postgres_name = format!("relay_{prefix}_{}", sanitize_ident(&upstream_name)?);

    let fields = schema
        .table_product(table)
        .ok_or_else(|| StorageError::Identifier(format!("table {upstream_name} has no product")))?;

    let mut columns = Vec::with_capacity(fields.len());
    for field in fields {
        columns.push(field_to_column(schema, field)?);
    }

    let primary_key_indices: Vec<usize> = table
        .primary_key
        .iter()
        .filter_map(|i| columns.get(*i as usize).map(|_| *i as usize))
        .collect();
    let primary_key_columns = primary_key_indices
        .iter()
        .map(|i| columns[*i].postgres_name.clone())
        .collect();

    Ok(TableSpec {
        upstream_name,
        postgres_name,
        columns,
        primary_key_columns,
        primary_key_indices,
    })
}

fn field_to_column(
    schema: &MirroredSchema,
    field: &MirroredField,
) -> Result<ColumnSpec, StorageError> {
    let upstream_name = field.name.clone().unwrap_or_else(|| "unnamed".to_string());
    let postgres_name = sanitize_ident(&upstream_name)?;
    let mapping = map_type(schema, &field.ty);
    Ok(ColumnSpec {
        upstream_name,
        postgres_name,
        sql_type: mapping.sql_type,
        nullable: mapping.nullable,
    })
}

struct TypeMapping {
    sql_type: String,
    nullable: bool,
}

fn map_type(schema: &MirroredSchema, ty: &MirroredType) -> TypeMapping {
    let resolved = schema.resolve(ty);
    match resolved {
        MirroredType::Bool => prim("BOOLEAN"),
        MirroredType::I8 | MirroredType::I16 => prim("SMALLINT"),
        MirroredType::I32 => prim("INTEGER"),
        MirroredType::I64 => prim("BIGINT"),
        MirroredType::I128 => prim("NUMERIC(39, 0)"),
        MirroredType::I256 => prim("NUMERIC(78, 0)"),
        MirroredType::U8 => prim("SMALLINT"),
        MirroredType::U16 => prim("INTEGER"),
        MirroredType::U32 => prim("BIGINT"),
        MirroredType::U64 => prim("BYTEA"),
        MirroredType::U128 => prim("BYTEA"),
        MirroredType::U256 => prim("BYTEA"),
        MirroredType::F32 => prim("REAL"),
        MirroredType::F64 => prim("DOUBLE PRECISION"),
        MirroredType::String => prim("TEXT"),
        MirroredType::Product(fields) if is_wrapper(fields) => map_type(schema, &fields[0].ty),
        MirroredType::Sum(variants) if is_optional(variants) => {
            let some_ty = optional_inner(variants).expect("checked by is_optional");
            let inner = map_type(schema, some_ty);
            TypeMapping {
                sql_type: inner.sql_type,
                nullable: true,
            }
        }
        MirroredType::Product(_) | MirroredType::Sum(_) | MirroredType::Array(_) => prim("JSONB"),
        MirroredType::Ref(_) => prim("JSONB"),
    }
}

fn prim(s: &str) -> TypeMapping {
    TypeMapping {
        sql_type: s.to_string(),
        nullable: false,
    }
}

fn is_wrapper(fields: &[MirroredField]) -> bool {
    fields.len() == 1
        && fields[0]
            .name
            .as_deref()
            .map(|n| n.starts_with("__") && n.ends_with("__"))
            .unwrap_or(false)
}

fn is_optional(variants: &[relay_protocol::MirroredVariant]) -> bool {
    variants.len() == 2
        && variants.iter().any(|v| v.name.as_deref() == Some("some"))
        && variants.iter().any(|v| v.name.as_deref() == Some("none"))
}

fn optional_inner(variants: &[relay_protocol::MirroredVariant]) -> Option<&MirroredType> {
    variants
        .iter()
        .find(|v| v.name.as_deref() == Some("some"))
        .map(|v| &v.ty)
}

/// Sanitize an upstream identifier to a Postgres identifier: lower-
/// cased, only `[a-z0-9_]`, must not start with a digit. Returns an
/// error on empty input or after sanitization yielding nothing.
/// Postgres caps identifier names at 63 bytes. Our mirrored-table
/// names follow `relay_<prefix>_<table>`, so the prefix has to leave
/// room for `relay_` + `_` + the table name. SpacetimeDB database
/// identities are up to ~60 chars and would shadow the table name
/// entirely under Postgres' silent truncation, collapsing every
/// mirrored table for that database into a single name. Cap the
/// readable form and fall back to a stable 16-hex-char hash beyond
/// that.
const MAX_READABLE_PREFIX: usize = 24;

pub fn database_prefix(upstream_database: &str) -> Result<String, StorageError> {
    let sanitized = sanitize_ident(upstream_database)?;
    if sanitized.len() <= MAX_READABLE_PREFIX {
        return Ok(sanitized);
    }
    let hash = relay_protocol::sats::hash::hash_bytes(upstream_database.as_bytes());
    Ok(hex::encode(&hash.data[..8]))
}

pub fn sanitize_ident(name: &str) -> Result<String, StorageError> {
    let mut s = String::with_capacity(name.len());
    for c in name.chars() {
        if c.is_ascii_alphanumeric() {
            s.push(c.to_ascii_lowercase());
        } else if c == '_' || c == '-' {
            s.push('_');
        }
    }
    if s.is_empty() {
        return Err(StorageError::Identifier(format!(
            "identifier `{name}` sanitized to empty"
        )));
    }
    if s.starts_with(|c: char| c.is_ascii_digit()) {
        s.insert(0, '_');
    }
    Ok(s)
}

pub fn create_table_sql(spec: &TableSpec) -> String {
    let mut sql = String::new();
    writeln!(&mut sql, "CREATE TABLE \"{}\" (", spec.postgres_name).unwrap();
    for col in spec.columns.iter() {
        let null = if col.nullable { "" } else { " NOT NULL" };
        writeln!(
            &mut sql,
            "    \"{}\" {}{},",
            col.postgres_name, col.sql_type, null
        )
        .unwrap();
    }
    let trailing = !spec.primary_key_columns.is_empty();
    writeln!(
        &mut sql,
        "    \"{}\" BYTEA NOT NULL{}",
        BSATN_COLUMN,
        if trailing { "," } else { "" }
    )
    .unwrap();
    if trailing {
        let cols: Vec<String> = spec
            .primary_key_columns
            .iter()
            .map(|c| format!("\"{c}\""))
            .collect();
        writeln!(&mut sql, "    PRIMARY KEY ({})", cols.join(", ")).unwrap();
    }
    writeln!(&mut sql, ")").unwrap();
    sql
}

pub fn drop_table_sql(postgres_name: &str) -> String {
    format!("DROP TABLE IF EXISTS \"{postgres_name}\" CASCADE")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_database_keeps_readable_prefix() {
        let p = database_prefix("test").unwrap();
        assert_eq!(p, "test");
    }

    #[test]
    fn hyphenated_short_database_kept_readable() {
        let p = database_prefix("relay-905cb325").unwrap();
        assert_eq!(p, "relay_905cb325");
    }

    #[test]
    fn long_database_falls_back_to_stable_hash() {
        let id = "spacetimedb-relay-5cdba495-301b-46b6-bc7a-89a7774347d2-wvz0s";
        let p = database_prefix(id).unwrap();
        assert_eq!(p.len(), 16);
        assert!(p.chars().all(|c| c.is_ascii_hexdigit()));
        // stable across runs
        assert_eq!(p, database_prefix(id).unwrap());
    }

    #[test]
    fn full_postgres_name_fits_within_63_bytes_for_long_id() {
        let id = "spacetimedb-relay-5cdba495-301b-46b6-bc7a-89a7774347d2-wvz0s";
        let prefix = database_prefix(id).unwrap();
        let table = "user_account";
        let full = format!("relay_{prefix}_{table}");
        assert!(full.len() <= 63, "name `{full}` is {} bytes", full.len());
    }
}
