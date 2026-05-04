// SPDX-License-Identifier: MIT

//! Subscription query compilation.
//!
//! Wraps `spacetimedb_sql_parser::parser::sub::parse_subscription` and
//! performs the schema-relative validation needed to evaluate the
//! query inside the relay:
//!   * resolves the FROM table to a `MirroredTable` index;
//!   * resolves any qualified column refs in the WHERE clause to
//!     column indices into the row;
//!   * coerces literals to a `Literal` whose representation matches
//!     the corresponding `Cell` variant produced by the BSATN decoder.
//!
//! Operator coverage tracks `predicate.rs`. PR3 (this commit) supports
//! `=`, `<>`, `<`, `>`, `<=`, `>=`, plus `AND` / `OR`, and resolves
//! `:sender` at compile time using the downstream client's identity.

use std::sync::Arc;

use spacetimedb_lib::Identity;
use spacetimedb_sql_parser::ast::{
    BinOp, LogOp, Project, ProjectElem, ProjectExpr, SqlExpr, SqlFrom, SqlIdent, SqlLiteral,
};
use spacetimedb_sql_parser::parser::sub::parse_subscription;
use thiserror::Error;

use relay_protocol::{MirroredField, MirroredSchema, MirroredType, MirroredVariant};

use crate::predicate::{Literal, LogicOp, Predicate, PredicateOp};

/// What the engine has learnt from a single subscription SQL string.
#[derive(Debug, Clone)]
pub struct CompiledQuery {
    pub original_sql: String,
    /// Upstream table name as it appears in the schema.
    pub table: Arc<str>,
    /// Index into `MirroredSchema.tables`.
    pub table_idx: usize,
    pub projection: Projection,
    pub predicate: Option<Predicate>,
}

#[derive(Debug, Clone)]
pub enum Projection {
    /// `SELECT *` — entire row passes through unchanged.
    Star,
    /// PR4: `SELECT col_a, col_b` — column subset by index.
    Cols(Vec<u16>),
}

#[derive(Debug, Error)]
pub enum CompileError {
    #[error("parse: {0}")]
    Parse(String),
    #[error("unknown table: {0}")]
    UnknownTable(String),
    #[error("unknown column `{column}` in table `{table}`")]
    UnknownColumn { table: String, column: String },
    #[error("type mismatch on column `{column}`: column is {column_ty}, literal is {got}")]
    TypeMismatch {
        column: String,
        column_ty: &'static str,
        got: &'static str,
    },
    #[error("could not parse `{value}` as {target}: {reason}")]
    BadLiteral {
        value: String,
        target: &'static str,
        reason: String,
    },
    #[error("`:sender` used but no downstream identity is bound")]
    UnresolvedSender,
    #[error("unsupported in this build: {0}")]
    Unsupported(&'static str),
}

/// Compile without binding `:sender`. Queries that reference the
/// parameter return [`CompileError::UnresolvedSender`].
pub fn compile(schema: &MirroredSchema, sql: &str) -> Result<CompiledQuery, CompileError> {
    compile_inner(schema, sql, None)
}

/// Compile a query whose `:sender` references resolve to `sender`.
pub fn compile_for_sender(
    schema: &MirroredSchema,
    sql: &str,
    sender: Identity,
) -> Result<CompiledQuery, CompileError> {
    compile_inner(schema, sql, Some(sender))
}

fn compile_inner(
    schema: &MirroredSchema,
    sql: &str,
    sender: Option<Identity>,
) -> Result<CompiledQuery, CompileError> {
    let mut select = parse_subscription(sql)
        .map_err(|e| CompileError::Parse(e.to_string()))?
        .qualify_vars();

    if select.has_parameter() {
        match sender {
            Some(s) => select = select.resolve_sender(s),
            None => return Err(CompileError::UnresolvedSender),
        }
    }

    let table_name = match &select.from {
        SqlFrom::Expr(name, _alias) => ident_str(name),
        SqlFrom::Join(_, _, _) => {
            return Err(CompileError::Unsupported("JOIN not yet implemented"));
        }
    };

    let (table_idx, table) = schema
        .tables
        .iter()
        .enumerate()
        .find(|(_, t)| t.name == table_name)
        .ok_or_else(|| CompileError::UnknownTable(table_name.clone()))?;

    let fields = schema
        .table_product(table)
        .ok_or_else(|| CompileError::UnknownTable(table_name.clone()))?;

    let projection = match &select.project {
        Project::Star(_) => Projection::Star,
        Project::Exprs(elems) => compile_projection(&table_name, fields, elems)?,
        Project::Count(_) => {
            return Err(CompileError::Unsupported("COUNT(*) not supported"));
        }
    };

    let predicate = match &select.filter {
        None => None,
        Some(expr) => Some(compile_predicate(schema, &table_name, fields, expr)?),
    };

    Ok(CompiledQuery {
        original_sql: sql.to_string(),
        table: Arc::from(table_name.as_str()),
        table_idx,
        projection,
        predicate,
    })
}

fn compile_projection(
    table_name: &str,
    fields: &[MirroredField],
    elems: &[ProjectElem],
) -> Result<Projection, CompileError> {
    let mut cols = Vec::with_capacity(elems.len());
    for ProjectElem(expr, _alias) in elems {
        let col_name = match expr {
            ProjectExpr::Field(_, col) => ident_str(col),
            ProjectExpr::Var(name) => ident_str(name),
        };
        let idx = fields
            .iter()
            .position(|f| f.name.as_deref() == Some(col_name.as_str()))
            .ok_or_else(|| CompileError::UnknownColumn {
                table: table_name.to_string(),
                column: col_name.clone(),
            })?;
        cols.push(idx as u16);
    }
    Ok(Projection::Cols(cols))
}

fn compile_predicate(
    schema: &MirroredSchema,
    table_name: &str,
    fields: &[MirroredField],
    expr: &SqlExpr,
) -> Result<Predicate, CompileError> {
    match expr {
        SqlExpr::Bin(lhs, rhs, op) => {
            compile_cmp(schema, table_name, fields, lhs, rhs, bin_to_op(*op))
        }
        SqlExpr::Log(lhs, rhs, op) => {
            let l = compile_predicate(schema, table_name, fields, lhs)?;
            let r = compile_predicate(schema, table_name, fields, rhs)?;
            Ok(Predicate::Logic {
                op: log_to_op(*op),
                lhs: Box::new(l),
                rhs: Box::new(r),
            })
        }
        SqlExpr::Param(_) => Err(CompileError::UnresolvedSender),
        SqlExpr::Lit(_) | SqlExpr::Var(_) | SqlExpr::Field(_, _) => Err(
            CompileError::Unsupported("expected a comparison or AND/OR expression"),
        ),
    }
}

fn compile_cmp(
    schema: &MirroredSchema,
    table_name: &str,
    fields: &[MirroredField],
    lhs: &SqlExpr,
    rhs: &SqlExpr,
    op: PredicateOp,
) -> Result<Predicate, CompileError> {
    let (col_name, lit, mirror_op) = match (lhs, rhs) {
        (SqlExpr::Field(_, col), SqlExpr::Lit(lit)) => (ident_str(col), lit, op),
        (SqlExpr::Lit(lit), SqlExpr::Field(_, col)) => (ident_str(col), lit, mirror_op(op)),
        _ => {
            return Err(CompileError::Unsupported(
                "comparison must be `<column> OP <literal>`",
            ));
        }
    };

    let (col_idx, field) = fields
        .iter()
        .enumerate()
        .find(|(_, f)| f.name.as_deref() == Some(col_name.as_str()))
        .ok_or_else(|| CompileError::UnknownColumn {
            table: table_name.to_string(),
            column: col_name.clone(),
        })?;

    let literal = literal_for_field(schema, &col_name, &field.ty, lit)?;
    Ok(Predicate::Cmp {
        col_idx,
        op: mirror_op,
        literal,
    })
}

/// `5 < x` is the same as `x > 5`. When the literal lands on the lhs,
/// flip the operator so the predicate stays `<column> OP <literal>`.
fn mirror_op(op: PredicateOp) -> PredicateOp {
    match op {
        PredicateOp::Eq => PredicateOp::Eq,
        PredicateOp::Ne => PredicateOp::Ne,
        PredicateOp::Lt => PredicateOp::Gt,
        PredicateOp::Gt => PredicateOp::Lt,
        PredicateOp::Lte => PredicateOp::Gte,
        PredicateOp::Gte => PredicateOp::Lte,
    }
}

fn bin_to_op(op: BinOp) -> PredicateOp {
    match op {
        BinOp::Eq => PredicateOp::Eq,
        BinOp::Ne => PredicateOp::Ne,
        BinOp::Lt => PredicateOp::Lt,
        BinOp::Gt => PredicateOp::Gt,
        BinOp::Lte => PredicateOp::Lte,
        BinOp::Gte => PredicateOp::Gte,
    }
}

fn log_to_op(op: LogOp) -> LogicOp {
    match op {
        LogOp::And => LogicOp::And,
        LogOp::Or => LogicOp::Or,
    }
}

fn literal_for_field(
    schema: &MirroredSchema,
    col: &str,
    ty: &MirroredType,
    lit: &SqlLiteral,
) -> Result<Literal, CompileError> {
    let resolved = unwrap_type(schema, ty);

    match (resolved, lit) {
        (MirroredType::Bool, SqlLiteral::Bool(b)) => Ok(Literal::Bool(*b)),
        (MirroredType::I8, SqlLiteral::Num(s)) => parse_int::<i8>(s, "i8")
            .map(|n| Literal::Smallint(n as i16)),
        (MirroredType::I16, SqlLiteral::Num(s)) => parse_int::<i16>(s, "i16").map(Literal::Smallint),
        (MirroredType::I32, SqlLiteral::Num(s)) => parse_int::<i32>(s, "i32").map(Literal::Integer),
        (MirroredType::I64, SqlLiteral::Num(s)) => parse_int::<i64>(s, "i64").map(Literal::Bigint),
        (MirroredType::U8, SqlLiteral::Num(s)) => parse_int::<u8>(s, "u8")
            .map(|n| Literal::Smallint(n as i16)),
        (MirroredType::U16, SqlLiteral::Num(s)) => parse_int::<u16>(s, "u16")
            .map(|n| Literal::Integer(n as i32)),
        (MirroredType::U32, SqlLiteral::Num(s)) => parse_int::<u32>(s, "u32")
            .map(|n| Literal::Bigint(n as i64)),
        (MirroredType::U64, SqlLiteral::Num(s)) => parse_int::<u64>(s, "u64").map(Literal::U64),
        (MirroredType::F32, SqlLiteral::Num(s)) => s
            .parse::<f32>()
            .map(Literal::Real)
            .map_err(|e| bad_literal(s, "f32", e)),
        (MirroredType::F64, SqlLiteral::Num(s)) => s
            .parse::<f64>()
            .map(Literal::DoublePrecision)
            .map_err(|e| bad_literal(s, "f64", e)),
        (MirroredType::String, SqlLiteral::Str(s)) => Ok(Literal::Text(s.to_string())),

        // Hex literal on a u64 column: interpret as a big-endian u64
        // value (the way humans read 0x...). Predicate eval converts
        // the LE-stored Cell::Bytea back to u64 numerically, so this
        // produces the user-expected ordering.
        (MirroredType::U64, SqlLiteral::Hex(s)) => {
            let bytes = decode_hex(s, Some(8))?;
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&bytes);
            Ok(Literal::U64(u64::from_be_bytes(buf)))
        }
        // Hex on a 128/256-bit column: SpacetimeDB stores these as LE
        // bytes; the user writes hex in big-endian order. Reverse so
        // equality byte-compares correctly against the Cell::Bytea
        // produced by the BSATN decoder.
        (MirroredType::U128 | MirroredType::I128, SqlLiteral::Hex(s)) => {
            let mut bytes = decode_hex(s, Some(16))?;
            bytes.reverse();
            Ok(Literal::Bytea(bytes))
        }
        (MirroredType::U256 | MirroredType::I256, SqlLiteral::Hex(s)) => {
            let mut bytes = decode_hex(s, Some(32))?;
            bytes.reverse();
            Ok(Literal::Bytea(bytes))
        }

        (column_ty, _) => Err(CompileError::TypeMismatch {
            column: col.to_string(),
            column_ty: type_name(column_ty),
            got: literal_kind(lit),
        }),
    }
}

/// Strip away wrapper-Product (`{ __identity__: U256 }`) and Optional
/// (`Sum { some, none }`) layers; literals match the inner concrete
/// type. Unwrap doesn't follow `Ref` — that's handled by `schema.resolve`.
fn unwrap_type<'a>(schema: &'a MirroredSchema, ty: &'a MirroredType) -> &'a MirroredType {
    let mut current = schema.resolve(ty);
    loop {
        match current {
            MirroredType::Product(fields) if is_wrapper(fields) => {
                current = schema.resolve(&fields[0].ty);
            }
            MirroredType::Sum(variants) if is_optional(variants) => {
                let some = variants
                    .iter()
                    .find(|v| v.name.as_deref() == Some("some"))
                    .map(|v| &v.ty);
                match some {
                    Some(t) => current = schema.resolve(t),
                    None => return current,
                }
            }
            _ => return current,
        }
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

fn is_optional(variants: &[MirroredVariant]) -> bool {
    variants.len() == 2
        && variants.iter().any(|v| v.name.as_deref() == Some("some"))
        && variants.iter().any(|v| v.name.as_deref() == Some("none"))
}

fn parse_int<T: std::str::FromStr>(s: &str, target: &'static str) -> Result<T, CompileError>
where
    <T as std::str::FromStr>::Err: std::fmt::Display,
{
    s.parse::<T>().map_err(|e| bad_literal(s, target, e))
}

fn bad_literal<E: std::fmt::Display>(s: &str, target: &'static str, e: E) -> CompileError {
    CompileError::BadLiteral {
        value: s.to_string(),
        target,
        reason: e.to_string(),
    }
}

fn decode_hex(s: &str, expected_len: Option<usize>) -> Result<Vec<u8>, CompileError> {
    let cleaned = strip_hex_wrapper(s);
    let bytes = hex::decode(cleaned).map_err(|e| bad_literal(s, "hex", e))?;
    if let Some(want) = expected_len {
        if bytes.len() != want {
            return Err(CompileError::BadLiteral {
                value: s.to_string(),
                target: "hex",
                reason: format!("expected {} bytes, got {}", want, bytes.len()),
            });
        }
    }
    Ok(bytes)
}

fn strip_hex_wrapper(s: &str) -> &str {
    if let Some(stripped) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        return stripped;
    }
    if (s.starts_with("x'") || s.starts_with("X'")) && s.ends_with('\'') {
        return &s[2..s.len() - 1];
    }
    s
}

fn ident_str(id: &SqlIdent) -> String {
    id.0.as_ref().to_string()
}

fn type_name(ty: &MirroredType) -> &'static str {
    match ty {
        MirroredType::Bool => "bool",
        MirroredType::I8 => "i8",
        MirroredType::I16 => "i16",
        MirroredType::I32 => "i32",
        MirroredType::I64 => "i64",
        MirroredType::I128 => "i128",
        MirroredType::I256 => "i256",
        MirroredType::U8 => "u8",
        MirroredType::U16 => "u16",
        MirroredType::U32 => "u32",
        MirroredType::U64 => "u64",
        MirroredType::U128 => "u128",
        MirroredType::U256 => "u256",
        MirroredType::F32 => "f32",
        MirroredType::F64 => "f64",
        MirroredType::String => "string",
        MirroredType::Product(_) => "product",
        MirroredType::Sum(_) => "sum",
        MirroredType::Array(_) => "array",
        MirroredType::Ref(_) => "ref",
    }
}

fn literal_kind(lit: &SqlLiteral) -> &'static str {
    match lit {
        SqlLiteral::Bool(_) => "bool",
        SqlLiteral::Hex(_) => "hex",
        SqlLiteral::Num(_) => "number",
        SqlLiteral::Str(_) => "string",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use relay_protocol::{
        MirroredField, MirroredSchema, MirroredTable, MirroredType, TableAccess, TableKind,
    };

    fn fixture_schema() -> MirroredSchema {
        let user_account = MirroredType::Product(vec![
            MirroredField {
                name: Some("identity".into()),
                ty: MirroredType::Product(vec![MirroredField {
                    name: Some("__identity__".into()),
                    ty: MirroredType::U256,
                }]),
            },
            MirroredField {
                name: Some("name".into()),
                ty: MirroredType::String,
            },
            MirroredField {
                name: Some("online".into()),
                ty: MirroredType::Bool,
            },
        ]);
        let item = MirroredType::Product(vec![
            MirroredField {
                name: Some("id".into()),
                ty: MirroredType::U64,
            },
            MirroredField {
                name: Some("kind".into()),
                ty: MirroredType::String,
            },
            MirroredField {
                name: Some("qty".into()),
                ty: MirroredType::I32,
            },
            MirroredField {
                name: Some("rarity".into()),
                ty: MirroredType::U8,
            },
        ]);
        MirroredSchema {
            typespace: vec![user_account, item],
            tables: vec![
                MirroredTable {
                    name: "user_account".into(),
                    product_type_ref: 0,
                    primary_key: vec![0],
                    access: TableAccess::Public,
                    kind: TableKind::User,
                },
                MirroredTable {
                    name: "item".into(),
                    product_type_ref: 1,
                    primary_key: vec![0],
                    access: TableAccess::Public,
                    kind: TableKind::User,
                },
            ],
        }
    }

    #[test]
    fn star_no_filter() {
        let schema = fixture_schema();
        let q = compile(&schema, "SELECT * FROM user_account").unwrap();
        assert_eq!(q.table.as_ref(), "user_account");
        assert_eq!(q.table_idx, 0);
        assert!(matches!(q.projection, Projection::Star));
        assert!(q.predicate.is_none());
    }

    #[test]
    fn eq_string_literal() {
        let schema = fixture_schema();
        let q = compile(&schema, "SELECT * FROM user_account WHERE name = 'alice'").unwrap();
        let pred = q.predicate.expect("predicate");
        match pred {
            Predicate::Cmp {
                col_idx,
                op,
                literal,
            } => {
                assert_eq!(col_idx, 1);
                assert_eq!(op, PredicateOp::Eq);
                assert_eq!(literal, Literal::Text("alice".into()));
            }
            other => panic!("expected Cmp, got {other:?}"),
        }
    }

    #[test]
    fn lt_gt_lte_gte_int() {
        let schema = fixture_schema();
        for (sql, want_op) in [
            ("SELECT * FROM item WHERE qty < 4", PredicateOp::Lt),
            ("SELECT * FROM item WHERE qty > 4", PredicateOp::Gt),
            ("SELECT * FROM item WHERE qty <= 4", PredicateOp::Lte),
            ("SELECT * FROM item WHERE qty >= 4", PredicateOp::Gte),
            ("SELECT * FROM item WHERE qty <> 4", PredicateOp::Ne),
        ] {
            let q = compile(&schema, sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
            match q.predicate.unwrap() {
                Predicate::Cmp { op, literal, .. } => {
                    assert_eq!(op, want_op, "sql={sql}");
                    assert_eq!(literal, Literal::Integer(4));
                }
                other => panic!("expected Cmp, got {other:?}"),
            }
        }
    }

    #[test]
    fn flipped_literal_lhs() {
        let schema = fixture_schema();
        // `5 < qty` should compile to `qty > 5` so eval works on
        // (column, literal) pairs.
        let q = compile(&schema, "SELECT * FROM item WHERE 5 < qty").unwrap();
        match q.predicate.unwrap() {
            Predicate::Cmp { op, literal, .. } => {
                assert_eq!(op, PredicateOp::Gt);
                assert_eq!(literal, Literal::Integer(5));
            }
            other => panic!("expected Cmp, got {other:?}"),
        }
    }

    #[test]
    fn and_or_compile() {
        let schema = fixture_schema();
        let q = compile(
            &schema,
            "SELECT * FROM item WHERE qty > 2 AND rarity = 1",
        )
        .unwrap();
        assert!(matches!(
            q.predicate.unwrap(),
            Predicate::Logic { op: LogicOp::And, .. }
        ));
        let q2 = compile(
            &schema,
            "SELECT * FROM item WHERE kind = 'sword' OR kind = 'shield'",
        )
        .unwrap();
        assert!(matches!(
            q2.predicate.unwrap(),
            Predicate::Logic { op: LogicOp::Or, .. }
        ));
    }

    #[test]
    fn eq_u64_decimal_literal() {
        let schema = fixture_schema();
        let q = compile(&schema, "SELECT * FROM item WHERE id = 42").unwrap();
        match q.predicate.unwrap() {
            Predicate::Cmp { literal, .. } => assert_eq!(literal, Literal::U64(42)),
            other => panic!("expected Cmp, got {other:?}"),
        }
    }

    #[test]
    fn eq_u64_hex_literal_be() {
        let schema = fixture_schema();
        let q = compile(&schema, "SELECT * FROM item WHERE id = 0x000000000000007b").unwrap();
        match q.predicate.unwrap() {
            Predicate::Cmp { literal, .. } => assert_eq!(literal, Literal::U64(0x7b)),
            other => panic!("expected Cmp, got {other:?}"),
        }
    }

    #[test]
    fn eq_identity_hex_literal() {
        let schema = fixture_schema();
        let bad_hex = "c0de0000000000000000000000000000000000000000000000000000000000a1ff";
        // 33 bytes — must be 32, expect a BadLiteral length error.
        let bad = compile(
            &schema,
            &format!("SELECT * FROM user_account WHERE identity = 0x{bad_hex}"),
        );
        assert!(
            matches!(bad, Err(CompileError::BadLiteral { .. })),
            "want BadLiteral, got {bad:?}"
        );
        // Big-endian hex `0xc0de...a1` should land in `Literal::Bytea`
        // as little-endian bytes — that's how BSATN stores u256.
        let good_hex = "c0de0000000000000000000000000000000000000000000000000000000000a1";
        let q = compile(
            &schema,
            &format!("SELECT * FROM user_account WHERE identity = 0x{good_hex}"),
        )
        .unwrap();
        match q.predicate.unwrap() {
            Predicate::Cmp { literal, .. } => match literal {
                Literal::Bytea(bytes) => {
                    assert_eq!(bytes.len(), 32);
                    assert_eq!(bytes[0], 0xa1, "LE byte 0 should be the BE-low byte");
                    assert_eq!(bytes[31], 0xc0, "LE byte 31 should be the BE-high byte");
                }
                other => panic!("expected Bytea, got {other:?}"),
            },
            other => panic!("expected Cmp, got {other:?}"),
        }
    }

    #[test]
    fn sender_unresolved_without_binding() {
        let schema = fixture_schema();
        let r = compile(&schema, "SELECT * FROM user_account WHERE identity = :sender");
        assert!(matches!(r, Err(CompileError::UnresolvedSender)));
    }

    #[test]
    fn sender_resolves_when_bound() {
        let schema = fixture_schema();
        // `from_byte_array` takes LE bytes (matches BSATN storage).
        let mut le = [0u8; 32];
        le[0] = 0xc0;
        le[31] = 0xa1;
        let id = Identity::from_byte_array(le);
        let q = compile_for_sender(
            &schema,
            "SELECT * FROM user_account WHERE identity = :sender",
            id,
        )
        .unwrap();
        // After resolve_sender + literal_for_field, the stored Bytea
        // should round-trip back to the original LE bytes.
        match q.predicate.unwrap() {
            Predicate::Cmp { literal, .. } => match literal {
                Literal::Bytea(b) => {
                    assert_eq!(b.len(), 32);
                    assert_eq!(b[0], 0xc0);
                    assert_eq!(b[31], 0xa1);
                }
                other => panic!("expected Bytea, got {other:?}"),
            },
            other => panic!("expected Cmp, got {other:?}"),
        }
    }

    #[test]
    fn unknown_table() {
        let schema = fixture_schema();
        assert!(matches!(
            compile(&schema, "SELECT * FROM nope"),
            Err(CompileError::UnknownTable(_))
        ));
    }

    #[test]
    fn unknown_column() {
        let schema = fixture_schema();
        assert!(matches!(
            compile(&schema, "SELECT * FROM user_account WHERE nope = 1"),
            Err(CompileError::UnknownColumn { .. })
        ));
    }

    #[test]
    fn type_mismatch() {
        let schema = fixture_schema();
        assert!(matches!(
            compile(&schema, "SELECT * FROM user_account WHERE name = 1"),
            Err(CompileError::TypeMismatch { .. })
        ));
    }

    #[test]
    fn join_unsupported() {
        let schema = fixture_schema();
        let r = compile(
            &schema,
            "SELECT * FROM user_account u JOIN item i ON u.identity = i.kind",
        );
        assert!(matches!(r, Err(CompileError::Unsupported(_))));
    }

    #[test]
    fn projection_simple_cols() {
        let schema = fixture_schema();
        let q = compile(&schema, "SELECT name, online FROM user_account").unwrap();
        match q.projection {
            Projection::Cols(cs) => assert_eq!(cs, vec![1, 2]),
            other => panic!("expected Cols, got {other:?}"),
        }
    }

    #[test]
    fn projection_with_qualified_alias() {
        let schema = fixture_schema();
        // `qualify_vars` rewrites `name` into `<table>.name`; this
        // exercises the ProjectExpr::Field branch.
        let q = compile(
            &schema,
            "SELECT user_account.name FROM user_account",
        )
        .unwrap();
        match q.projection {
            Projection::Cols(cs) => assert_eq!(cs, vec![1]),
            other => panic!("expected Cols, got {other:?}"),
        }
    }

    #[test]
    fn projection_unknown_column() {
        let schema = fixture_schema();
        let r = compile(&schema, "SELECT nope FROM user_account");
        assert!(matches!(r, Err(CompileError::UnknownColumn { .. })));
    }
}
