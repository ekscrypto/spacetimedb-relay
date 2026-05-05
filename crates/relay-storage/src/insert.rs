// SPDX-License-Identifier: MIT

use sqlx::{Pool, Postgres};
use tracing::{debug, warn};

use relay_protocol::{Cell, DecodedRow};

use crate::ddl::{TableSpec, BSATN_COLUMN};
use crate::{DiffOutcome, StorageError};

/// Build and execute parameterized multi-row `INSERT INTO ... VALUES
/// (...), (...), ...` batches in a single transaction. The `_bsatn`
/// column is populated with the raw row bytes for fast downstream
/// forwarding. Batching is necessary because per-row inserts blow up
/// snapshot reconcile time on tables like BitCraft's
/// `footprint_tile_state` (millions of rows) — every insert is a
/// network round-trip.
pub async fn insert_rows(
    pool: &Pool<Postgres>,
    spec: &TableSpec,
    rows: &[DecodedRow],
) -> Result<u64, StorageError> {
    if rows.is_empty() {
        return Ok(0);
    }
    let mut tx = pool.begin().await?;
    let count = batched_insert(&mut tx, spec, rows).await?;
    tx.commit().await?;
    Ok(count)
}

/// Execute the parameterized multi-row INSERT in chunks small enough
/// to stay under Postgres' 65535-bind-parameter ceiling, against a
/// caller-managed transaction.
async fn batched_insert(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    spec: &TableSpec,
    rows: &[DecodedRow],
) -> Result<u64, StorageError> {
    if rows.is_empty() {
        return Ok(0);
    }
    let params_per_row = spec.columns.len() + 1;
    let max_rows_by_params = (u16::MAX as usize) / params_per_row.max(1);
    let chunk = max_rows_by_params.clamp(1, 1000);

    let mut count = 0u64;
    for batch in rows.chunks(chunk) {
        let sql = build_insert_sql(spec, batch.len());
        debug!(
            target: "relay::storage",
            n_rows = batch.len(),
            params_per_row,
            "insert batch"
        );
        let mut q = sqlx::query(&sql);
        for row in batch {
            for cell in &row.cells {
                q = bind_cell(q, cell);
            }
            q = q.bind(row.bsatn.as_ref());
        }
        let res = q.execute(&mut **tx).await?;
        count += res.rows_affected();
    }
    Ok(count)
}

fn build_insert_sql(spec: &TableSpec, n_rows: usize) -> String {
    debug_assert!(n_rows >= 1);
    let cols_per_row = spec.columns.len() + 1;
    let mut cols: Vec<String> = spec
        .columns
        .iter()
        .map(|c| format!("\"{}\"", c.postgres_name))
        .collect();
    cols.push(format!("\"{BSATN_COLUMN}\""));
    let mut rows_sql: Vec<String> = Vec::with_capacity(n_rows);
    for r in 0..n_rows {
        let placeholders: Vec<String> = (1..=cols_per_row)
            .map(|i| format!("${}", r * cols_per_row + i))
            .collect();
        rows_sql.push(format!("({})", placeholders.join(", ")));
    }
    let base = format!(
        "INSERT INTO \"{}\" ({}) VALUES {}",
        spec.postgres_name,
        cols.join(", "),
        rows_sql.join(", ")
    );
    if spec.primary_key_columns.is_empty() {
        return base;
    }
    let pk_cols: Vec<String> = spec
        .primary_key_columns
        .iter()
        .map(|c| format!("\"{c}\""))
        .collect();
    let non_pk_set: Vec<String> = spec
        .columns
        .iter()
        .map(|c| format!("\"{0}\" = EXCLUDED.\"{0}\"", c.postgres_name))
        .chain(std::iter::once(format!(
            "\"{BSATN_COLUMN}\" = EXCLUDED.\"{BSATN_COLUMN}\""
        )))
        .collect();
    format!(
        "{base} ON CONFLICT ({}) DO UPDATE SET {}",
        pk_cols.join(", "),
        non_pk_set.join(", ")
    )
}

/// Apply a delete-then-insert diff for one table. Deletes match by
/// primary key when one is defined; tables without a PK don't yet
/// support deletes (we log a warning and skip).
pub async fn apply_diff(
    pool: &Pool<Postgres>,
    spec: &TableSpec,
    deletes: &[DecodedRow],
    inserts_rows: &[DecodedRow],
) -> Result<DiffOutcome, StorageError> {
    let mut tx = pool.begin().await?;
    let mut outcome = DiffOutcome::default();

    if !deletes.is_empty() {
        if spec.primary_key_indices.is_empty() {
            warn!(
                target: "relay::storage",
                table = %spec.postgres_name,
                n_deletes = deletes.len(),
                "table has no primary key — skipping deletes"
            );
        } else {
            let where_clause: Vec<String> = spec
                .primary_key_indices
                .iter()
                .enumerate()
                .map(|(i, idx)| format!("\"{}\" = ${}", spec.columns[*idx].postgres_name, i + 1))
                .collect();
            let sql = format!(
                "DELETE FROM \"{}\" WHERE {}",
                spec.postgres_name,
                where_clause.join(" AND ")
            );
            debug!(
                target: "relay::storage",
                n_rows = deletes.len(),
                sql,
                "delete by PK"
            );
            for row in deletes {
                let mut q = sqlx::query(&sql);
                for &idx in &spec.primary_key_indices {
                    q = bind_cell(q, &row.cells[idx]);
                }
                let res = q.execute(&mut *tx).await?;
                outcome.deleted += res.rows_affected();
            }
        }
    }

    if !inserts_rows.is_empty() {
        outcome.inserted += batched_insert(&mut tx, spec, inserts_rows).await?;
    }

    tx.commit().await?;
    Ok(outcome)
}

fn bind_cell<'q>(
    q: sqlx::query::Query<'q, Postgres, sqlx::postgres::PgArguments>,
    cell: &'q Cell,
) -> sqlx::query::Query<'q, Postgres, sqlx::postgres::PgArguments> {
    match cell {
        Cell::Bool(v) => q.bind(v),
        Cell::Smallint(v) => q.bind(v),
        Cell::Integer(v) => q.bind(v),
        Cell::Bigint(v) => q.bind(v),
        Cell::Real(v) => q.bind(v),
        Cell::DoublePrecision(v) => q.bind(v),
        Cell::Bytea(v) => q.bind(v.as_deref()),
        Cell::Text(v) => match v {
            // Postgres TEXT can't store 0x00 (NUL) even though it's
            // valid UTF-8; binding such a string makes the whole
            // INSERT fail. SpacetimeDB strings carry whatever the
            // module wrote, including NULs (BitCraft's
            // `prospecting_desc` is one example). Replace NULs with
            // the Unicode replacement character so the row lands.
            Some(s) if s.contains('\0') => q.bind(s.replace('\0', "\u{FFFD}")),
            _ => q.bind(v.as_deref()),
        },
        Cell::Jsonb(v) => q.bind(v),
    }
}
