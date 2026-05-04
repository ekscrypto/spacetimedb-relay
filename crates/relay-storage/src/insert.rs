// SPDX-License-Identifier: MIT

use std::collections::HashMap;

use sqlx::{Pool, Postgres};
use tracing::{debug, warn};

use relay_protocol::{
    decode_row, field_byte_ranges, BsatnError, Cell, DecodedRow, MirroredField, MirroredSchema,
};

use crate::ddl::{TableSpec, BSATN_COLUMN};
use crate::{DiffOutcome, SnapshotDiff, StorageError};

/// Build and execute a parameterized `INSERT INTO ... VALUES (...)`
/// for each row, all in a single transaction. The `_bsatn` column is
/// populated with the raw row bytes for fast downstream forwarding.
pub async fn insert_rows(
    pool: &Pool<Postgres>,
    spec: &TableSpec,
    rows: &[DecodedRow],
) -> Result<u64, StorageError> {
    if rows.is_empty() {
        return Ok(0);
    }

    let sql = build_insert_sql(spec);
    debug!(
        target: "relay::storage",
        n_rows = rows.len(),
        sql,
        "insert"
    );

    let mut tx = pool.begin().await?;
    let mut count = 0u64;
    for row in rows {
        let mut q = sqlx::query(&sql);
        for cell in &row.cells {
            q = bind_cell(q, cell);
        }
        let raw = row.bsatn.as_ref();
        q = q.bind(raw);
        let res = q.execute(&mut *tx).await?;
        count += res.rows_affected();
    }
    tx.commit().await?;
    Ok(count)
}

fn build_insert_sql(spec: &TableSpec) -> String {
    let n = spec.columns.len() + 1;
    let placeholders: Vec<String> = (1..=n).map(|i| format!("${i}")).collect();
    let mut cols: Vec<String> = spec
        .columns
        .iter()
        .map(|c| format!("\"{}\"", c.postgres_name))
        .collect();
    cols.push(format!("\"{BSATN_COLUMN}\""));
    let base = format!(
        "INSERT INTO \"{}\" ({}) VALUES ({})",
        spec.postgres_name,
        cols.join(", "),
        placeholders.join(", ")
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
        let sql = build_insert_sql(spec);
        debug!(
            target: "relay::storage",
            n_rows = inserts_rows.len(),
            sql,
            "insert (diff)"
        );
        for row in inserts_rows {
            let mut q = sqlx::query(&sql);
            for cell in &row.cells {
                q = bind_cell(q, cell);
            }
            let raw = row.bsatn.as_ref();
            q = q.bind(raw);
            let res = q.execute(&mut *tx).await?;
            outcome.inserted += res.rows_affected();
        }
    }

    tx.commit().await?;
    Ok(outcome)
}

/// Reconcile an upstream snapshot against the current PG mirror.
///
/// Used on `SubscribeApplied` to handle the gap between the relay's
/// last upstream session and the new one: rows present upstream but
/// missing locally are inserted, rows whose payload changed are
/// updated, rows absent from the snapshot are deleted, and identical
/// rows are left alone. The returned [`SnapshotDiff`] is what the
/// caller fans out to already-subscribed downstream clients via
/// `engine.route_table_diff`.
///
/// Tables without a primary key cannot be diffed by identity, so we
/// fall back to a plain bulk insert and emit no diff.
pub async fn apply_snapshot_diff(
    pool: &Pool<Postgres>,
    spec: &TableSpec,
    incoming: &[DecodedRow],
    fields: &[MirroredField],
    schema: &MirroredSchema,
) -> Result<SnapshotDiff, StorageError> {
    if spec.primary_key_indices.is_empty() {
        warn!(
            target: "relay::storage",
            table = %spec.postgres_name,
            n_rows = incoming.len(),
            "no primary key — gap-fill diff not possible; bulk inserting"
        );
        insert_rows(pool, spec, incoming).await?;
        return Ok(SnapshotDiff::default());
    }

    let current_bytes = fetch_all_bsatn(pool, spec).await?;
    let mut current = Vec::with_capacity(current_bytes.len());
    for bytes in current_bytes {
        let cells = decode_row(&bytes, fields, schema).map_err(map_bsatn)?;
        let row = DecodedRow {
            cells,
            bsatn: bytes,
        };
        let key = pk_key(&row, fields, schema, &spec.primary_key_indices)?;
        current.push((key, row));
    }

    let mut keyed_incoming = Vec::with_capacity(incoming.len());
    for row in incoming {
        let key = pk_key(row, fields, schema, &spec.primary_key_indices)?;
        keyed_incoming.push((key, row.clone()));
    }

    let diff = classify_snapshot_diff(keyed_incoming, current);

    if !diff.deletes.is_empty() || !diff.inserts.is_empty() {
        apply_diff(pool, spec, &diff.deletes, &diff.inserts).await?;
    }
    Ok(diff)
}

/// Pure diff classification — split out so it's unit-testable without
/// a live Postgres.
///
/// Inputs are `(pk_bytes, row)` pairs. A row in `incoming` whose key
/// is absent from `current` is an insert; a key in both whose `bsatn`
/// payload differs is encoded as delete-then-insert (matching
/// SpacetimeDB's `TransactionUpdate` shape for updates); a key in
/// `current` but not `incoming` is a delete; identical pairs produce
/// nothing.
pub(crate) fn classify_snapshot_diff(
    incoming: Vec<(Vec<u8>, DecodedRow)>,
    current: Vec<(Vec<u8>, DecodedRow)>,
) -> SnapshotDiff {
    let mut current_by_pk: HashMap<Vec<u8>, DecodedRow> = HashMap::with_capacity(current.len());
    for (key, row) in current {
        current_by_pk.insert(key, row);
    }

    let mut deletes = Vec::new();
    let mut inserts = Vec::new();

    for (key, row) in incoming {
        match current_by_pk.remove(&key) {
            None => inserts.push(row),
            Some(existing) if existing.bsatn != row.bsatn => {
                deletes.push(existing);
                inserts.push(row);
            }
            Some(_) => {}
        }
    }

    for (_, existing) in current_by_pk {
        deletes.push(existing);
    }

    SnapshotDiff { deletes, inserts }
}

async fn fetch_all_bsatn(
    pool: &Pool<Postgres>,
    spec: &TableSpec,
) -> Result<Vec<bytes::Bytes>, StorageError> {
    let sql = format!(
        "SELECT \"{}\" FROM \"{}\"",
        BSATN_COLUMN, spec.postgres_name
    );
    let rows: Vec<(Vec<u8>,)> = sqlx::query_as(&sql).fetch_all(pool).await?;
    Ok(rows.into_iter().map(|(b,)| bytes::Bytes::from(b)).collect())
}

fn pk_key(
    row: &DecodedRow,
    fields: &[MirroredField],
    schema: &MirroredSchema,
    pk_indices: &[usize],
) -> Result<Vec<u8>, StorageError> {
    let ranges = field_byte_ranges(&row.bsatn, fields, schema).map_err(map_bsatn)?;
    let mut key = Vec::new();
    for &idx in pk_indices {
        let slice = ranges
            .get(idx)
            .ok_or_else(|| StorageError::Identifier(format!("pk index {idx} out of range")))?;
        key.extend_from_slice(slice);
    }
    Ok(key)
}

fn map_bsatn(e: BsatnError) -> StorageError {
    StorageError::Identifier(format!("bsatn decode: {e}"))
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
        Cell::Text(v) => q.bind(v.as_deref()),
        Cell::Jsonb(v) => q.bind(v),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn row(pk: &[u8], payload: &[u8]) -> (Vec<u8>, DecodedRow) {
        (
            pk.to_vec(),
            DecodedRow {
                cells: Vec::new(),
                bsatn: Bytes::from(payload.to_vec()),
            },
        )
    }

    fn payloads(rows: &[DecodedRow]) -> Vec<Vec<u8>> {
        let mut out: Vec<Vec<u8>> = rows.iter().map(|r| r.bsatn.to_vec()).collect();
        out.sort();
        out
    }

    #[test]
    fn empty_current_yields_pure_inserts() {
        let incoming = vec![row(b"a", b"a-v1"), row(b"b", b"b-v1"), row(b"c", b"c-v1")];
        let diff = classify_snapshot_diff(incoming, Vec::new());
        assert!(diff.deletes.is_empty());
        assert_eq!(
            payloads(&diff.inserts),
            vec![b"a-v1".to_vec(), b"b-v1".to_vec(), b"c-v1".to_vec()]
        );
    }

    #[test]
    fn empty_incoming_yields_pure_deletes() {
        let current = vec![row(b"a", b"a-v1"), row(b"b", b"b-v1")];
        let diff = classify_snapshot_diff(Vec::new(), current);
        assert!(diff.inserts.is_empty());
        assert_eq!(
            payloads(&diff.deletes),
            vec![b"a-v1".to_vec(), b"b-v1".to_vec()]
        );
    }

    #[test]
    fn identical_rows_produce_no_diff() {
        let current = vec![row(b"a", b"a-v1"), row(b"b", b"b-v1")];
        let incoming = vec![row(b"a", b"a-v1"), row(b"b", b"b-v1")];
        let diff = classify_snapshot_diff(incoming, current);
        assert!(diff.deletes.is_empty(), "deletes: {:?}", diff.deletes);
        assert!(diff.inserts.is_empty(), "inserts: {:?}", diff.inserts);
    }

    #[test]
    fn changed_payload_for_same_pk_is_delete_then_insert() {
        let current = vec![row(b"a", b"a-v1")];
        let incoming = vec![row(b"a", b"a-v2")];
        let diff = classify_snapshot_diff(incoming, current);
        assert_eq!(payloads(&diff.deletes), vec![b"a-v1".to_vec()]);
        assert_eq!(payloads(&diff.inserts), vec![b"a-v2".to_vec()]);
    }

    #[test]
    fn missing_row_during_outage_is_emitted_as_delete() {
        // Initial state had A, B, C. While upstream was disconnected,
        // B was deleted. The fresh snapshot contains only A and C.
        // The classifier must surface B as a delete so downstream
        // clients see the removal.
        let current = vec![row(b"A", b"a"), row(b"B", b"b"), row(b"C", b"c")];
        let incoming = vec![row(b"A", b"a"), row(b"C", b"c")];
        let diff = classify_snapshot_diff(incoming, current);
        assert!(diff.inserts.is_empty(), "inserts: {:?}", diff.inserts);
        assert_eq!(payloads(&diff.deletes), vec![b"b".to_vec()]);
    }

    #[test]
    fn mixed_insert_update_delete_noop() {
        // A unchanged, B updated, C deleted, D newly inserted.
        let current = vec![row(b"A", b"a-v1"), row(b"B", b"b-v1"), row(b"C", b"c-v1")];
        let incoming = vec![row(b"A", b"a-v1"), row(b"B", b"b-v2"), row(b"D", b"d-v1")];
        let diff = classify_snapshot_diff(incoming, current);
        assert_eq!(
            payloads(&diff.deletes),
            vec![b"b-v1".to_vec(), b"c-v1".to_vec()]
        );
        assert_eq!(
            payloads(&diff.inserts),
            vec![b"b-v2".to_vec(), b"d-v1".to_vec()]
        );
    }
}
