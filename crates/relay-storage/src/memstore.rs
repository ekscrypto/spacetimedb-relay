// SPDX-License-Identifier: MIT

//! In-memory mirror of upstream tables.
//!
//! Mirrors the API surface of the Postgres-backed [`crate::Storage`]:
//! `sync_schema`, `insert_rows`, `apply_diff`, `apply_snapshot_diff`,
//! `fetch_all_bsatn`. Each table holds its rows in a
//! `BTreeMap<Pk, Bytes>` keyed by the BSATN-encoded primary key so PK
//! lookups, ordered iteration, and replacement-on-update are O(log n)
//! without touching disk.
//!
//! Tables without a primary key fall back to insert-only behaviour and
//! are not diffable; callers that need diffing must define a PK
//! upstream.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;
use tracing::warn;

use relay_protocol::{
    field_byte_ranges, BsatnError, DecodedRow, MirroredField, MirroredSchema, MirroredTable,
};

use crate::ddl::TableSpec;
use crate::{DiffOutcome, SnapshotDiff, StorageError};

pub(crate) type Pk = Vec<u8>;

#[derive(Default)]
pub struct MemStore {
    inner: RwLock<Inner>,
}

#[derive(Default)]
struct Inner {
    schema: Option<Arc<MirroredSchema>>,
    tables: HashMap<String, MemTable>,
}

struct MemTable {
    spec: TableSpec,
    /// Cached copy of the table's product fields, in column order.
    /// Lets `apply_diff`/`apply_snapshot_diff` extract PKs without
    /// re-resolving the typespace each call.
    fields: Vec<MirroredField>,
    rows: BTreeMap<Pk, Bytes>,
}

impl MemStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Sync the in-memory store to a new schema. Tables present in
    /// `specs` keep their rows when their `TableSpec` is unchanged; the
    /// PG path drops + recreates on drift, so a callsite that hits drift
    /// will pass a fresh schema and the rows we keep here would be
    /// stale. Mirror that semantic by clearing every table's rows on
    /// any sync — drift detection lives in [`crate::meta::sync_schema`].
    pub fn sync_schema(&self, schema: Arc<MirroredSchema>, specs: &[TableSpec]) {
        let mut inner = self.inner.write();
        let mut tables = HashMap::with_capacity(specs.len());
        for spec in specs {
            let table_meta = schema.tables.iter().find(|t| t.name == spec.upstream_name);
            let fields = table_meta
                .and_then(|t| schema.table_product(t))
                .map(<[MirroredField]>::to_vec)
                .unwrap_or_default();
            tables.insert(
                spec.upstream_name.clone(),
                MemTable {
                    spec: spec.clone(),
                    fields,
                    rows: BTreeMap::new(),
                },
            );
        }
        inner.schema = Some(schema);
        inner.tables = tables;
    }

    /// Bulk insert. Replaces existing rows on PK collision (matching the
    /// PG path's `ON CONFLICT ... DO UPDATE`).
    pub fn insert_rows(
        &self,
        upstream_table: &str,
        rows: &[DecodedRow],
    ) -> Result<u64, StorageError> {
        let mut inner = self.inner.write();
        let schema = inner
            .schema
            .clone()
            .ok_or_else(|| StorageError::Identifier("memstore: schema not synced".into()))?;
        let table = inner
            .tables
            .get_mut(upstream_table)
            .ok_or_else(|| StorageError::Identifier(format!("unknown table {upstream_table}")))?;
        if table.spec.primary_key_indices.is_empty() {
            // Without a PK we can't dedupe. Generate a synthetic key
            // from the row bytes — collisions on identical payloads
            // are correct (one row per distinct payload).
            for row in rows {
                table.rows.insert(row.bsatn.to_vec(), row.bsatn.clone());
            }
            return Ok(rows.len() as u64);
        }
        let mut count = 0u64;
        for row in rows {
            let key = pk_key(row, &table.fields, &schema, &table.spec.primary_key_indices)?;
            table.rows.insert(key, row.bsatn.clone());
            count += 1;
        }
        Ok(count)
    }

    /// Apply a delete-then-insert diff for one table.
    pub fn apply_diff(
        &self,
        upstream_table: &str,
        deletes: &[DecodedRow],
        inserts: &[DecodedRow],
    ) -> Result<DiffOutcome, StorageError> {
        let mut inner = self.inner.write();
        let schema = inner
            .schema
            .clone()
            .ok_or_else(|| StorageError::Identifier("memstore: schema not synced".into()))?;
        let table = inner
            .tables
            .get_mut(upstream_table)
            .ok_or_else(|| StorageError::Identifier(format!("unknown table {upstream_table}")))?;
        let mut outcome = DiffOutcome::default();

        if !deletes.is_empty() {
            if table.spec.primary_key_indices.is_empty() {
                warn!(
                    target: "relay::memstore",
                    table = %upstream_table,
                    n_deletes = deletes.len(),
                    "table has no primary key — skipping deletes"
                );
            } else {
                for row in deletes {
                    let key = pk_key(row, &table.fields, &schema, &table.spec.primary_key_indices)?;
                    if table.rows.remove(&key).is_some() {
                        outcome.deleted += 1;
                    }
                }
            }
        }

        if !inserts.is_empty() {
            if table.spec.primary_key_indices.is_empty() {
                for row in inserts {
                    table.rows.insert(row.bsatn.to_vec(), row.bsatn.clone());
                }
                outcome.inserted += inserts.len() as u64;
            } else {
                for row in inserts {
                    let key = pk_key(row, &table.fields, &schema, &table.spec.primary_key_indices)?;
                    table.rows.insert(key, row.bsatn.clone());
                    outcome.inserted += 1;
                }
            }
        }

        Ok(outcome)
    }

    /// Reconcile an upstream snapshot against the current in-memory
    /// state. Identical row semantics to [`crate::Storage::apply_snapshot_diff`].
    pub fn apply_snapshot_diff(
        &self,
        upstream_table: &str,
        incoming: &[DecodedRow],
    ) -> Result<SnapshotDiff, StorageError> {
        let mut inner = self.inner.write();
        let schema = inner
            .schema
            .clone()
            .ok_or_else(|| StorageError::Identifier("memstore: schema not synced".into()))?;
        let table = inner
            .tables
            .get_mut(upstream_table)
            .ok_or_else(|| StorageError::Identifier(format!("unknown table {upstream_table}")))?;

        if table.spec.primary_key_indices.is_empty() {
            warn!(
                target: "relay::memstore",
                table = %upstream_table,
                n_rows = incoming.len(),
                "no primary key — gap-fill diff not possible; bulk inserting"
            );
            for row in incoming {
                table.rows.insert(row.bsatn.to_vec(), row.bsatn.clone());
            }
            return Ok(SnapshotDiff::default());
        }

        let mut keyed_incoming: Vec<(Pk, DecodedRow)> = Vec::with_capacity(incoming.len());
        for row in incoming {
            let key = pk_key(row, &table.fields, &schema, &table.spec.primary_key_indices)?;
            keyed_incoming.push((key, row.clone()));
        }

        let mut deletes = Vec::new();
        let mut inserts = Vec::new();
        let mut next = BTreeMap::new();

        for (key, row) in keyed_incoming {
            match table.rows.remove(&key) {
                None => inserts.push(row.clone()),
                Some(existing_bytes) if existing_bytes != row.bsatn => {
                    let existing = DecodedRow {
                        cells: row.cells.clone(),
                        bsatn: existing_bytes,
                    };
                    deletes.push(existing);
                    inserts.push(row.clone());
                }
                Some(_) => {}
            }
            next.insert(key, row.bsatn);
        }

        // Anything left in `table.rows` is missing from the incoming
        // snapshot — emit as a delete.
        for (_key, bytes) in table.rows.iter() {
            deletes.push(DecodedRow {
                cells: Vec::new(),
                bsatn: bytes.clone(),
            });
        }

        table.rows = next;
        Ok(SnapshotDiff { deletes, inserts })
    }

    pub fn fetch_all_bsatn(&self, upstream_table: &str) -> Result<Vec<Bytes>, StorageError> {
        let inner = self.inner.read();
        let table = inner
            .tables
            .get(upstream_table)
            .ok_or_else(|| StorageError::Identifier(format!("unknown table {upstream_table}")))?;
        Ok(table.rows.values().cloned().collect())
    }

    pub fn table_names(&self) -> Vec<String> {
        self.inner.read().tables.keys().cloned().collect()
    }

    pub fn row_count(&self, upstream_table: &str) -> Option<usize> {
        self.inner
            .read()
            .tables
            .get(upstream_table)
            .map(|t| t.rows.len())
    }
}

pub(crate) fn pk_key(
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

#[allow(dead_code)]
fn _table_meta<'a>(schema: &'a MirroredSchema, name: &str) -> Option<&'a MirroredTable> {
    schema.tables.iter().find(|t| t.name == name)
}
