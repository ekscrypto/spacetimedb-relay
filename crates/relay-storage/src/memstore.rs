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
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;
use tracing::{info, warn};

use relay_protocol::{
    decode_row, field_byte_ranges, BsatnError, DecodedRow, MirroredField, MirroredSchema,
    MirroredTable,
};

use crate::ddl::{sanitize_ident, TableSpec};
use crate::snapshot;
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
                    let cells =
                        decode_row(&existing_bytes, &table.fields, &schema).map_err(map_bsatn)?;
                    deletes.push(DecodedRow {
                        cells,
                        bsatn: existing_bytes,
                    });
                    inserts.push(row.clone());
                }
                Some(_) => {}
            }
            next.insert(key, row.bsatn);
        }

        // Anything left in `table.rows` is missing from the incoming
        // snapshot — emit as a delete. Decode bsatn so callers that
        // need typed PK cells (Postgres delete-by-PK) can use it.
        for (_key, bytes) in table.rows.iter() {
            let cells = decode_row(bytes, &table.fields, &schema).map_err(map_bsatn)?;
            deletes.push(DecodedRow {
                cells,
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

    /// Persist every table in the store to `<dir>/<sanitized_name>.snapshot`.
    /// Returns the number of tables written. Each file's header carries
    /// the current schema's fingerprint hex; mismatched files are
    /// rejected on the next load.
    pub fn write_snapshots(&self, dir: &Path) -> std::io::Result<SnapshotStats> {
        let inner = self.inner.read();
        let Some(schema) = &inner.schema else {
            return Ok(SnapshotStats::default());
        };
        let hash = schema.fingerprint_hex();
        let mut stats = SnapshotStats::default();
        for (upstream_name, table) in inner.tables.iter() {
            let sanitized = sanitize_ident(upstream_name).map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("{e}"))
            })?;
            let path = snapshot::table_path(dir, &sanitized);
            let rows: Vec<(Vec<u8>, Bytes)> = table
                .rows
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            let n = rows.len();
            snapshot::write_table(&path, &hash, n, rows)?;
            stats.tables_written += 1;
            stats.rows_written += n as u64;
        }
        Ok(stats)
    }

    /// Restore rows from `<dir>` for tables that match the current
    /// schema fingerprint. Files whose header hash doesn't match (i.e.
    /// produced under a previous schema) are skipped — the relay's
    /// next `SubscribeApplied` will gap-fill those.
    ///
    /// Must be called after [`MemStore::sync_schema`] so we know the
    /// expected fingerprint and the table topology.
    pub fn load_snapshots(&self, dir: &Path) -> std::io::Result<SnapshotStats> {
        let mut inner = self.inner.write();
        let Some(schema) = inner.schema.clone() else {
            return Ok(SnapshotStats::default());
        };
        let expected_hash = schema.fingerprint_hex();
        let mut stats = SnapshotStats::default();
        for (upstream_name, table) in inner.tables.iter_mut() {
            let sanitized = sanitize_ident(upstream_name).map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("{e}"))
            })?;
            let path = snapshot::table_path(dir, &sanitized);
            let rows = match snapshot::read_table(&path, &expected_hash) {
                Ok(Some(rows)) => rows,
                Ok(None) => {
                    info!(
                        target: "relay::memstore",
                        table = %upstream_name,
                        "snapshot found but schema hash mismatched; skipping"
                    );
                    continue;
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
            };
            for (key, bsatn) in rows {
                table.rows.insert(key, bsatn);
            }
            stats.tables_loaded += 1;
            stats.rows_loaded += table.rows.len() as u64;
        }
        Ok(stats)
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SnapshotStats {
    pub tables_written: usize,
    pub rows_written: u64,
    pub tables_loaded: usize,
    pub rows_loaded: u64,
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};
    use relay_protocol::{
        decode_row, MirroredField, MirroredTable, MirroredType, TableAccess, TableKind,
    };

    use crate::ddl::build_table_specs;

    fn schema() -> Arc<MirroredSchema> {
        Arc::new(MirroredSchema {
            typespace: vec![MirroredType::Product(vec![
                MirroredField {
                    name: Some("id".into()),
                    ty: MirroredType::I32,
                },
                MirroredField {
                    name: Some("payload".into()),
                    ty: MirroredType::String,
                },
            ])],
            tables: vec![MirroredTable {
                name: "thing".into(),
                product_type_ref: 0,
                primary_key: vec![0],
                access: TableAccess::Public,
                kind: TableKind::User,
            }],
        })
    }

    fn row(id: i32, payload: &str, schema: &MirroredSchema) -> DecodedRow {
        let mut buf = BytesMut::new();
        buf.put_i32_le(id);
        buf.put_u32_le(payload.len() as u32);
        buf.put_slice(payload.as_bytes());
        let bsatn = buf.freeze();
        let fields = schema
            .table_product(&schema.tables[0])
            .expect("product fields");
        let cells = decode_row(&bsatn, fields, schema).expect("decode");
        DecodedRow { cells, bsatn }
    }

    fn fresh_store() -> MemStore {
        let store = MemStore::new();
        let s = schema();
        let specs = build_table_specs(&s, "test_db").expect("specs");
        store.sync_schema(s, &specs);
        store
    }

    fn payloads_from_bytes(bytes: &[Bytes]) -> Vec<String> {
        let mut out = Vec::new();
        for b in bytes {
            // After 4 bytes for id and 4 for length, the rest is the
            // string. The schema is fixed in the helper.
            let len = u32::from_le_bytes(b[4..8].try_into().unwrap()) as usize;
            out.push(String::from_utf8(b[8..8 + len].to_vec()).unwrap());
        }
        out.sort();
        out
    }

    fn payloads_from_rows(rows: &[DecodedRow]) -> Vec<String> {
        let raw: Vec<Bytes> = rows.iter().map(|r| r.bsatn.clone()).collect();
        payloads_from_bytes(&raw)
    }

    #[test]
    fn empty_current_yields_pure_inserts() {
        let store = fresh_store();
        let s = schema();
        let incoming = vec![row(1, "a-v1", &s), row(2, "b-v1", &s), row(3, "c-v1", &s)];
        let diff = store.apply_snapshot_diff("thing", &incoming).unwrap();
        assert!(diff.deletes.is_empty());
        assert_eq!(
            payloads_from_rows(&diff.inserts),
            vec!["a-v1".to_string(), "b-v1".to_string(), "c-v1".to_string()]
        );
    }

    #[test]
    fn missing_row_during_outage_is_emitted_as_delete() {
        let store = fresh_store();
        let s = schema();
        let phase1 = vec![row(1, "a", &s), row(2, "b", &s), row(3, "c", &s)];
        store.apply_snapshot_diff("thing", &phase1).unwrap();
        let phase2 = vec![row(1, "a", &s), row(3, "c", &s)];
        let diff = store.apply_snapshot_diff("thing", &phase2).unwrap();
        assert!(diff.inserts.is_empty(), "inserts: {:?}", diff.inserts);
        assert_eq!(payloads_from_rows(&diff.deletes), vec!["b".to_string()]);
    }

    #[test]
    fn changed_payload_for_same_pk_is_delete_then_insert() {
        let store = fresh_store();
        let s = schema();
        store
            .apply_snapshot_diff("thing", &[row(1, "a-v1", &s)])
            .unwrap();
        let diff = store
            .apply_snapshot_diff("thing", &[row(1, "a-v2", &s)])
            .unwrap();
        assert_eq!(payloads_from_rows(&diff.deletes), vec!["a-v1".to_string()]);
        assert_eq!(payloads_from_rows(&diff.inserts), vec!["a-v2".to_string()]);
    }

    #[test]
    fn replay_same_snapshot_is_noop() {
        let store = fresh_store();
        let s = schema();
        let phase = vec![row(1, "a", &s), row(2, "b", &s)];
        store.apply_snapshot_diff("thing", &phase).unwrap();
        let diff = store.apply_snapshot_diff("thing", &phase).unwrap();
        assert!(diff.deletes.is_empty());
        assert!(diff.inserts.is_empty());
    }

    #[test]
    fn apply_diff_inserts_and_deletes_by_pk() {
        let store = fresh_store();
        let s = schema();
        let initial = vec![row(1, "a", &s), row(2, "b", &s), row(3, "c", &s)];
        store.insert_rows("thing", &initial).unwrap();

        let outcome = store
            .apply_diff(
                "thing",
                &[row(2, "b", &s)],
                &[row(4, "d", &s), row(5, "e", &s)],
            )
            .unwrap();
        assert_eq!(outcome.deleted, 1);
        assert_eq!(outcome.inserted, 2);

        let final_rows = store.fetch_all_bsatn("thing").unwrap();
        assert_eq!(
            payloads_from_bytes(&final_rows),
            vec![
                "a".to_string(),
                "c".to_string(),
                "d".to_string(),
                "e".to_string()
            ]
        );
    }

    #[test]
    fn fetch_all_returns_inserted_rows() {
        let store = fresh_store();
        let s = schema();
        store
            .insert_rows("thing", &[row(1, "a", &s), row(2, "b", &s)])
            .unwrap();
        let raw = store.fetch_all_bsatn("thing").unwrap();
        assert_eq!(payloads_from_bytes(&raw), vec!["a", "b"]);
    }
}
