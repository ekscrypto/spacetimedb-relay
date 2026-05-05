// SPDX-License-Identifier: MIT

//! In-memory mirror of upstream tables, persisted to disk via
//! per-table snapshots.
//!
//! Replaces the previous Postgres mirror. The relay holds the entire
//! current state of every subscribed table in memory; the
//! [`crate::snapshot`] module persists it to disk on a timer and on
//! shutdown so a restart doesn't have to refetch the whole dataset.
//!
//! Schema-drift handling is implicit: snapshot files carry the
//! schema's fingerprint as a header, so a relay that comes back up
//! against a changed upstream schema simply ignores files from the
//! old layout and lets `SubscribeApplied` repopulate the tables.

mod ddl;
mod memstore;
pub mod snapshot;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;
use thiserror::Error;
use tracing::info;

use relay_protocol::{DecodedRow, MirroredField, MirroredSchema};

pub use ddl::{build_table_specs, database_prefix, ColumnSpec, TableSpec};
pub use memstore::{MemStore, SnapshotStats};

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("invalid identifier: {0}")]
    Identifier(String),
}

#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub upstream_host: String,
    pub upstream_database: String,
}

#[derive(Debug)]
pub enum SyncOutcome {
    /// First time we've ever applied this schema in the current
    /// process. Snapshots from previous runs may still load if their
    /// fingerprint matches.
    Applied { tables: Vec<String> },
    /// Same fingerprint as the previously-applied schema — no-op.
    Unchanged,
    /// Different fingerprint than the previously-applied schema. The
    /// in-memory tables have been recreated empty; on-disk snapshots
    /// from the old fingerprint will be ignored on the next load.
    Drifted { tables: Vec<String> },
}

pub struct Storage {
    upstream_host: String,
    upstream_database: String,
    table_specs: Arc<RwLock<HashMap<String, TableSpec>>>,
    last_fingerprint: Arc<RwLock<Option<String>>>,
    mem: Arc<MemStore>,
}

impl Storage {
    pub fn new(config: StorageConfig) -> Self {
        info!(
            target: "relay::storage",
            upstream_host = %config.upstream_host,
            upstream_database = %config.upstream_database,
            "in-memory storage initialised"
        );
        Self {
            upstream_host: config.upstream_host,
            upstream_database: config.upstream_database,
            table_specs: Arc::new(RwLock::new(HashMap::new())),
            last_fingerprint: Arc::new(RwLock::new(None)),
            mem: Arc::new(MemStore::new()),
        }
    }

    pub fn upstream_host(&self) -> &str {
        &self.upstream_host
    }

    pub fn upstream_database(&self) -> &str {
        &self.upstream_database
    }

    pub fn table_spec(&self, upstream_table: &str) -> Option<TableSpec> {
        self.table_specs.read().get(upstream_table).cloned()
    }

    pub fn mem(&self) -> &Arc<MemStore> {
        &self.mem
    }

    /// Apply a parsed upstream schema. Builds per-table specs, sets
    /// them on the in-memory store, and surfaces whether the schema
    /// is unchanged, brand-new for this process, or drifted from what
    /// was previously applied.
    pub fn sync_schema(&self, schema: &MirroredSchema) -> Result<SyncOutcome, StorageError> {
        let table_specs = ddl::build_table_specs(schema, &self.upstream_database)?;
        let new_fp = schema.fingerprint_hex();

        let mut last_fp = self.last_fingerprint.write();
        let outcome = match last_fp.as_deref() {
            Some(prev) if prev == new_fp => SyncOutcome::Unchanged,
            Some(_) => SyncOutcome::Drifted {
                tables: table_specs
                    .iter()
                    .map(|s| s.upstream_name.clone())
                    .collect(),
            },
            None => SyncOutcome::Applied {
                tables: table_specs
                    .iter()
                    .map(|s| s.upstream_name.clone())
                    .collect(),
            },
        };
        *last_fp = Some(new_fp);
        drop(last_fp);

        self.mem.sync_schema(Arc::new(schema.clone()), &table_specs);

        let mut map = self.table_specs.write();
        map.clear();
        for spec in table_specs {
            map.insert(spec.upstream_name.clone(), spec);
        }

        Ok(outcome)
    }

    /// Compute the per-database directory under `data_dir` where
    /// snapshot files live.
    pub fn snapshot_dir(&self, data_dir: &Path) -> PathBuf {
        let prefix =
            ddl::database_prefix(&self.upstream_database).unwrap_or_else(|_| "default".into());
        data_dir.join(prefix)
    }

    /// Walk the in-memory store and write every table to disk.
    pub fn write_snapshots(&self, data_dir: &Path) -> std::io::Result<SnapshotStats> {
        let dir = self.snapshot_dir(data_dir);
        self.mem.write_snapshots(&dir)
    }

    /// Restore mem from snapshots in `data_dir`. Files whose schema
    /// fingerprint doesn't match the current schema are skipped. Must
    /// be called after [`Storage::sync_schema`].
    pub fn load_snapshots(&self, data_dir: &Path) -> std::io::Result<SnapshotStats> {
        let dir = self.snapshot_dir(data_dir);
        self.mem.load_snapshots(&dir)
    }

    /// Insert the given decoded rows into the named upstream table.
    /// Returns the number of rows inserted.
    pub fn insert_rows(
        &self,
        upstream_table: &str,
        rows: &[DecodedRow],
    ) -> Result<u64, StorageError> {
        self.mem.insert_rows(upstream_table, rows)
    }

    /// Apply a `TransactionUpdate` diff for one table: delete rows
    /// matching the given delete-rows by primary key, then insert the
    /// given insert-rows.
    pub fn apply_diff(
        &self,
        upstream_table: &str,
        deletes: &[DecodedRow],
        inserts_rows: &[DecodedRow],
    ) -> Result<DiffOutcome, StorageError> {
        self.mem.apply_diff(upstream_table, deletes, inserts_rows)
    }

    /// Reconcile the current mirror against an upstream snapshot. The
    /// returned diff is what the caller fans out to already-subscribed
    /// downstream clients.
    pub fn apply_snapshot_diff(
        &self,
        upstream_table: &str,
        incoming: &[DecodedRow],
        _fields: &[MirroredField],
        _schema: &MirroredSchema,
    ) -> Result<SnapshotDiff, StorageError> {
        self.mem.apply_snapshot_diff(upstream_table, incoming)
    }

    /// Fetch the raw BSATN bytes of every row in the given upstream
    /// table.
    pub fn fetch_all_bsatn(&self, upstream_table: &str) -> Result<Vec<Bytes>, StorageError> {
        self.mem.fetch_all_bsatn(upstream_table)
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct DiffOutcome {
    pub deleted: u64,
    pub inserted: u64,
}

/// Result of [`Storage::apply_snapshot_diff`]: the rows the snapshot
/// implies were removed (PK absent from the snapshot, or PK present
/// with a different payload — the old version) and the rows the
/// snapshot implies were added (PK new, or PK present with a different
/// payload — the new version). Identical rows produce no entries.
#[derive(Debug, Default, Clone)]
pub struct SnapshotDiff {
    pub deletes: Vec<DecodedRow>,
    pub inserts: Vec<DecodedRow>,
}
