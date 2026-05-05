// SPDX-License-Identifier: MIT

//! PostgreSQL mirror of upstream tables.
//!
//! - One Postgres table per upstream table, columns mirroring the
//!   SpacetimeDB column types as closely as the SQL type system
//!   allows.
//! - DDL is issued at runtime when we first observe a database's
//!   schema.
//! - On schema drift we drop the mirrored tables for that database
//!   and re-fetch from upstream. We do not attempt to migrate row
//!   data — the upstream module may have applied a transformation we
//!   cannot replicate.

mod ddl;
mod insert;
mod memstore;
mod meta;

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use sqlx::postgres::{PgPoolOptions, Postgres};
use sqlx::Pool;
use thiserror::Error;
use tracing::{info, warn};

use bytes::Bytes;
use relay_protocol::{DecodedRow, MirroredField, MirroredSchema};

pub use ddl::{ColumnSpec, TableSpec};
pub use memstore::MemStore;
pub use meta::SyncOutcome;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("sqlx: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("invalid identifier: {0}")]
    Identifier(String),
}

#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub database_url: String,
    pub upstream_host: String,
    pub upstream_database: String,
}

pub struct Storage {
    pool: Pool<Postgres>,
    upstream_host: String,
    upstream_database: String,
    table_specs: Arc<RwLock<HashMap<String, TableSpec>>>,
    mem: Arc<MemStore>,
}

impl Storage {
    pub async fn connect(config: StorageConfig) -> Result<Self, StorageError> {
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect(&config.database_url)
            .await?;

        meta::run_initial_migrations(&pool).await?;

        info!(
            target: "relay::storage",
            upstream_host = %config.upstream_host,
            upstream_database = %config.upstream_database,
            "connected to postgres"
        );

        Ok(Self {
            pool,
            upstream_host: config.upstream_host,
            upstream_database: config.upstream_database,
            table_specs: Arc::new(RwLock::new(HashMap::new())),
            mem: Arc::new(MemStore::new()),
        })
    }

    /// Compare the schema fingerprint to what we have stored. On
    /// first-ever sync or drift, drop existing mirrored tables for
    /// this upstream and CREATE TABLE for each new one.
    pub async fn sync_schema(&self, schema: &MirroredSchema) -> Result<SyncOutcome, StorageError> {
        let table_specs = ddl::build_table_specs(schema, &self.upstream_database)?;
        let outcome = meta::sync_schema(
            &self.pool,
            &self.upstream_host,
            &self.upstream_database,
            schema,
            &table_specs,
        )
        .await?;
        self.mem.sync_schema(Arc::new(schema.clone()), &table_specs);
        match &outcome {
            SyncOutcome::Unchanged => {
                info!(target: "relay::storage", "schema unchanged");
            }
            SyncOutcome::CreatedFresh { created } => {
                info!(
                    target: "relay::storage",
                    n_tables = created.len(),
                    tables = ?created,
                    "schema mirrored for the first time"
                );
            }
            SyncOutcome::DriftWiped { wiped, created } => {
                warn!(
                    target: "relay::storage",
                    n_wiped = wiped.len(),
                    n_created = created.len(),
                    wiped = ?wiped,
                    "schema drift — dropped mirror tables and recreated"
                );
            }
        }

        let mut map = self.table_specs.write();
        map.clear();
        for spec in table_specs {
            map.insert(spec.upstream_name.clone(), spec);
        }
        Ok(outcome)
    }

    pub fn pool(&self) -> &Pool<Postgres> {
        &self.pool
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

    /// Insert the given decoded rows into the named upstream table.
    /// Returns the number of rows inserted. Uses a single transaction.
    pub async fn insert_rows(
        &self,
        upstream_table: &str,
        rows: &[DecodedRow],
    ) -> Result<u64, StorageError> {
        let spec = self
            .table_spec(upstream_table)
            .ok_or_else(|| StorageError::Identifier(format!("unknown table {upstream_table}")))?;
        let count = insert::insert_rows(&self.pool, &spec, rows).await?;
        self.mem.insert_rows(upstream_table, rows)?;
        Ok(count)
    }

    /// Apply a `TransactionUpdate` diff for one table: delete rows
    /// matching the given delete-rows by primary key, then insert the
    /// given insert-rows. All in a single transaction.
    pub async fn apply_diff(
        &self,
        upstream_table: &str,
        deletes: &[DecodedRow],
        inserts_rows: &[DecodedRow],
    ) -> Result<DiffOutcome, StorageError> {
        let spec = self
            .table_spec(upstream_table)
            .ok_or_else(|| StorageError::Identifier(format!("unknown table {upstream_table}")))?;
        let outcome = insert::apply_diff(&self.pool, &spec, deletes, inserts_rows).await?;
        self.mem.apply_diff(upstream_table, deletes, inserts_rows)?;
        Ok(outcome)
    }

    /// Reconcile the current PG mirror against an upstream snapshot
    /// (typically the rows in a fresh `SubscribeApplied` after the
    /// upstream WS reconnects). Inserts new rows, updates changed
    /// rows, deletes rows the snapshot dropped, and leaves identical
    /// rows untouched. Returns the resulting diff so the caller can
    /// fan it out to already-subscribed downstream clients.
    pub async fn apply_snapshot_diff(
        &self,
        upstream_table: &str,
        incoming: &[DecodedRow],
        _fields: &[MirroredField],
        _schema: &MirroredSchema,
    ) -> Result<SnapshotDiff, StorageError> {
        let spec = self
            .table_spec(upstream_table)
            .ok_or_else(|| StorageError::Identifier(format!("unknown table {upstream_table}")))?;
        // Stage 2: compute the diff against the in-memory store
        // (sub-second even on million-row tables) and apply just that
        // delta to Postgres. The previous implementation read every
        // row in the PG mirror to compute the diff, which on
        // BitCraft-scale databases took ~10 minutes.
        let diff = self.mem.apply_snapshot_diff(upstream_table, incoming)?;
        if !diff.deletes.is_empty() || !diff.inserts.is_empty() {
            insert::apply_diff(&self.pool, &spec, &diff.deletes, &diff.inserts).await?;
        }
        Ok(diff)
    }

    /// Fetch the raw BSATN bytes of every row in the given upstream
    /// table. Reads from the in-memory store; the PG mirror is kept in
    /// sync as a paranoid checker but no longer on the hot path for
    /// downstream snapshots.
    pub fn fetch_all_bsatn(&self, upstream_table: &str) -> Result<Vec<Bytes>, StorageError> {
        self.mem.fetch_all_bsatn(upstream_table)
    }

    /// Read every row directly from the Postgres mirror. Used by the
    /// parity test to confirm the in-memory store has not diverged.
    pub async fn fetch_all_bsatn_pg(
        &self,
        upstream_table: &str,
    ) -> Result<Vec<Bytes>, StorageError> {
        let spec = self
            .table_spec(upstream_table)
            .ok_or_else(|| StorageError::Identifier(format!("unknown table {upstream_table}")))?;
        let sql = format!(
            "SELECT \"{}\" FROM \"{}\"",
            ddl::BSATN_COLUMN,
            spec.postgres_name
        );
        let rows: Vec<(Vec<u8>,)> = sqlx::query_as(&sql).fetch_all(&self.pool).await?;
        Ok(rows.into_iter().map(|(b,)| Bytes::from(b)).collect())
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
