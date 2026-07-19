// SPDX-License-Identifier: MIT

//! Per-region subscription task: connect → SubscribeApplied bulk load →
//! stream TransactionUpdates into the columnar store, with reconnect.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};
use bytes::Bytes;
use parking_lot::RwLock;
use relay_protocol::{MirroredField, MirroredSchema};
use spacetimedb_client_api_messages::websocket::v2::{
    ServerMessage, SubscribeApplied, TableUpdateRows, TransactionUpdate,
};
use url::Url;

use crate::decode::{
    self, ColMaps, BUILDING_DESC_TABLE, BUILDING_NICKNAME_TABLE, BUILDING_TABLE, CLAIM_TABLE,
    INVENTORY_TABLE,
};
use crate::store::RegionStore;
use crate::wire;

const PING_INTERVAL: Duration = Duration::from_secs(10);
const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);
const STABLE_AFTER: Duration = Duration::from_secs(5);

/// Shared handle for one region's in-memory store. HTTP handlers hold
/// `Arc<ShardHandle>` and take a read lock for queries.
pub struct ShardHandle {
    pub region: u32,
    pub store: Arc<RwLock<RegionStore>>,
}

/// Cached field slices + column indices for the tables we hold.
struct TableMeta {
    cols: ColMaps,
    claim_fields: Vec<MirroredField>,
    building_fields: Vec<MirroredField>,
    inventory_fields: Vec<MirroredField>,
    building_desc_fields: Vec<MirroredField>,
    building_nickname_fields: Vec<MirroredField>,
}

impl TableMeta {
    fn from_schema(schema: &MirroredSchema) -> Result<Self> {
        let cols = decode::resolve_cols(schema)?;
        Ok(Self {
            cols,
            claim_fields: fields_owned(schema, CLAIM_TABLE)?,
            building_fields: fields_owned(schema, BUILDING_TABLE)?,
            inventory_fields: fields_owned(schema, INVENTORY_TABLE)?,
            building_desc_fields: fields_owned(schema, BUILDING_DESC_TABLE)?,
            building_nickname_fields: fields_owned(schema, BUILDING_NICKNAME_TABLE)?,
        })
    }
}

fn fields_owned(schema: &MirroredSchema, table: &str) -> Result<Vec<MirroredField>> {
    let tbl = schema
        .tables
        .iter()
        .find(|t| t.name == table)
        .ok_or_else(|| anyhow!("schema has no table `{table}`"))?;
    let fields = schema
        .table_product(tbl)
        .ok_or_else(|| anyhow!("table `{table}` is not a Product"))?;
    Ok(fields.to_vec())
}

/// Spawn the reconnect loop for one region. Returns the handle HTTP uses.
pub fn spawn_shard(
    region: u32,
    database: String,
    bind_url: Url,
    schema: Arc<MirroredSchema>,
    mut shutdown: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
) -> Arc<ShardHandle> {
    let handle = Arc::new(ShardHandle {
        region,
        store: Arc::new(RwLock::new(RegionStore::empty(region))),
    });
    let store = handle.store.clone();
    tokio::spawn(async move {
        if let Err(e) =
            run_shard_loop(region, database, bind_url, schema, store, &mut shutdown).await
        {
            tracing::error!(
                target: "relay_cache::shard",
                region,
                error = %e,
                "shard task exited"
            );
        }
    });
    handle
}

async fn run_shard_loop(
    region: u32,
    database: String,
    bind_url: Url,
    schema: Arc<MirroredSchema>,
    store: Arc<RwLock<RegionStore>>,
    shutdown: &mut std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
) -> Result<()> {
    let meta = TableMeta::from_schema(&schema)?;
    let mut backoff = BACKOFF_MIN;

    loop {
        let result = session(
            region, &database, &bind_url, &schema, &meta, &store, shutdown,
        )
        .await;

        match result {
            Ok(SessionEnd::Shutdown) => {
                tracing::info!(target: "relay_cache::shard", region, "shard shutting down");
                clear_store(&store, region);
                return Ok(());
            }
            Ok(SessionEnd::Disconnected { connected_at }) => {
                clear_store(&store, region);
                if connected_at.elapsed() >= STABLE_AFTER {
                    backoff = BACKOFF_MIN;
                }
                tracing::warn!(
                    target: "relay_cache::shard",
                    region,
                    backoff_secs = backoff.as_secs(),
                    "disconnected; reconnecting"
                );
            }
            Err(e) => {
                clear_store(&store, region);
                tracing::warn!(
                    target: "relay_cache::shard",
                    region,
                    error = %e,
                    backoff_secs = backoff.as_secs(),
                    "session error; reconnecting"
                );
            }
        }

        tokio::select! {
            biased;
            _ = &mut *shutdown => {
                return Ok(());
            }
            _ = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff * 2).min(BACKOFF_MAX);
    }
}

enum SessionEnd {
    Shutdown,
    Disconnected { connected_at: Instant },
}

async fn session(
    region: u32,
    database: &str,
    bind_url: &Url,
    schema: &MirroredSchema,
    meta: &TableMeta,
    store: &Arc<RwLock<RegionStore>>,
    shutdown: &mut std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
) -> Result<SessionEnd> {
    tracing::info!(
        target: "relay_cache::shard",
        region,
        database,
        %bind_url,
        "connecting"
    );
    let mut conn = wire::open_connection(bind_url, database).await?;
    let connected_at = Instant::now();
    let _ = wire::expect_initial_connection(&mut conn).await?;

    let queries = vec![
        format!("SELECT * FROM {CLAIM_TABLE}"),
        format!("SELECT * FROM {BUILDING_TABLE}"),
        format!("SELECT * FROM {INVENTORY_TABLE}"),
        format!("SELECT * FROM {BUILDING_DESC_TABLE}"),
        format!("SELECT * FROM {BUILDING_NICKNAME_TABLE}"),
    ];
    wire::send_subscribe(&mut conn, 1, 1, queries).await?;

    let sa = wire::expect_subscribe_applied(&mut conn).await?;
    match bulk_load(region, schema, meta, &sa) {
        Ok(fresh) => {
            let n_claim = fresh.claim.len();
            let n_building = fresh.building.len();
            let n_inventory = fresh.inventory.len();
            let n_building_desc = fresh.building_desc.len();
            let n_building_nickname = fresh.building_nickname.len();
            {
                let mut guard = store.write();
                *guard = fresh;
            }
            tracing::info!(
                target: "relay_cache::shard",
                region,
                n_claim,
                n_building,
                n_inventory,
                n_building_desc,
                n_building_nickname,
                "SubscribeApplied loaded"
            );
        }
        Err(e) => {
            bail!("bulk load failed: {e}");
        }
    }

    let mut ping = tokio::time::interval(PING_INTERVAL);
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            biased;
            _ = &mut *shutdown => {
                return Ok(SessionEnd::Shutdown);
            }
            _ = ping.tick() => {
                if let Err(e) = wire::send_ping(&mut conn).await {
                    tracing::warn!(
                        target: "relay_cache::shard",
                        region,
                        error = %e,
                        "ping failed"
                    );
                    return Ok(SessionEnd::Disconnected { connected_at });
                }
            }
            msg = wire::recv_server_message(&mut conn) => {
                match msg {
                    Ok(ServerMessage::TransactionUpdate(tu)) => {
                        if let Err(e) = apply_transaction(store, schema, meta, &tu) {
                            tracing::warn!(
                                target: "relay_cache::shard",
                                region,
                                error = %e,
                                "apply TransactionUpdate failed; reconnecting"
                            );
                            return Err(e);
                        }
                    }
                    Ok(ServerMessage::SubscriptionError(err)) => {
                        bail!("subscription error: {}", err.error);
                    }
                    Ok(other) => {
                        tracing::debug!(
                            target: "relay_cache::shard",
                            region,
                            ?other,
                            "ignoring server message"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            target: "relay_cache::shard",
                            region,
                            error = %e,
                            "recv failed"
                        );
                        return Ok(SessionEnd::Disconnected { connected_at });
                    }
                }
            }
        }
    }
}

fn clear_store(store: &Arc<RwLock<RegionStore>>, region: u32) {
    let mut guard = store.write();
    *guard = RegionStore::empty(region);
}

fn bulk_load(
    region: u32,
    schema: &MirroredSchema,
    meta: &TableMeta,
    sa: &SubscribeApplied,
) -> Result<RegionStore> {
    let mut fresh = RegionStore::empty(region);
    for table in sa.rows.tables.iter() {
        let name: &str = table.table.as_ref();
        let rows: Vec<Bytes> = table.rows.into_iter().collect();
        apply_rows(&mut fresh, schema, meta, name, &[], &rows)?;
    }
    fresh.ready = true;
    Ok(fresh)
}

fn apply_transaction(
    store: &Arc<RwLock<RegionStore>>,
    schema: &MirroredSchema,
    meta: &TableMeta,
    tu: &TransactionUpdate,
) -> Result<()> {
    let mut guard = store.write();
    if !guard.ready {
        // Shouldn't happen in the stream phase, but stay defensive.
        return Ok(());
    }
    for set in tu.query_sets.iter() {
        for table in set.tables.iter() {
            let name: &str = table.table_name.as_ref();
            let mut deletes: Vec<Bytes> = Vec::new();
            let mut inserts: Vec<Bytes> = Vec::new();
            for rows in table.rows.iter() {
                match rows {
                    TableUpdateRows::PersistentTable(p) => {
                        deletes.extend((&p.deletes).into_iter());
                        inserts.extend((&p.inserts).into_iter());
                    }
                    TableUpdateRows::EventTable(e) => {
                        inserts.extend((&e.events).into_iter());
                    }
                }
            }
            if deletes.is_empty() && inserts.is_empty() {
                continue;
            }
            apply_rows(&mut guard, schema, meta, name, &deletes, &inserts)?;
        }
    }
    Ok(())
}

fn apply_rows(
    store: &mut RegionStore,
    schema: &MirroredSchema,
    meta: &TableMeta,
    table: &str,
    deletes: &[Bytes],
    inserts: &[Bytes],
) -> Result<()> {
    match table {
        CLAIM_TABLE => {
            for row in deletes {
                let decoded = decode::decode_claim_with_fields(
                    row,
                    &meta.claim_fields,
                    meta.cols.claim,
                    schema,
                )?;
                store.claim.delete(decoded.entity_id);
            }
            for row in inserts {
                let decoded = decode::decode_claim_with_fields(
                    row,
                    &meta.claim_fields,
                    meta.cols.claim,
                    schema,
                )?;
                store.claim.upsert(decoded);
            }
        }
        BUILDING_TABLE => {
            for row in deletes {
                let decoded = decode::decode_building_with_fields(
                    row,
                    &meta.building_fields,
                    meta.cols.building,
                    schema,
                )?;
                store.building.delete(decoded.entity_id);
            }
            for row in inserts {
                let decoded = decode::decode_building_with_fields(
                    row,
                    &meta.building_fields,
                    meta.cols.building,
                    schema,
                )?;
                store.building.upsert(decoded);
            }
        }
        INVENTORY_TABLE => {
            for row in deletes {
                let decoded = decode::decode_inventory_with_fields(
                    row,
                    &meta.inventory_fields,
                    meta.cols.inventory,
                    schema,
                )?;
                store.inventory.delete(decoded.entity_id);
            }
            for row in inserts {
                let decoded = decode::decode_inventory_with_fields(
                    row,
                    &meta.inventory_fields,
                    meta.cols.inventory,
                    schema,
                )?;
                store.inventory.upsert(decoded);
            }
        }
        BUILDING_DESC_TABLE => {
            for row in deletes {
                let decoded = decode::decode_building_desc_with_fields(
                    row,
                    &meta.building_desc_fields,
                    meta.cols.building_desc,
                    schema,
                )?;
                store.building_desc.delete(decoded.id);
            }
            for row in inserts {
                let decoded = decode::decode_building_desc_with_fields(
                    row,
                    &meta.building_desc_fields,
                    meta.cols.building_desc,
                    schema,
                )?;
                store.building_desc.upsert(decoded);
            }
        }
        BUILDING_NICKNAME_TABLE => {
            for row in deletes {
                let decoded = decode::decode_building_nickname_with_fields(
                    row,
                    &meta.building_nickname_fields,
                    meta.cols.building_nickname,
                    schema,
                )?;
                store.building_nickname.delete(decoded.entity_id);
            }
            for row in inserts {
                let decoded = decode::decode_building_nickname_with_fields(
                    row,
                    &meta.building_nickname_fields,
                    meta.cols.building_nickname,
                    schema,
                )?;
                store.building_nickname.upsert(decoded);
            }
        }
        other => {
            tracing::debug!(
                target: "relay_cache::shard",
                table = %other,
                "ignoring unexpected table in update"
            );
        }
    }
    Ok(())
}
