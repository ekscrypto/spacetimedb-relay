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
    self, ColMaps, BUILDING_DESC_TABLE, BUILDING_NICKNAME_TABLE, BUILDING_TABLE, CLAIM_LOCAL_TABLE,
    CLAIM_MEMBER_TABLE, CLAIM_TABLE, CLAIM_TECH_DESC_TABLE, CLAIM_TECH_STATE_TABLE,
    CLAIM_TILE_COST_TABLE, CRAFTING_RECIPE_DESC_TABLE, DEPLETED_HEXITE_DEPOSIT_RESOURCE_ID,
    DEPLOYABLE_DESC_TABLE, DEPLOYABLE_TABLE, DIMENSION_NETWORK_TABLE, EXPERIENCE_TABLE,
    GROWTH_TABLE, HEXITE_DEPOSIT_RESOURCE_ID, INVENTORY_TABLE, LOCATION_TABLE, PASSIVE_CRAFT_TABLE,
    PLAYER_HOUSING_DESC_TABLE, PLAYER_HOUSING_TABLE, PLAYER_STATE_TABLE, PLAYER_USERNAME_TABLE,
    PROGRESSIVE_ACTION_TABLE, RENT_TABLE, RESOURCE_TABLE, SKILL_DESC_TABLE,
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
    claim_local_fields: Vec<MirroredField>,
    claim_member_fields: Vec<MirroredField>,
    claim_tech_state_fields: Vec<MirroredField>,
    claim_tech_desc_fields: Vec<MirroredField>,
    claim_tile_cost_fields: Vec<MirroredField>,
    building_fields: Vec<MirroredField>,
    inventory_fields: Vec<MirroredField>,
    building_desc_fields: Vec<MirroredField>,
    building_nickname_fields: Vec<MirroredField>,
    location_fields: Vec<MirroredField>,
    dimension_network_fields: Vec<MirroredField>,
    player_username_fields: Vec<MirroredField>,
    player_state_fields: Vec<MirroredField>,
    deployable_fields: Vec<MirroredField>,
    deployable_desc_fields: Vec<MirroredField>,
    player_housing_fields: Vec<MirroredField>,
    player_housing_desc_fields: Vec<MirroredField>,
    rent_fields: Vec<MirroredField>,
    experience_fields: Vec<MirroredField>,
    skill_desc_fields: Vec<MirroredField>,
    progressive_action_fields: Vec<MirroredField>,
    passive_craft_fields: Vec<MirroredField>,
    crafting_recipe_desc_fields: Vec<MirroredField>,
    resource_fields: Vec<MirroredField>,
    growth_fields: Vec<MirroredField>,
}

impl TableMeta {
    fn from_schema(schema: &MirroredSchema) -> Result<Self> {
        let cols = decode::resolve_cols(schema)?;
        Ok(Self {
            cols,
            claim_fields: fields_owned(schema, CLAIM_TABLE)?,
            claim_local_fields: fields_owned(schema, CLAIM_LOCAL_TABLE)?,
            claim_member_fields: fields_owned(schema, CLAIM_MEMBER_TABLE)?,
            claim_tech_state_fields: fields_owned(schema, CLAIM_TECH_STATE_TABLE)?,
            claim_tech_desc_fields: fields_owned(schema, CLAIM_TECH_DESC_TABLE)?,
            claim_tile_cost_fields: fields_owned(schema, CLAIM_TILE_COST_TABLE)?,
            building_fields: fields_owned(schema, BUILDING_TABLE)?,
            inventory_fields: fields_owned(schema, INVENTORY_TABLE)?,
            building_desc_fields: fields_owned(schema, BUILDING_DESC_TABLE)?,
            building_nickname_fields: fields_owned(schema, BUILDING_NICKNAME_TABLE)?,
            location_fields: fields_owned(schema, LOCATION_TABLE)?,
            dimension_network_fields: fields_owned(schema, DIMENSION_NETWORK_TABLE)?,
            player_username_fields: fields_owned(schema, PLAYER_USERNAME_TABLE)?,
            player_state_fields: fields_owned(schema, PLAYER_STATE_TABLE)?,
            deployable_fields: fields_owned(schema, DEPLOYABLE_TABLE)?,
            deployable_desc_fields: fields_owned(schema, DEPLOYABLE_DESC_TABLE)?,
            player_housing_fields: fields_owned(schema, PLAYER_HOUSING_TABLE)?,
            player_housing_desc_fields: fields_owned(schema, PLAYER_HOUSING_DESC_TABLE)?,
            rent_fields: fields_owned(schema, RENT_TABLE)?,
            experience_fields: fields_owned(schema, EXPERIENCE_TABLE)?,
            skill_desc_fields: fields_owned(schema, SKILL_DESC_TABLE)?,
            progressive_action_fields: fields_owned(schema, PROGRESSIVE_ACTION_TABLE)?,
            passive_craft_fields: fields_owned(schema, PASSIVE_CRAFT_TABLE)?,
            crafting_recipe_desc_fields: fields_owned(schema, CRAFTING_RECIPE_DESC_TABLE)?,
            resource_fields: fields_owned(schema, RESOURCE_TABLE)?,
            growth_fields: fields_owned(schema, GROWTH_TABLE)?,
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

    let base_queries = base_subscribe_queries();
    // Phase 1: discover hexite entity_ids (coords need a follow-up PK
    // subscribe — full location_state is ~13M rows/region).
    wire::send_subscribe(&mut conn, 1, 1, base_queries.clone()).await?;
    let sa1 = wire::expect_subscribe_applied(&mut conn).await?;
    let hexite_ids = collect_hexite_entity_ids(schema, meta, &sa1)?;

    let mut queries = base_queries;
    for entity_id in &hexite_ids {
        queries.push(format!(
            "SELECT * FROM {LOCATION_TABLE} WHERE entity_id = {entity_id}"
        ));
    }
    if !hexite_ids.is_empty() {
        wire::send_subscribe(&mut conn, 2, 1, queries).await?;
        let sa = wire::expect_subscribe_applied(&mut conn).await?;
        install_bulk_load(region, schema, meta, store, &sa)?;
    } else {
        install_bulk_load(region, schema, meta, store, &sa1)?;
    }

    let n_resource = store.read().resource.len();
    tracing::info!(
        target: "relay_cache::shard",
        region,
        n_hexite = hexite_ids.len(),
        n_resource,
        "hexite location subscribe complete"
    );

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

fn base_subscribe_queries() -> Vec<String> {
    vec![
        format!("SELECT * FROM {CLAIM_TABLE}"),
        format!("SELECT * FROM {CLAIM_LOCAL_TABLE}"),
        format!("SELECT * FROM {CLAIM_MEMBER_TABLE}"),
        format!("SELECT * FROM {CLAIM_TECH_STATE_TABLE}"),
        format!("SELECT * FROM {CLAIM_TECH_DESC_TABLE}"),
        format!("SELECT * FROM {CLAIM_TILE_COST_TABLE}"),
        format!("SELECT * FROM {BUILDING_TABLE}"),
        format!("SELECT * FROM {INVENTORY_TABLE}"),
        format!("SELECT * FROM {BUILDING_DESC_TABLE}"),
        format!("SELECT * FROM {BUILDING_NICKNAME_TABLE}"),
        // Full location_state is ~13M rows/region; interiors-only is enough
        // because overworld buildings default to dimension 1 when absent.
        // Hexite deposit coords are added as per-entity PK queries after
        // phase-1 discovers their entity_ids.
        format!("SELECT * FROM {LOCATION_TABLE} WHERE dimension != 1"),
        format!("SELECT * FROM {DIMENSION_NETWORK_TABLE}"),
        format!("SELECT * FROM {PLAYER_USERNAME_TABLE}"),
        format!("SELECT * FROM {PLAYER_STATE_TABLE}"),
        format!("SELECT * FROM {DEPLOYABLE_TABLE}"),
        format!("SELECT * FROM {DEPLOYABLE_DESC_TABLE}"),
        format!("SELECT * FROM {PLAYER_HOUSING_TABLE}"),
        format!("SELECT * FROM {PLAYER_HOUSING_DESC_TABLE}"),
        format!("SELECT * FROM {RENT_TABLE}"),
        format!("SELECT * FROM {EXPERIENCE_TABLE}"),
        format!("SELECT * FROM {SKILL_DESC_TABLE}"),
        format!("SELECT * FROM {PROGRESSIVE_ACTION_TABLE}"),
        format!("SELECT * FROM {PASSIVE_CRAFT_TABLE}"),
        format!("SELECT * FROM {CRAFTING_RECIPE_DESC_TABLE}"),
        // Two equality filters — safer than OR for SpacetimeDB SQL.
        format!(
            "SELECT * FROM {RESOURCE_TABLE} WHERE resource_id = {HEXITE_DEPOSIT_RESOURCE_ID}"
        ),
        format!(
            "SELECT * FROM {RESOURCE_TABLE} WHERE resource_id = {DEPLETED_HEXITE_DEPOSIT_RESOURCE_ID}"
        ),
        // Public growth countdowns (Hexite depleted→grown, Maker's Tree, …).
        // Exact respawn_at for depleted Hexite is growth_state.end_timestamp.
        format!("SELECT * FROM {GROWTH_TABLE}"),
    ]
}

fn collect_hexite_entity_ids(
    schema: &MirroredSchema,
    meta: &TableMeta,
    sa: &SubscribeApplied,
) -> Result<Vec<u64>> {
    let mut ids = Vec::new();
    for table in sa.rows.tables.iter() {
        let name: &str = table.table.as_ref();
        if name != RESOURCE_TABLE {
            continue;
        }
        for row in (&table.rows).into_iter() {
            let decoded = decode::decode_resource_with_fields(
                &row,
                &meta.resource_fields,
                meta.cols.resource,
                schema,
            )?;
            ids.push(decoded.entity_id);
        }
    }
    ids.sort_unstable();
    ids.dedup();
    Ok(ids)
}

fn install_bulk_load(
    region: u32,
    schema: &MirroredSchema,
    meta: &TableMeta,
    store: &Arc<RwLock<RegionStore>>,
    sa: &SubscribeApplied,
) -> Result<()> {
    match bulk_load(region, schema, meta, sa) {
        Ok(fresh) => {
            let n_claim = fresh.claim.len();
            let n_claim_local = fresh.claim_local.len();
            let n_claim_member = fresh.claim_member.len();
            let n_claim_tech_state = fresh.claim_tech_state.len();
            let n_claim_tech_desc = fresh.claim_tech_desc.len();
            let n_claim_tile_cost = fresh.claim_tile_cost.len();
            let n_building = fresh.building.len();
            let n_inventory = fresh.inventory.len();
            let n_building_desc = fresh.building_desc.len();
            let n_building_nickname = fresh.building_nickname.len();
            let n_location_dim = fresh.location_dim.len();
            let n_dimension_network = fresh.dimension_network.len();
            let n_player_username = fresh.player_username.len();
            let n_player_state = fresh.player_state.len();
            let n_deployable = fresh.deployable.len();
            let n_deployable_desc = fresh.deployable_desc.len();
            let n_player_housing = fresh.player_housing.len();
            let n_player_housing_desc = fresh.player_housing_desc.len();
            let n_rent = fresh.rent.len();
            let n_experience = fresh.experience.len();
            let n_skill_desc = fresh.skill_desc.len();
            let n_progressive_action = fresh.progressive_action.len();
            let n_passive_craft = fresh.passive_craft.len();
            let n_crafting_recipe_desc = fresh.crafting_recipe_desc.len();
            let n_resource = fresh.resource.len();
            let n_growth = fresh.growth.len();
            {
                let mut guard = store.write();
                *guard = fresh;
            }
            tracing::info!(
                target: "relay_cache::shard",
                region,
                n_claim,
                n_claim_local,
                n_claim_member,
                n_claim_tech_state,
                n_claim_tech_desc,
                n_claim_tile_cost,
                n_building,
                n_inventory,
                n_building_desc,
                n_building_nickname,
                n_location_dim,
                n_dimension_network,
                n_player_username,
                n_player_state,
                n_deployable,
                n_deployable_desc,
                n_player_housing,
                n_player_housing_desc,
                n_rent,
                n_experience,
                n_skill_desc,
                n_progressive_action,
                n_passive_craft,
                n_crafting_recipe_desc,
                n_resource,
                n_growth,
                "SubscribeApplied loaded"
            );
            Ok(())
        }
        Err(e) => bail!("bulk load failed: {e}"),
    }
}

fn bulk_load(
    region: u32,
    schema: &MirroredSchema,
    meta: &TableMeta,
    sa: &SubscribeApplied,
) -> Result<RegionStore> {
    let mut fresh = RegionStore::empty(region);
    // Resources before locations so hexite PK location rows can attach x/z.
    for table in sa.rows.tables.iter() {
        let name: &str = table.table.as_ref();
        if name == LOCATION_TABLE {
            continue;
        }
        let rows: Vec<Bytes> = table.rows.into_iter().collect();
        apply_rows(&mut fresh, schema, meta, name, &[], &rows)?;
    }
    for table in sa.rows.tables.iter() {
        let name: &str = table.table.as_ref();
        if name != LOCATION_TABLE {
            continue;
        }
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
        LOCATION_TABLE => {
            for row in deletes {
                let decoded = decode::decode_location_with_fields(
                    row,
                    &meta.location_fields,
                    meta.cols.location,
                    schema,
                )?;
                store.location_dim.delete(decoded.entity_id);
                store.resource.clear_location(decoded.entity_id);
            }
            for row in inserts {
                let decoded = decode::decode_location_with_fields(
                    row,
                    &meta.location_fields,
                    meta.cols.location,
                    schema,
                )?;
                store.location_dim.upsert(decode::LocationDimRow {
                    entity_id: decoded.entity_id,
                    dimension: decoded.dimension,
                });
                // Hexite PK location subscribes land here too; stash x/z
                // onto the resource row (overworld deposits are dimension 1).
                store
                    .resource
                    .set_location(decoded.entity_id, decoded.x, decoded.z);
            }
        }
        DIMENSION_NETWORK_TABLE => {
            for row in deletes {
                let decoded = decode::decode_dimension_network_with_fields(
                    row,
                    &meta.dimension_network_fields,
                    meta.cols.dimension_network,
                    schema,
                )?;
                store.dimension_network.delete_by_entity(decoded.entity_id);
            }
            for row in inserts {
                let decoded = decode::decode_dimension_network_with_fields(
                    row,
                    &meta.dimension_network_fields,
                    meta.cols.dimension_network,
                    schema,
                )?;
                store.dimension_network.upsert(decoded);
            }
        }
        PLAYER_USERNAME_TABLE => {
            for row in deletes {
                let decoded = decode::decode_player_username_with_fields(
                    row,
                    &meta.player_username_fields,
                    meta.cols.player_username,
                    schema,
                )?;
                store.player_username.delete(decoded.entity_id);
            }
            for row in inserts {
                let decoded = decode::decode_player_username_with_fields(
                    row,
                    &meta.player_username_fields,
                    meta.cols.player_username,
                    schema,
                )?;
                store.player_username.upsert(decoded);
            }
        }
        PLAYER_STATE_TABLE => {
            for row in deletes {
                let decoded = decode::decode_player_state_with_fields(
                    row,
                    &meta.player_state_fields,
                    meta.cols.player_state,
                    schema,
                )?;
                store.player_state.delete(decoded.entity_id);
            }
            for row in inserts {
                let decoded = decode::decode_player_state_with_fields(
                    row,
                    &meta.player_state_fields,
                    meta.cols.player_state,
                    schema,
                )?;
                store.player_state.upsert(decoded);
            }
        }
        DEPLOYABLE_TABLE => {
            for row in deletes {
                let decoded = decode::decode_deployable_with_fields(
                    row,
                    &meta.deployable_fields,
                    meta.cols.deployable,
                    schema,
                )?;
                store.deployable.delete(decoded.entity_id);
            }
            for row in inserts {
                let decoded = decode::decode_deployable_with_fields(
                    row,
                    &meta.deployable_fields,
                    meta.cols.deployable,
                    schema,
                )?;
                store.deployable.upsert(decoded);
            }
        }
        DEPLOYABLE_DESC_TABLE => {
            for row in deletes {
                let decoded = decode::decode_deployable_desc_with_fields(
                    row,
                    &meta.deployable_desc_fields,
                    meta.cols.deployable_desc,
                    schema,
                )?;
                store.deployable_desc.delete(decoded.id);
            }
            for row in inserts {
                let decoded = decode::decode_deployable_desc_with_fields(
                    row,
                    &meta.deployable_desc_fields,
                    meta.cols.deployable_desc,
                    schema,
                )?;
                store.deployable_desc.upsert(decoded);
            }
        }
        PLAYER_HOUSING_TABLE => {
            for row in deletes {
                let decoded = decode::decode_player_housing_with_fields(
                    row,
                    &meta.player_housing_fields,
                    meta.cols.player_housing,
                    schema,
                )?;
                store.player_housing.delete(decoded.entity_id);
            }
            for row in inserts {
                let decoded = decode::decode_player_housing_with_fields(
                    row,
                    &meta.player_housing_fields,
                    meta.cols.player_housing,
                    schema,
                )?;
                store.player_housing.upsert(decoded);
            }
        }
        PLAYER_HOUSING_DESC_TABLE => {
            for row in deletes {
                let decoded = decode::decode_player_housing_desc_with_fields(
                    row,
                    &meta.player_housing_desc_fields,
                    meta.cols.player_housing_desc,
                    schema,
                )?;
                store.player_housing_desc.delete_rank(decoded.rank);
            }
            for row in inserts {
                let decoded = decode::decode_player_housing_desc_with_fields(
                    row,
                    &meta.player_housing_desc_fields,
                    meta.cols.player_housing_desc,
                    schema,
                )?;
                store.player_housing_desc.upsert(decoded);
            }
        }
        RENT_TABLE => {
            for row in deletes {
                let decoded = decode::decode_rent_with_fields(
                    row,
                    &meta.rent_fields,
                    meta.cols.rent,
                    schema,
                )?;
                store.rent.delete(decoded.entity_id);
            }
            for row in inserts {
                let decoded = decode::decode_rent_with_fields(
                    row,
                    &meta.rent_fields,
                    meta.cols.rent,
                    schema,
                )?;
                store.rent.upsert(decoded);
            }
        }
        CLAIM_LOCAL_TABLE => {
            for row in deletes {
                let decoded = decode::decode_claim_local_with_fields(
                    row,
                    &meta.claim_local_fields,
                    meta.cols.claim_local,
                    schema,
                )?;
                store.claim_local.delete(decoded.entity_id);
            }
            for row in inserts {
                let decoded = decode::decode_claim_local_with_fields(
                    row,
                    &meta.claim_local_fields,
                    meta.cols.claim_local,
                    schema,
                )?;
                store.claim_local.upsert(decoded);
            }
        }
        CLAIM_MEMBER_TABLE => {
            for row in deletes {
                let decoded = decode::decode_claim_member_with_fields(
                    row,
                    &meta.claim_member_fields,
                    meta.cols.claim_member,
                    schema,
                )?;
                store.claim_member.delete(decoded.entity_id);
            }
            for row in inserts {
                let decoded = decode::decode_claim_member_with_fields(
                    row,
                    &meta.claim_member_fields,
                    meta.cols.claim_member,
                    schema,
                )?;
                store.claim_member.upsert(decoded);
            }
        }
        CLAIM_TECH_STATE_TABLE => {
            for row in deletes {
                let decoded = decode::decode_claim_tech_state_with_fields(
                    row,
                    &meta.claim_tech_state_fields,
                    meta.cols.claim_tech_state,
                    schema,
                )?;
                store.claim_tech_state.delete(decoded.entity_id);
            }
            for row in inserts {
                let decoded = decode::decode_claim_tech_state_with_fields(
                    row,
                    &meta.claim_tech_state_fields,
                    meta.cols.claim_tech_state,
                    schema,
                )?;
                store.claim_tech_state.upsert(decoded);
            }
        }
        CLAIM_TECH_DESC_TABLE => {
            for row in deletes {
                let decoded = decode::decode_claim_tech_desc_with_fields(
                    row,
                    &meta.claim_tech_desc_fields,
                    meta.cols.claim_tech_desc,
                    schema,
                )?;
                store.claim_tech_desc.delete(decoded.id);
            }
            for row in inserts {
                let decoded = decode::decode_claim_tech_desc_with_fields(
                    row,
                    &meta.claim_tech_desc_fields,
                    meta.cols.claim_tech_desc,
                    schema,
                )?;
                store.claim_tech_desc.upsert(decoded);
            }
        }
        CLAIM_TILE_COST_TABLE => {
            for row in deletes {
                let decoded = decode::decode_claim_tile_cost_with_fields(
                    row,
                    &meta.claim_tile_cost_fields,
                    meta.cols.claim_tile_cost,
                    schema,
                )?;
                store.claim_tile_cost.delete(decoded.tile_count);
            }
            for row in inserts {
                let decoded = decode::decode_claim_tile_cost_with_fields(
                    row,
                    &meta.claim_tile_cost_fields,
                    meta.cols.claim_tile_cost,
                    schema,
                )?;
                store.claim_tile_cost.upsert(decoded);
            }
        }
        EXPERIENCE_TABLE => {
            for row in deletes {
                let decoded = decode::decode_experience_with_fields(
                    row,
                    &meta.experience_fields,
                    meta.cols.experience,
                    schema,
                )?;
                store.experience.delete(decoded.entity_id);
            }
            for row in inserts {
                let decoded = decode::decode_experience_with_fields(
                    row,
                    &meta.experience_fields,
                    meta.cols.experience,
                    schema,
                )?;
                store.experience.upsert(decoded);
            }
        }
        SKILL_DESC_TABLE => {
            for row in deletes {
                let decoded = decode::decode_skill_desc_with_fields(
                    row,
                    &meta.skill_desc_fields,
                    meta.cols.skill_desc,
                    schema,
                )?;
                store.skill_desc.delete(decoded.id);
            }
            for row in inserts {
                let decoded = decode::decode_skill_desc_with_fields(
                    row,
                    &meta.skill_desc_fields,
                    meta.cols.skill_desc,
                    schema,
                )?;
                store.skill_desc.upsert(decoded);
            }
        }
        PROGRESSIVE_ACTION_TABLE => {
            for row in deletes {
                let decoded = decode::decode_progressive_action_with_fields(
                    row,
                    &meta.progressive_action_fields,
                    meta.cols.progressive_action,
                    schema,
                )?;
                store.progressive_action.delete(decoded.entity_id);
            }
            for row in inserts {
                let decoded = decode::decode_progressive_action_with_fields(
                    row,
                    &meta.progressive_action_fields,
                    meta.cols.progressive_action,
                    schema,
                )?;
                store.progressive_action.upsert(decoded);
            }
        }
        PASSIVE_CRAFT_TABLE => {
            for row in deletes {
                let decoded = decode::decode_passive_craft_with_fields(
                    row,
                    &meta.passive_craft_fields,
                    meta.cols.passive_craft,
                    schema,
                )?;
                store.passive_craft.delete(decoded.entity_id);
            }
            for row in inserts {
                let decoded = decode::decode_passive_craft_with_fields(
                    row,
                    &meta.passive_craft_fields,
                    meta.cols.passive_craft,
                    schema,
                )?;
                store.passive_craft.upsert(decoded);
            }
        }
        CRAFTING_RECIPE_DESC_TABLE => {
            for row in deletes {
                let decoded = decode::decode_crafting_recipe_desc_with_fields(
                    row,
                    &meta.crafting_recipe_desc_fields,
                    meta.cols.crafting_recipe_desc,
                    schema,
                )?;
                store.crafting_recipe_desc.delete(decoded.id);
            }
            for row in inserts {
                let decoded = decode::decode_crafting_recipe_desc_with_fields(
                    row,
                    &meta.crafting_recipe_desc_fields,
                    meta.cols.crafting_recipe_desc,
                    schema,
                )?;
                store.crafting_recipe_desc.upsert(decoded);
            }
        }
        RESOURCE_TABLE => {
            for row in deletes {
                let decoded = decode::decode_resource_with_fields(
                    row,
                    &meta.resource_fields,
                    meta.cols.resource,
                    schema,
                )?;
                store.resource.delete(decoded.entity_id);
            }
            for row in inserts {
                let decoded = decode::decode_resource_with_fields(
                    row,
                    &meta.resource_fields,
                    meta.cols.resource,
                    schema,
                )?;
                store.resource.upsert(decoded);
            }
        }
        GROWTH_TABLE => {
            for row in deletes {
                let decoded = decode::decode_growth_with_fields(
                    row,
                    &meta.growth_fields,
                    meta.cols.growth,
                    schema,
                )?;
                store.growth.delete(decoded.entity_id);
            }
            for row in inserts {
                let decoded = decode::decode_growth_with_fields(
                    row,
                    &meta.growth_fields,
                    meta.cols.growth,
                    schema,
                )?;
                store.growth.upsert(decoded);
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
