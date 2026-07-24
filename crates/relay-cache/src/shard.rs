// SPDX-License-Identifier: MIT

//! Per-region subscription task: connect → SubscribeApplied bulk load →
//! stream TransactionUpdates into the columnar store, with reconnect.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
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
    GROWTH_TABLE, HEXITE_DEPOSIT_RESOURCE_ID, INVENTORY_TABLE, LOCATION_TABLE, MOBILE_ENTITY_TABLE,
    OVERWORLD_DIMENSION, PASSIVE_CRAFT_TABLE, PLAYER_HOUSING_DESC_TABLE, PLAYER_HOUSING_TABLE,
    PLAYER_STATE_TABLE, PLAYER_USERNAME_TABLE, PROGRESSIVE_ACTION_TABLE, RENT_TABLE,
    RESOURCE_TABLE, SKILL_DESC_TABLE, STORAGE_LOG_TABLE,
};
use crate::interest::{InterestHub, TouchBatch};
use crate::store::RegionStore;
use crate::wire;

const PING_INTERVAL: Duration = Duration::from_secs(10);
const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);
const STABLE_AFTER: Duration = Duration::from_secs(5);
/// How often to re-check hexite location coverage after ready.
const HEXITE_INTEGRITY_INTERVAL: Duration = Duration::from_secs(30);
/// Don't trip integrity reconnect until the location PK phase has had
/// time to land (and claim_local joins to settle).
const HEXITE_INTEGRITY_GRACE: Duration = Duration::from_secs(15);
const METRICS_POLL_TIMEOUT: Duration = Duration::from_secs(2);
const READY_GATE_BACKOFF_MIN: Duration = Duration::from_secs(2);
const READY_GATE_BACKOFF_MAX: Duration = Duration::from_secs(30);

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
    mobile_entity_fields: Vec<MirroredField>,
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
    storage_log_fields: Vec<MirroredField>,
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
            mobile_entity_fields: fields_owned(schema, MOBILE_ENTITY_TABLE)?,
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
            storage_log_fields: fields_owned(schema, STORAGE_LOG_TABLE)?,
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
#[allow(clippy::too_many_arguments)]
pub fn spawn_shard(
    region: u32,
    database: String,
    bind_url: Url,
    dashboard_port: u16,
    schema: Arc<MirroredSchema>,
    interest: Arc<InterestHub>,
    debug_mode: bool,
    mut shutdown: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
) -> Arc<ShardHandle> {
    let handle = Arc::new(ShardHandle {
        region,
        store: Arc::new(RwLock::new(RegionStore::empty(region))),
    });
    let store = handle.store.clone();
    tokio::spawn(async move {
        if let Err(e) = run_shard_loop(
            region,
            database,
            bind_url,
            dashboard_port,
            schema,
            store,
            interest,
            debug_mode,
            &mut shutdown,
        )
        .await
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

#[allow(clippy::too_many_arguments)]
async fn run_shard_loop(
    region: u32,
    database: String,
    bind_url: Url,
    dashboard_port: u16,
    schema: Arc<MirroredSchema>,
    store: Arc<RwLock<RegionStore>>,
    interest: Arc<InterestHub>,
    debug_mode: bool,
    shutdown: &mut std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
) -> Result<()> {
    let meta = TableMeta::from_schema(&schema)?;
    let mut backoff = BACKOFF_MIN;
    let http = reqwest::Client::builder()
        .timeout(METRICS_POLL_TIMEOUT)
        .build()
        .context("build metrics HTTP client")?;

    loop {
        match wait_for_relay_ready(region, dashboard_port, &http, shutdown).await? {
            ReadyGate::Shutdown => {
                tracing::info!(target: "relay_cache::shard", region, "shard shutting down");
                clear_store(&store, region);
                return Ok(());
            }
            ReadyGate::Ready => {}
        }

        let result = session(
            region, &database, &bind_url, &schema, &meta, &store, &interest, debug_mode, shutdown,
        )
        .await;

        match result {
            Ok(SessionEnd::Shutdown) => {
                tracing::info!(target: "relay_cache::shard", region, "shard shutting down");
                clear_store(&store, region);
                return Ok(());
            }
            Ok(end) => {
                let (connected_at, reason) = match &end {
                    SessionEnd::Disconnected { connected_at } => (*connected_at, "disconnected"),
                    SessionEnd::PrematureEmpty { connected_at } => {
                        (*connected_at, "empty bulk load")
                    }
                    SessionEnd::HexiteLocationsMissing { connected_at } => {
                        (*connected_at, "hexite locations missing")
                    }
                    SessionEnd::Shutdown => unreachable!(),
                };
                clear_store(&store, region);
                if connected_at.elapsed() >= STABLE_AFTER {
                    backoff = BACKOFF_MIN;
                }
                tracing::warn!(
                    target: "relay_cache::shard",
                    region,
                    reason,
                    backoff_secs = backoff.as_secs(),
                    "reconnecting"
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

enum ReadyGate {
    Ready,
    Shutdown,
}

enum SessionEnd {
    Shutdown,
    Disconnected {
        connected_at: Instant,
    },
    /// Base SubscribeApplied had no claims — mirror was empty/mid-sync.
    PrematureEmpty {
        connected_at: Instant,
    },
    /// Hexite resources present without location_state attach.
    HexiteLocationsMissing {
        connected_at: Instant,
    },
}

/// Poll the region's loopback dashboard until upstream + local_stdb are
/// up and `initial_subscribe_complete` is true.
async fn wait_for_relay_ready(
    region: u32,
    dashboard_port: u16,
    http: &reqwest::Client,
    shutdown: &mut std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
) -> Result<ReadyGate> {
    let url = format!("http://127.0.0.1:{dashboard_port}/metrics");
    let mut backoff = READY_GATE_BACKOFF_MIN;
    loop {
        match http.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => {
                match resp.json::<serde_json::Value>().await {
                    Ok(body) if metrics_indicate_ready(&body) => {
                        tracing::info!(
                            target: "relay_cache::shard",
                            region,
                            dashboard_port,
                            "relay ready (upstream+stdb up, initial subscribe complete)"
                        );
                        return Ok(ReadyGate::Ready);
                    }
                    Ok(_) => {
                        tracing::debug!(
                            target: "relay_cache::shard",
                            region,
                            dashboard_port,
                            "relay metrics not ready yet; waiting"
                        );
                    }
                    Err(e) => {
                        tracing::debug!(
                            target: "relay_cache::shard",
                            region,
                            error = %e,
                            "metrics JSON decode failed; waiting"
                        );
                    }
                }
            }
            Ok(resp) => {
                tracing::debug!(
                    target: "relay_cache::shard",
                    region,
                    status = %resp.status(),
                    "metrics HTTP non-success; waiting"
                );
            }
            Err(e) => {
                tracing::debug!(
                    target: "relay_cache::shard",
                    region,
                    error = %e,
                    "metrics poll failed; waiting"
                );
            }
        }

        tokio::select! {
            biased;
            _ = &mut *shutdown => {
                return Ok(ReadyGate::Shutdown);
            }
            _ = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff * 2).min(READY_GATE_BACKOFF_MAX);
    }
}

fn metrics_indicate_ready(body: &serde_json::Value) -> bool {
    let link_up = |key: &str| {
        body.get(key)
            .and_then(|v| v.get("state"))
            .and_then(|s| s.as_str())
            == Some("up")
    };
    let complete = body
        .get("initial_subscribe_complete")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    link_up("upstream") && link_up("local_stdb") && complete
}

#[allow(clippy::too_many_arguments)]
async fn session(
    region: u32,
    database: &str,
    bind_url: &Url,
    schema: &MirroredSchema,
    meta: &TableMeta,
    store: &Arc<RwLock<RegionStore>>,
    interest: &InterestHub,
    debug_mode: bool,
    shutdown: &mut std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
) -> Result<SessionEnd> {
    tracing::info!(
        target: "relay_cache::shard",
        region,
        database,
        %bind_url,
        debug_mode,
        "connecting"
    );
    let mut conn = wire::open_connection(bind_url, database).await?;
    let connected_at = Instant::now();
    let _ = wire::expect_initial_connection(&mut conn).await?;
    tracing::info!(
        target: "relay_cache::shard",
        region,
        handshake_ms = connected_at.elapsed().as_millis() as u64,
        "InitialConnection received"
    );

    let base_queries = base_subscribe_queries();
    // Local stdb handles a single multi-query SubscribeApplied fine (even
    // ~40 MB busy regions finish in ~2s). Hexite location PKs need a second
    // additive query_set after we know entity_ids from resource filters.
    // (An earlier hang on the second Subscribe was a frontend bug: raw
    // ClientMessage Subscribe with request_id=2 was misread as OneOffQuery
    // and dropped — not a local-stdb capacity limit.)
    let load_started = Instant::now();
    let mut building = RegionStore::empty(region);

    const BASE_PHASE: &str = "base_query_set";
    tracing::info!(
        target: "relay_cache::shard",
        region,
        n_queries = base_queries.len(),
        query_set_id = 1,
        "subscribing base query set"
    );
    wire::send_subscribe(&mut conn, 1, 1, base_queries, region, BASE_PHASE).await?;
    let (sa_base, base_wire_bytes) =
        wire::expect_subscribe_applied(&mut conn, region, BASE_PHASE, debug_mode, |tu| {
            apply_transaction_to(&mut building, schema, meta, tu, None)
        })
        .await?;
    let mut hexite_ids = collect_hexite_entity_ids(schema, meta, &sa_base)?;
    merge_subscribe_applied(&mut building, schema, meta, &sa_base)?;
    tracing::info!(
        target: "relay_cache::shard",
        region,
        base_wire_bytes,
        n_hexite = hexite_ids.len(),
        n_claim = building.claim.len(),
        "base query set Applied and merged"
    );

    // Empty mirror (frontend up before sequential sync finished, or race
    // after stdb restart). Never mark ready — reconnect and re-gate.
    if building.claim.len() == 0 {
        tracing::warn!(
            target: "relay_cache::shard",
            region,
            base_wire_bytes,
            "empty bulk load (0 claims); refusing ready"
        );
        return Ok(SessionEnd::PrematureEmpty { connected_at });
    }

    hexite_ids.sort_unstable();
    hexite_ids.dedup();
    let n_hexite = hexite_ids.len();
    if n_hexite > 0 {
        let loc_queries: Vec<String> = hexite_ids
            .iter()
            .map(|entity_id| {
                format!("SELECT * FROM {LOCATION_TABLE} WHERE entity_id = {entity_id}")
            })
            .collect();
        const HEXITE_PHASE: &str = "hexite_locations_query_set";
        tracing::info!(
            target: "relay_cache::shard",
            region,
            n_hexite,
            n_queries = loc_queries.len(),
            query_set_id = 2,
            "subscribing hexite location query set (additive)"
        );
        wire::send_subscribe(&mut conn, 2, 2, loc_queries, region, HEXITE_PHASE).await?;
        let (sa_loc, loc_wire_bytes) =
            wire::expect_subscribe_applied(&mut conn, region, HEXITE_PHASE, debug_mode, |tu| {
                apply_transaction_to(&mut building, schema, meta, tu, None)
            })
            .await?;
        merge_subscribe_applied(&mut building, schema, meta, &sa_loc)?;
        tracing::info!(
            target: "relay_cache::shard",
            region,
            loc_wire_bytes,
            n_hexite,
            "hexite location query set Applied and merged"
        );
    }

    building.ready = true;
    let ready_at = Instant::now();
    let n_resource = building.resource.len();
    let n_claim = building.claim.len();
    let n_growth = building.growth.len();
    {
        let mut guard = store.write();
        *guard = building;
    }
    tracing::info!(
        target: "relay_cache::shard",
        region,
        n_hexite,
        n_resource,
        n_claim,
        n_growth,
        bulk_load_ms = load_started.elapsed().as_millis() as u64,
        session_ms = connected_at.elapsed().as_millis() as u64,
        "subscribe complete; entering stream loop"
    );

    let mut ping = tokio::time::interval(PING_INTERVAL);
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut integrity = tokio::time::interval(HEXITE_INTEGRITY_INTERVAL);
    integrity.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Skip the immediate first tick so grace can elapse.
    integrity.tick().await;

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
            _ = integrity.tick() => {
                if ready_at.elapsed() < HEXITE_INTEGRITY_GRACE {
                    continue;
                }
                let missing = {
                    let s = store.read();
                    s.resource.len() > 0 && s.resource.any_missing_location()
                };
                if missing {
                    tracing::warn!(
                        target: "relay_cache::shard",
                        region,
                        "hexite resources missing location_state; reconnecting to reload"
                    );
                    return Ok(SessionEnd::HexiteLocationsMissing { connected_at });
                }
            }
            frame = wire::recv_server_message(&mut conn) => {
                match frame {
                    Ok(frame) => match frame.message {
                        ServerMessage::TransactionUpdate(tu) => {
                            if let Err(e) =
                                apply_transaction(store, schema, meta, &tu, Some(interest))
                            {
                                tracing::warn!(
                                    target: "relay_cache::shard",
                                    region,
                                    error = %e,
                                    wire_bytes = frame.wire_bytes,
                                    "apply TransactionUpdate failed; reconnecting"
                                );
                                return Err(e);
                            }
                            if debug_mode {
                                tracing::debug!(
                                    target: "relay_cache::shard",
                                    region,
                                    wire_bytes = frame.wire_bytes,
                                    "applied TransactionUpdate"
                                );
                            }
                        }
                        ServerMessage::SubscriptionError(err) => {
                            bail!("subscription error: {}", err.error);
                        }
                        other => {
                            tracing::debug!(
                                target: "relay_cache::shard",
                                region,
                                wire_bytes = frame.wire_bytes,
                                ?other,
                                "ignoring server message"
                            );
                        }
                    },
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
        // Hexite deposit coords are subscribed sequentially after the
        // resource filters below have yielded entity_ids.
        format!("SELECT * FROM {LOCATION_TABLE} WHERE dimension != 1"),
        format!("SELECT * FROM {DIMENSION_NETWORK_TABLE}"),
        format!("SELECT * FROM {PLAYER_USERNAME_TABLE}"),
        format!("SELECT * FROM {PLAYER_STATE_TABLE}"),
        // ~20–25k rows/region; last-active proxy after logout (sign_in_timestamp
        // is zeroed). Private player_timestamp_state is not subscribeable.
        format!("SELECT * FROM {MOBILE_ENTITY_TABLE}"),
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
        // Append-only deposit/withdraw history; upstream cleanup_loop deletes
        // rows older than the retention window (~15–16 days).
        format!("SELECT * FROM {STORAGE_LOG_TABLE}"),
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

fn merge_subscribe_applied(
    store: &mut RegionStore,
    schema: &MirroredSchema,
    meta: &TableMeta,
    sa: &SubscribeApplied,
) -> Result<()> {
    // Non-location tables first so hexite PK location rows can attach x/z.
    for table in sa.rows.tables.iter() {
        let name: &str = table.table.as_ref();
        if name == LOCATION_TABLE {
            continue;
        }
        let rows: Vec<Bytes> = table.rows.into_iter().collect();
        apply_rows(store, schema, meta, name, &[], &rows, None)?;
    }
    for table in sa.rows.tables.iter() {
        let name: &str = table.table.as_ref();
        if name != LOCATION_TABLE {
            continue;
        }
        let rows: Vec<Bytes> = table.rows.into_iter().collect();
        apply_rows(store, schema, meta, name, &[], &rows, None)?;
    }
    Ok(())
}

fn apply_transaction(
    store: &Arc<RwLock<RegionStore>>,
    schema: &MirroredSchema,
    meta: &TableMeta,
    tu: &TransactionUpdate,
    interest: Option<&InterestHub>,
) -> Result<()> {
    let collect = interest.is_some_and(|h| h.has_subscribers());
    let mut touches = collect.then(TouchBatch::default);
    {
        let mut guard = store.write();
        apply_transaction_to(&mut guard, schema, meta, tu, touches.as_mut())?;
    }
    if let (Some(hub), Some(batch)) = (interest, touches) {
        batch.flush(hub);
    }
    Ok(())
}

fn apply_transaction_to(
    store: &mut RegionStore,
    schema: &MirroredSchema,
    meta: &TableMeta,
    tu: &TransactionUpdate,
    mut touches: Option<&mut TouchBatch>,
) -> Result<()> {
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
            apply_rows(
                store,
                schema,
                meta,
                name,
                &deletes,
                &inserts,
                touches.as_deref_mut(),
            )?;
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
    mut touches: Option<&mut TouchBatch>,
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
                if let Some(t) = touches.as_deref_mut() {
                    touch_building(store, decoded.entity_id, decoded.claim_entity_id, t);
                }
                store.building.delete(decoded.entity_id);
            }
            for row in inserts {
                let decoded = decode::decode_building_with_fields(
                    row,
                    &meta.building_fields,
                    meta.cols.building,
                    schema,
                )?;
                store.building.upsert(decoded.clone());
                if let Some(t) = touches.as_deref_mut() {
                    touch_building(store, decoded.entity_id, decoded.claim_entity_id, t);
                }
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
                if let Some(t) = touches.as_deref_mut() {
                    touch_inventory(
                        store,
                        decoded.entity_id,
                        decoded.owner_entity_id,
                        decoded.player_owner_entity_id,
                        t,
                    );
                }
                store.inventory.delete(decoded.entity_id);
            }
            for row in inserts {
                let decoded = decode::decode_inventory_with_fields(
                    row,
                    &meta.inventory_fields,
                    meta.cols.inventory,
                    schema,
                )?;
                if let Some(t) = touches.as_deref_mut() {
                    touch_inventory(
                        store,
                        decoded.entity_id,
                        decoded.owner_entity_id,
                        decoded.player_owner_entity_id,
                        t,
                    );
                }
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
                if let Some(t) = touches.as_deref_mut() {
                    touch_building_entity(store, decoded.entity_id, t);
                }
                store.building_nickname.delete(decoded.entity_id);
            }
            for row in inserts {
                let decoded = decode::decode_building_nickname_with_fields(
                    row,
                    &meta.building_nickname_fields,
                    meta.cols.building_nickname,
                    schema,
                )?;
                store.building_nickname.upsert(decoded.clone());
                if let Some(t) = touches.as_deref_mut() {
                    touch_building_entity(store, decoded.entity_id, t);
                }
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
                if let Some(t) = touches.as_deref_mut() {
                    touch_location_entity(store, decoded.entity_id, decoded.dimension, t);
                }
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
                if let Some(t) = touches.as_deref_mut() {
                    touch_location_entity(store, decoded.entity_id, decoded.dimension, t);
                }
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
                if let Some(t) = touches.as_deref_mut() {
                    touch_dimension_network(store, decoded.entity_id, t);
                }
                store.dimension_network.delete_by_entity(decoded.entity_id);
            }
            for row in inserts {
                let decoded = decode::decode_dimension_network_with_fields(
                    row,
                    &meta.dimension_network_fields,
                    meta.cols.dimension_network,
                    schema,
                )?;
                store.dimension_network.upsert(decoded.clone());
                if let Some(t) = touches.as_deref_mut() {
                    touch_dimension_network(store, decoded.entity_id, t);
                }
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
        MOBILE_ENTITY_TABLE => {
            for row in deletes {
                let decoded = decode::decode_mobile_entity_with_fields(
                    row,
                    &meta.mobile_entity_fields,
                    meta.cols.mobile_entity,
                    schema,
                )?;
                store.mobile_entity.delete(decoded.entity_id);
            }
            for row in inserts {
                let decoded = decode::decode_mobile_entity_with_fields(
                    row,
                    &meta.mobile_entity_fields,
                    meta.cols.mobile_entity,
                    schema,
                )?;
                store.mobile_entity.upsert(decoded);
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
                if let Some(t) = touches.as_deref_mut() {
                    t.player_inv(decoded.owner_id);
                }
                store.deployable.delete(decoded.entity_id);
            }
            for row in inserts {
                let decoded = decode::decode_deployable_with_fields(
                    row,
                    &meta.deployable_fields,
                    meta.cols.deployable,
                    schema,
                )?;
                if let Some(t) = touches.as_deref_mut() {
                    // Owner change: notify both old (if overwrite) and new.
                    if let Some(slot) = store.deployable.find(decoded.entity_id) {
                        t.player_inv(store.deployable.owner_id[slot as usize]);
                    }
                    t.player_inv(decoded.owner_id);
                }
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
                if let Some(t) = touches.as_deref_mut() {
                    t.player_housing(decoded.entity_id);
                }
                store.player_housing.delete(decoded.entity_id);
            }
            for row in inserts {
                let decoded = decode::decode_player_housing_with_fields(
                    row,
                    &meta.player_housing_fields,
                    meta.cols.player_housing,
                    schema,
                )?;
                if let Some(t) = touches.as_deref_mut() {
                    t.player_housing(decoded.entity_id);
                }
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
                if let Some(t) = touches.as_deref_mut() {
                    touch_craft(
                        store,
                        decoded.owner_entity_id,
                        decoded.building_entity_id,
                        t,
                    );
                }
                store.progressive_action.delete(decoded.entity_id);
            }
            for row in inserts {
                let decoded = decode::decode_progressive_action_with_fields(
                    row,
                    &meta.progressive_action_fields,
                    meta.cols.progressive_action,
                    schema,
                )?;
                if let Some(t) = touches.as_deref_mut() {
                    if let Some(slot) = store.progressive_action.find(decoded.entity_id) {
                        let i = slot as usize;
                        touch_craft(
                            store,
                            store.progressive_action.owner_entity_id[i],
                            store.progressive_action.building_entity_id[i],
                            t,
                        );
                    }
                    touch_craft(
                        store,
                        decoded.owner_entity_id,
                        decoded.building_entity_id,
                        t,
                    );
                }
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
                if let Some(t) = touches.as_deref_mut() {
                    touch_craft(
                        store,
                        decoded.owner_entity_id,
                        decoded.building_entity_id,
                        t,
                    );
                }
                store.passive_craft.delete(decoded.entity_id);
            }
            for row in inserts {
                let decoded = decode::decode_passive_craft_with_fields(
                    row,
                    &meta.passive_craft_fields,
                    meta.cols.passive_craft,
                    schema,
                )?;
                if let Some(t) = touches.as_deref_mut() {
                    if let Some(slot) = store.passive_craft.find(decoded.entity_id) {
                        let i = slot as usize;
                        touch_craft(
                            store,
                            store.passive_craft.owner_entity_id[i],
                            store.passive_craft.building_entity_id[i],
                            t,
                        );
                    }
                    touch_craft(
                        store,
                        decoded.owner_entity_id,
                        decoded.building_entity_id,
                        t,
                    );
                }
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
        STORAGE_LOG_TABLE => {
            for row in deletes {
                match decode::decode_storage_log_with_fields(
                    row,
                    &meta.storage_log_fields,
                    meta.cols.storage_log,
                    schema,
                ) {
                    Ok(decoded) => store.storage_log.delete(decoded.id),
                    Err(e) => tracing::warn!(
                        target: "relay_cache::shard",
                        error = %e,
                        "skip malformed storage_log_state delete"
                    ),
                }
            }
            for row in inserts {
                match decode::decode_storage_log_with_fields(
                    row,
                    &meta.storage_log_fields,
                    meta.cols.storage_log,
                    schema,
                ) {
                    Ok(decoded) => store.storage_log.upsert(decoded),
                    Err(e) => tracing::warn!(
                        target: "relay_cache::shard",
                        error = %e,
                        "skip malformed storage_log_state insert"
                    ),
                }
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

fn touch_inventory(
    store: &RegionStore,
    entity_id: u64,
    owner: u64,
    player_owner: u64,
    touches: &mut TouchBatch,
) {
    if player_owner != 0 {
        touches.player_inv(player_owner);
    }
    if owner != 0 {
        if let Some(d_slot) = store.deployable.find(owner) {
            touches.player_inv(store.deployable.owner_id[d_slot as usize]);
        } else if let Some(b_slot) = store.building.find(owner) {
            touch_building(
                store,
                owner,
                store.building.claim_entity_id[b_slot as usize],
                touches,
            );
        } else {
            // Body bags: owner_entity_id == player.
            touches.player_inv(owner);
        }
    }
    if let Some(d_slot) = store.deployable.find(entity_id) {
        touches.player_inv(store.deployable.owner_id[d_slot as usize]);
    }
}

fn touch_building(
    store: &RegionStore,
    building_entity_id: u64,
    claim_entity_id: u64,
    touches: &mut TouchBatch,
) {
    if claim_entity_id != 0 {
        touches.claim_inv(claim_entity_id);
        touches.claim_crafts(claim_entity_id);
    }
    touch_housing_for_entity(store, building_entity_id, touches);
}

fn touch_craft(
    store: &RegionStore,
    owner_entity_id: u64,
    building_entity_id: u64,
    touches: &mut TouchBatch,
) {
    touches.player_crafts(owner_entity_id);
    if let Some(b_slot) = store.building.find(building_entity_id) {
        touches.claim_crafts(store.building.claim_entity_id[b_slot as usize]);
    }
}

fn touch_building_entity(store: &RegionStore, building_entity_id: u64, touches: &mut TouchBatch) {
    if let Some(b_slot) = store.building.find(building_entity_id) {
        touch_building(
            store,
            building_entity_id,
            store.building.claim_entity_id[b_slot as usize],
            touches,
        );
    } else {
        touch_housing_for_entity(store, building_entity_id, touches);
    }
}

fn touch_housing_for_entity(store: &RegionStore, entity_id: u64, touches: &mut TouchBatch) {
    let dim = store.location_dim.get_or_overworld(entity_id);
    if dim == OVERWORLD_DIMENSION {
        return;
    }
    let Some(net) = store.dimension_network.by_entrance_dim(dim) else {
        return;
    };
    if let Some(h_slot) = store.player_housing.by_network(net.entity_id) {
        touches.player_housing(store.player_housing.entity_id[h_slot as usize]);
    }
}

fn touch_location_entity(
    store: &RegionStore,
    entity_id: u64,
    dimension: u32,
    touches: &mut TouchBatch,
) {
    if dimension == OVERWORLD_DIMENSION {
        return;
    }
    if let Some(net) = store.dimension_network.by_entrance_dim(dimension) {
        if let Some(h_slot) = store.player_housing.by_network(net.entity_id) {
            touches.player_housing(store.player_housing.entity_id[h_slot as usize]);
        }
    }
    // Building moved into/out of a housing dim — also wake claim if any.
    if let Some(b_slot) = store.building.find(entity_id) {
        let claim = store.building.claim_entity_id[b_slot as usize];
        if claim != 0 {
            touches.claim_inv(claim);
        }
    }
}

fn touch_dimension_network(store: &RegionStore, network_entity_id: u64, touches: &mut TouchBatch) {
    if let Some(h_slot) = store.player_housing.by_network(network_entity_id) {
        touches.player_housing(store.player_housing.entity_id[h_slot as usize]);
    }
}

#[cfg(test)]
mod readiness_tests {
    use super::metrics_indicate_ready;
    use serde_json::json;

    #[test]
    fn metrics_ready_requires_all_three() {
        assert!(!metrics_indicate_ready(&json!({})));
        assert!(!metrics_indicate_ready(&json!({
            "upstream": { "state": "up" },
            "local_stdb": { "state": "up" },
            "initial_subscribe_complete": false
        })));
        assert!(!metrics_indicate_ready(&json!({
            "upstream": { "state": "up" },
            "local_stdb": { "state": "down" },
            "initial_subscribe_complete": true
        })));
        assert!(metrics_indicate_ready(&json!({
            "upstream": { "state": "up" },
            "local_stdb": { "state": "up" },
            "initial_subscribe_complete": true
        })));
    }
}
