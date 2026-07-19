// SPDX-License-Identifier: MIT

//! Loopback HTTP read API over the in-memory fleet stores.
//!
//! Success bodies on the three data routes negotiate JSON (default) vs
//! protobuf via `Accept: application/x-protobuf`. `/healthz` and all
//! error envelopes stay JSON.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use prost::Message;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::decode::{DeployableKind, OVERWORLD_DIMENSION};
use crate::shard::ShardHandle;
use crate::store::{Pocket, RegionStore};

mod pb {
    include!(concat!(env!("OUT_DIR"), "/relay_cache.rs"));
}

const PROTOBUF_MIME: &str = "application/x-protobuf";
const PROTO_SOURCE_MIME: &str = "text/plain; charset=utf-8";

/// Checked-in `.proto` schemas embedded at compile time (whitelist only).
const PROTO_FILES: &[(&str, &str)] = &[(
    "relay_cache.proto",
    include_str!("../proto/relay_cache.proto"),
)];

fn proto_body(name: &str) -> Option<&'static str> {
    PROTO_FILES
        .iter()
        .find(|(n, _)| *n == name)
        .map(|(_, body)| *body)
}

/// Shared axum state: all region shards plus the memory-pressure flag.
#[derive(Clone)]
pub struct Fleet {
    pub shards: Vec<Arc<ShardHandle>>,
    /// Set by the RSS sampler when resident set approaches the ceiling.
    pub memory_pressure: Arc<AtomicBool>,
}

pub async fn serve(
    bind: SocketAddr,
    fleet: Fleet,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/proto", get(list_protos))
        .route("/proto/:name", get(get_proto))
        .route("/claim", get(claim_by_name))
        .route("/claim/:entity_id", get(claim_by_pk))
        .route("/claim/:entity_id/inventory", get(claim_inventory))
        .route("/player", get(player_by_name))
        .route("/player/:entity_id", get(player_by_pk))
        .route("/player/:entity_id/inventory", get(player_inventory))
        .route("/player/:entity_id/housing", get(player_housing))
        .with_state(fleet);

    let listener = tokio::net::TcpListener::bind(bind).await?;
    tracing::info!(
        target: "relay_cache::serve",
        %bind,
        "HTTP read API listening"
    );
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await?;
    Ok(())
}

fn wants_protobuf(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .is_some_and(accept_wants_protobuf)
}

/// True when `Accept` lists `application/x-protobuf` (q-values ignored).
fn accept_wants_protobuf(accept: &str) -> bool {
    accept.split(',').map(str::trim).any(|part| {
        let mime = part.split(';').next().unwrap_or(part).trim();
        mime.eq_ignore_ascii_case(PROTOBUF_MIME)
    })
}

fn no_store_json(body: Value) -> impl IntoResponse {
    let mut headers = HeaderMap::new();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    (headers, axum::Json(body))
}

fn no_store_status(status: StatusCode, body: Value) -> impl IntoResponse {
    let mut headers = HeaderMap::new();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    (status, headers, axum::Json(body))
}

fn no_store_protobuf(bytes: Vec<u8>) -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(PROTOBUF_MIME),
    );
    (headers, bytes).into_response()
}

fn respond_claim(headers: &HeaderMap, claim: pb::Claim) -> Response {
    if wants_protobuf(headers) {
        no_store_protobuf(claim.encode_to_vec())
    } else {
        no_store_json(claim_to_json(&claim)).into_response()
    }
}

fn respond_claim_list(headers: &HeaderMap, claims: Vec<pb::Claim>) -> Response {
    if wants_protobuf(headers) {
        no_store_protobuf(pb::ClaimList { claims }.encode_to_vec())
    } else {
        let arr: Vec<Value> = claims.iter().map(claim_to_json).collect();
        no_store_json(json!(arr)).into_response()
    }
}

fn respond_claim_inventory(headers: &HeaderMap, inv: pb::ClaimInventory) -> Response {
    if wants_protobuf(headers) {
        no_store_protobuf(inv.encode_to_vec())
    } else {
        no_store_json(claim_inventory_to_json(&inv)).into_response()
    }
}

fn claim_to_json(c: &pb::Claim) -> Value {
    json!({
        "entity_id": c.entity_id.to_string(),
        "name": c.name,
        "owner_player_entity_id": c.owner_player_entity_id.to_string(),
        "owner_building_entity_id": c.owner_building_entity_id.to_string(),
        "neutral": c.neutral,
        "region": c.region,
    })
}

fn claim_inventory_to_json(inv: &pb::ClaimInventory) -> Value {
    let claim_json = inv.claim.as_ref().map_or(Value::Null, |claim| {
        json!({
            "entity_id": claim.entity_id.to_string(),
            "name": claim.name,
            "region": claim.region,
        })
    });
    let dimensions: Vec<Value> = inv
        .dimensions
        .iter()
        .map(|d| {
            let entrance = match &d.entrance {
                Some(e) => json!({
                    "entity_id": e.entity_id.to_string(),
                    "name": e.name,
                    "nickname": e.nickname,
                }),
                None => Value::Null,
            };
            let buildings: Vec<Value> = d
                .buildings
                .iter()
                .map(|b| {
                    let items: Vec<Value> = b
                        .items
                        .iter()
                        .map(|it| {
                            json!({
                                "item_id": it.item_id,
                                "item_type": it.item_type,
                                "quantity": it.quantity,
                            })
                        })
                        .collect();
                    json!({
                        "entity_id": b.entity_id.to_string(),
                        "name": b.name,
                        "nickname": b.nickname,
                        "items": items,
                    })
                })
                .collect();
            json!({
                "dimension_id": d.dimension_id,
                "kind": d.kind,
                "entrance": entrance,
                "buildings": buildings,
            })
        })
        .collect();
    json!({
        "claim": claim_json,
        "dimensions": dimensions,
    })
}

fn claim_from_store(s: &RegionStore, slot: usize) -> pb::Claim {
    pb::Claim {
        entity_id: s.claim.entity_id[slot],
        name: s.claim.name[slot].to_string(),
        owner_player_entity_id: s.claim.owner_player_entity_id[slot],
        owner_building_entity_id: s.claim.owner_building_entity_id[slot],
        neutral: s.claim.neutral[slot],
        region: s.region,
    }
}

async fn list_protos() -> impl IntoResponse {
    let files: Vec<Value> = PROTO_FILES
        .iter()
        .map(|(name, body)| {
            json!({
                "name": name,
                "path": format!("/proto/{name}"),
                "bytes": body.len(),
            })
        })
        .collect();
    no_store_json(json!({ "protos": files }))
}

async fn get_proto(Path(name): Path<String>) -> Response {
    let Some(body) = proto_body(&name) else {
        return no_store_status(
            StatusCode::NOT_FOUND,
            json!({"error": "proto not found", "name": name}),
        )
        .into_response();
    };

    let mut headers = HeaderMap::new();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(PROTO_SOURCE_MIME),
    );
    if let Ok(cd) = HeaderValue::from_str(&format!("attachment; filename=\"{name}\"")) {
        headers.insert(header::CONTENT_DISPOSITION, cd);
    }
    (headers, body).into_response()
}

async fn healthz(State(fleet): State<Fleet>) -> impl IntoResponse {
    let pressure = fleet.memory_pressure.load(Ordering::Relaxed);
    let mut regions = Vec::with_capacity(fleet.shards.len());
    let mut all_ready = !pressure;
    for shard in &fleet.shards {
        let s = shard.store.read();
        if !s.ready {
            all_ready = false;
        }
        regions.push(json!({
            "region": shard.region,
            "ready": s.ready,
            "rows": {
                "claim": s.claim.len(),
                "building": s.building.len(),
                "inventory": s.inventory.len(),
                "building_desc": s.building_desc.len(),
                "building_nickname": s.building_nickname.len(),
                "location_dim": s.location_dim.len(),
                "dimension_network": s.dimension_network.len(),
                "player_username": s.player_username.len(),
                "deployable": s.deployable.len(),
                "deployable_desc": s.deployable_desc.len(),
                "player_housing": s.player_housing.len(),
                "player_housing_desc": s.player_housing_desc.len(),
                "rent": s.rent.len(),
            }
        }));
    }
    no_store_json(json!({
        "ready": all_ready,
        "memory_pressure": pressure,
        "regions": regions,
    }))
}

#[derive(Debug, Deserialize)]
struct NameQuery {
    name: Option<String>,
}

async fn claim_by_name(
    State(fleet): State<Fleet>,
    headers: HeaderMap,
    Query(q): Query<NameQuery>,
) -> Response {
    let Some(needle) = q.name.as_deref().filter(|s| !s.is_empty()) else {
        return no_store_status(
            StatusCode::BAD_REQUEST,
            json!({"error": "missing or empty `name` query parameter"}),
        )
        .into_response();
    };

    let mut hits = Vec::new();
    for shard in &fleet.shards {
        let s = shard.store.read();
        if !s.ready {
            continue;
        }
        for slot in s.claim.search_name(needle) {
            hits.push(claim_from_store(&s, slot as usize));
        }
    }
    respond_claim_list(&headers, hits)
}

async fn claim_by_pk(
    State(fleet): State<Fleet>,
    headers: HeaderMap,
    Path(entity_id): Path<String>,
) -> Response {
    let Ok(pk) = entity_id.parse::<u64>() else {
        return no_store_status(
            StatusCode::BAD_REQUEST,
            json!({"error": "entity_id must be a u64"}),
        )
        .into_response();
    };

    for shard in &fleet.shards {
        let s = shard.store.read();
        if !s.ready {
            continue;
        }
        if let Some(slot) = s.claim.find(pk) {
            return respond_claim(&headers, claim_from_store(&s, slot as usize));
        }
    }
    no_store_status(StatusCode::NOT_FOUND, json!({"error": "claim not found"})).into_response()
}

async fn claim_inventory(
    State(fleet): State<Fleet>,
    headers: HeaderMap,
    Path(entity_id): Path<String>,
) -> Response {
    let Ok(pk) = entity_id.parse::<u64>() else {
        return no_store_status(
            StatusCode::BAD_REQUEST,
            json!({"error": "entity_id must be a u64"}),
        )
        .into_response();
    };

    for shard in &fleet.shards {
        let s = shard.store.read();
        if !s.ready {
            continue;
        }
        let Some(claim_slot) = s.claim.find(pk) else {
            continue;
        };
        let ci = claim_slot as usize;
        let claim_name = s.claim.name[ci].as_ref();
        let region = s.region;

        // dimension_id → buildings in that dimension
        let mut by_dim: HashMap<u32, Vec<pb::BuildingInventory>> = HashMap::new();

        for &b_slot in s.building.by_claim(pk) {
            let bi = b_slot as usize;
            let building_entity_id = s.building.entity_id[bi];
            let building_description_id = s.building.building_description_id[bi];
            // Skip walls/totems/crafting stations (no storage slots) and
            // Town Banks (personal storage — BitJita omits these too).
            if !s
                .building_desc
                .include_in_claim_inventory(building_description_id)
            {
                continue;
            }
            let name = s
                .building_desc
                .get(building_description_id)
                .map(str::to_owned);
            let nickname = s
                .building_nickname
                .get(building_entity_id)
                .map(str::to_owned);
            let dimension_id = s.location_dim.get_or_overworld(building_entity_id);

            let mut agg: HashMap<(i32, u8), i64> = HashMap::new();
            for &inv_slot in s.inventory.by_owner(building_entity_id) {
                for p in s.inventory.pockets[inv_slot as usize].iter() {
                    if p.has_contents {
                        *agg.entry((p.item_id, p.item_type)).or_default() += i64::from(p.quantity);
                    }
                }
            }

            let mut items: Vec<pb::InventoryItem> = agg
                .into_iter()
                .map(|((item_id, item_type), quantity)| pb::InventoryItem {
                    item_id,
                    item_type: item_type_label(item_type).to_owned(),
                    quantity,
                })
                .collect();
            items.sort_by(|a, b| {
                a.item_id
                    .cmp(&b.item_id)
                    .then_with(|| a.item_type.cmp(&b.item_type))
            });

            by_dim
                .entry(dimension_id)
                .or_default()
                .push(pb::BuildingInventory {
                    entity_id: building_entity_id,
                    name,
                    nickname,
                    items,
                });
        }

        let mut dimensions_out: Vec<(u32, String, pb::DimensionGroup)> =
            Vec::with_capacity(by_dim.len());
        for (dimension_id, mut buildings) in by_dim {
            buildings.sort_by(|a, b| a.entity_id.cmp(&b.entity_id));

            let (kind, entrance) = dimension_meta(&s, dimension_id);
            let sort_key = entrance
                .as_ref()
                .and_then(|e| e.name.as_deref())
                .unwrap_or("")
                .to_ascii_lowercase();
            dimensions_out.push((
                dimension_id,
                sort_key,
                pb::DimensionGroup {
                    dimension_id,
                    kind: kind.to_owned(),
                    entrance,
                    buildings,
                },
            ));
        }

        dimensions_out.sort_by(|a, b| {
            // Overworld first, then by entrance name, then dimension_id.
            let a_ow = a.0 == OVERWORLD_DIMENSION;
            let b_ow = b.0 == OVERWORLD_DIMENSION;
            b_ow.cmp(&a_ow)
                .then_with(|| a.1.cmp(&b.1))
                .then_with(|| a.0.cmp(&b.0))
        });

        let dimensions: Vec<pb::DimensionGroup> =
            dimensions_out.into_iter().map(|(_, _, v)| v).collect();

        return respond_claim_inventory(
            &headers,
            pb::ClaimInventory {
                claim: Some(pb::ClaimSummary {
                    entity_id: pk,
                    name: claim_name.to_owned(),
                    region,
                }),
                dimensions,
            },
        );
    }

    no_store_status(StatusCode::NOT_FOUND, json!({"error": "claim not found"})).into_response()
}

fn dimension_meta(s: &RegionStore, dimension_id: u32) -> (&'static str, Option<pb::Entrance>) {
    if dimension_id == OVERWORLD_DIMENSION {
        return ("overworld", None);
    }
    let Some(net) = s.dimension_network.by_entrance_dim(dimension_id) else {
        return ("unknown", None);
    };
    let (name, nickname) = entrance_labels(s, net.building_id);
    (
        "building_interior",
        Some(pb::Entrance {
            entity_id: net.building_id,
            name: name.map(str::to_owned),
            nickname: nickname.map(str::to_owned),
        }),
    )
}

fn entrance_labels(s: &RegionStore, building_id: u64) -> (Option<&str>, Option<&str>) {
    let nickname = s.building_nickname.get(building_id);
    let name = s
        .building
        .find(building_id)
        .map(|slot| s.building.building_description_id[slot as usize])
        .and_then(|desc_id| s.building_desc.get(desc_id));
    (name, nickname)
}

fn item_type_label(t: u8) -> &'static str {
    match t {
        Pocket::CARGO => "Cargo",
        _ => "Item",
    }
}

fn aggregate_pockets(s: &RegionStore, inv_slots: impl Iterator<Item = u32>) -> Vec<pb::InventoryItem> {
    let mut agg: HashMap<(i32, u8), i64> = HashMap::new();
    for inv_slot in inv_slots {
        for p in s.inventory.pockets[inv_slot as usize].iter() {
            if p.has_contents {
                *agg.entry((p.item_id, p.item_type)).or_default() += i64::from(p.quantity);
            }
        }
    }
    let mut items: Vec<pb::InventoryItem> = agg
        .into_iter()
        .map(|((item_id, item_type), quantity)| pb::InventoryItem {
            item_id,
            item_type: item_type_label(item_type).to_owned(),
            quantity,
        })
        .collect();
    items.sort_by(|a, b| {
        a.item_id
            .cmp(&b.item_id)
            .then_with(|| a.item_type.cmp(&b.item_type))
    });
    items
}

fn respond_player(headers: &HeaderMap, player: pb::Player) -> Response {
    if wants_protobuf(headers) {
        no_store_protobuf(player.encode_to_vec())
    } else {
        no_store_json(player_to_json(&player)).into_response()
    }
}

fn respond_player_list(headers: &HeaderMap, players: Vec<pb::Player>) -> Response {
    if wants_protobuf(headers) {
        no_store_protobuf(pb::PlayerList { players }.encode_to_vec())
    } else {
        let arr: Vec<Value> = players.iter().map(player_to_json).collect();
        no_store_json(json!(arr)).into_response()
    }
}

fn respond_player_inventory(headers: &HeaderMap, inv: pb::PlayerInventory) -> Response {
    if wants_protobuf(headers) {
        no_store_protobuf(inv.encode_to_vec())
    } else {
        no_store_json(player_inventory_to_json(&inv)).into_response()
    }
}

fn respond_player_housing(headers: &HeaderMap, housing: pb::PlayerHousing) -> Response {
    if wants_protobuf(headers) {
        no_store_protobuf(housing.encode_to_vec())
    } else {
        no_store_json(player_housing_to_json(&housing)).into_response()
    }
}

fn player_to_json(p: &pb::Player) -> Value {
    json!({
        "entity_id": p.entity_id.to_string(),
        "username": p.username,
        "region": p.region,
    })
}

fn player_inventory_to_json(inv: &pb::PlayerInventory) -> Value {
    let player = inv.player.as_ref().map_or(Value::Null, player_to_json);
    let inventories: Vec<Value> = inv
        .inventories
        .iter()
        .map(|bag| {
            let items: Vec<Value> = bag
                .items
                .iter()
                .map(|it| {
                    json!({
                        "item_id": it.item_id,
                        "item_type": it.item_type,
                        "quantity": it.quantity,
                    })
                })
                .collect();
            json!({
                "entity_id": bag.entity_id.to_string(),
                "name": bag.name,
                "nickname": bag.nickname,
                "category": bag.category,
                "claim_entity_id": bag.claim_entity_id.map(|id| id.to_string()),
                "claim_name": bag.claim_name,
                "items": items,
            })
        })
        .collect();
    json!({
        "player": player,
        "inventories": inventories,
    })
}

fn player_housing_to_json(h: &pb::PlayerHousing) -> Value {
    let player = h.player.as_ref().map_or(Value::Null, player_to_json);
    let house = match &h.house {
        Some(house) => json!({
            "entity_id": house.entity_id.to_string(),
            "name": house.name,
            "region": house.region,
        }),
        None => Value::Null,
    };
    let buildings: Vec<Value> = h
        .buildings
        .iter()
        .map(|b| {
            let items: Vec<Value> = b
                .items
                .iter()
                .map(|it| {
                    json!({
                        "item_id": it.item_id,
                        "item_type": it.item_type,
                        "quantity": it.quantity,
                    })
                })
                .collect();
            json!({
                "entity_id": b.entity_id.to_string(),
                "name": b.name,
                "nickname": b.nickname,
                "items": items,
            })
        })
        .collect();
    json!({
        "status": h.status,
        "player": player,
        "house": house,
        "buildings": buildings,
    })
}

fn find_player(fleet: &Fleet, pk: u64) -> Option<pb::Player> {
    for shard in &fleet.shards {
        let s = shard.store.read();
        if !s.ready {
            continue;
        }
        if let Some(slot) = s.player_username.find(pk) {
            return Some(pb::Player {
                entity_id: pk,
                username: s.player_username.username[slot as usize].to_string(),
                region: s.region,
            });
        }
    }
    None
}

async fn player_by_name(
    State(fleet): State<Fleet>,
    headers: HeaderMap,
    Query(q): Query<NameQuery>,
) -> Response {
    let Some(needle) = q.name.as_deref().filter(|s| !s.is_empty()) else {
        return no_store_status(
            StatusCode::BAD_REQUEST,
            json!({"error": "missing or empty `name` query parameter"}),
        )
        .into_response();
    };

    let mut hits = Vec::new();
    let mut seen = HashMap::<u64, ()>::new();
    for shard in &fleet.shards {
        let s = shard.store.read();
        if !s.ready {
            continue;
        }
        for slot in s.player_username.search_name(needle) {
            let entity_id = s.player_username.entity_id[slot as usize];
            if seen.insert(entity_id, ()).is_some() {
                continue;
            }
            hits.push(pb::Player {
                entity_id,
                username: s.player_username.username[slot as usize].to_string(),
                region: s.region,
            });
        }
    }
    respond_player_list(&headers, hits)
}

async fn player_by_pk(
    State(fleet): State<Fleet>,
    headers: HeaderMap,
    Path(entity_id): Path<String>,
) -> Response {
    let Ok(pk) = entity_id.parse::<u64>() else {
        return no_store_status(
            StatusCode::BAD_REQUEST,
            json!({"error": "entity_id must be a u64"}),
        )
        .into_response();
    };
    match find_player(&fleet, pk) {
        Some(player) => respond_player(&headers, player),
        None => {
            no_store_status(StatusCode::NOT_FOUND, json!({"error": "player not found"}))
                .into_response()
        }
    }
}

/// Classify one inventory row for a player. Returns `None` for Toolbelt,
/// Wallet, and unrecognized owners.
fn classify_player_bag(
    s: &RegionStore,
    inv_slot: u32,
    player_id: u64,
) -> Option<pb::PlayerInventoryBag> {
    let i = inv_slot as usize;
    let entity_id = s.inventory.entity_id[i];
    let owner = s.inventory.owner_entity_id[i];
    let player_owner = s.inventory.player_owner_entity_id[i];
    let inventory_index = s.inventory.inventory_index[i];

    let (category, name, nickname, claim_entity_id, claim_name) = if owner == player_id {
        // Body bags: Inventory / Toolbelt / Wallet.
        match inventory_index {
            0 => ("pockets", "Pockets".to_owned(), None, None, None),
            _ => return None, // Toolbelt, Wallet, unknown
        }
    } else if player_owner == player_id {
        if let Some(b_slot) = s.building.find(owner) {
            let desc_id = s.building.building_description_id[b_slot as usize];
            let building_name = s.building_desc.get(desc_id).unwrap_or("Storage").to_owned();
            let claim_id = s.building.claim_entity_id[b_slot as usize];
            let claim_name = s
                .claim
                .find(claim_id)
                .map(|cs| s.claim.name[cs as usize].to_string());
            let category = categorize_building_bag(&building_name)?;
            (
                category,
                building_name,
                None,
                Some(claim_id).filter(|&id| id != 0),
                claim_name,
            )
        } else if let Some(d_slot) = s.deployable.find(owner).or_else(|| s.deployable.find(entity_id))
        {
            let desc_id = s.deployable.deployable_description_id[d_slot as usize];
            let (desc_name, kind) = s
                .deployable_desc
                .get(desc_id)
                .unwrap_or(("Deployable", DeployableKind::Other));
            let nick = s.deployable.nickname[d_slot as usize].as_ref();
            let nickname = if nick.is_empty() {
                None
            } else {
                Some(nick.to_owned())
            };
            let name = nickname
                .clone()
                .unwrap_or_else(|| desc_name.to_owned());
            let category = match kind {
                DeployableKind::Cart => "wagon",
                DeployableKind::Cache => "cache",
                _ => "deployable",
            };
            let claim_id = s.deployable.claim_entity_id[d_slot as usize];
            let claim_name = s
                .claim
                .find(claim_id)
                .map(|cs| s.claim.name[cs as usize].to_string());
            (
                category,
                name,
                nickname,
                Some(claim_id).filter(|&id| id != 0),
                claim_name,
            )
        } else {
            return None;
        }
    } else {
        return None;
    };

    let items = aggregate_pockets(s, std::iter::once(inv_slot));
    Some(pb::PlayerInventoryBag {
        entity_id,
        name,
        nickname,
        category: category.to_owned(),
        claim_entity_id,
        claim_name,
        items,
    })
}

fn categorize_building_bag(name: &str) -> Option<&'static str> {
    let lower = name.to_ascii_lowercase();
    if lower.contains("bank") {
        Some("bank")
    } else if lower.contains("recovery") {
        Some("recovery")
    } else if lower.contains("personal cache") || lower.contains("cache") {
        Some("cache")
    } else if lower.contains("wagon") || lower.contains("cart") {
        Some("wagon")
    } else {
        None
    }
}

fn collect_player_bags(fleet: &Fleet, player_id: u64) -> Vec<pb::PlayerInventoryBag> {
    let mut bags = Vec::new();
    let mut seen = HashMap::<u64, ()>::new();
    for shard in &fleet.shards {
        let s = shard.store.read();
        if !s.ready {
            continue;
        }
        let mut slots: Vec<u32> = s.inventory.by_owner(player_id).to_vec();
        slots.extend_from_slice(s.inventory.by_player_owner(player_id));
        slots.sort_unstable();
        slots.dedup();
        for slot in slots {
            let Some(bag) = classify_player_bag(&s, slot, player_id) else {
                continue;
            };
            if seen.insert(bag.entity_id, ()).is_some() {
                continue;
            }
            bags.push(bag);
        }
    }
    bags.sort_by(|a, b| {
        a.category
            .cmp(&b.category)
            .then_with(|| a.entity_id.cmp(&b.entity_id))
    });
    bags
}

async fn player_inventory(
    State(fleet): State<Fleet>,
    headers: HeaderMap,
    Path(entity_id): Path<String>,
) -> Response {
    let Ok(pk) = entity_id.parse::<u64>() else {
        return no_store_status(
            StatusCode::BAD_REQUEST,
            json!({"error": "entity_id must be a u64"}),
        )
        .into_response();
    };

    let player = find_player(&fleet, pk);
    let inventories = collect_player_bags(&fleet, pk);
    let Some(player) = player.or_else(|| {
        if inventories.is_empty() {
            None
        } else {
            Some(pb::Player {
                entity_id: pk,
                username: String::new(),
                region: 0,
            })
        }
    }) else {
        return no_store_status(StatusCode::NOT_FOUND, json!({"error": "player not found"}))
            .into_response();
    };

    respond_player_inventory(
        &headers,
        pb::PlayerInventory {
            player: Some(player),
            inventories,
        },
    )
}

fn collect_housing_buildings(
    s: &RegionStore,
    claim_entity_id: u64,
    entrance_dimension_id: u32,
) -> Vec<pb::BuildingInventory> {
    let mut buildings = Vec::new();
    for &b_slot in s.building.by_claim(claim_entity_id) {
        let bi = b_slot as usize;
        let building_entity_id = s.building.entity_id[bi];
        if s.location_dim.get_or_overworld(building_entity_id) != entrance_dimension_id {
            continue;
        }
        let building_description_id = s.building.building_description_id[bi];
        if !s
            .building_desc
            .include_in_claim_inventory(building_description_id)
        {
            continue;
        }
        let name = s
            .building_desc
            .get(building_description_id)
            .map(str::to_owned);
        let nickname = s
            .building_nickname
            .get(building_entity_id)
            .map(str::to_owned);
        let items = aggregate_pockets(s, s.inventory.by_owner(building_entity_id).iter().copied());
        buildings.push(pb::BuildingInventory {
            entity_id: building_entity_id,
            name,
            nickname,
            items,
        });
    }
    buildings.sort_by(|a, b| a.entity_id.cmp(&b.entity_id));
    buildings
}

async fn player_housing(
    State(fleet): State<Fleet>,
    headers: HeaderMap,
    Path(entity_id): Path<String>,
) -> Response {
    let Ok(pk) = entity_id.parse::<u64>() else {
        return no_store_status(
            StatusCode::BAD_REQUEST,
            json!({"error": "entity_id must be a u64"}),
        )
        .into_response();
    };

    let Some(player) = find_player(&fleet, pk) else {
        return no_store_status(StatusCode::NOT_FOUND, json!({"error": "player not found"}))
            .into_response();
    };

    for shard in &fleet.shards {
        let s = shard.store.read();
        if !s.ready {
            continue;
        }
        let Some(&rent_slot) = s.rent.by_player(pk).first() else {
            continue;
        };
        let network_id = s.rent.dimension_network_id[rent_slot as usize];
        let claim_entity_id = s.rent.claim_entity_id[rent_slot as usize];
        let Some(net) = s.dimension_network.by_entity_id(network_id) else {
            continue;
        };
        let entrance_dimension_id = net.entrance_dimension_id;
        let claim_for_buildings = if claim_entity_id != 0 {
            claim_entity_id
        } else {
            net.claim_entity_id
        };

        let (house_entity_id, house_name) =
            if let Some(h_slot) = s.player_housing.by_network(network_id) {
                let hi = h_slot as usize;
                let rank = s.player_housing.rank[hi];
                let name = s
                    .player_housing_desc
                    .name_for_rank(rank)
                    .unwrap_or("Player Housing")
                    .to_owned();
                (s.player_housing.entity_id[hi], name)
            } else {
                (net.building_id, "Player Housing".to_owned())
            };

        let buildings = collect_housing_buildings(&s, claim_for_buildings, entrance_dimension_id);
        return respond_player_housing(
            &headers,
            pb::PlayerHousing {
                status: "ok".into(),
                player: Some(player),
                house: Some(pb::HouseSummary {
                    entity_id: house_entity_id,
                    name: house_name,
                    region: s.region,
                }),
                buildings,
            },
        );
    }

    respond_player_housing(
        &headers,
        pb::PlayerHousing {
            status: "noHouse".into(),
            player: Some(player),
            house: None,
            buildings: Vec::new(),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode::{
        BuildingDescRow, BuildingRow, DimensionNetworkRow, LocationDimRow, OVERWORLD_DIMENSION,
        PlayerUsernameRow, RentRow,
    };
    use crate::store::RegionStore;
    use prost::Message;

    fn seed_concordia_like(s: &mut RegionStore) {
        // Outdoor chest in overworld (no location row).
        s.building.upsert(BuildingRow {
            entity_id: 1,
            claim_entity_id: 99,
            building_description_id: 10,
        });
        s.building_desc.upsert(BuildingDescRow {
            id: 10,
            name: "Sturdy Large Chest".into(),
            is_storage: true,
        });

        // Storehouse entrance (not storage itself).
        s.building.upsert(BuildingRow {
            entity_id: 200,
            claim_entity_id: 99,
            building_description_id: 20,
        });
        s.building_desc.upsert(BuildingDescRow {
            id: 20,
            name: "Sturdy Storehouse".into(),
            is_storage: false,
        });
        s.dimension_network.upsert(DimensionNetworkRow {
            entity_id: 500,
            building_id: 200,
            claim_entity_id: 99,
            rent_entity_id: 0,
            entrance_dimension_id: 1649,
            is_collapsed: false,
        });

        // Interior wicker storage.
        s.building.upsert(BuildingRow {
            entity_id: 300,
            claim_entity_id: 99,
            building_description_id: 30,
        });
        s.building_desc.upsert(BuildingDescRow {
            id: 30,
            name: "Wicker Item Storage".into(),
            is_storage: true,
        });
        s.location_dim.upsert(LocationDimRow {
            entity_id: 300,
            dimension: 1649,
        });
    }

    #[test]
    fn grouping_defaults_missing_location_to_overworld() {
        let mut s = RegionStore::empty(14);
        seed_concordia_like(&mut s);
        assert_eq!(s.location_dim.get_or_overworld(1), OVERWORLD_DIMENSION);
        assert_eq!(s.location_dim.get_or_overworld(300), 1649);
        let net = s.dimension_network.by_entrance_dim(1649).unwrap();
        assert_eq!(net.building_id, 200);
        let (name, _) = entrance_labels(&s, net.building_id);
        assert_eq!(name, Some("Sturdy Storehouse"));
    }

    #[test]
    fn proto_registry_includes_relay_cache() {
        let body = proto_body("relay_cache.proto").expect("embedded");
        assert!(body.contains("message Claim"));
        assert!(body.contains("message ClaimInventory"));
        assert!(body.contains("message Player"));
        assert!(body.contains("message PlayerInventory"));
        assert!(body.contains("message PlayerHousing"));
        assert!(proto_body("../etc/passwd").is_none());
        assert!(proto_body("missing.proto").is_none());
    }

    #[test]
    fn accept_selects_protobuf() {
        assert!(accept_wants_protobuf("application/x-protobuf"));
        assert!(accept_wants_protobuf(
            "application/json, application/x-protobuf"
        ));
        assert!(accept_wants_protobuf("application/x-protobuf;q=0.9"));
        assert!(accept_wants_protobuf("APPLICATION/X-PROTOBUF"));
        assert!(!accept_wants_protobuf("*/*"));
        assert!(!accept_wants_protobuf("application/json"));
        assert!(!accept_wants_protobuf(""));
        assert!(!accept_wants_protobuf("application/protobuf"));
    }

    #[test]
    fn claim_protobuf_roundtrip() {
        let claim = pb::Claim {
            entity_id: 99,
            name: "Concordia".into(),
            owner_player_entity_id: 1,
            owner_building_entity_id: 2,
            neutral: false,
            region: 14,
        };
        let bytes = claim.encode_to_vec();
        let decoded = pb::Claim::decode(bytes.as_slice()).unwrap();
        assert_eq!(decoded, claim);

        let json = claim_to_json(&claim);
        assert_eq!(json["entity_id"], "99");
        assert_eq!(json["name"], "Concordia");
        assert_eq!(json["region"], 14);
    }

    #[test]
    fn claim_inventory_protobuf_roundtrip() {
        let inv = pb::ClaimInventory {
            claim: Some(pb::ClaimSummary {
                entity_id: 99,
                name: "Concordia".into(),
                region: 14,
            }),
            dimensions: vec![pb::DimensionGroup {
                dimension_id: OVERWORLD_DIMENSION,
                kind: "overworld".into(),
                entrance: None,
                buildings: vec![pb::BuildingInventory {
                    entity_id: 1,
                    name: Some("Sturdy Large Chest".into()),
                    nickname: None,
                    items: vec![pb::InventoryItem {
                        item_id: 42,
                        item_type: "Item".into(),
                        quantity: 3,
                    }],
                }],
            }],
        };
        let bytes = inv.encode_to_vec();
        let decoded = pb::ClaimInventory::decode(bytes.as_slice()).unwrap();
        assert_eq!(decoded, inv);

        let json = claim_inventory_to_json(&inv);
        assert_eq!(json["claim"]["entity_id"], "99");
        assert_eq!(json["dimensions"][0]["kind"], "overworld");
        assert_eq!(json["dimensions"][0]["entrance"], Value::Null);
        assert_eq!(json["dimensions"][0]["buildings"][0]["entity_id"], "1");
        assert_eq!(
            json["dimensions"][0]["buildings"][0]["items"][0]["item_id"],
            42
        );
    }

    #[test]
    fn claim_list_wraps_for_protobuf() {
        let claims = vec![pb::Claim {
            entity_id: 1,
            name: "A".into(),
            owner_player_entity_id: 0,
            owner_building_entity_id: 0,
            neutral: true,
            region: 3,
        }];
        let list = pb::ClaimList {
            claims: claims.clone(),
        };
        let decoded = pb::ClaimList::decode(list.encode_to_vec().as_slice()).unwrap();
        assert_eq!(decoded.claims.len(), 1);
        assert_eq!(decoded.claims[0].name, "A");
    }

    #[test]
    fn categorize_building_bag_names() {
        assert_eq!(categorize_building_bag("Town Bank"), Some("bank"));
        assert_eq!(categorize_building_bag("Ancient Bank"), Some("bank"));
        assert_eq!(categorize_building_bag("Recovery Chest"), Some("recovery"));
        assert_eq!(
            categorize_building_bag("Personal Cache"),
            Some("cache")
        );
        assert_eq!(categorize_building_bag("Sturdy Large Chest"), None);
    }

    #[test]
    fn classify_drops_toolbelt_keeps_pockets_and_bank() {
        use crate::decode::InventoryRow;
        use crate::store::Pocket;

        let mut s = RegionStore::empty(14);
        s.ready = true;
        s.player_username.upsert(PlayerUsernameRow {
            entity_id: 7,
            username: "Tester".into(),
        });
        s.building.upsert(BuildingRow {
            entity_id: 50,
            claim_entity_id: 99,
            building_description_id: 1,
        });
        s.building_desc.upsert(BuildingDescRow {
            id: 1,
            name: "Town Bank".into(),
            is_storage: true,
        });
        s.claim.upsert(crate::decode::ClaimRow {
            entity_id: 99,
            owner_player_entity_id: 0,
            owner_building_entity_id: 0,
            name: "Concordia".into(),
            neutral: false,
        });

        let pocket = Pocket {
            volume: 100,
            has_contents: true,
            item_id: 42,
            quantity: 3,
            item_type: Pocket::ITEM,
            has_durability: false,
            durability: 0,
        };
        s.inventory.upsert(InventoryRow {
            entity_id: 1,
            pockets: Box::from([pocket]),
            inventory_index: 0,
            cargo_index: 0,
            owner_entity_id: 7,
            player_owner_entity_id: 0,
        });
        s.inventory.upsert(InventoryRow {
            entity_id: 2,
            pockets: Box::from([pocket]),
            inventory_index: 1,
            cargo_index: 0,
            owner_entity_id: 7,
            player_owner_entity_id: 0,
        });
        s.inventory.upsert(InventoryRow {
            entity_id: 3,
            pockets: Box::from([pocket]),
            inventory_index: 0,
            cargo_index: 0,
            owner_entity_id: 50,
            player_owner_entity_id: 7,
        });

        let pockets = classify_player_bag(&s, s.inventory.find(1).unwrap(), 7).unwrap();
        assert_eq!(pockets.category, "pockets");
        assert!(classify_player_bag(&s, s.inventory.find(2).unwrap(), 7).is_none());
        let bank = classify_player_bag(&s, s.inventory.find(3).unwrap(), 7).unwrap();
        assert_eq!(bank.category, "bank");
        assert_eq!(bank.claim_name.as_deref(), Some("Concordia"));
        assert_eq!(bank.items[0].quantity, 3);
    }

    #[test]
    fn housing_no_rent_is_no_house_path_data() {
        let mut s = RegionStore::empty(14);
        s.ready = true;
        s.player_username.upsert(PlayerUsernameRow {
            entity_id: 7,
            username: "Tester".into(),
        });
        assert!(s.rent.by_player(7).is_empty());
    }

    #[test]
    fn housing_rent_join_finds_interior_buildings() {
        use crate::decode::{InventoryRow, PlayerHousingDescRow, PlayerHousingRow};
        use crate::store::Pocket;

        let mut s = RegionStore::empty(14);
        s.ready = true;
        s.rent.upsert(RentRow {
            entity_id: 1,
            dimension_network_id: 500,
            claim_entity_id: 99,
            white_list: Box::from([7u64]),
            active: true,
        });
        s.dimension_network.upsert(DimensionNetworkRow {
            entity_id: 500,
            building_id: 200,
            claim_entity_id: 99,
            rent_entity_id: 1,
            entrance_dimension_id: 415,
            is_collapsed: false,
        });
        s.player_housing.upsert(PlayerHousingRow {
            entity_id: 900,
            entrance_building_entity_id: 200,
            network_entity_id: 500,
            rank: 1,
            is_empty: false,
        });
        s.player_housing_desc.upsert(PlayerHousingDescRow {
            secondary_knowledge_id: 1,
            rank: 1,
            name: "Player Housing Catacombs".into(),
        });
        s.building.upsert(BuildingRow {
            entity_id: 300,
            claim_entity_id: 99,
            building_description_id: 30,
        });
        s.building_desc.upsert(BuildingDescRow {
            id: 30,
            name: "Wicker Item Storage".into(),
            is_storage: true,
        });
        s.location_dim.upsert(LocationDimRow {
            entity_id: 300,
            dimension: 415,
        });
        let pocket = Pocket {
            volume: 100,
            has_contents: true,
            item_id: 9,
            quantity: 2,
            item_type: Pocket::CARGO,
            has_durability: false,
            durability: 0,
        };
        s.inventory.upsert(InventoryRow {
            entity_id: 400,
            pockets: Box::from([pocket]),
            inventory_index: 0,
            cargo_index: 0,
            owner_entity_id: 300,
            player_owner_entity_id: 0,
        });

        let buildings = collect_housing_buildings(&s, 99, 415);
        assert_eq!(buildings.len(), 1);
        assert_eq!(buildings[0].entity_id, 300);
        assert_eq!(buildings[0].items[0].item_type, "Cargo");
        assert_eq!(buildings[0].items[0].quantity, 2);
        assert_eq!(
            s.player_housing_desc.name_for_rank(1),
            Some("Player Housing Catacombs")
        );
    }

    #[test]
    fn player_protobuf_roundtrip() {
        let player = pb::Player {
            entity_id: 7,
            username: "Tester".into(),
            region: 14,
        };
        let decoded = pb::Player::decode(player.encode_to_vec().as_slice()).unwrap();
        assert_eq!(decoded, player);
        assert_eq!(player_to_json(&player)["entity_id"], "7");
    }
}
