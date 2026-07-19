// SPDX-License-Identifier: MIT

//! Loopback HTTP/JSON read API over the in-memory fleet stores.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::shard::ShardHandle;
use crate::store::Pocket;

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
        .route("/claim", get(claim_by_name))
        .route("/claim/:entity_id", get(claim_by_pk))
        .route("/claim/:entity_id/inventory", get(claim_inventory))
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
    Query(q): Query<NameQuery>,
) -> impl IntoResponse {
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
            let i = slot as usize;
            hits.push(json!({
                "entity_id": s.claim.entity_id[i].to_string(),
                "name": &*s.claim.name[i],
                "owner_player_entity_id": s.claim.owner_player_entity_id[i].to_string(),
                "owner_building_entity_id": s.claim.owner_building_entity_id[i].to_string(),
                "neutral": s.claim.neutral[i],
                "region": s.region,
            }));
        }
    }
    no_store_json(json!(hits)).into_response()
}

async fn claim_by_pk(
    State(fleet): State<Fleet>,
    Path(entity_id): Path<String>,
) -> impl IntoResponse {
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
            let i = slot as usize;
            return no_store_json(json!({
                "entity_id": s.claim.entity_id[i].to_string(),
                "name": &*s.claim.name[i],
                "owner_player_entity_id": s.claim.owner_player_entity_id[i].to_string(),
                "owner_building_entity_id": s.claim.owner_building_entity_id[i].to_string(),
                "neutral": s.claim.neutral[i],
                "region": s.region,
            }))
            .into_response();
        }
    }
    no_store_status(StatusCode::NOT_FOUND, json!({"error": "claim not found"})).into_response()
}

async fn claim_inventory(
    State(fleet): State<Fleet>,
    Path(entity_id): Path<String>,
) -> impl IntoResponse {
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

        let mut buildings_out = Vec::new();
        for &b_slot in s.building.by_claim(pk) {
            let bi = b_slot as usize;
            let building_entity_id = s.building.entity_id[bi];
            let building_description_id = s.building.building_description_id[bi];
            // Skip walls, totems, crafting stations, etc. — only types
            // whose catalog functions advertise storage/cargo slots.
            if !s.building_desc.is_storage(building_description_id) {
                continue;
            }
            let name = s.building_desc.get(building_description_id);
            let nickname = s.building_nickname.get(building_entity_id);

            let mut agg: HashMap<(i32, u8), i64> = HashMap::new();
            for &inv_slot in s.inventory.by_owner(building_entity_id) {
                for p in s.inventory.pockets[inv_slot as usize].iter() {
                    if p.has_contents {
                        *agg.entry((p.item_id, p.item_type)).or_default() += i64::from(p.quantity);
                    }
                }
            }

            let mut items: Vec<Value> = agg
                .into_iter()
                .map(|((item_id, item_type), quantity)| {
                    json!({
                        "item_id": item_id,
                        "item_type": item_type_label(item_type),
                        "quantity": quantity,
                    })
                })
                .collect();
            items.sort_by(|a, b| {
                let aid = a.get("item_id").and_then(|v| v.as_i64()).unwrap_or(0);
                let bid = b.get("item_id").and_then(|v| v.as_i64()).unwrap_or(0);
                aid.cmp(&bid).then_with(|| {
                    let at = a.get("item_type").and_then(|v| v.as_str()).unwrap_or("");
                    let bt = b.get("item_type").and_then(|v| v.as_str()).unwrap_or("");
                    at.cmp(bt)
                })
            });

            buildings_out.push(json!({
                "entity_id": building_entity_id.to_string(),
                "name": name,
                "nickname": nickname,
                "items": items,
            }));
        }

        buildings_out.sort_by(|a, b| {
            let aid = a
                .get("entity_id")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            let bid = b
                .get("entity_id")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            aid.cmp(&bid)
        });

        return no_store_json(json!({
            "claim": {
                "entity_id": pk.to_string(),
                "name": claim_name,
                "region": region,
            },
            "buildings": buildings_out,
        }))
        .into_response();
    }

    no_store_status(StatusCode::NOT_FOUND, json!({"error": "claim not found"})).into_response()
}

fn item_type_label(t: u8) -> &'static str {
    match t {
        Pocket::CARGO => "Cargo",
        _ => "Item",
    }
}
