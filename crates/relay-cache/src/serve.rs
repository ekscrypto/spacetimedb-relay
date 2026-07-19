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

use crate::decode::OVERWORLD_DIMENSION;
use crate::shard::ShardHandle;
use crate::store::{Pocket, RegionStore};

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
                "location_dim": s.location_dim.len(),
                "dimension_network": s.dimension_network.len(),
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

        // dimension_id → buildings in that dimension
        let mut by_dim: HashMap<u32, Vec<Value>> = HashMap::new();

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
            let name = s.building_desc.get(building_description_id);
            let nickname = s.building_nickname.get(building_entity_id);
            let dimension_id = s.location_dim.get_or_overworld(building_entity_id);

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

            by_dim.entry(dimension_id).or_default().push(json!({
                "entity_id": building_entity_id.to_string(),
                "name": name,
                "nickname": nickname,
                "items": items,
            }));
        }

        let mut dimensions_out = Vec::with_capacity(by_dim.len());
        for (dimension_id, mut buildings) in by_dim {
            buildings.sort_by(|a, b| {
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

            let (kind, entrance) = dimension_meta(&s, dimension_id);
            dimensions_out.push((
                dimension_id,
                entrance_sort_key(&entrance),
                json!({
                    "dimension_id": dimension_id,
                    "kind": kind,
                    "entrance": entrance,
                    "buildings": buildings,
                }),
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

        let dimensions: Vec<Value> = dimensions_out.into_iter().map(|(_, _, v)| v).collect();

        return no_store_json(json!({
            "claim": {
                "entity_id": pk.to_string(),
                "name": claim_name,
                "region": region,
            },
            "dimensions": dimensions,
        }))
        .into_response();
    }

    no_store_status(StatusCode::NOT_FOUND, json!({"error": "claim not found"})).into_response()
}

fn dimension_meta(s: &RegionStore, dimension_id: u32) -> (&'static str, Value) {
    if dimension_id == OVERWORLD_DIMENSION {
        return ("overworld", Value::Null);
    }
    let Some(net) = s.dimension_network.by_entrance_dim(dimension_id) else {
        return ("unknown", Value::Null);
    };
    let (name, nickname) = entrance_labels(s, net.building_id);
    (
        "building_interior",
        json!({
            "entity_id": net.building_id.to_string(),
            "name": name,
            "nickname": nickname,
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

fn entrance_sort_key(entrance: &Value) -> String {
    entrance
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_ascii_lowercase()
}

fn item_type_label(t: u8) -> &'static str {
    match t {
        Pocket::CARGO => "Cargo",
        _ => "Item",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode::{
        BuildingDescRow, BuildingRow, DimensionNetworkRow, LocationDimRow, OVERWORLD_DIMENSION,
    };
    use crate::store::RegionStore;

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
            building_id: 200,
            claim_entity_id: 99,
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
}
