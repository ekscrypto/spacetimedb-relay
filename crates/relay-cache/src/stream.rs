// SPDX-License-Identifier: MIT

//! Live inventory WebSocket streams.
//!
//! Three endpoints mirror the HTTP snapshot builders:
//! - `GET /player/:id/inventory/ws`
//! - `GET /player/:id/housing/ws`
//! - `GET /claim/:id/inventory/ws`
//!
//! On connect: one JSON text frame with the current snapshot (or close
//! with an error if the entity is unknown). On interest-hub notify:
//! coalesce ~75 ms, rebuild, push another full snapshot. Slow clients
//! skip intermediate rebuilds (watch coalesces generations).

use std::time::Duration;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;

use crate::interest::Topic;
use crate::serve::{
    build_claim_inventory, build_player_housing, build_player_inventory, claim_inventory_to_json,
    player_housing_to_json, player_inventory_to_json, Fleet,
};

const COALESCE: Duration = Duration::from_millis(75);
/// Soft cap on concurrent inventory streams (all topics).
const MAX_STREAMS: u64 = 512;

pub async fn player_inventory_ws(
    ws: WebSocketUpgrade,
    State(fleet): State<Fleet>,
    Path(entity_id): Path<String>,
) -> impl IntoResponse {
    upgrade_or_reject(ws, fleet, entity_id, Topic::PlayerInventory).await
}

pub async fn player_housing_ws(
    ws: WebSocketUpgrade,
    State(fleet): State<Fleet>,
    Path(entity_id): Path<String>,
) -> impl IntoResponse {
    upgrade_or_reject(ws, fleet, entity_id, Topic::PlayerHousing).await
}

pub async fn claim_inventory_ws(
    ws: WebSocketUpgrade,
    State(fleet): State<Fleet>,
    Path(entity_id): Path<String>,
) -> impl IntoResponse {
    upgrade_or_reject(ws, fleet, entity_id, Topic::ClaimInventory).await
}

async fn upgrade_or_reject(
    ws: WebSocketUpgrade,
    fleet: Fleet,
    entity_id: String,
    topic: Topic,
) -> axum::response::Response {
    let Ok(pk) = entity_id.parse::<u64>() else {
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(json!({"error": "entity_id must be a u64"})),
        )
            .into_response();
    };

    if fleet.interest.active_streams() >= MAX_STREAMS {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(json!({"error": "too many active inventory streams"})),
        )
            .into_response();
    }

    // Validate entity exists before upgrading (same 404 semantics as HTTP).
    let initial = match topic {
        Topic::PlayerInventory => build_player_inventory(&fleet, pk).map(Snapshot::PlayerInv),
        Topic::PlayerHousing => build_player_housing(&fleet, pk).map(Snapshot::PlayerHousing),
        Topic::ClaimInventory => build_claim_inventory(&fleet, pk).map(Snapshot::ClaimInv),
    };
    let Some(initial) = initial else {
        let msg = match topic {
            Topic::ClaimInventory => "claim not found",
            _ => "player not found",
        };
        return (StatusCode::NOT_FOUND, axum::Json(json!({"error": msg}))).into_response();
    };

    ws.on_upgrade(move |socket| run_stream(socket, fleet, topic, pk, initial))
}

enum Snapshot {
    PlayerInv(crate::serve::PlayerInventory),
    PlayerHousing(crate::serve::PlayerHousing),
    ClaimInv(crate::serve::ClaimInventory),
}

impl Snapshot {
    fn to_json(&self) -> serde_json::Value {
        match self {
            Snapshot::PlayerInv(v) => player_inventory_to_json(v),
            Snapshot::PlayerHousing(v) => player_housing_to_json(v),
            Snapshot::ClaimInv(v) => claim_inventory_to_json(v),
        }
    }

    fn rebuild(fleet: &Fleet, topic: Topic, pk: u64) -> Option<Self> {
        match topic {
            Topic::PlayerInventory => build_player_inventory(fleet, pk).map(Snapshot::PlayerInv),
            Topic::PlayerHousing => build_player_housing(fleet, pk).map(Snapshot::PlayerHousing),
            Topic::ClaimInventory => build_claim_inventory(fleet, pk).map(Snapshot::ClaimInv),
        }
    }
}

async fn run_stream(socket: WebSocket, fleet: Fleet, topic: Topic, pk: u64, initial: Snapshot) {
    let mut sub = fleet.interest.subscribe(topic, pk);
    let (mut sink, mut source) = socket.split();

    tracing::info!(
        target: "relay_cache::stream",
        topic = topic.as_str(),
        entity_id = pk,
        active = fleet.interest.active_streams(),
        "inventory stream connected"
    );

    if send_json(&mut sink, &initial.to_json()).await.is_err() {
        return;
    }
    fleet.interest.record_push();

    loop {
        tokio::select! {
            biased;
            msg = source.next() => {
                match msg {
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(Message::Ping(p))) => {
                        if sink.send(Message::Pong(p)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Pong(_))) | Some(Ok(Message::Text(_)))
                    | Some(Ok(Message::Binary(_))) => {}
                    Some(Err(_)) => break,
                }
            }
            changed = sub.receiver().changed() => {
                if changed.is_err() {
                    break;
                }
                // Coalesce rapid notifies into one rebuild.
                tokio::time::sleep(COALESCE).await;
                while sub.receiver().has_changed().unwrap_or(false) {
                    let _ = sub.receiver().borrow_and_update();
                }
                let Some(snap) = Snapshot::rebuild(&fleet, topic, pk) else {
                    // Entity disappeared — close cleanly.
                    let _ = sink.send(Message::Close(None)).await;
                    break;
                };
                if send_json(&mut sink, &snap.to_json()).await.is_err() {
                    break;
                }
                fleet.interest.record_push();
            }
        }
    }

    tracing::info!(
        target: "relay_cache::stream",
        topic = topic.as_str(),
        entity_id = pk,
        "inventory stream disconnected"
    );
}

async fn send_json(
    sink: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    value: &serde_json::Value,
) -> Result<(), ()> {
    let text = serde_json::to_string(value).map_err(|_| ())?;
    sink.send(Message::Text(text)).await.map_err(|_| ())
}
