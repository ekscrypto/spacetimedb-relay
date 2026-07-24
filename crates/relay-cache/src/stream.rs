// SPDX-License-Identifier: MIT

//! Live inventory / crafts WebSocket streams.
//!
//! Per-entity endpoints (still supported):
//! - `GET /player/:id/inventory/ws`
//! - `GET /player/:id/housing/ws`
//! - `GET /claim/:id/inventory/ws`
//!
//! Multiplexed page stream (preferred for mats):
//! - `GET /inventory/ws` — after connect, client sends a subscribe frame:
//!   ```json
//!   { "players": ["…"], "houses": ["…"], "claims": ["…"],
//!     "player_crafts": ["…"], "claim_crafts": ["…"] }
//!   ```
//!   Server replies with one tagged snapshot per entity, then pushes
//!   further tagged snapshots when any subscribed key changes. A later
//!   subscribe frame replaces the set. Browser WebSocket cannot POST a
//!   body on connect, so the list rides in the first text frame.
//!
//! Tagged frame shape:
//! ```json
//! { "type": "player_inventory"|"player_housing"|"claim_inventory"
//!            |"player_crafts"|"claim_crafts",
//!   "entity_id": "<u64 string>",
//!   "data": { …same as HTTP… } }
//! ```
//! or `{ "type", "entity_id", "error": "…" }` when missing.
//! Plus `{ "ts": <unix ms UTC> }` every 5s for connectivity detection.

use std::collections::HashSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::interest::{InterestHub, Subscription, Topic};
use crate::serve::{
    build_claim_crafts, build_claim_inventory, build_player_crafts, build_player_housing,
    build_player_inventory, claim_crafts_to_json, claim_inventory_to_json, player_crafts_to_json,
    player_housing_to_json, player_inventory_to_json, Fleet,
};

const COALESCE: Duration = Duration::from_millis(75);
/// Soft cap on concurrent interest leases (single-key + multiplexed).
const MAX_STREAMS: u64 = 512;
/// Max entities in one multiplexed subscribe (all topic lists combined).
const MAX_BUNDLE_KEYS: usize = 64;
/// Wait for the first subscribe frame on `/inventory/ws`.
const SUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(15);
/// Application-level heartbeat so clients can detect a half-open socket.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

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

/// Multiplexed inventory + crafts stream — one WS for many entities.
pub async fn inventory_bundle_ws(
    ws: WebSocketUpgrade,
    State(fleet): State<Fleet>,
) -> impl IntoResponse {
    if fleet.interest.active_streams() >= MAX_STREAMS {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(json!({"error": "too many active inventory streams"})),
        )
            .into_response();
    }
    ws.on_upgrade(move |socket| run_bundle_stream(socket, fleet))
        .into_response()
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
        Topic::PlayerCrafts | Topic::ClaimCrafts => {
            // Per-entity crafts WS not exposed; multiplexed path only.
            None
        }
    };
    let Some(initial) = initial else {
        let msg = match topic {
            Topic::ClaimInventory | Topic::ClaimCrafts => "claim not found",
            _ => "player not found",
        };
        return (StatusCode::NOT_FOUND, axum::Json(json!({"error": msg}))).into_response();
    };

    ws.on_upgrade(move |socket| run_stream(socket, fleet, topic, pk, initial))
}

#[derive(Debug, Deserialize)]
struct SubscribeMsg {
    /// Player entity ids for personal bags (pockets / bank / wagon / …).
    #[serde(default)]
    players: Vec<EntityId>,
    /// Player entity ids for housing interiors.
    #[serde(default)]
    houses: Vec<EntityId>,
    /// Claim entity ids for shared claim storage.
    #[serde(default)]
    claims: Vec<EntityId>,
    /// Player entity ids for crafts (progressive + passive).
    #[serde(default)]
    player_crafts: Vec<EntityId>,
    /// Claim entity ids for crafts at claim buildings.
    #[serde(default)]
    claim_crafts: Vec<EntityId>,
}

/// Accept string or number u64 (JS number is unsafe above 2^53; prefer string).
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum EntityId {
    Str(String),
    Num(u64),
}

impl EntityId {
    fn parse(&self) -> Result<u64, ()> {
        match self {
            EntityId::Num(n) => Ok(*n),
            EntityId::Str(s) => s.parse().map_err(|_| ()),
        }
    }
}

enum Snapshot {
    PlayerInv(crate::serve::PlayerInventory),
    PlayerHousing(crate::serve::PlayerHousing),
    ClaimInv(crate::serve::ClaimInventory),
    PlayerCrafts(crate::serve::PlayerCrafts),
    ClaimCrafts(crate::serve::ClaimCrafts),
}

impl Snapshot {
    fn to_json(&self) -> serde_json::Value {
        match self {
            Snapshot::PlayerInv(v) => player_inventory_to_json(v),
            Snapshot::PlayerHousing(v) => player_housing_to_json(v),
            Snapshot::ClaimInv(v) => claim_inventory_to_json(v),
            Snapshot::PlayerCrafts(v) => player_crafts_to_json(v),
            Snapshot::ClaimCrafts(v) => claim_crafts_to_json(v),
        }
    }

    fn rebuild(fleet: &Fleet, topic: Topic, pk: u64) -> Option<Self> {
        match topic {
            Topic::PlayerInventory => build_player_inventory(fleet, pk).map(Snapshot::PlayerInv),
            Topic::PlayerHousing => build_player_housing(fleet, pk).map(Snapshot::PlayerHousing),
            Topic::ClaimInventory => build_claim_inventory(fleet, pk).map(Snapshot::ClaimInv),
            Topic::PlayerCrafts => {
                build_player_crafts(fleet, pk, None).map(Snapshot::PlayerCrafts)
            }
            Topic::ClaimCrafts => build_claim_crafts(fleet, pk, None).map(Snapshot::ClaimCrafts),
        }
    }
}

fn tagged_frame(topic: Topic, pk: u64, snap: Option<&Snapshot>) -> serde_json::Value {
    match snap {
        Some(s) => json!({
            "type": topic.as_str(),
            "entity_id": pk.to_string(),
            "data": s.to_json(),
        }),
        None => {
            let err = match topic {
                Topic::ClaimInventory | Topic::ClaimCrafts => "claim not found",
                _ => "player not found",
            };
            json!({
                "type": topic.as_str(),
                "entity_id": pk.to_string(),
                "error": err,
            })
        }
    }
}

fn heartbeat_frame() -> serde_json::Value {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    json!({ "ts": ts })
}

fn new_heartbeat_interval() -> tokio::time::Interval {
    let mut interval = tokio::time::interval(HEARTBEAT_INTERVAL);
    // Don't fire immediately — first heartbeat after one full period.
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval
}

async fn run_stream(socket: WebSocket, fleet: Fleet, topic: Topic, pk: u64, initial: Snapshot) {
    let mut sub = fleet.interest.subscribe(topic, pk);
    let (mut sink, mut source) = socket.split();
    let mut heartbeat = new_heartbeat_interval();
    // Consume the immediate first tick so the period starts now.
    heartbeat.tick().await;

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
            _ = heartbeat.tick() => {
                if send_json(&mut sink, &heartbeat_frame()).await.is_err() {
                    break;
                }
            }
            changed = sub.receiver().changed() => {
                if changed.is_err() {
                    break;
                }
                tokio::time::sleep(COALESCE).await;
                while sub.receiver().has_changed().unwrap_or(false) {
                    let _ = sub.receiver().borrow_and_update();
                }
                let Some(snap) = Snapshot::rebuild(&fleet, topic, pk) else {
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

async fn run_bundle_stream(socket: WebSocket, fleet: Fleet) {
    let (mut sink, mut source) = socket.split();

    tracing::info!(
        target: "relay_cache::stream",
        active = fleet.interest.active_streams(),
        "inventory bundle stream connected"
    );

    // First frame must be a subscribe (with timeout).
    let first = tokio::time::timeout(SUBSCRIBE_TIMEOUT, source.next()).await;
    let subscribe_text = match first {
        Ok(Some(Ok(Message::Text(t)))) => t.to_string(),
        Ok(Some(Ok(Message::Ping(p)))) => {
            let _ = sink.send(Message::Pong(p)).await;
            // One more try after a ping.
            match tokio::time::timeout(SUBSCRIBE_TIMEOUT, source.next()).await {
                Ok(Some(Ok(Message::Text(t)))) => t.to_string(),
                _ => {
                    let _ = send_json(
                        &mut sink,
                        &json!({"error": "expected subscribe JSON text frame"}),
                    )
                    .await;
                    let _ = sink.send(Message::Close(None)).await;
                    return;
                }
            }
        }
        _ => {
            let _ = send_json(
                &mut sink,
                &json!({"error": "expected subscribe JSON text frame within 15s"}),
            )
            .await;
            let _ = sink.send(Message::Close(None)).await;
            return;
        }
    };

    let mut keys = match parse_subscribe(&subscribe_text) {
        Ok(k) => k,
        Err(err) => {
            let _ = send_json(&mut sink, &json!({"error": err})).await;
            let _ = sink.send(Message::Close(None)).await;
            return;
        }
    };

    if fleet.interest.active_streams() + keys.len() as u64 > MAX_STREAMS {
        let _ = send_json(
            &mut sink,
            &json!({"error": "too many active inventory streams"}),
        )
        .await;
        let _ = sink.send(Message::Close(None)).await;
        return;
    }

    let (dirty_tx, mut dirty_rx) = mpsc::unbounded_channel::<(Topic, u64)>();
    let mut watchers = spawn_watchers(&fleet.interest, &keys, dirty_tx.clone());

    if push_snapshots(&mut sink, &fleet, &keys).await.is_err() {
        return;
    }

    let mut heartbeat = new_heartbeat_interval();
    heartbeat.tick().await;

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
                    Some(Ok(Message::Text(t))) => {
                        match parse_subscribe(&t) {
                            Ok(new_keys) => {
                                // Net budget after dropping current leases.
                                let projected = fleet
                                    .interest
                                    .active_streams()
                                    .saturating_sub(keys.len() as u64)
                                    + new_keys.len() as u64;
                                if projected > MAX_STREAMS {
                                    if send_json(
                                        &mut sink,
                                        &json!({"error": "too many active inventory streams"}),
                                    )
                                    .await
                                    .is_err()
                                    {
                                        break;
                                    }
                                    continue;
                                }
                                // Replace interest set.
                                drop(watchers);
                                keys = new_keys;
                                watchers =
                                    spawn_watchers(&fleet.interest, &keys, dirty_tx.clone());
                                // Drain stale dirties from the old set.
                                while dirty_rx.try_recv().is_ok() {}
                                if push_snapshots(&mut sink, &fleet, &keys).await.is_err() {
                                    break;
                                }
                            }
                            Err(err) => {
                                if send_json(&mut sink, &json!({"error": err})).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                    Some(Ok(Message::Pong(_))) | Some(Ok(Message::Binary(_))) => {}
                    Some(Err(_)) => break,
                }
            }
            _ = heartbeat.tick() => {
                if send_json(&mut sink, &heartbeat_frame()).await.is_err() {
                    break;
                }
            }
            dirty = dirty_rx.recv() => {
                let Some((topic, pk)) = dirty else { break; };
                let mut pending: HashSet<(Topic, u64)> = HashSet::new();
                pending.insert((topic, pk));
                tokio::time::sleep(COALESCE).await;
                while let Ok((t, id)) = dirty_rx.try_recv() {
                    pending.insert((t, id));
                }
                // Only push keys still in the current subscribe set.
                let live: HashSet<(Topic, u64)> = keys.iter().copied().collect();
                for (topic, pk) in pending {
                    if !live.contains(&(topic, pk)) {
                        continue;
                    }
                    let snap = Snapshot::rebuild(&fleet, topic, pk);
                    let frame = tagged_frame(topic, pk, snap.as_ref());
                    if send_json(&mut sink, &frame).await.is_err() {
                        return;
                    }
                    fleet.interest.record_push();
                }
            }
        }
    }

    drop(watchers);
    tracing::info!(
        target: "relay_cache::stream",
        "inventory bundle stream disconnected"
    );
}

fn parse_subscribe(text: &str) -> Result<Vec<(Topic, u64)>, String> {
    let msg: SubscribeMsg =
        serde_json::from_str(text).map_err(|e| format!("invalid subscribe JSON: {e}"))?;
    let mut keys = Vec::new();
    for id in &msg.players {
        let pk = id
            .parse()
            .map_err(|_| "players entries must be u64 (prefer string)".to_owned())?;
        if pk != 0 {
            keys.push((Topic::PlayerInventory, pk));
        }
    }
    for id in &msg.houses {
        let pk = id
            .parse()
            .map_err(|_| "houses entries must be u64 (prefer string)".to_owned())?;
        if pk != 0 {
            keys.push((Topic::PlayerHousing, pk));
        }
    }
    for id in &msg.claims {
        let pk = id
            .parse()
            .map_err(|_| "claims entries must be u64 (prefer string)".to_owned())?;
        if pk != 0 {
            keys.push((Topic::ClaimInventory, pk));
        }
    }
    for id in &msg.player_crafts {
        let pk = id
            .parse()
            .map_err(|_| "player_crafts entries must be u64 (prefer string)".to_owned())?;
        if pk != 0 {
            keys.push((Topic::PlayerCrafts, pk));
        }
    }
    for id in &msg.claim_crafts {
        let pk = id
            .parse()
            .map_err(|_| "claim_crafts entries must be u64 (prefer string)".to_owned())?;
        if pk != 0 {
            keys.push((Topic::ClaimCrafts, pk));
        }
    }
    keys.sort_unstable_by_key(|(t, id)| (*t as u8, *id));
    keys.dedup();
    if keys.is_empty() {
        return Err(
            "subscribe requires at least one of players/houses/claims/player_crafts/claim_crafts"
                .into(),
        );
    }
    if keys.len() > MAX_BUNDLE_KEYS {
        return Err(format!(
            "too many entities (max {MAX_BUNDLE_KEYS}, got {})",
            keys.len()
        ));
    }
    Ok(keys)
}

struct Watchers {
    handles: Vec<JoinHandle<()>>,
    /// Keep leases alive until watchers are replaced/dropped.
    _leases: Vec<Subscription>,
}

impl Drop for Watchers {
    fn drop(&mut self) {
        for h in &self.handles {
            h.abort();
        }
    }
}

fn spawn_watchers(
    hub: &std::sync::Arc<InterestHub>,
    keys: &[(Topic, u64)],
    dirty_tx: mpsc::UnboundedSender<(Topic, u64)>,
) -> Watchers {
    let mut handles = Vec::with_capacity(keys.len());
    let mut leases = Vec::with_capacity(keys.len());
    for &(topic, id) in keys {
        let sub = hub.subscribe(topic, id);
        let mut rx = sub.clone_receiver();
        leases.push(sub);
        let tx = dirty_tx.clone();
        handles.push(tokio::spawn(async move {
            loop {
                if rx.changed().await.is_err() {
                    break;
                }
                if tx.send((topic, id)).is_err() {
                    break;
                }
            }
        }));
    }
    Watchers {
        handles,
        _leases: leases,
    }
}

async fn push_snapshots(
    sink: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    fleet: &Fleet,
    keys: &[(Topic, u64)],
) -> Result<(), ()> {
    for &(topic, pk) in keys {
        let snap = Snapshot::rebuild(fleet, topic, pk);
        let frame = tagged_frame(topic, pk, snap.as_ref());
        send_json(sink, &frame).await?;
        fleet.interest.record_push();
    }
    // Ack so clients know the initial burst is complete.
    send_json(
        sink,
        &json!({
            "type": "subscribed",
            "count": keys.len(),
        }),
    )
    .await
}

async fn send_json(
    sink: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    value: &serde_json::Value,
) -> Result<(), ()> {
    let text = serde_json::to_string(value).map_err(|_| ())?;
    sink.send(Message::Text(text)).await.map_err(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_subscribe_accepts_string_and_number_ids() {
        let keys = parse_subscribe(
            r#"{"players":["1","2"],"houses":[1],"claims":["9"],"player_crafts":["1"],"claim_crafts":[9]}"#,
        )
        .unwrap();
        // player 1 appears as both PlayerInventory, PlayerHousing, and PlayerCrafts.
        assert!(keys.contains(&(Topic::PlayerInventory, 1)));
        assert!(keys.contains(&(Topic::PlayerInventory, 2)));
        assert!(keys.contains(&(Topic::PlayerHousing, 1)));
        assert!(keys.contains(&(Topic::ClaimInventory, 9)));
        assert!(keys.contains(&(Topic::PlayerCrafts, 1)));
        assert!(keys.contains(&(Topic::ClaimCrafts, 9)));
        assert_eq!(keys.len(), 6);
    }

    #[test]
    fn parse_subscribe_crafts_only() {
        let keys = parse_subscribe(r#"{"player_crafts":["42"],"claim_crafts":["7"]}"#).unwrap();
        assert_eq!(
            keys,
            vec![(Topic::PlayerCrafts, 42), (Topic::ClaimCrafts, 7)]
        );
    }

    #[test]
    fn parse_subscribe_rejects_empty_and_oversize() {
        assert!(parse_subscribe(r#"{"players":[]}"#).is_err());
        let many: Vec<String> = (1..=65).map(|i| i.to_string()).collect();
        let oversized = format!(r#"{{"players":{}}}"#, serde_json::to_string(&many).unwrap());
        assert!(parse_subscribe(&oversized).is_err());
    }
}
