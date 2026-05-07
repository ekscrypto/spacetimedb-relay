// SPDX-License-Identifier: MIT

//! Per-downstream-connection task. Pairs the inbound socket with a
//! fresh socket to the local SpacetimeDB and shuttles frames between
//! them, applying [`crate::rewrite`] on local→client traffic for v1
//! clients.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http::header::{HeaderName, HeaderValue, AUTHORIZATION, SEC_WEBSOCKET_PROTOCOL};
use spacetimedb_sats::bsatn as sats_bsatn;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::codec;
use crate::metrics::{ClientStats, FrontendMetrics};
use crate::rewrite::{self, RewriteError};
use crate::state::{ActiveClients, ClientHandle};
use crate::Subprotocol;

const APPLY_PREFIX: &str = "relay_apply_";
const META_TABLE: &str = "_relay_meta";

type DownstreamSocket = WebSocketStream<TcpStream>;
type LocalSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub struct ClientCtx {
    pub remote: SocketAddr,
    pub subprotocol: Subprotocol,
    pub local_url: Url,
    pub local_database: String,
    pub local_token: Option<String>,
    pub idle_timeout: Duration,
    pub metrics: Arc<FrontendMetrics>,
    pub clients: ActiveClients,
}

/// Run a single client connection to completion. Registers + deregisters
/// itself in `clients`, updates `metrics`, and tears the local socket
/// down on cancellation.
pub async fn run(downstream: DownstreamSocket, ctx: ClientCtx) {
    let stats = Arc::new(ClientStats::new(ctx.remote, ctx.subprotocol));
    let cancel = CancellationToken::new();
    ctx.clients.insert(ClientHandle {
        stats: stats.clone(),
        cancel: cancel.clone(),
    });
    ctx.metrics.record_connect();
    tracing::info!(
        target: "relay::frontend",
        client_id = %stats.id,
        remote = %stats.remote_addr,
        subprotocol = %stats.subprotocol.name(),
        "downstream connected"
    );

    let id = stats.id;
    let result = drive(downstream, &ctx, &stats, cancel.clone()).await;

    match &result {
        Ok(reason) => tracing::info!(
            target: "relay::frontend",
            client_id = %id, reason = %reason, "downstream disconnected"
        ),
        Err(e) => tracing::warn!(
            target: "relay::frontend",
            client_id = %id, error = %e, "downstream task ended with error"
        ),
    }

    ctx.metrics.record_disconnect();
    ctx.clients.remove(id);
}

async fn drive(
    mut downstream: DownstreamSocket,
    ctx: &ClientCtx,
    stats: &Arc<ClientStats>,
    cancel: CancellationToken,
) -> Result<String, ClientError> {
    let mut local = connect_local(
        &ctx.local_url,
        &ctx.local_database,
        ctx.subprotocol,
        ctx.local_token.as_deref(),
    )
    .await?;

    let idle = ctx.idle_timeout;
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                let _ = downstream.send(Message::Close(None)).await;
                let _ = local.send(Message::Close(None)).await;
                return Ok("cancelled".into());
            }
            msg = downstream.next() => {
                let Some(msg) = msg else { return Ok("client closed".into()); };
                match msg.map_err(ClientError::DownstreamWs)? {
                    Message::Binary(b) => {
                        let bytes = Bytes::from(b);
                        tracing::debug!(
                            target: "relay::frontend",
                            client_id = %stats.id,
                            len = bytes.len(),
                            tag = ?bytes.first().copied(),
                            "client→local frame"
                        );
                        observe_inbound(&ctx.metrics, stats, &bytes);
                        local.send(Message::Binary(bytes.to_vec())).await
                            .map_err(ClientError::LocalWs)?;
                    }
                    Message::Text(t) => {
                        // Spec-compliance: forward text frames opaquely.
                        // SpacetimeDB doesn't use them on the bsatn path.
                        local.send(Message::Text(t)).await.map_err(ClientError::LocalWs)?;
                    }
                    Message::Ping(p) => {
                        downstream.send(Message::Pong(p)).await
                            .map_err(ClientError::DownstreamWs)?;
                    }
                    Message::Pong(_) | Message::Frame(_) => {}
                    Message::Close(frame) => {
                        let _ = local.send(Message::Close(None)).await;
                        let reason = frame
                            .map(|f| format!("{}: {}", f.code, f.reason))
                            .unwrap_or_else(|| "client close".into());
                        return Ok(reason);
                    }
                }
            }
            msg = local.next() => {
                let Some(msg) = msg else { return Ok("local stdb closed".into()); };
                match msg.map_err(ClientError::LocalWs)? {
                    Message::Binary(b) => {
                        let mut bytes = Bytes::from(b);
                        tracing::debug!(
                            target: "relay::frontend",
                            client_id = %stats.id,
                            len = bytes.len(),
                            tag = ?codec::message_tag(&bytes),
                            "local→client frame"
                        );
                        if ctx.subprotocol == Subprotocol::V1 {
                            // v1 clients get the full upstream-meta
                            // injection; we hide internal traffic as a
                            // side-effect.
                            match handle_local_v1_frame(bytes.clone(), stats, &ctx.metrics) {
                                Ok(Some(rewritten)) => bytes = rewritten,
                                Ok(None) => {
                                    // Silently dropped (relay's own
                                    // bind/housekeeping or _relay_meta
                                    // tables — see hide_internal_v1).
                                    continue;
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        target: "relay::frontend",
                                        client_id = %stats.id,
                                        error = %e,
                                        "v1 rewrite failed; forwarding original frame"
                                    );
                                }
                            }
                        }
                        observe_outbound(&ctx.metrics, stats, &bytes);
                        downstream.send(Message::Binary(bytes.to_vec())).await
                            .map_err(ClientError::DownstreamWs)?;
                    }
                    Message::Text(t) => {
                        downstream.send(Message::Text(t)).await
                            .map_err(ClientError::DownstreamWs)?;
                    }
                    Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {}
                    Message::Close(frame) => {
                        let _ = downstream.send(Message::Close(None)).await;
                        let reason = frame
                            .map(|f| format!("local: {}: {}", f.code, f.reason))
                            .unwrap_or_else(|| "local stdb close".into());
                        return Ok(reason);
                    }
                }
            }
            _ = tokio::time::sleep(idle) => {
                let _ = downstream.send(Message::Ping(Vec::new())).await;
                let _ = local.send(Message::Ping(Vec::new())).await;
            }
        }
    }
}

fn observe_inbound(metrics: &FrontendMetrics, stats: &ClientStats, bytes: &Bytes) {
    let n = bytes.len() as u64;
    metrics.record_inbound(n);
    stats.record_inbound(n);
    inspect_client_message(stats, bytes);
}

fn observe_outbound(metrics: &FrontendMetrics, stats: &ClientStats, bytes: &Bytes) {
    let n = bytes.len() as u64;
    metrics.record_outbound(n);
    stats.record_outbound(n);
}

/// Decode just the message tag of a frame from the client and bump the
/// matching counter. Subscribes are also captured so the dashboard can
/// list each client's active queries.
fn inspect_client_message(stats: &ClientStats, bytes: &Bytes) {
    let Some(tag) = codec::message_tag(bytes) else {
        return;
    };
    use relay_protocol::tags;
    match tag {
        tags::CLIENT_ONE_OFF_QUERY => {
            stats
                .one_off_queries
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        tags::CLIENT_SUBSCRIBE => {
            // Best-effort SQL extraction. The shape differs between
            // v1 and v2; we record what we can and silently skip when
            // decoding fails.
            if let Ok(body) = codec::body(bytes) {
                if let Some(queries) = try_decode_subscribe_queries(stats.subprotocol, body) {
                    let mut subs = stats.subscriptions.lock();
                    for q in queries {
                        subs.insert(q);
                    }
                }
            }
        }
        _ => {}
    }
}

fn try_decode_subscribe_queries(sp: Subprotocol, body: &[u8]) -> Option<Vec<String>> {
    match sp {
        Subprotocol::V2 => {
            use spacetimedb_client_api_messages::websocket::v2;
            let m: v2::ClientMessage = sats_bsatn::from_slice(body).ok()?;
            match m {
                v2::ClientMessage::Subscribe(s) => {
                    Some(s.query_strings.iter().map(|s| s.to_string()).collect())
                }
                _ => None,
            }
        }
        Subprotocol::V1 => {
            use spacetimedb_client_api_messages_v1::websocket as v1;
            let m: v1::ClientMessage<Box<[u8]>> =
                spacetimedb_lib_v1::bsatn::from_slice(body).ok()?;
            match m {
                v1::ClientMessage::Subscribe(s) => {
                    Some(s.query_strings.iter().map(|s| s.to_string()).collect())
                }
                v1::ClientMessage::SubscribeMulti(s) => {
                    Some(s.query_strings.iter().map(|s| s.to_string()).collect())
                }
                _ => None,
            }
        }
    }
}

/// Result of inspecting a v1 frame from local stdb.
/// * `Some(bytes)` → forward `bytes` to the client (rewritten or original).
/// * `None` → drop the frame entirely (relay-internal traffic the
///   downstream client should never see).
fn handle_local_v1_frame(
    frame: Bytes,
    stats: &ClientStats,
    metrics: &FrontendMetrics,
) -> Result<Option<Bytes>, RewriteError> {
    if should_hide_v1(&frame) {
        return Ok(None);
    }
    match rewrite::rewrite_local_to_v1_client(&frame)? {
        rewrite::Rewritten::Owned(v) => {
            stats
                .rewrites
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            metrics.record_rewrite();
            Ok(Some(Bytes::from(v)))
        }
        rewrite::Rewritten::Passthrough => Ok(Some(frame)),
    }
}

/// True for frames the proxy should hide from downstream clients.
/// The only relay-internal traffic on the local stdb that surfaces as
/// a v1 broadcast today is `relay_bind_writer`'s `_relay_meta` insert
/// at proxy startup — there's no reason for downstream clients to ever
/// see it.
fn should_hide_v1(frame: &[u8]) -> bool {
    let Ok(body) = codec::body(frame) else {
        return false;
    };
    use spacetimedb_client_api_messages_v1::websocket as v1;
    let Ok(msg) = spacetimedb_lib_v1::bsatn::from_slice::<v1::ServerMessage<v1::BsatnFormat>>(body)
    else {
        return false;
    };
    let v1::ServerMessage::TransactionUpdate(tu) = msg else {
        return false;
    };
    let v1::UpdateStatus::Committed(db) = tu.status else {
        return false;
    };
    if db.tables.is_empty() {
        return false;
    }
    let touched_only_meta = db
        .tables
        .iter()
        .all(|t| t.table_name.as_ref() == META_TABLE);
    let is_apply = tu.reducer_call.reducer_name.starts_with(APPLY_PREFIX);
    touched_only_meta && !is_apply
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("downstream ws: {0}")]
    DownstreamWs(tokio_tungstenite::tungstenite::Error),
    #[error("local stdb ws: {0}")]
    LocalWs(tokio_tungstenite::tungstenite::Error),
    #[error("local connect: {0}")]
    LocalConnect(String),
    #[error("local handshake: {0}")]
    LocalHandshake(String),
}

async fn connect_local(
    base: &Url,
    database: &str,
    subprotocol: Subprotocol,
    token: Option<&str>,
) -> Result<LocalSocket, ClientError> {
    let mut url = base.clone();
    let scheme = match url.scheme() {
        "http" => "ws",
        "https" => "wss",
        s => s,
    }
    .to_string();
    url.set_scheme(&scheme)
        .map_err(|_| ClientError::LocalConnect("bad local stdb scheme".into()))?;
    url.set_path(&format!("/v1/database/{}/subscribe", database));
    url.set_query(Some("compression=None"));
    let mut req = url
        .as_str()
        .into_client_request()
        .map_err(|e| ClientError::LocalConnect(e.to_string()))?;
    req.headers_mut().insert(
        HeaderName::from_static("sec-websocket-protocol"),
        HeaderValue::from_static(subprotocol.name()),
    );
    if let Some(t) = token {
        if !t.is_empty() {
            let v = HeaderValue::from_str(&format!("Bearer {t}"))
                .map_err(|e| ClientError::LocalConnect(format!("auth header: {e}")))?;
            req.headers_mut().insert(AUTHORIZATION, v);
        }
    }
    let (ws, resp) = tokio_tungstenite::connect_async(req)
        .await
        .map_err(|e| ClientError::LocalConnect(e.to_string()))?;
    if let Some(got) = resp.headers().get(SEC_WEBSOCKET_PROTOCOL) {
        let got = got.to_str().unwrap_or("");
        if got != subprotocol.name() {
            return Err(ClientError::LocalHandshake(format!(
                "local stdb negotiated `{got}`, wanted `{}`",
                subprotocol.name()
            )));
        }
    }
    Ok(ws)
}
