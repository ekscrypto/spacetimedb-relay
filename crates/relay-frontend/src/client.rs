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
use relay_mirror_driver::MetaRegistry;

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
    /// Lookup table the relay-mirror-driver populates with
    /// `(request_id, UpstreamReducerMeta)` for each `relay_apply_*`
    /// `CallReducer`. The proxy joins it against incoming v1
    /// `TransactionUpdateLight` frames (whose `request_id` echoes the
    /// caller's) to synthesise full v1 TUs for v1 subscribers.
    pub meta_registry: Option<Arc<MetaRegistry>>,
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
                        observe_inbound(&ctx.metrics, stats, &bytes, ctx.subprotocol);
                        // Reject OneOffQuery before it reaches local stdb.
                        // Large result sets (e.g. SELECT * FROM player_state)
                        // force stdb to serialize all rows into one frame,
                        // exhausting the shared stdb instance's memory under
                        // concurrent load and crashing all relay instances.
                        // Clients should subscribe to tables they need instead.
                        if codec::message_tag(&bytes) == Some(relay_protocol::tags::CLIENT_ONE_OFF_QUERY) {
                            if let Some(err_frame) = reject_one_off_query(&bytes, ctx.subprotocol, stats) {
                                downstream.send(Message::Binary(err_frame)).await
                                    .map_err(ClientError::DownstreamWs)?;
                            }
                            continue;
                        }
                        // Reject CallReducer / CallProcedure before they
                        // reach local stdb. The relay is read-only fan-out;
                        // clients that need to mutate state call reducers
                        // directly on the upstream. Forwarding these would
                        // also expose the writer-bind race on
                        // `relay_bind_writer` (first-caller-wins) during
                        // the startup pre-bind window.
                        if let Some(kind) = inbound_write_tag(&bytes, ctx.subprotocol) {
                            let frame = match kind {
                                WriteTag::CallReducer => {
                                    reject_call_reducer(&bytes, ctx.subprotocol, stats)
                                }
                                WriteTag::CallProcedure => {
                                    reject_call_procedure(&bytes, ctx.subprotocol, stats)
                                }
                            };
                            if let Some(err_frame) = frame {
                                downstream.send(Message::Binary(err_frame)).await
                                    .map_err(ClientError::DownstreamWs)?;
                            }
                            continue;
                        }
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
                            match handle_local_v1_frame(
                                bytes.clone(),
                                stats,
                                &ctx.metrics,
                                ctx.meta_registry.as_deref(),
                            ) {
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

fn observe_inbound(
    metrics: &FrontendMetrics,
    stats: &ClientStats,
    bytes: &Bytes,
    subprotocol: Subprotocol,
) {
    let n = bytes.len() as u64;
    metrics.record_inbound(n);
    stats.record_inbound(n);
    inspect_client_message(stats, bytes, subprotocol);
}

fn observe_outbound(metrics: &FrontendMetrics, stats: &ClientStats, bytes: &Bytes) {
    let n = bytes.len() as u64;
    metrics.record_outbound(n);
    stats.record_outbound(n);
}

/// Decode just the message tag of a frame from the client and bump the
/// matching counter. Subscribes are also captured so the dashboard can
/// list each client's active queries.
fn inspect_client_message(stats: &ClientStats, bytes: &Bytes, subprotocol: Subprotocol) {
    // CallReducer / CallProcedure have different BSATN discriminants in
    // v1 vs v2 (v1's `ClientMessage` enum orders them differently), so
    // we dispatch those via the subprotocol-aware `inbound_write_tag`.
    // OneOffQuery and Subscribe share the same tag in both protocols.
    if let Some(kind) = inbound_write_tag(bytes, subprotocol) {
        match kind {
            WriteTag::CallReducer => {
                stats
                    .call_reducers
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            WriteTag::CallProcedure => {
                stats
                    .call_procedures
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }
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
    registry: Option<&MetaRegistry>,
) -> Result<Option<Bytes>, RewriteError> {
    if should_hide_v1(&frame) {
        return Ok(None);
    }
    // Decode once; dispatch on variant.
    let body = codec::body(&frame)?;
    use spacetimedb_client_api_messages_v1::websocket as v1;
    let msg: v1::ServerMessage<v1::BsatnFormat> = spacetimedb_lib_v1::bsatn::from_slice(body)
        .map_err(|e| RewriteError::Decode(e.to_string()))?;
    match msg {
        v1::ServerMessage::TransactionUpdate(mut tu) => {
            // Already-full TU: rewrite in place if it's a relay_apply_*
            // call. (V2 local stdb rarely emits this for subscribers;
            // kept as a fallback for hosts that do send full v1 TUs.)
            if !matches!(tu.status, v1::UpdateStatus::Committed(_)) {
                return Ok(Some(frame));
            }
            if !tu.reducer_call.reducer_name.starts_with("relay_apply_") {
                return Ok(Some(frame));
            }
            let Some(meta) = rewrite::extract_upstream_meta(&tu.reducer_call.args)? else {
                return Ok(Some(frame));
            };
            apply_meta_into_v1_tu(&mut tu, meta);
            let body = spacetimedb_lib_v1::bsatn::to_vec(
                &v1::ServerMessage::<v1::BsatnFormat>::TransactionUpdate(tu),
            )
            .map_err(|e| RewriteError::Encode(e.to_string()))?;
            stats
                .rewrites
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            metrics.record_rewrite();
            Ok(Some(Bytes::from(codec::wrap_uncompressed(body))))
        }
        v1::ServerMessage::TransactionUpdateLight(tul) => {
            // V2 local stdb sends TUL on the v1 subprotocol — rows
            // only, no caller info. Look up the meta the driver
            // recorded for this CallReducer's request_id and synthesise
            // a full v1 TU.
            let Some(reg) = registry else {
                return Ok(Some(frame));
            };
            let Some(meta_opt) = reg.get(tul.request_id) else {
                // Unknown request_id (race or non-relay writer). Pass
                // through verbatim.
                return Ok(Some(frame));
            };
            let Some(meta) = meta_opt else {
                // Driver knew about this call but had no upstream meta
                // (e.g. the initial subscribe-applied apply path). Pass
                // through.
                return Ok(Some(frame));
            };
            let synth = rewrite::synthesize_v1_tu_from_tul(tul, meta);
            stats
                .rewrites
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            metrics.record_rewrite();
            Ok(Some(Bytes::from(synth)))
        }
        _ => Ok(Some(frame)),
    }
}

fn apply_meta_into_v1_tu(
    tu: &mut spacetimedb_client_api_messages_v1::websocket::TransactionUpdate<
        spacetimedb_client_api_messages_v1::websocket::BsatnFormat,
    >,
    meta: relay_protocol::UpstreamReducerMeta,
) {
    tu.caller_identity =
        spacetimedb_lib_v1::Identity::from_byte_array(meta.caller_identity.to_byte_array());
    tu.caller_connection_id = spacetimedb_lib_v1::ConnectionId::from_be_byte_array(
        meta.caller_connection_id.as_be_byte_array(),
    );
    tu.timestamp = spacetimedb_lib_v1::Timestamp::from_micros_since_unix_epoch(
        meta.timestamp.to_micros_since_unix_epoch(),
    );
    tu.reducer_call.reducer_name = meta.reducer_name.into_boxed_str();
    tu.reducer_call.args = meta.args.into_boxed_slice();
    tu.reducer_call.request_id = meta.request_id;
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

/// Synthesise an error response for a OneOffQuery client message. Returns
/// the framed bytes to send back to the client, or `None` if decoding the
/// request fails (encoding errors are also treated as None). The caller must
/// always skip forwarding the original message to local stdb regardless of
/// whether a response frame is returned.
fn reject_one_off_query(
    bytes: &Bytes,
    subprotocol: Subprotocol,
    stats: &ClientStats,
) -> Option<Vec<u8>> {
    const ERR: &str =
        "OneOffQuery is not supported through the relay frontend; subscribe to the table instead";

    let body = codec::body(bytes).ok()?;

    match subprotocol {
        Subprotocol::V2 => {
            use spacetimedb_client_api_messages::websocket::v2;
            let v2::ClientMessage::OneOffQuery(q) =
                sats_bsatn::from_slice::<v2::ClientMessage>(body).ok()?
            else {
                return None;
            };
            tracing::warn!(
                target: "relay::frontend",
                client_id = %stats.id,
                request_id = q.request_id,
                query = %q.query_string,
                "rejecting OneOffQuery — use subscriptions"
            );
            let reply = v2::ServerMessage::OneOffQueryResult(v2::OneOffQueryResult {
                request_id: q.request_id,
                result: Err(ERR.into()),
            });
            let encoded = sats_bsatn::to_vec(&reply).ok()?;
            Some(codec::wrap_uncompressed(encoded))
        }
        Subprotocol::V1 => {
            use spacetimedb_client_api_messages_v1::websocket as v1;
            let v1::ClientMessage::OneOffQuery(q) =
                spacetimedb_lib_v1::bsatn::from_slice::<v1::ClientMessage<Box<[u8]>>>(body).ok()?
            else {
                return None;
            };
            tracing::warn!(
                target: "relay::frontend",
                client_id = %stats.id,
                query = %q.query_string,
                "rejecting OneOffQuery — use subscriptions"
            );
            let reply = v1::ServerMessage::<v1::BsatnFormat>::OneOffQueryResponse(
                v1::OneOffQueryResponse {
                    message_id: q.message_id,
                    error: Some(ERR.into()),
                    tables: Box::new([]),
                    total_host_execution_duration: spacetimedb_lib_v1::TimeDuration::from_micros(0),
                },
            );
            let encoded = spacetimedb_lib_v1::bsatn::to_vec(&reply).ok()?;
            Some(codec::wrap_uncompressed(encoded))
        }
    }
}

/// Which write-path client message a frame carries, if any. The relay
/// is read-only and refuses both: `CallReducer` (which would let a
/// downstream client invoke `relay_bind_writer` or any mirror reducer)
/// and `CallProcedure` (the sibling side-effecting call).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriteTag {
    CallReducer,
    CallProcedure,
}

/// Returns the write-path kind of an inbound frame under the
/// connection's subprotocol, or `None` if it's not a CallReducer /
/// CallProcedure.
///
/// V1 and V2 assign different BSATN discriminants to these variants:
/// v2's `ClientMessage` orders them `CallReducer=3, CallProcedure=4`,
/// while v1's orders them `CallReducer=0, CallProcedure=7`. The tag
/// check must therefore be subprotocol-aware — a naive compare against
/// the v2 constants would miss every v1 reducer/procedure call.
fn inbound_write_tag(bytes: &Bytes, subprotocol: Subprotocol) -> Option<WriteTag> {
    let tag = codec::message_tag(bytes)?;
    Some(match subprotocol {
        Subprotocol::V2 => match tag {
            relay_protocol::tags::CLIENT_CALL_REDUCER => WriteTag::CallReducer,
            relay_protocol::tags::CLIENT_CALL_PROCEDURE => WriteTag::CallProcedure,
            _ => return None,
        },
        Subprotocol::V1 => match tag {
            0x00 => WriteTag::CallReducer,
            0x07 => WriteTag::CallProcedure,
            _ => return None,
        },
    })
}

/// Synthesise an error response for a CallReducer client message, mirroring
/// [`reject_one_off_query`]. Returns the framed bytes to send back to the
/// client, or `None` if decoding the request fails. The caller must always
/// skip forwarding the original message to local stdb regardless of whether
/// a response frame is returned.
fn reject_call_reducer(
    bytes: &Bytes,
    subprotocol: Subprotocol,
    stats: &ClientStats,
) -> Option<Vec<u8>> {
    const ERR: &str =
        "CallReducer is not supported through the relay frontend; the relay is read-only";

    let body = codec::body(bytes).ok()?;

    match subprotocol {
        Subprotocol::V2 => {
            use spacetimedb_client_api_messages::websocket::v2;
            let v2::ClientMessage::CallReducer(cr) =
                sats_bsatn::from_slice::<v2::ClientMessage>(body).ok()?
            else {
                return None;
            };
            tracing::warn!(
                target: "relay::frontend",
                client_id = %stats.id,
                request_id = cr.request_id,
                reducer = %cr.reducer,
                "rejecting CallReducer — the relay is read-only"
            );
            let reply = v2::ServerMessage::ReducerResult(v2::ReducerResult {
                request_id: cr.request_id,
                timestamp: relay_protocol::lib::Timestamp::UNIX_EPOCH,
                result: v2::ReducerOutcome::InternalError(ERR.into()),
            });
            let encoded = sats_bsatn::to_vec(&reply).ok()?;
            Some(codec::wrap_uncompressed(encoded))
        }
        Subprotocol::V1 => {
            use spacetimedb_client_api_messages_v1::websocket as v1;
            let v1::ClientMessage::CallReducer(cr) =
                spacetimedb_lib_v1::bsatn::from_slice::<v1::ClientMessage<Box<[u8]>>>(body).ok()?
            else {
                return None;
            };
            tracing::warn!(
                target: "relay::frontend",
                client_id = %stats.id,
                request_id = cr.request_id,
                reducer = %cr.reducer,
                "rejecting CallReducer — the relay is read-only"
            );
            let reply = v1::ServerMessage::<v1::BsatnFormat>::TransactionUpdate(
                v1::TransactionUpdate {
                    status: v1::UpdateStatus::Failed(ERR.into()),
                    timestamp: spacetimedb_lib_v1::Timestamp::UNIX_EPOCH,
                    caller_identity: spacetimedb_lib_v1::Identity::ZERO,
                    caller_connection_id: spacetimedb_lib_v1::ConnectionId::ZERO,
                    reducer_call: v1::ReducerCallInfo {
                        reducer_name: cr.reducer,
                        reducer_id: 0,
                        args: cr.args,
                        request_id: cr.request_id,
                    },
                    energy_quanta_used: spacetimedb_client_api_messages_v1::energy::EnergyQuanta::ZERO,
                    total_host_execution_duration: spacetimedb_lib_v1::TimeDuration::from_micros(0),
                },
            );
            let encoded = spacetimedb_lib_v1::bsatn::to_vec(&reply).ok()?;
            Some(codec::wrap_uncompressed(encoded))
        }
    }
}

/// Synthesise an error response for a CallProcedure client message. Same
/// contract as [`reject_call_reducer`].
fn reject_call_procedure(
    bytes: &Bytes,
    subprotocol: Subprotocol,
    stats: &ClientStats,
) -> Option<Vec<u8>> {
    const ERR: &str =
        "CallProcedure is not supported through the relay frontend; the relay is read-only";

    let body = codec::body(bytes).ok()?;

    match subprotocol {
        Subprotocol::V2 => {
            use spacetimedb_client_api_messages::websocket::v2;
            let v2::ClientMessage::CallProcedure(cp) =
                sats_bsatn::from_slice::<v2::ClientMessage>(body).ok()?
            else {
                return None;
            };
            tracing::warn!(
                target: "relay::frontend",
                client_id = %stats.id,
                request_id = cp.request_id,
                procedure = %cp.procedure,
                "rejecting CallProcedure — the relay is read-only"
            );
            let reply = v2::ServerMessage::ProcedureResult(v2::ProcedureResult {
                status: v2::ProcedureStatus::InternalError(ERR.into()),
                timestamp: relay_protocol::lib::Timestamp::UNIX_EPOCH,
                total_host_execution_duration: relay_protocol::lib::TimeDuration::from_micros(0),
                request_id: cp.request_id,
            });
            let encoded = sats_bsatn::to_vec(&reply).ok()?;
            Some(codec::wrap_uncompressed(encoded))
        }
        Subprotocol::V1 => {
            use spacetimedb_client_api_messages_v1::websocket as v1;
            let v1::ClientMessage::CallProcedure(cp) =
                spacetimedb_lib_v1::bsatn::from_slice::<v1::ClientMessage<Box<[u8]>>>(body).ok()?
            else {
                return None;
            };
            tracing::warn!(
                target: "relay::frontend",
                client_id = %stats.id,
                request_id = cp.request_id,
                procedure = %cp.procedure,
                "rejecting CallProcedure — the relay is read-only"
            );
            let reply = v1::ServerMessage::<v1::BsatnFormat>::ProcedureResult(
                v1::ProcedureResult {
                    status: v1::ProcedureStatus::InternalError(ERR.into()),
                    timestamp: spacetimedb_lib_v1::Timestamp::UNIX_EPOCH,
                    total_host_execution_duration: spacetimedb_lib_v1::TimeDuration::from_micros(0),
                    request_id: cp.request_id,
                },
            );
            let encoded = spacetimedb_lib_v1::bsatn::to_vec(&reply).ok()?;
            Some(codec::wrap_uncompressed(encoded))
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::ClientStats;

    /// Build a `ClientStats` for tests — address and id are arbitrary.
    fn fake_stats(sp: Subprotocol) -> ClientStats {
        ClientStats::new("127.0.0.1:9999".parse().unwrap(), sp)
    }

    /// Decode a v2 reply frame (strip compression byte, bsatn-decode).
    fn decode_v2_reply(frame: &[u8]) -> Option<spacetimedb_client_api_messages::websocket::v2::ServerMessage> {
        let body = codec::body(frame).ok()?;
        sats_bsatn::from_slice(body).ok()
    }

    /// Decode a v1 reply frame.
    fn decode_v1_reply(
        frame: &[u8],
    ) -> Option<
        spacetimedb_client_api_messages_v1::websocket::ServerMessage<
            spacetimedb_client_api_messages_v1::websocket::BsatnFormat,
        >,
    > {
        let body = codec::body(frame).ok()?;
        spacetimedb_lib_v1::bsatn::from_slice(body).ok()
    }

    // ---- inbound_write_tag: the V1/V2 discriminant asymmetry ----

    #[test]
    fn v2_call_reducer_tag_is_0x03() {
        use spacetimedb_client_api_messages::websocket::v2;
        let msg = v2::ClientMessage::CallReducer(v2::CallReducer {
            request_id: 42,
            flags: v2::CallReducerFlags::Default,
            reducer: "relay_bind_writer".into(),
            args: bytes::Bytes::new(),
        });
        let body = sats_bsatn::to_vec(&msg).unwrap();
        let frame = codec::wrap_uncompressed(body);
        assert_eq!(
            inbound_write_tag(&Bytes::from(frame), Subprotocol::V2),
            Some(WriteTag::CallReducer)
        );
    }

    #[test]
    fn v2_call_procedure_tag_is_0x04() {
        use spacetimedb_client_api_messages::websocket::v2;
        let msg = v2::ClientMessage::CallProcedure(v2::CallProcedure {
            request_id: 7,
            flags: v2::CallProcedureFlags::Default,
            procedure: "do_thing".into(),
            args: bytes::Bytes::new(),
        });
        let body = sats_bsatn::to_vec(&msg).unwrap();
        let frame = codec::wrap_uncompressed(body);
        assert_eq!(
            inbound_write_tag(&Bytes::from(frame), Subprotocol::V2),
            Some(WriteTag::CallProcedure)
        );
    }

    #[test]
    fn v1_call_reducer_tag_is_0x00_not_0x03() {
        // V1's ClientMessage puts CallReducer at discriminant 0, so a
        // naive v2-constant compare would miss it. This test exists to
        // fail loudly if either the wire format or the helper changes.
        use spacetimedb_client_api_messages_v1::websocket as v1;
        let msg: v1::ClientMessage<Box<[u8]>> = v1::ClientMessage::CallReducer(v1::CallReducer {
            reducer: "relay_bind_writer".into(),
            args: Box::new([]),
            request_id: 1,
            flags: v1::CallReducerFlags::FullUpdate,
        });
        let body = spacetimedb_lib_v1::bsatn::to_vec(&msg).unwrap();
        assert_eq!(body.first().copied(), Some(0x00));
        let frame = codec::wrap_uncompressed(body);
        assert_eq!(
            inbound_write_tag(&Bytes::from(frame), Subprotocol::V1),
            Some(WriteTag::CallReducer)
        );
    }

    #[test]
    fn v1_call_procedure_tag_is_0x07() {
        use spacetimedb_client_api_messages_v1::websocket as v1;
        let msg: v1::ClientMessage<Box<[u8]>> =
            v1::ClientMessage::CallProcedure(v1::CallProcedure {
                procedure: "do_thing".into(),
                args: Box::new([]),
                request_id: 1,
                flags: v1::CallProcedureFlags::Default,
            });
        let body = spacetimedb_lib_v1::bsatn::to_vec(&msg).unwrap();
        assert_eq!(body.first().copied(), Some(0x07));
        let frame = codec::wrap_uncompressed(body);
        assert_eq!(
            inbound_write_tag(&Bytes::from(frame), Subprotocol::V1),
            Some(WriteTag::CallProcedure)
        );
    }

    #[test]
    fn subscribe_is_not_a_write_tag() {
        use spacetimedb_client_api_messages::websocket::v2;
        let msg = v2::ClientMessage::Subscribe(v2::Subscribe {
            request_id: 0,
            query_set_id: v2::QuerySetId::new(0),
            query_strings: vec!["SELECT * FROM foo".into()].into(),
        });
        let body = sats_bsatn::to_vec(&msg).unwrap();
        let frame = codec::wrap_uncompressed(body);
        assert_eq!(
            inbound_write_tag(&Bytes::from(frame), Subprotocol::V2),
            None
        );
    }

    // ---- reject_call_reducer: response shape and request_id echo ----

    #[test]
    fn reject_call_reducer_v2_returns_internal_error_result() {
        use spacetimedb_client_api_messages::websocket::v2;
        let msg = v2::ClientMessage::CallReducer(v2::CallReducer {
            request_id: 1234,
            flags: v2::CallReducerFlags::Default,
            reducer: "relay_bind_writer".into(),
            args: bytes::Bytes::new(),
        });
        let body = sats_bsatn::to_vec(&msg).unwrap();
        let frame = Bytes::from(codec::wrap_uncompressed(body));
        let stats = fake_stats(Subprotocol::V2);
        let reply = reject_call_reducer(&frame, Subprotocol::V2, &stats).expect("v2 reply");

        let decoded = decode_v2_reply(&reply).expect("decodable v2 reply");
        match decoded {
            v2::ServerMessage::ReducerResult(rr) => {
                assert_eq!(rr.request_id, 1234);
                match rr.result {
                    v2::ReducerOutcome::InternalError(msg) => {
                        assert!(msg.contains("read-only"), "got: {msg}");
                    }
                    other => panic!("expected InternalError, got {other:?}"),
                }
            }
            other => panic!("expected ReducerResult, got {other:?}"),
        }
    }

    #[test]
    fn reject_call_reducer_v1_returns_failed_transaction_update() {
        use spacetimedb_client_api_messages_v1::websocket as v1;
        let msg: v1::ClientMessage<Box<[u8]>> = v1::ClientMessage::CallReducer(v1::CallReducer {
            reducer: "relay_bind_writer".into(),
            args: Box::new([9, 9, 9]),
            request_id: 5678,
            flags: v1::CallReducerFlags::FullUpdate,
        });
        let body = spacetimedb_lib_v1::bsatn::to_vec(&msg).unwrap();
        let frame = Bytes::from(codec::wrap_uncompressed(body));
        let stats = fake_stats(Subprotocol::V1);
        let reply = reject_call_reducer(&frame, Subprotocol::V1, &stats).expect("v1 reply");

        let decoded = decode_v1_reply(&reply).expect("decodable v1 reply");
        match decoded {
            v1::ServerMessage::TransactionUpdate(tu) => {
                assert_eq!(tu.reducer_call.request_id, 5678);
                assert_eq!(tu.reducer_call.reducer_name.as_ref(), "relay_bind_writer");
                // Args echoed back.
                assert_eq!(tu.reducer_call.args.as_ref(), &[9, 9, 9]);
                match tu.status {
                    v1::UpdateStatus::Failed(msg) => {
                        assert!(msg.contains("read-only"), "got: {msg}");
                    }
                    other => panic!("expected Failed, got {other:?}"),
                }
            }
            _ => panic!("expected TransactionUpdate, got a different variant"),
        }
    }

    // ---- reject_call_procedure ----

    #[test]
    fn reject_call_procedure_v2_returns_internal_error_result() {
        use spacetimedb_client_api_messages::websocket::v2;
        let msg = v2::ClientMessage::CallProcedure(v2::CallProcedure {
            request_id: 99,
            flags: v2::CallProcedureFlags::Default,
            procedure: "do_thing".into(),
            args: bytes::Bytes::new(),
        });
        let body = sats_bsatn::to_vec(&msg).unwrap();
        let frame = Bytes::from(codec::wrap_uncompressed(body));
        let stats = fake_stats(Subprotocol::V2);
        let reply = reject_call_procedure(&frame, Subprotocol::V2, &stats).expect("v2 reply");

        let decoded = decode_v2_reply(&reply).expect("decodable v2 reply");
        match decoded {
            v2::ServerMessage::ProcedureResult(pr) => {
                assert_eq!(pr.request_id, 99);
                match pr.status {
                    v2::ProcedureStatus::InternalError(msg) => {
                        assert!(msg.contains("read-only"), "got: {msg}");
                    }
                    other => panic!("expected InternalError, got {other:?}"),
                }
            }
            other => panic!("expected ProcedureResult, got {other:?}"),
        }
    }

    #[test]
    fn reject_call_procedure_v1_returns_failed_procedure_result() {
        use spacetimedb_client_api_messages_v1::websocket as v1;
        let msg: v1::ClientMessage<Box<[u8]>> =
            v1::ClientMessage::CallProcedure(v1::CallProcedure {
                procedure: "do_thing".into(),
                args: Box::new([]),
                request_id: 31337,
                flags: v1::CallProcedureFlags::Default,
            });
        let body = spacetimedb_lib_v1::bsatn::to_vec(&msg).unwrap();
        let frame = Bytes::from(codec::wrap_uncompressed(body));
        let stats = fake_stats(Subprotocol::V1);
        let reply = reject_call_procedure(&frame, Subprotocol::V1, &stats).expect("v1 reply");

        let decoded = decode_v1_reply(&reply).expect("decodable v1 reply");
        match decoded {
            v1::ServerMessage::ProcedureResult(pr) => {
                assert_eq!(pr.request_id, 31337);
                match pr.status {
                    v1::ProcedureStatus::InternalError(msg) => {
                        assert!(msg.contains("read-only"), "got: {msg}");
                    }
                    other => panic!("expected InternalError, got {other:?}"),
                }
            }
            _ => panic!("expected ProcedureResult, got a different variant"),
        }
    }

    // ---- decode-failure path: returns None, original still dropped by caller ----

    #[test]
    fn reject_call_reducer_returns_none_on_garbage() {
        let frame = Bytes::from(codec::wrap_uncompressed(vec![0xFF, 0xFF, 0xFF]));
        let stats = fake_stats(Subprotocol::V2);
        assert!(reject_call_reducer(&frame, Subprotocol::V2, &stats).is_none());
    }

    #[test]
    fn reject_call_producer_returns_none_on_wrong_variant() {
        // A Subscribe frame is not a CallProcedure; the helper must
        // return None rather than misinterpreting the bytes.
        use spacetimedb_client_api_messages::websocket::v2;
        let msg = v2::ClientMessage::Subscribe(v2::Subscribe {
            request_id: 0,
            query_set_id: v2::QuerySetId::new(0),
            query_strings: vec!["SELECT 1".into()].into(),
        });
        let body = sats_bsatn::to_vec(&msg).unwrap();
        let frame = Bytes::from(codec::wrap_uncompressed(body));
        let stats = fake_stats(Subprotocol::V2);
        assert!(reject_call_procedure(&frame, Subprotocol::V2, &stats).is_none());
    }
}
