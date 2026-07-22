// SPDX-License-Identifier: MIT

//! Minimal SpacetimeDB v2 WebSocket client for relay-cache.
//!
//! Adapted from `crates/relay-test-harness/src/stdb_client.rs` with
//! `CallReducer` / `encode_string_arg` removed — this binary never calls
//! reducers (architecture invariant #0). Speaks the same wire protocol as
//! `relay-upstream` and the relay frontend.

use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};
use futures_util::{SinkExt, StreamExt};
use http::header::SEC_WEBSOCKET_PROTOCOL;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use url::Url;

use spacetimedb_client_api_messages::websocket::common::QuerySetId;
use spacetimedb_client_api_messages::websocket::v2::{
    ClientMessage, InitialConnection, ServerMessage, Subscribe, SubscribeApplied, TransactionUpdate,
};
use spacetimedb_sats::bsatn;

const SUBPROTOCOL: &str = "v2.bsatn.spacetimedb";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// How often `--debug` logs "still waiting" while a SubscribeApplied
/// (often a multi-hundred-MB fragmented Binary) is still assembling.
const WAIT_HEARTBEAT: Duration = Duration::from_secs(5);

pub type Conn = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Outcome of a completed server Binary frame.
pub struct RecvFrame {
    pub message: ServerMessage,
    /// Full WS Binary payload length (compression byte + BSATN body).
    pub wire_bytes: usize,
}

pub async fn open_connection(host: &Url, database: &str) -> Result<Conn> {
    let mut url = host.clone();
    match url.scheme() {
        "ws" | "wss" => {}
        "http" => url
            .set_scheme("ws")
            .map_err(|_| anyhow!("scheme rewrite failed"))?,
        "https" => url
            .set_scheme("wss")
            .map_err(|_| anyhow!("scheme rewrite failed"))?,
        other => bail!("unsupported scheme: {other}"),
    }
    {
        let mut path = url.path().trim_end_matches('/').to_string();
        path.push_str("/v1/database/");
        path.push_str(database);
        path.push_str("/subscribe");
        url.set_path(&path);
    }
    url.query_pairs_mut()
        .clear()
        .append_pair("compression", "None");

    let mut request = url.to_string().into_client_request()?;
    request
        .headers_mut()
        .insert(SEC_WEBSOCKET_PROTOCOL, SUBPROTOCOL.parse()?);

    // Busy region inventories can exceed tungstenite's 64 MiB default.
    let ws_config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
        max_message_size: None,
        max_frame_size: None,
        ..Default::default()
    };
    let (stream, _resp) = tokio::time::timeout(
        CONNECT_TIMEOUT,
        tokio_tungstenite::connect_async_with_config(request, Some(ws_config), false),
    )
    .await
    .map_err(|_| anyhow!("connect timeout"))??;
    Ok(stream)
}

pub async fn recv_server_message(conn: &mut Conn) -> Result<RecvFrame> {
    loop {
        let Some(msg) = conn.next().await else {
            bail!("upstream closed before sending a server message");
        };
        let msg = msg?;
        match msg {
            Message::Binary(data) => {
                if data.is_empty() {
                    bail!("empty binary frame");
                }
                let wire_bytes = data.len();
                let compression = data[0];
                if compression != 0 {
                    bail!("compression {compression} not supported");
                }
                let body = &data[1..];
                let decode_started = Instant::now();
                let server_msg: ServerMessage = bsatn::from_slice(body)
                    .map_err(|e| anyhow!("ServerMessage decode failed: {e}"))?;
                tracing::debug!(
                    target: "relay_cache::wire",
                    wire_bytes,
                    body_bytes = body.len(),
                    decode_ms = decode_started.elapsed().as_millis() as u64,
                    "decoded ServerMessage"
                );
                return Ok(RecvFrame {
                    message: server_msg,
                    wire_bytes,
                });
            }
            Message::Ping(_) | Message::Pong(_) => continue,
            Message::Close(frame) => {
                bail!("server closed: {frame:?}");
            }
            Message::Text(t) => {
                bail!("unexpected text frame: {t}");
            }
            Message::Frame(_) => continue,
        }
    }
}

pub async fn expect_initial_connection(conn: &mut Conn) -> Result<InitialConnection> {
    match recv_server_message(conn).await?.message {
        ServerMessage::InitialConnection(ic) => Ok(ic),
        other => bail!("expected InitialConnection, got {other:?}"),
    }
}

/// Block until `SubscribeApplied`. When `debug_mode`, emit a 5s heartbeat
/// (and a WS Ping). `TransactionUpdate`s during the wait are forwarded to
/// `on_update` so sequential bootstrap can stay consistent.
pub async fn expect_subscribe_applied(
    conn: &mut Conn,
    region: u32,
    phase: &str,
    debug_mode: bool,
    mut on_update: impl FnMut(&TransactionUpdate) -> Result<()>,
) -> Result<(SubscribeApplied, usize)> {
    let started = Instant::now();
    let mut heartbeat = tokio::time::interval(WAIT_HEARTBEAT);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Skip the immediate first tick so we don't log before any wait.
    heartbeat.tick().await;

    loop {
        tokio::select! {
            frame = recv_server_message(conn) => {
                let frame = frame?;
                match frame.message {
                    ServerMessage::SubscribeApplied(sa) => {
                        let n_tables = sa.rows.tables.len();
                        tracing::info!(
                            target: "relay_cache::wire",
                            region,
                            phase,
                            wire_bytes = frame.wire_bytes,
                            n_tables,
                            wait_ms = started.elapsed().as_millis() as u64,
                            "SubscribeApplied received"
                        );
                        return Ok((sa, frame.wire_bytes));
                    }
                    ServerMessage::TransactionUpdate(tu) => {
                        on_update(&tu)?;
                    }
                    ServerMessage::SubscriptionError(err) => {
                        bail!(
                            "subscription error (region={region} phase={phase}): {}",
                            err.error
                        );
                    }
                    other => {
                        tracing::debug!(
                            target: "relay_cache::wire",
                            region,
                            phase,
                            wire_bytes = frame.wire_bytes,
                            ?other,
                            "ignoring frame while waiting for SubscribeApplied"
                        );
                    }
                }
            }
            _ = heartbeat.tick(), if debug_mode => {
                tracing::info!(
                    target: "relay_cache::wire",
                    region,
                    phase,
                    elapsed_secs = started.elapsed().as_secs(),
                    "still waiting for SubscribeApplied (WS message may still be assembling)"
                );
                if let Err(e) = send_ping(conn).await {
                    tracing::warn!(
                        target: "relay_cache::wire",
                        region,
                        phase,
                        error = %e,
                        "debug wait ping failed"
                    );
                    return Err(e);
                }
            }
        }
    }
}

/// Additive multi-query `Subscribe` (v2). Each call should use a fresh
/// `query_set_id` — reusing an id replaces that set; a new id appends.
pub async fn send_subscribe(
    conn: &mut Conn,
    request_id: u32,
    query_set_id: u32,
    queries: Vec<String>,
    region: u32,
    phase: &str,
) -> Result<()> {
    let n_queries = queries.len();
    let msg = ClientMessage::Subscribe(Subscribe {
        request_id,
        query_set_id: QuerySetId::new(query_set_id),
        query_strings: queries
            .into_iter()
            .map(|s| s.into_boxed_str())
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    });
    let bytes = bsatn::to_vec(&msg).map_err(|e| anyhow!("encode Subscribe: {e}"))?;
    tracing::info!(
        target: "relay_cache::wire",
        region,
        phase,
        request_id,
        query_set_id,
        n_queries,
        subscribe_wire_bytes = bytes.len(),
        "sending Subscribe"
    );
    conn.send(Message::Binary(bytes)).await?;
    Ok(())
}

/// Send an unconditional WebSocket Ping to keep the idle TCP flow alive.
pub async fn send_ping(conn: &mut Conn) -> Result<()> {
    conn.send(Message::Ping(Vec::new())).await?;
    Ok(())
}
