// SPDX-License-Identifier: MIT

//! Minimal SpacetimeDB v2 WebSocket client for relay-cache.
//!
//! Adapted from `crates/relay-test-harness/src/stdb_client.rs` with
//! `CallReducer` / `encode_string_arg` removed — this binary never calls
//! reducers (architecture invariant #0). Speaks the same wire protocol as
//! `relay-upstream` and the relay frontend.

use std::time::Duration;

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
    ClientMessage, InitialConnection, ServerMessage, Subscribe, SubscribeApplied,
};
use spacetimedb_sats::bsatn;

const SUBPROTOCOL: &str = "v2.bsatn.spacetimedb";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

pub type Conn = WebSocketStream<MaybeTlsStream<TcpStream>>;

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

pub async fn recv_server_message(conn: &mut Conn) -> Result<ServerMessage> {
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
                let compression = data[0];
                if compression != 0 {
                    bail!("compression {compression} not supported");
                }
                let body = &data[1..];
                let server_msg: ServerMessage = bsatn::from_slice(body)
                    .map_err(|e| anyhow!("ServerMessage decode failed: {e}"))?;
                return Ok(server_msg);
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
    match recv_server_message(conn).await? {
        ServerMessage::InitialConnection(ic) => Ok(ic),
        other => bail!("expected InitialConnection, got {other:?}"),
    }
}

pub async fn expect_subscribe_applied(conn: &mut Conn) -> Result<SubscribeApplied> {
    loop {
        match recv_server_message(conn).await? {
            ServerMessage::SubscribeApplied(sa) => return Ok(sa),
            ServerMessage::SubscriptionError(err) => {
                bail!("subscription error: {}", err.error);
            }
            other => {
                tracing::debug!(
                    target: "relay_cache::wire",
                    ?other,
                    "ignoring frame while waiting for SubscribeApplied"
                );
            }
        }
    }
}

pub async fn send_subscribe(
    conn: &mut Conn,
    request_id: u32,
    query_set_id: u32,
    queries: Vec<String>,
) -> Result<()> {
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
    conn.send(Message::Binary(bytes)).await?;
    Ok(())
}

/// Send an unconditional WebSocket Ping to keep the idle TCP flow alive.
pub async fn send_ping(conn: &mut Conn) -> Result<()> {
    conn.send(Message::Ping(Vec::new())).await?;
    Ok(())
}
