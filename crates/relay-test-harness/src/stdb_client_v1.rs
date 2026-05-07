// SPDX-License-Identifier: MIT

//! Minimal SpacetimeDB v1 WebSocket client — used by the harness's
//! subscriber when the harness is asked to verify the relay frontend's
//! v1 rewrite path.
//!
//! Mirrors `stdb_client.rs` but speaks `v1.bsatn.spacetimedb`. We
//! deliberately keep the two side-by-side rather than parameterising
//! the v2 client — the wire types are different enough that a single
//! generic abstraction would just obscure the asserts.

use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use futures_util::{SinkExt, StreamExt};
use http::header::SEC_WEBSOCKET_PROTOCOL;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use url::Url;

use spacetimedb_client_api_messages_v1::websocket as v1;
use spacetimedb_lib_v1::bsatn as v1_bsatn;

const SUBPROTOCOL: &str = "v1.bsatn.spacetimedb";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

pub type Conn = WebSocketStream<MaybeTlsStream<TcpStream>>;
pub type ServerMessage = v1::ServerMessage<v1::BsatnFormat>;

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

    let ws_config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
        max_message_size: None,
        max_frame_size: None,
        ..Default::default()
    };
    let (stream, resp) = tokio::time::timeout(
        CONNECT_TIMEOUT,
        tokio_tungstenite::connect_async_with_config(request, Some(ws_config), false),
    )
    .await
    .map_err(|_| anyhow!("connect timeout"))??;
    if let Some(got) = resp.headers().get(SEC_WEBSOCKET_PROTOCOL) {
        let got = got.to_str().unwrap_or("");
        if got != SUBPROTOCOL {
            bail!("server negotiated `{got}`, wanted `{SUBPROTOCOL}` — does the proxy/local stdb support v1?");
        }
    } else {
        bail!("server did not echo a subprotocol; refusing v1 connection");
    }
    Ok(stream)
}

pub async fn recv_server_message(conn: &mut Conn) -> Result<ServerMessage> {
    loop {
        let Some(msg) = conn.next().await else {
            bail!("server closed before sending a message");
        };
        let msg = msg?;
        match msg {
            Message::Binary(data) => {
                if data.is_empty() {
                    bail!("empty binary frame");
                }
                let compression = data[0];
                if compression != 0 {
                    bail!("compression {compression} not supported in harness");
                }
                let body = &data[1..];
                let server_msg: ServerMessage = v1_bsatn::from_slice(body)
                    .map_err(|e| anyhow!("v1 ServerMessage decode failed: {e}"))?;
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

pub async fn expect_identity_token(conn: &mut Conn) -> Result<v1::IdentityToken> {
    match recv_server_message(conn).await? {
        v1::ServerMessage::IdentityToken(it) => Ok(it),
        other => bail!(
            "expected IdentityToken, got {}",
            super::v1_variant_name(&other)
        ),
    }
}

pub async fn send_subscribe(conn: &mut Conn, request_id: u32, queries: Vec<String>) -> Result<()> {
    let msg = v1::ClientMessage::<Box<[u8]>>::Subscribe(v1::Subscribe {
        query_strings: queries
            .into_iter()
            .map(|s| s.into_boxed_str())
            .collect::<Vec<_>>()
            .into_boxed_slice(),
        request_id,
    });
    let bytes = v1_bsatn::to_vec(&msg).map_err(|e| anyhow!("encode v1 Subscribe: {e}"))?;
    conn.send(Message::Binary(bytes)).await?;
    Ok(())
}
