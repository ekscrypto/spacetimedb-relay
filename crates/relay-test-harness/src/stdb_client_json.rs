// SPDX-License-Identifier: MIT

//! Minimal SpacetimeDB v1 **JSON** WebSocket client — the text-frame
//! analogue of [`stdb_client_v1`]. Used by the harness's
//! `--check-integrity` mode to confirm the relay's `v1.json.spacetimedb`
//! path end-to-end (nginx TLS → relay frontend → local stdb, with JSON
//! frames serviced by stdb directly).
//!
//! Same connection shape as the v1 BSATN client, but:
//!   * negotiates `v1.json.spacetimedb`,
//!   * sends/receives WS **text** frames carrying JSON, and
//!   * (de)serialises via the SDK's own sats↔serde bridge
//!     (`SerdeWrapper` + `serde_json`) — the same path the relay's
//!     `reject_json_*` helpers use — so the wire bytes match what
//!     SpacetimeDB itself emits.

use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use futures_util::{SinkExt, StreamExt};
use http::header::SEC_WEBSOCKET_PROTOCOL;
use spacetimedb_client_api_messages_v1::websocket as v1;
use spacetimedb_lib_v1::sats::serde::SerdeWrapper;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use url::Url;

const SUBPROTOCOL: &str = "v1.json.spacetimedb";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

pub type Conn = WebSocketStream<MaybeTlsStream<TcpStream>>;
pub type ServerMessage = v1::ServerMessage<v1::JsonFormat>;

/// The args type for `v1::ClientMessage<JsonFormat>` —
/// `<JsonFormat as WebsocketFormat>::Single` (a `ByteString` carrying
/// the raw JSON value the client supplied). Spelled via the trait alias
/// so we don't take a direct dependency on the `bytestring` crate.
type JsonSingle = <v1::JsonFormat as v1::WebsocketFormat>::Single;

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
            bail!(
                "server negotiated `{got}`, wanted `{SUBPROTOCOL}` — \
                 does the proxy/local stdb support v1.json?"
            );
        }
    } else {
        bail!("server did not echo a subprotocol; refusing v1.json connection");
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
            Message::Text(t) => {
                let wrap: SerdeWrapper<ServerMessage> = serde_json::from_str(&t)
                    .map_err(|e| anyhow!("v1.json ServerMessage decode failed: {e}"))?;
                return Ok(wrap.0);
            }
            Message::Ping(_) | Message::Pong(_) => continue,
            Message::Close(frame) => {
                bail!("server closed: {frame:?}");
            }
            Message::Binary(b) => {
                bail!("unexpected binary frame on v1.json connection ({}B)", b.len());
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
            super::v1_variant_name_any(&other)
        ),
    }
}

pub async fn send_subscribe(conn: &mut Conn, request_id: u32, queries: Vec<String>) -> Result<()> {
    let msg = v1::ClientMessage::<JsonSingle>::Subscribe(v1::Subscribe {
        query_strings: queries
            .into_iter()
            .map(|s| s.into_boxed_str())
            .collect::<Vec<_>>()
            .into_boxed_slice(),
        request_id,
    });
    let text = serde_json::to_string(&SerdeWrapper::new(msg))
        .map_err(|e| anyhow!("encode v1.json Subscribe: {e}"))?;
    conn.send(Message::Text(text)).await?;
    Ok(())
}
