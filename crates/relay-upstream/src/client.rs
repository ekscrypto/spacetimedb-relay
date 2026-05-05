// SPDX-License-Identifier: MIT

use std::time::Duration;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http::header::{AUTHORIZATION, SEC_WEBSOCKET_PROTOCOL};
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::Message;
use tracing::{debug, info, warn};
use url::Url;

use relay_protocol::api_messages::websocket::common::QuerySetId;
use relay_protocol::api_messages::websocket::v2::{ClientMessage, ServerMessage, Subscribe};
use relay_protocol::sats::bsatn;
use relay_protocol::tags;

use crate::v1_compat;

const SUBPROTOCOL_V2: &str = "v2.bsatn.spacetimedb";
const SUBPROTOCOL_V1: &str = "v1.bsatn.spacetimedb";

/// Which SpacetimeDB WebSocket subprotocol the upstream speaks.
///
/// `V2` is the current stable. `V1` is for older deployments still on
/// `v1.bsatn.spacetimedb` (pre-2.0 SpacetimeDB releases). When `V1`,
/// the upstream client decodes v1 wire types and translates them into
/// the v2 shape the rest of the relay uses internally.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProtocolVersion {
    V1,
    #[default]
    V2,
}

impl ProtocolVersion {
    fn subprotocol(self) -> &'static str {
        match self {
            ProtocolVersion::V1 => SUBPROTOCOL_V1,
            ProtocolVersion::V2 => SUBPROTOCOL_V2,
        }
    }
}

impl std::fmt::Display for ProtocolVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            ProtocolVersion::V1 => "v1",
            ProtocolVersion::V2 => "v2",
        })
    }
}

impl std::str::FromStr for ProtocolVersion {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "v1" | "V1" | "1" => Ok(ProtocolVersion::V1),
            "v2" | "V2" | "2" => Ok(ProtocolVersion::V2),
            other => Err(format!(
                "unknown upstream protocol `{other}` (expected v1 or v2)"
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    None,
    Brotli,
    Gzip,
}

impl Compression {
    fn query_value(self) -> &'static str {
        match self {
            Compression::None => "None",
            Compression::Brotli => "Brotli",
            Compression::Gzip => "Gzip",
        }
    }

    fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0 => Some(Compression::None),
            1 => Some(Compression::Brotli),
            2 => Some(Compression::Gzip),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct UpstreamConfig {
    pub host: Url,
    pub database: String,
    pub auth_token: Option<String>,
    pub compression: Compression,
    pub connect_timeout: Duration,
    pub protocol: ProtocolVersion,
}

/// One decoded WebSocket frame from the upstream.
///
/// `bsatn` is the BSATN-encoded `ServerMessage` after stripping the
/// outer compression byte and decompressing if needed. Its first byte
/// is the sum discriminant; for `protocol = V2` the discriminant is
/// the v2 `ServerMessage` tag, for `V1` it is the v1 tag (which differs
/// — `decode()` handles the translation).
#[derive(Debug, Clone)]
pub struct UpstreamFrame {
    pub bsatn: Bytes,
    pub protocol: ProtocolVersion,
}

impl UpstreamFrame {
    pub fn server_tag(&self) -> u8 {
        self.bsatn.first().copied().unwrap_or(0xff)
    }

    pub fn decode(&self) -> Result<ServerMessage, UpstreamError> {
        match self.protocol {
            ProtocolVersion::V2 => bsatn::from_slice::<ServerMessage>(&self.bsatn)
                .map_err(|e| UpstreamError::Decode(e.to_string())),
            ProtocolVersion::V1 => v1_compat::decode_and_translate(&self.bsatn),
        }
    }
}

#[derive(Debug)]
pub enum UpstreamEvent {
    Connected,
    Frame(UpstreamFrame),
    /// Inbound WS Ping or Pong (keep-alive). Tungstenite still answers
    /// Pings automatically; we only surface them so the relay can
    /// expose "last keep-alive" on the dashboard.
    Ping,
    Disconnected {
        reason: String,
    },
}

#[derive(Debug)]
pub enum UpstreamCommand {
    Subscribe {
        request_id: u32,
        query_set_id: u32,
        queries: Vec<String>,
    },
    Shutdown,
}

#[derive(Debug, Error)]
pub enum UpstreamError {
    #[error("invalid upstream url: {0}")]
    Url(String),
    #[error("connection failed: {0}")]
    Connect(String),
    #[error("websocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("event channel closed")]
    EventChannelClosed,
    #[error("compression {0:?} not yet supported")]
    UnsupportedCompression(Compression),
    #[error("unknown compression tag {0}")]
    UnknownCompression(u8),
    #[error("frame too short ({0} bytes)")]
    FrameTooShort(usize),
    #[error("BSATN decode failed: {0}")]
    Decode(String),
    #[error("BSATN encode failed: {0}")]
    Encode(String),
}

pub fn server_tag_name(tag: u8, protocol: ProtocolVersion) -> &'static str {
    match protocol {
        ProtocolVersion::V2 => match tag {
            tags::SERVER_INITIAL_CONNECTION => "InitialConnection",
            tags::SERVER_SUBSCRIBE_APPLIED => "SubscribeApplied",
            tags::SERVER_UNSUBSCRIBE_APPLIED => "UnsubscribeApplied",
            tags::SERVER_SUBSCRIPTION_ERROR => "SubscriptionError",
            tags::SERVER_TRANSACTION_UPDATE => "TransactionUpdate",
            tags::SERVER_ONE_OFF_QUERY_RESULT => "OneOffQueryResult",
            tags::SERVER_REDUCER_RESULT => "ReducerResult",
            tags::SERVER_PROCEDURE_RESULT => "ProcedureResult",
            _ => "Unknown",
        },
        ProtocolVersion::V1 => match tag {
            0 => "InitialSubscription",
            1 => "TransactionUpdate",
            2 => "TransactionUpdateLight",
            3 => "IdentityToken",
            4 => "OneOffQueryResponse",
            5 => "SubscribeApplied",
            6 => "UnsubscribeApplied",
            7 => "SubscriptionError",
            8 => "SubscribeMultiApplied",
            9 => "UnsubscribeMultiApplied",
            10 => "ProcedureResult",
            _ => "Unknown",
        },
    }
}

/// Open the upstream WebSocket and forward decoded frames.
///
/// Reads from `commands_rx` to send `ClientMessage`s (Subscribe etc.)
/// while concurrently streaming `ServerMessage` frames out via
/// `events_tx`. Returns once the connection ends, the command channel
/// closes, or a fatal error occurs.
pub async fn connect_and_run(
    config: UpstreamConfig,
    events_tx: mpsc::Sender<UpstreamEvent>,
    mut commands_rx: mpsc::Receiver<UpstreamCommand>,
) -> Result<(), UpstreamError> {
    let request = build_connect_request(&config)?;
    info!(
        target: "relay::upstream",
        url = %request.uri(),
        compression = ?config.compression,
        "connecting to upstream"
    );

    // SpacetimeDB v1's `InitialSubscription` carries the entire snapshot
    // for every subscribed table in a single frame. On large databases
    // (e.g. BitCraft's `bitcraft-live-{N}` modules with 250 public-user
    // tables and tens of thousands of rows) that one frame can exceed
    // tungstenite's 64 MiB default and abort the connection. Disable
    // the cap — operators control memory pressure by choosing which
    // tables to subscribe to via `--subscribe-table`. The server side
    // has no chunking story for v1 InitialSubscription.
    let ws_config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
        max_message_size: None,
        max_frame_size: None,
        ..Default::default()
    };
    let connect_fut =
        tokio_tungstenite::connect_async_with_config(request, Some(ws_config), false);
    let (ws_stream, response) = tokio::time::timeout(config.connect_timeout, connect_fut)
        .await
        .map_err(|_| UpstreamError::Connect("connect timeout".into()))?
        .map_err(|e| UpstreamError::Connect(e.to_string()))?;

    info!(
        target: "relay::upstream",
        status = %response.status(),
        "upstream websocket established"
    );

    events_tx
        .send(UpstreamEvent::Connected)
        .await
        .map_err(|_| UpstreamError::EventChannelClosed)?;

    let (mut writer, mut reader) = ws_stream.split();

    loop {
        tokio::select! {
            biased;
            cmd = commands_rx.recv() => {
                match cmd {
                    Some(UpstreamCommand::Subscribe { request_id, query_set_id, queries }) => {
                        let frame = match config.protocol {
                            ProtocolVersion::V2 => {
                                let msg = ClientMessage::Subscribe(Subscribe {
                                    request_id,
                                    query_set_id: QuerySetId::new(query_set_id),
                                    query_strings: queries
                                        .iter()
                                        .map(|s| s.clone().into_boxed_str())
                                        .collect::<Vec<_>>()
                                        .into_boxed_slice(),
                                });
                                bsatn::to_vec(&msg)
                                    .map_err(|e| UpstreamError::Encode(e.to_string()))?
                            }
                            ProtocolVersion::V1 => v1_compat::encode_subscribe(request_id, &queries)?,
                        };
                        debug!(
                            target: "relay::upstream",
                            protocol = %config.protocol,
                            request_id, query_set_id, n_queries = queries.len(),
                            frame_len = frame.len(),
                            "sending Subscribe"
                        );
                        writer.send(Message::Binary(frame)).await?;
                    }
                    Some(UpstreamCommand::Shutdown) | None => {
                        let _ = writer.send(Message::Close(None)).await;
                        let _ = events_tx
                            .send(UpstreamEvent::Disconnected { reason: "shutdown".into() })
                            .await;
                        return Ok(());
                    }
                }
            }
            msg = reader.next() => {
                let Some(msg) = msg else { break };
                match msg? {
                    Message::Binary(data) => {
                        match decode_frame(&data, config.compression, config.protocol) {
                            Ok(frame) => {
                                debug!(
                                    target: "relay::upstream",
                                    tag = frame.server_tag(),
                                    kind = server_tag_name(frame.server_tag(), frame.protocol),
                                    bsatn_len = frame.bsatn.len(),
                                    "frame"
                                );
                                if events_tx.send(UpstreamEvent::Frame(frame)).await.is_err() {
                                    return Err(UpstreamError::EventChannelClosed);
                                }
                            }
                            Err(e) => {
                                warn!(target: "relay::upstream", error = %e, "frame decode error");
                            }
                        }
                    }
                    Message::Text(t) => {
                        warn!(target: "relay::upstream", "unexpected text frame: {} bytes", t.len());
                    }
                    Message::Close(frame) => {
                        let reason = frame
                            .map(|f| format!("{}: {}", f.code, f.reason))
                            .unwrap_or_else(|| "no close frame".to_string());
                        let _ = events_tx
                            .send(UpstreamEvent::Disconnected { reason: reason.clone() })
                            .await;
                        info!(target: "relay::upstream", %reason, "upstream closed");
                        return Ok(());
                    }
                    Message::Ping(_) | Message::Pong(_) => {
                        let _ = events_tx.send(UpstreamEvent::Ping).await;
                    }
                    Message::Frame(_) => {}
                }
            }
        }
    }

    let _ = events_tx
        .send(UpstreamEvent::Disconnected {
            reason: "stream ended".into(),
        })
        .await;
    Ok(())
}

fn build_connect_request(
    config: &UpstreamConfig,
) -> Result<tokio_tungstenite::tungstenite::handshake::client::Request, UpstreamError> {
    let mut url = config.host.clone();
    match url.scheme() {
        "ws" | "wss" => {}
        "http" => url
            .set_scheme("ws")
            .map_err(|_| UpstreamError::Url("scheme rewrite failed".into()))?,
        "https" => url
            .set_scheme("wss")
            .map_err(|_| UpstreamError::Url("scheme rewrite failed".into()))?,
        other => return Err(UpstreamError::Url(format!("unsupported scheme: {other}"))),
    }
    {
        let mut path = url.path().trim_end_matches('/').to_string();
        path.push_str("/v1/database/");
        path.push_str(&config.database);
        path.push_str("/subscribe");
        url.set_path(&path);
    }
    url.query_pairs_mut()
        .clear()
        .append_pair("compression", config.compression.query_value());

    let mut request = url
        .to_string()
        .into_client_request()
        .map_err(|e| UpstreamError::Url(e.to_string()))?;
    request.headers_mut().insert(
        SEC_WEBSOCKET_PROTOCOL,
        config
            .protocol
            .subprotocol()
            .parse()
            .map_err(|_| UpstreamError::Url("invalid subprotocol header".into()))?,
    );
    if let Some(token) = &config.auth_token {
        let value = format!("Bearer {token}");
        request.headers_mut().insert(
            AUTHORIZATION,
            value
                .parse()
                .map_err(|_| UpstreamError::Url("invalid auth header".into()))?,
        );
    }
    Ok(request)
}

fn decode_frame(
    data: &[u8],
    expected: Compression,
    protocol: ProtocolVersion,
) -> Result<UpstreamFrame, UpstreamError> {
    if data.is_empty() {
        return Err(UpstreamError::FrameTooShort(0));
    }
    let compression =
        Compression::from_tag(data[0]).ok_or(UpstreamError::UnknownCompression(data[0]))?;
    if compression != Compression::None {
        if compression != expected {
            warn!(
                target: "relay::upstream",
                got = ?compression, want = ?expected,
                "compression mismatch (server compressed despite our request)"
            );
        }
        return Err(UpstreamError::UnsupportedCompression(compression));
    }
    if data.len() < 2 {
        return Err(UpstreamError::FrameTooShort(data.len()));
    }
    Ok(UpstreamFrame {
        bsatn: Bytes::copy_from_slice(&data[1..]),
        protocol,
    })
}
