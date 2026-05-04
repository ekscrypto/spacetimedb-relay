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

const SUBPROTOCOL: &str = "v2.bsatn.spacetimedb";

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
}

/// One decoded WebSocket frame from the upstream.
///
/// `bsatn` is the BSATN-encoded `ServerMessage` after stripping the
/// outer compression byte and decompressing if needed. Its first byte
/// is the sum discriminant, so `from_slice::<ServerMessage>(&bsatn)`
/// is the canonical way to consume it.
#[derive(Debug, Clone)]
pub struct UpstreamFrame {
    pub bsatn: Bytes,
}

impl UpstreamFrame {
    pub fn server_tag(&self) -> u8 {
        self.bsatn.first().copied().unwrap_or(0xff)
    }

    pub fn decode(&self) -> Result<ServerMessage, UpstreamError> {
        bsatn::from_slice::<ServerMessage>(&self.bsatn)
            .map_err(|e| UpstreamError::Decode(e.to_string()))
    }
}

#[derive(Debug)]
pub enum UpstreamEvent {
    Connected,
    Frame(UpstreamFrame),
    Disconnected { reason: String },
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

pub fn server_tag_name(tag: u8) -> &'static str {
    match tag {
        tags::SERVER_INITIAL_CONNECTION => "InitialConnection",
        tags::SERVER_SUBSCRIBE_APPLIED => "SubscribeApplied",
        tags::SERVER_UNSUBSCRIBE_APPLIED => "UnsubscribeApplied",
        tags::SERVER_SUBSCRIPTION_ERROR => "SubscriptionError",
        tags::SERVER_TRANSACTION_UPDATE => "TransactionUpdate",
        tags::SERVER_ONE_OFF_QUERY_RESULT => "OneOffQueryResult",
        tags::SERVER_REDUCER_RESULT => "ReducerResult",
        tags::SERVER_PROCEDURE_RESULT => "ProcedureResult",
        _ => "Unknown",
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

    let connect_fut = tokio_tungstenite::connect_async(request);
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
                        let msg = ClientMessage::Subscribe(Subscribe {
                            request_id,
                            query_set_id: QuerySetId::new(query_set_id),
                            query_strings: queries
                                .iter()
                                .map(|s| s.clone().into_boxed_str())
                                .collect::<Vec<_>>()
                                .into_boxed_slice(),
                        });
                        let frame = bsatn::to_vec(&msg)
                            .map_err(|e| UpstreamError::Encode(e.to_string()))?;
                        debug!(
                            target: "relay::upstream",
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
                        match decode_frame(&data, config.compression) {
                            Ok(frame) => {
                                debug!(
                                    target: "relay::upstream",
                                    tag = frame.server_tag(),
                                    kind = server_tag_name(frame.server_tag()),
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
                    Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {}
                }
            }
        }
    }

    let _ = events_tx
        .send(UpstreamEvent::Disconnected { reason: "stream ended".into() })
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
        SUBPROTOCOL
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

fn decode_frame(data: &[u8], expected: Compression) -> Result<UpstreamFrame, UpstreamError> {
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
    })
}
