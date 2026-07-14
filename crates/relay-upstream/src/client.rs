// SPDX-License-Identifier: MIT

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
use relay_protocol::UpstreamReducerMeta;

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

    /// Decode the frame's `ServerMessage` plus any upstream reducer
    /// provenance recovered alongside it. Provenance is currently only
    /// present for v1 `TransactionUpdate(Committed)` (v2's
    /// `TransactionUpdate` doesn't carry caller info on the wire).
    pub fn decode(&self) -> Result<(ServerMessage, Option<UpstreamReducerMeta>), UpstreamError> {
        match self.protocol {
            ProtocolVersion::V2 => bsatn::from_slice::<ServerMessage>(&self.bsatn)
                .map(|m| (m, None))
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
    /// Add a single query to the active subscription set, additively.
    /// Wire format: v1 `SubscribeMulti` for `protocol = V1`. Each call
    /// adds one query identified by `query_id`; the server responds
    /// with `SubscribeMultiApplied` (translated to v2 `SubscribeApplied`
    /// in `v1_compat`) carrying just that query's initial rows. v2 has
    /// a similar additive `SubscribeMulti` shape, but we currently
    /// only encode the v1 form since that's the only path that needs
    /// it (BitCraft v1).
    SubscribeOne {
        request_id: u32,
        query_id: u32,
        query: String,
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
    #[error("liveness probe timed out — upstream did not respond to OneOffQuery within {0}s")]
    ProbeTimeout(u64),
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
    let connect_fut = tokio_tungstenite::connect_async_with_config(request, Some(ws_config), false);
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

    // Mirror SpacetimeDB Rust SDK pattern (sdks/rust/src/websocket.rs):
    // keep the WebSocketStream un-split. Splitting via futures::split
    // creates a BiLock between the read and write halves, which means
    // tungstenite's auto-Pong replies (queued during read polls)
    // never get flushed until the write half is independently polled.
    // For multi-hundred-MB fragmented messages from BitCraft this can
    // never happen — the read poll monopolises the future for the
    // duration of message reassembly, by which point the upstream's
    // ~30 s ping-timeout fires and resets the connection. With an
    // un-split socket every read poll also flushes the write buffer
    // as a side-effect inside tungstenite, so auto-Pongs get out
    // even mid-1GB-message.
    let mut sock = ws_stream;

    // Watchdog state. The outer select loop bumps `iter_count` every
    // iteration; reader/writer paths bump their respective counters
    // and stamp `last_event_ms`. A spawned watchdog task logs deltas
    // every 2 s so we can tell — without attaching a debugger —
    // whether the upstream task is making progress, parked on a
    // future, or wedged inside tungstenite mid-message.
    let iter_count = Arc::new(AtomicU64::new(0));
    let frame_count = Arc::new(AtomicU64::new(0));
    let cmd_processed = Arc::new(AtomicU64::new(0));
    let last_event_ms = Arc::new(AtomicU64::new(now_ms()));
    let watchdog_handle = {
        let iter_count = iter_count.clone();
        let frame_count = frame_count.clone();
        let cmd_processed = cmd_processed.clone();
        let last_event_ms = last_event_ms.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(2));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut prev_iter = 0u64;
            let mut prev_frames = 0u64;
            loop {
                interval.tick().await;
                let cur_iter = iter_count.load(Ordering::Relaxed);
                let cur_frames = frame_count.load(Ordering::Relaxed);
                let cur_cmds = cmd_processed.load(Ordering::Relaxed);
                let last_ms = last_event_ms.load(Ordering::Relaxed);
                let silence_ms = now_ms().saturating_sub(last_ms);
                info!(
                    target: "relay::upstream::watchdog",
                    iter_delta = cur_iter - prev_iter,
                    iter_total = cur_iter,
                    frame_delta = cur_frames - prev_frames,
                    frame_total = cur_frames,
                    cmds_processed = cur_cmds,
                    silence_ms,
                    "upstream task heartbeat"
                );
                prev_iter = cur_iter;
                prev_frames = cur_frames;
            }
        })
    };

    // Unconditional client Ping every 10 s. We tested SDK-style "only
    // ping when idle for 30 s" and it never fires during a multi-100MB
    // InitialSubscription burst (idle stays false the whole time), yet
    // BitCraft's path RSTs the connection at ~90 s anyway — almost
    // certainly a NAT/load-balancer dropping a half-idle TCP flow with
    // no outbound traffic. Sending an unconditional Ping every 10 s
    // keeps something flowing outbound to satisfy the middlebox, with
    // RTT well under any reasonable middlebox idle threshold.
    const PING_INTERVAL: Duration = Duration::from_secs(10);
    let mut ping_interval =
        tokio::time::interval_at(tokio::time::Instant::now() + PING_INTERVAL, PING_INTERVAL);
    ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // --- Liveness probe ---
    //
    // WS-level Pings/Pongs only prove the TCP flow (or a proxy in
    // between) is alive — they don't prove the upstream *application*
    // is processing requests. Every PROBE_INTERVAL we send a v1
    // OneOffQuery ("SELECT 1"); if we don't receive any frame within
    // PROBE_TIMEOUT seconds, the connection is presumed dead and we
    // force a reconnect. This catches the "up but silent" failure mode
    // where the WebSocket stays open but the game server behind it has
    // stopped sending data.
    const PROBE_INTERVAL: Duration = Duration::from_secs(60);
    const PROBE_TIMEOUT: Duration = Duration::from_secs(30);
    const PROBE_MESSAGE_ID: &[u8] = b"RELAY_PROBE";
    // v1 server tag for OneOffQueryResponse is 4, v2 is 5.
    const V1_TAG_ONE_OFF_QUERY_RESPONSE: u8 = 4;
    const V2_TAG_ONE_OFF_QUERY_RESULT: u8 = 5;
    let probe_response_tag = match config.protocol {
        ProtocolVersion::V1 => V1_TAG_ONE_OFF_QUERY_RESPONSE,
        ProtocolVersion::V2 => V2_TAG_ONE_OFF_QUERY_RESULT,
    };
    let mut probe_interval =
        tokio::time::interval_at(tokio::time::Instant::now() + PROBE_INTERVAL, PROBE_INTERVAL);
    probe_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // probe_answered = true means no probe is in-flight (either we
    // haven't sent the first one yet, or the last one was answered).
    // false means we sent a probe and are awaiting a response.
    let mut probe_answered = true;
    let mut probe_sent_at: Option<std::time::Instant> = None;
    // Poll every 5s — cheap, and fine-grained enough for a 30s timeout.
    let mut deadline_poll = tokio::time::interval(Duration::from_secs(5));
    deadline_poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Don't fire immediately (tick 0); first poll after 5s.
    deadline_poll.tick().await;

    let result: Result<(), UpstreamError> = async {
        loop {
            iter_count.fetch_add(1, Ordering::Relaxed);
            tokio::select! {
                biased;
                msg = sock.next() => {
                    let Some(msg) = msg else { break };
                    match msg? {
                        Message::Binary(data) => {
                            frame_count.fetch_add(1, Ordering::Relaxed);
                            last_event_ms.store(now_ms(), Ordering::Relaxed);
                            match decode_frame(&data, config.compression, config.protocol) {
                                Ok(frame) => {
                                    // Intercept probe responses — don't
                                    // forward them downstream. Any
                                    // OneOffQuery response means our probe
                                    // was answered.
                                    if frame.server_tag() == probe_response_tag {
                                        if !probe_answered {
                                            debug!(
                                                target: "relay::upstream",
                                                "probe response received"
                                            );
                                            probe_answered = true;
                                            probe_sent_at = None;
                                        }
                                        continue;
                                    }
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
                        Message::Ping(_) => {
                            // Auto-Pong is queued by tungstenite; it
                            // flushes on the next read poll because
                            // we don't split the socket.
                            last_event_ms.store(now_ms(), Ordering::Relaxed);
                            let _ = events_tx.send(UpstreamEvent::Ping).await;
                        }
                        Message::Pong(_) => {
                            last_event_ms.store(now_ms(), Ordering::Relaxed);
                            let _ = events_tx.send(UpstreamEvent::Ping).await;
                        }
                        Message::Frame(_) => {}
                    }
                }
                _ = ping_interval.tick() => {
                    debug!(target: "relay::upstream", "sending client ping");
                    if let Err(e) = sock.send(Message::Ping(Vec::new())).await {
                        warn!(target: "relay::upstream", error = %e, "client ping failed");
                        return Err(e.into());
                    }
                }
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
                            sock.send(Message::Binary(frame)).await?;
                            cmd_processed.fetch_add(1, Ordering::Relaxed);
                            last_event_ms.store(now_ms(), Ordering::Relaxed);
                        }
                        Some(UpstreamCommand::SubscribeOne { request_id, query_id, query }) => {
                            let frame = match config.protocol {
                                ProtocolVersion::V1 => {
                                    v1_compat::encode_subscribe_multi(request_id, query_id, &query)?
                                }
                                ProtocolVersion::V2 => {
                                    return Err(UpstreamError::Encode(
                                        "SubscribeOne not implemented for v2 protocol".into(),
                                    ));
                                }
                            };
                            debug!(
                                target: "relay::upstream",
                                protocol = %config.protocol,
                                request_id, query_id, query = %query,
                                frame_len = frame.len(),
                                "sending SubscribeMulti"
                            );
                            sock.send(Message::Binary(frame)).await?;
                            cmd_processed.fetch_add(1, Ordering::Relaxed);
                            last_event_ms.store(now_ms(), Ordering::Relaxed);
                        }
                        Some(UpstreamCommand::Shutdown) | None => {
                            let _ = sock.send(Message::Close(None)).await;
                            let _ = events_tx
                                .send(UpstreamEvent::Disconnected { reason: "shutdown".into() })
                                .await;
                            return Ok(());
                        }
                    }
                }
                _ = probe_interval.tick() => {
                    // Send the liveness probe. Any frame received before
                    // the deadline resets probe_answered.
                    let frame = match config.protocol {
                        ProtocolVersion::V1 => {
                            v1_compat::encode_one_off_query(PROBE_MESSAGE_ID, "SELECT 1")?
                        }
                        ProtocolVersion::V2 => {
                            return Err(UpstreamError::Encode(
                                "probe not implemented for v2 protocol".into(),
                            ));
                        }
                    };
                    debug!(target: "relay::upstream", "sending liveness probe");
                    sock.send(Message::Binary(frame)).await?;
                    probe_answered = false;
                    probe_sent_at = Some(std::time::Instant::now());
                }
                _ = deadline_poll.tick() => {
                    // Check if the probe has timed out. This poll fires
                    // every 5s; we only act when a probe is in-flight
                    // and the timeout has elapsed.
                    if !probe_answered {
                        if let Some(sent_at) = probe_sent_at {
                            if sent_at.elapsed() >= PROBE_TIMEOUT {
                                warn!(
                                    target: "relay::upstream",
                                    timeout_secs = PROBE_TIMEOUT.as_secs(),
                                    "liveness probe timed out — forcing reconnect"
                                );
                                return Err(UpstreamError::ProbeTimeout(PROBE_TIMEOUT.as_secs()));
                            }
                        }
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
    .await;

    watchdog_handle.abort();
    result
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
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
