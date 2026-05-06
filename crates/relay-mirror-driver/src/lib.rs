// SPDX-License-Identifier: MIT

//! Replays upstream-decoded TableUpdates onto a local SpacetimeDB by
//! invoking the codegen'd `relay_apply_<table>(deletes, inserts)` reducers.
//!
//! Pairing of deletes with inserts (i.e. emitting an atomic update when
//! the same primary key appears in both) is performed inside the wasm
//! module — the driver just hands over the row lists. That keeps the
//! wire shape symmetric (one `relay_apply` per table per call) and
//! preserves single-transaction atomicity from the perspective of
//! downstream subscribers reading the local SpacetimeDB.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use http::header::{HeaderName, HeaderValue, AUTHORIZATION};
pub use relay_protocol::UpstreamReducerMeta;
use spacetimedb_client_api_messages::websocket::v2::{
    CallReducer, CallReducerFlags, ClientMessage, ServerMessage,
};
use spacetimedb_sats::bsatn;
use thiserror::Error;
use tokio::net::TcpStream;
use tokio::sync::Semaphore;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use url::Url;

const SUBPROTOCOL: &str = "v2.bsatn.spacetimedb";

#[derive(Debug, Error)]
pub enum DriverError {
    #[error("invalid url: {0}")]
    Url(String),
    #[error("connect failed: {0}")]
    Connect(String),
    #[error("websocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("encode failed: {0}")]
    Encode(String),
    #[error("send failed: {0}")]
    Send(String),
    #[error("driver shut down")]
    Closed,
}

#[derive(Debug, Clone)]
pub struct DriverConfig {
    /// Local SpacetimeDB URL (e.g. `ws://127.0.0.1:3000` or `http://...`).
    pub stdb_url: Url,
    /// Database name (matches what the publisher used for `spacetime publish`).
    pub database: String,
    /// Bearer identity token. Should be the same identity that ran
    /// `spacetime publish` so the writer-bind in the wasm module
    /// recognises us. `None` connects anonymously — only useful when the
    /// module's writer was never bound (first connection wins).
    pub identity_token: Option<String>,
    /// Per-call backpressure cap. Server's incoming queue is 16 384;
    /// keep some headroom.
    pub max_in_flight: usize,
    /// Maximum row count (deletes + inserts) per single
    /// `relay_apply_<table>` call. Larger TableUpdates get split into
    /// multiple calls. Cross-call atomicity is not guaranteed, but
    /// pairing within a call still works because we keep paired
    /// delete+insert within the same chunk.
    pub max_rows_per_apply: usize,
    /// Maximum total payload bytes per single `relay_apply_<table>`
    /// call. SpacetimeDB caps incoming WS frames around 32 MB, so we
    /// stay well under that. Chunks split when **either** the row
    /// count or the byte budget is hit.
    pub max_bytes_per_apply: usize,
}

impl Default for DriverConfig {
    fn default() -> Self {
        Self {
            stdb_url: "ws://127.0.0.1:3000".parse().unwrap(),
            database: String::new(),
            identity_token: None,
            max_in_flight: 8000,
            max_rows_per_apply: 4096,
            max_bytes_per_apply: 16 * 1024 * 1024,
        }
    }
}

type Conn = WebSocketStream<MaybeTlsStream<TcpStream>>;
type Sink = SplitSink<Conn, Message>;
type Stream = SplitStream<Conn>;

#[derive(Debug, Default, Clone, Copy)]
pub struct ApplyStats {
    pub calls: u64,
    pub bytes_sent: u64,
    pub deletes: u64,
    pub inserts: u64,
}

pub struct MirrorDriver {
    sink: Sink,
    in_flight: Arc<Semaphore>,
    request_id: AtomicU32,
    drain_handle: Option<tokio::task::JoinHandle<()>>,
    max_rows_per_apply: usize,
    max_bytes_per_apply: usize,
    captured: Option<InitialConnectionInfo>,
}

#[derive(Debug, Clone)]
pub struct InitialConnectionInfo {
    pub identity_hex: String,
    pub token: String,
}

impl MirrorDriver {
    pub async fn connect(cfg: DriverConfig) -> Result<Self, DriverError> {
        let mut conn = open_ws(&cfg).await?;
        // SpacetimeDB sends `InitialConnection` as the first frame
        // post-handshake. Capture the issued identity + token so the
        // caller can persist it and reconnect as the same identity
        // across restarts. We do this synchronously here, BEFORE
        // splitting + spawning the drainer — otherwise the drainer
        // might consume it.
        let captured = read_initial_connection(&mut conn).await?;
        let (sink, stream) = conn.split();
        let in_flight = Arc::new(Semaphore::new(cfg.max_in_flight));
        let drain_handle = Some(tokio::spawn(drain_responses(stream, in_flight.clone())));
        Ok(Self {
            sink,
            in_flight,
            request_id: AtomicU32::new(1),
            drain_handle,
            max_rows_per_apply: cfg.max_rows_per_apply,
            max_bytes_per_apply: cfg.max_bytes_per_apply,
            captured: Some(captured),
        })
    }

    /// The identity + token reported by the local SpacetimeDB on this
    /// connection's `InitialConnection`. Persist the token to disk and
    /// pass it back as `DriverConfig::identity_token` on the next run
    /// to reconnect as the same identity.
    pub fn captured(&self) -> Option<&InitialConnectionInfo> {
        self.captured.as_ref()
    }

    /// Idempotent. Calls the module's `relay_bind_writer` reducer, which
    /// records `ctx.sender()` as the bound writer if no writer is bound
    /// yet, returns ok if we're already the bound writer, and errors if
    /// a different identity owns the slot.
    pub async fn bind_writer(&mut self) -> Result<(), DriverError> {
        self.send_call("relay_bind_writer", &[]).await
    }

    /// Available permits on the in-flight semaphore. The "used" count
    /// is `max_in_flight - available`; callers that want the absolute
    /// number should remember the configured cap themselves.
    pub fn available_permits(&self) -> usize {
        self.in_flight.available_permits()
    }

    /// Apply one TableUpdate's worth of changes for `table`. Splits into
    /// multiple `relay_apply_<table>` calls when the row count exceeds
    /// `max_rows_per_apply`; pairing within each chunk still happens
    /// inside the wasm module, so paired delete+insert across the chunk
    /// boundary will degrade into separate delete and insert (degraded
    /// from atomic update to delete-then-insert). Acceptable on the
    /// initial subscribe-applied firehose where deletes are typically
    /// empty.
    ///
    /// `upstream` carries the upstream reducer's provenance (caller,
    /// timestamp, name, args). When the upstream protocol can't supply
    /// it (e.g. v2 upstream, or initial `SubscribeApplied`), pass
    /// `None`; downstream subscribers will see the local reducer's own
    /// context as usual. Same `upstream` is reused across all chunks
    /// produced from a single upstream `TableUpdate`.
    pub async fn apply(
        &mut self,
        table: &str,
        upstream: Option<&UpstreamReducerMeta>,
        deletes: Vec<Bytes>,
        inserts: Vec<Bytes>,
    ) -> Result<ApplyStats, DriverError> {
        let reducer = format!("relay_apply_{table}");
        let chunks = chunk_apply(
            deletes,
            inserts,
            self.max_rows_per_apply,
            self.max_bytes_per_apply,
        );
        let mut stats = ApplyStats::default();
        for (deletes_chunk, inserts_chunk) in chunks {
            let args = encode_apply_args(upstream, &deletes_chunk, &inserts_chunk);
            stats.calls += 1;
            stats.bytes_sent += args.len() as u64;
            stats.deletes += deletes_chunk.len() as u64;
            stats.inserts += inserts_chunk.len() as u64;
            self.send_call(&reducer, &args).await?;
        }
        Ok(stats)
    }

    pub async fn close(mut self) -> Result<(), DriverError> {
        let _ = self.sink.close().await;
        if let Some(h) = self.drain_handle.take() {
            let _ = h.await;
        }
        Ok(())
    }

    async fn send_call(&mut self, reducer: &str, args: &[u8]) -> Result<(), DriverError> {
        let permit = self
            .in_flight
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| DriverError::Closed)?;
        permit.forget();
        let request_id = self.request_id.fetch_add(1, Ordering::Relaxed);
        let msg = ClientMessage::CallReducer(CallReducer {
            request_id,
            flags: CallReducerFlags::Default,
            reducer: reducer.into(),
            args: Bytes::copy_from_slice(args),
        });
        let frame = bsatn::to_vec(&msg).map_err(|e| DriverError::Encode(e.to_string()))?;
        self.sink
            .send(Message::Binary(frame))
            .await
            .map_err(|e| DriverError::Send(e.to_string()))?;
        Ok(())
    }
}

async fn read_initial_connection(conn: &mut Conn) -> Result<InitialConnectionInfo, DriverError> {
    use std::time::Duration;
    let timeout = Duration::from_secs(10);
    loop {
        let msg = tokio::time::timeout(timeout, conn.next())
            .await
            .map_err(|_| DriverError::Connect("InitialConnection not received within 10s".into()))?
            .ok_or_else(|| DriverError::Connect("ws closed before InitialConnection".into()))??;
        let bytes = match msg {
            Message::Binary(b) => b,
            Message::Text(_) | Message::Ping(_) | Message::Pong(_) => continue,
            Message::Close(_) => {
                return Err(DriverError::Connect("ws closed before InitialConnection".into()))
            }
            Message::Frame(_) => continue,
        };
        if bytes.is_empty() {
            return Err(DriverError::Connect("empty initial frame".into()));
        }
        // Compression byte. We negotiated ?compression=None so anything
        // non-zero is unexpected.
        if bytes[0] != 0 {
            return Err(DriverError::Connect(format!(
                "unexpected compression tag {} on initial frame (compression=None requested)",
                bytes[0]
            )));
        }
        let body = &bytes[1..];
        let server_msg: ServerMessage = bsatn::from_slice(body)
            .map_err(|e| DriverError::Encode(format!("decode initial ServerMessage: {e}")))?;
        match server_msg {
            ServerMessage::InitialConnection(ic) => {
                return Ok(InitialConnectionInfo {
                    identity_hex: ic.identity.to_hex().as_str().to_string(),
                    token: ic.token.to_string(),
                });
            }
            other => {
                return Err(DriverError::Connect(format!(
                    "expected InitialConnection as first frame, got {other:?}"
                )))
            }
        }
    }
}

async fn open_ws(cfg: &DriverConfig) -> Result<Conn, DriverError> {
    let mut url = cfg.stdb_url.clone();
    let scheme = match url.scheme() {
        "http" => "ws",
        "https" => "wss",
        s => s,
    }
    .to_string();
    url.set_scheme(&scheme).map_err(|_| DriverError::Url("bad scheme".into()))?;
    url.set_path(&format!("/v1/database/{}/subscribe", cfg.database));
    url.set_query(Some("compression=None"));
    let mut req = url
        .as_str()
        .into_client_request()
        .map_err(|e| DriverError::Url(e.to_string()))?;
    req.headers_mut().insert(
        HeaderName::from_static("sec-websocket-protocol"),
        HeaderValue::from_static(SUBPROTOCOL),
    );
    if let Some(token) = &cfg.identity_token {
        let v = HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|e| DriverError::Url(format!("invalid identity token: {e}")))?;
        req.headers_mut().insert(AUTHORIZATION, v);
    }
    let (ws, _resp) = tokio_tungstenite::connect_async(req)
        .await
        .map_err(|e| DriverError::Connect(e.to_string()))?;
    Ok(ws)
}

async fn drain_responses(mut stream: Stream, in_flight: Arc<Semaphore>) {
    let mut warn_budget = 5u8;
    while let Some(msg) = stream.next().await {
        match msg {
            Ok(Message::Binary(_)) | Ok(Message::Text(_)) => {
                in_flight.add_permits(1);
            }
            Ok(_) => {}
            Err(e) => {
                if warn_budget > 0 {
                    tracing::warn!(target: "relay::mirror_driver", "ws recv error: {e}");
                    warn_budget -= 1;
                }
            }
        }
    }
    tracing::debug!(target: "relay::mirror_driver", "drainer task exited");
}

/// Split the row lists so each chunk obeys both `max_rows` and
/// `max_bytes`. Bytes are counted as the inner row payload sum (the
/// CallReducer envelope adds a few bytes of overhead per row, easily
/// covered by the headroom we leave before the 32 MB WS frame cap).
fn chunk_apply(
    deletes: Vec<Bytes>,
    inserts: Vec<Bytes>,
    max_rows: usize,
    max_bytes: usize,
) -> Vec<(Vec<Bytes>, Vec<Bytes>)> {
    let mut out = Vec::new();
    let mut d_iter = deletes.into_iter().peekable();
    let mut i_iter = inserts.into_iter().peekable();
    loop {
        let mut d_chunk = Vec::new();
        let mut i_chunk = Vec::new();
        let mut rows = 0usize;
        let mut bytes = 0usize;
        loop {
            // Peek at next available row from either side; stop if
            // adding it would blow either budget.
            let next = if d_iter.peek().is_some() {
                Some(true)
            } else if i_iter.peek().is_some() {
                Some(false)
            } else {
                None
            };
            let Some(is_delete) = next else { break };
            let next_len = if is_delete {
                d_iter.peek().map(|b| b.len()).unwrap_or(0)
            } else {
                i_iter.peek().map(|b| b.len()).unwrap_or(0)
            };
            // Always include at least one row per chunk so we don't
            // deadlock on a single oversize row (it'll still fail at
            // the server, but the failure is per-row not per-chunk).
            if rows >= max_rows || (rows > 0 && bytes + next_len > max_bytes) {
                break;
            }
            if is_delete {
                let b = d_iter.next().expect("peeked");
                bytes += b.len();
                d_chunk.push(b);
            } else {
                let b = i_iter.next().expect("peeked");
                bytes += b.len();
                i_chunk.push(b);
            }
            rows += 1;
        }
        if d_chunk.is_empty() && i_chunk.is_empty() {
            break;
        }
        out.push((d_chunk, i_chunk));
    }
    out
}

/// Encode the BSATN body for
/// `relay_apply_<table>(upstream: Option<UpstreamReducerInfo>, deletes: Vec<Vec<u8>>, inserts: Vec<Vec<u8>>)`.
///
/// Wire shape:
/// * `[u8 some_tag] (0 = Some, 1 = None)` per `spacetimedb_sats`'s
///   `Option<T>` encoding
/// * if `Some`: BSATN-encoded `UpstreamReducerMeta` (delegated to
///   `bsatn::to_vec` since `SpacetimeType` is derived on it)
/// * `[u32 deletes_count][per-delete: u32 len, bytes]`
/// * `[u32 inserts_count][per-insert: u32 len, bytes]`
fn encode_apply_args(
    upstream: Option<&UpstreamReducerMeta>,
    deletes: &[Bytes],
    inserts: &[Bytes],
) -> Vec<u8> {
    let total_inner: usize = deletes.iter().map(|b| b.len()).sum::<usize>()
        + inserts.iter().map(|b| b.len()).sum::<usize>();
    let mut buf = Vec::with_capacity(8 + 8 * (deletes.len() + inserts.len()) + total_inner);
    match upstream {
        Some(meta) => {
            buf.push(0); // Some — `spacetimedb_sats` encodes Some as variant 0
            // BSATN-encoding a struct of primitives + String + Vec<u8>
            // never fails (no IO, no fallible conversions), so we
            // surface a panic if it ever does — that would be a
            // programmer error in `UpstreamReducerMeta`'s SpacetimeType
            // derive, not a runtime condition we can recover from.
            let meta_bytes =
                bsatn::to_vec(meta).expect("UpstreamReducerMeta BSATN encode is infallible");
            buf.extend_from_slice(&meta_bytes);
        }
        None => {
            buf.push(1); // None — variant 1 in `spacetimedb_sats` Option encoding
        }
    }
    push_vec_vec_u8(&mut buf, deletes);
    push_vec_vec_u8(&mut buf, inserts);
    buf
}

fn push_vec_vec_u8(buf: &mut Vec<u8>, rows: &[Bytes]) {
    buf.extend_from_slice(&(rows.len() as u32).to_le_bytes());
    for r in rows {
        buf.extend_from_slice(&(r.len() as u32).to_le_bytes());
        buf.extend_from_slice(r);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_returns_single_when_under_limits() {
        let d = vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")];
        let i = vec![Bytes::from_static(b"c")];
        let chunks = chunk_apply(d, i, 100, 100);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].0.len(), 2);
        assert_eq!(chunks[0].1.len(), 1);
    }

    #[test]
    fn chunk_splits_by_row_count() {
        let d: Vec<Bytes> = (0..3).map(|n| Bytes::from(vec![n as u8])).collect();
        let i: Vec<Bytes> = (0..5).map(|n| Bytes::from(vec![n as u8 + 100])).collect();
        let chunks = chunk_apply(d, i, 3, 1_000_000);
        let total_d: usize = chunks.iter().map(|(d, _)| d.len()).sum();
        let total_i: usize = chunks.iter().map(|(_, i)| i.len()).sum();
        assert_eq!(total_d, 3);
        assert_eq!(total_i, 5);
        for (d, i) in &chunks {
            assert!(d.len() + i.len() <= 3);
        }
    }

    #[test]
    fn chunk_splits_by_byte_budget() {
        // 10 rows of 1000 bytes each; budget is 3500 bytes per chunk.
        let i: Vec<Bytes> = (0..10).map(|_| Bytes::from(vec![0u8; 1000])).collect();
        let chunks = chunk_apply(Vec::new(), i, 100_000, 3500);
        // 3 rows fit per chunk (3000 < 3500, 4000 > 3500).
        for (_, ic) in &chunks {
            let total: usize = ic.iter().map(|b| b.len()).sum();
            assert!(total <= 3500);
        }
        let total_rows: usize = chunks.iter().map(|(_, i)| i.len()).sum();
        assert_eq!(total_rows, 10);
    }

    #[test]
    fn chunk_one_oversize_row_passes_through() {
        // A single row larger than the byte budget still gets emitted
        // (server will reject; better than the driver deadlocking).
        let i = vec![Bytes::from(vec![0u8; 10_000])];
        let chunks = chunk_apply(Vec::new(), i, 100, 1000);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].1.len(), 1);
    }

    #[test]
    fn encode_apply_args_none_upstream() {
        // None tag (0x01) + two empty Vec<Vec<u8>> length prefixes.
        let buf = encode_apply_args(None, &[], &[]);
        assert_eq!(buf, vec![0x01, 0, 0, 0, 0, 0, 0, 0, 0]);

        // None tag + one delete + one insert with single-byte bodies
        let buf = encode_apply_args(
            None,
            &[Bytes::from_static(&[0xAA])],
            &[Bytes::from_static(&[0xBB, 0xCC])],
        );
        let expected = vec![
            0x01, // None tag
            // deletes: count=1
            0x01, 0x00, 0x00, 0x00,
            // delete[0]: len=1
            0x01, 0x00, 0x00, 0x00, 0xAA,
            // inserts: count=1
            0x01, 0x00, 0x00, 0x00,
            // insert[0]: len=2
            0x02, 0x00, 0x00, 0x00, 0xBB, 0xCC,
        ];
        assert_eq!(buf, expected);
    }

    #[test]
    fn encode_apply_args_some_upstream_round_trips() {
        use spacetimedb_client_api_messages::websocket::v2::CallReducer;
        use spacetimedb_sats::bsatn;
        let meta = UpstreamReducerMeta {
            reducer_name: "send_message".into(),
            caller_identity: relay_protocol::lib::Identity::ZERO,
            caller_connection_id: relay_protocol::lib::ConnectionId::ZERO,
            timestamp: relay_protocol::lib::Timestamp::UNIX_EPOCH,
            request_id: 42,
            args: b"\x01\x02\x03".to_vec(),
        };
        let buf = encode_apply_args(
            Some(&meta),
            &[Bytes::from_static(&[0xAA])],
            &[Bytes::from_static(&[0xBB])],
        );
        // First byte must be Some-tag (0x00).
        assert_eq!(buf[0], 0x00);
        // The encoded meta should round-trip through bsatn::from_slice
        // when the trailing Vec<Vec<u8>> bytes are dropped. We verify
        // by decoding the prefix and checking the field values.
        let (decoded, consumed) = bsatn_decode_meta(&buf[1..]);
        assert_eq!(decoded.reducer_name, "send_message");
        assert_eq!(decoded.request_id, 42);
        assert_eq!(decoded.args, b"\x01\x02\x03");
        // Remaining bytes after meta should be the existing
        // Vec<Vec<u8>> deletes+inserts encoding.
        let tail = &buf[1 + consumed..];
        assert_eq!(
            tail,
            &[
                // deletes: count=1, len=1, 0xAA
                0x01, 0, 0, 0, 0x01, 0, 0, 0, 0xAA,
                // inserts: count=1, len=1, 0xBB
                0x01, 0, 0, 0, 0x01, 0, 0, 0, 0xBB,
            ]
        );
        // Ensure the whole CallReducer envelope still encodes (no
        // panic / size overflow on the args buffer).
        let msg = ClientMessage::CallReducer(CallReducer {
            request_id: 1,
            flags: CallReducerFlags::Default,
            reducer: "relay_apply_x".into(),
            args: Bytes::copy_from_slice(&buf),
        });
        bsatn::to_vec(&msg).unwrap();
    }

    /// Decode a single `UpstreamReducerMeta` from BSATN; returns the
    /// decoded value + bytes consumed.
    fn bsatn_decode_meta(input: &[u8]) -> (UpstreamReducerMeta, usize) {
        use spacetimedb_sats::bsatn;
        // bsatn::from_slice consumes the full slice, so we discover
        // the meta length by trial-encoding then re-decoding.
        let m: UpstreamReducerMeta = bsatn::from_slice(&input[..meta_byte_len(input)]).unwrap();
        (m, meta_byte_len(input))
    }

    fn meta_byte_len(input: &[u8]) -> usize {
        // reducer_name: u32 LE len + bytes
        let name_len = u32::from_le_bytes(input[0..4].try_into().unwrap()) as usize;
        let mut p = 4 + name_len;
        // identity: 32, connection_id: 16, timestamp: 8, request_id: 4
        p += 32 + 16 + 8 + 4;
        // args: u32 LE len + bytes
        let args_len = u32::from_le_bytes(input[p..p + 4].try_into().unwrap()) as usize;
        p += 4 + args_len;
        p
    }
}
