// SPDX-License-Identifier: MIT

//! Minimal plain-HTTP handling bolted onto the WebSocket listener.
//!
//! The frontend port is, first and foremost, a `v1.bsatn.spacetimedb` /
//! `v2.bsatn.spacetimedb` WebSocket listener — that's what downstream
//! subscribers connect to. We also expose the cached upstream schema over
//! plain HTTP on the same port, so clients without a developer token can
//! discover the row shape the mirror is serving. The path mirrors
//! SpacetimeDB's own API:
//!
//! ```text
//! GET /v1/database/<mirror-db>/schema?version=9
//! ```
//!
//! Tungstenite's server handshake rejects any request lacking
//! `Upgrade: websocket` with a `ProtocolError` *before* the header
//! callback runs, so we cannot short-circuit from inside the WS
//! handshake. Instead we [`TcpStream::peek`] the first bytes
//! non-destructively, classify the request, and either answer it as HTTP
//! or hand the untouched stream to `accept_hdr_async`. `peek` consumes
//! nothing, so the WebSocket path is byte-identical to a build without
//! this feature.

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

/// Outcome of inspecting the first bytes of an incoming connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpProbe {
    /// A plain-HTTP `GET .../schema` request we should answer inline.
    Schema,
    /// Anything else (WebSocket upgrade, non-schema path, unparseable) —
    /// hand the untouched stream to the WebSocket handshake.
    Passthrough,
}

/// Bound the peek loop so a silent or hostile client can't hold a task
/// indefinitely. Generous enough for the largest legitimate header set.
const PROBE_TIMEOUT: Duration = Duration::from_secs(10);
/// Cap how many bytes we inspect. HTTP request lines + headers fit in
/// well under this; if a client sends more, we treat it as Passthrough
/// and let the WS handshake (or its 400) handle it.
const PROBE_MAX_BYTES: usize = 64 * 1024;
/// Bound [`drain_request`] so a client that keeps its write half open
/// (or a half-open connection) can't pin a frontend task after the
/// schema response is already written. Generous for any real HTTP
/// client, which sends its full request in one burst and then waits.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Inspect the first bytes of `stream` without consuming them and decide
/// whether this is a schema HTTP request. Returns `Passthrough` on any
/// doubt, timeout, or error — the safe default is to let the existing
/// WebSocket path run, which preserves today's behavior exactly.
pub async fn probe(stream: &TcpStream) -> HttpProbe {
    match timeout(PROBE_TIMEOUT, probe_inner(stream)).await {
        Ok(probe) => probe,
        Err(_) => HttpProbe::Passthrough,
    }
}

async fn probe_inner(stream: &TcpStream) -> HttpProbe {
    // `peek` is non-destructive: every call refills from byte 0 of the
    // socket buffer, overwriting our view with whatever prefix has
    // arrived so far. We reuse one buffer sized to the inspection cap
    // and track the high-water mark of bytes seen — never appending,
    // because the same leading bytes come back on every peek. The bytes
    // stay in the socket buffer so the WS handshake sees them intact
    // when we return Passthrough.
    let mut buf = vec![0u8; PROBE_MAX_BYTES];
    let mut len = 0usize;
    loop {
        // `peek` awaits new bytes itself (no separate readiness wait).
        // PROBE_TIMEOUT (on the outer `probe`) bounds the total time.
        match stream.peek(&mut buf[..]).await {
            Ok(0) => return HttpProbe::Passthrough, // client closed / nothing
            Ok(n) => {
                // peek reports the total bytes currently buffered on the
                // socket (capped at buf.len()). It grows as more data
                // arrives; a non-growing n is spurious — loop and wait
                // for more rather than classifying stale data.
                if n <= len {
                    continue;
                }
                len = n;
                if classify(&buf[..len]) != ProbeState::NeedMore {
                    break;
                }
            }
            Err(_) => return HttpProbe::Passthrough,
        }
    }
    classify_final(&buf[..len])
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProbeState {
    Schema,
    Passthrough,
    NeedMore,
}

fn classify(buf: &[u8]) -> ProbeState {
    // Request line must be present before we can decide anything.
    let Some(req_line_end) = find_subsequence(buf, b"\r\n") else {
        return ProbeState::NeedMore;
    };
    let req_line = match std::str::from_utf8(&buf[..req_line_end]) {
        Ok(s) => s,
        Err(_) => return ProbeState::Passthrough,
    };
    let mut parts = req_line.splitn(3, ' ');
    let method = parts.next().unwrap_or("");
    let target = parts.next().unwrap_or("");

    if method != "GET" {
        return ProbeState::Passthrough;
    }
    // `target` is the request target (path + optional `?query`). We
    // care only about the path. A trailing `/schema` is the
    // SpacetimeDB convention; the `?version=9` query (if present) is
    // accepted but ignored.
    let path = target.split('?').next().unwrap_or("");
    if !path.ends_with("/schema") {
        return ProbeState::Passthrough;
    }

    // If the headers haven't fully arrived we can't yet rule out an
    // `Upgrade: websocket` on the same request. Keep reading until we
    // see the end of the header block, then decide in classify_final.
    if find_subsequence(buf, b"\r\n\r\n").is_none() {
        return ProbeState::NeedMore;
    }
    // Headers are complete — check for a websocket upgrade inline.
    if is_websocket_upgrade(buf) {
        return ProbeState::Passthrough;
    }
    ProbeState::Schema
}

fn classify_final(buf: &[u8]) -> HttpProbe {
    match classify(buf) {
        ProbeState::Schema => HttpProbe::Schema,
        ProbeState::Passthrough | ProbeState::NeedMore => HttpProbe::Passthrough,
    }
}

/// Case-insensitive scan of the header block for an
/// `Upgrade: websocket` line. HTTP header *names* are case-insensitive
/// (RFC 7230 §3.2), so a client may send `Upgrade`, `UPGRADE`, etc.
/// Mirrors the check tungstenite performs in `create_parts()` — we
/// defer to the WS path whenever a client offers the upgrade, so
/// websocket connections are never answered as HTTP.
fn is_websocket_upgrade(buf: &[u8]) -> bool {
    let mut search_from = 0;
    while let Some(rel) = find_ci(&buf[search_from..], b"upgrade:") {
        let value_start = search_from + rel + b"upgrade:".len();
        // The value runs to the next CRLF (or end of buffer).
        let value_end = find_subsequence(&buf[value_start..], b"\r\n")
            .map(|p| value_start + p)
            .unwrap_or(buf.len());
        let value = &buf[value_start..value_end];
        let trimmed = std::str::from_utf8(value)
            .unwrap_or("")
            .trim_matches(|c: char| c == ' ' || c == '\t');
        if trimmed.eq_ignore_ascii_case("websocket") {
            return true;
        }
        search_from = value_end;
    }
    false
}

/// Case-insensitive subsequence search (ASCII). Used for HTTP header
/// names, which are case-insensitive per RFC 7230.
fn find_ci(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|w| w.eq_ignore_ascii_case(needle))
}

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Build the raw HTTP/1.1 200 response bytes for a schema payload.
/// Pure function so the wire format is unit-testable without a socket.
fn build_response(body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(256 + body.len());
    out.extend_from_slice(b"HTTP/1.1 200 OK\r\n");
    out.extend_from_slice(b"Content-Type: application/json\r\n");
    out.extend_from_slice(format!("Content-Length: {}\r\n", body.len()).as_bytes());
    out.extend_from_slice(b"Cache-Control: public, max-age=60\r\n");
    // Schema is public read-only. Match coordinator `/health` and
    // relay-cache (`CorsLayer::permissive`) so browser tools — including
    // a local static server hitting production frontends — can fetch it.
    // Cross-origin from :443 (or localhost) to :3000+N is the normal path.
    out.extend_from_slice(b"Access-Control-Allow-Origin: *\r\n");
    out.extend_from_slice(b"Connection: close\r\n");
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(body);
    out
}

/// Write a minimal HTTP/1.1 200 response carrying `body` as JSON, then
/// flush and leave the stream for the caller to close.
///
/// After writing, drains any bytes the client sent that [`probe`] left
/// unread in the socket's receive buffer. The probe classifies requests
/// via non-destructive `peek`, so the full HTTP request line + headers
/// (and anything else the client pipelined) are still sitting in the
/// kernel receive buffer when we come to write the response. If we let
/// the caller drop the stream with those bytes unread, the kernel sends
/// an RST rather than a FIN — which discards whatever remains in the
/// *send* buffer too. For the ~580 KB BitCraft schema that truncated
/// every client at ~127 KB. Draining the receive buffer first lets the
/// subsequent close deliver the full payload cleanly.
pub async fn serve_schema(stream: &mut TcpStream, body: &[u8]) -> std::io::Result<()> {
    let response = build_response(body);
    stream.write_all(&response).await?;
    stream.flush().await?;
    drain_request(stream).await;
    Ok(())
}

/// Read and discard whatever the client sent that hasn't been read yet,
/// so the caller can drop the stream without the kernel converting an
/// unread receive buffer into a connection-reset. Bounded by
/// [`DRAIN_TIMEOUT`] so a client that holds its write side open (or a
/// half-open connection) can't pin a frontend task. A client that has
/// already closed its side returns promptly with `Ok(0)`.
async fn drain_request(stream: &mut TcpStream) {
    let mut buf = [0u8; 4096];
    loop {
        match timeout(DRAIN_TIMEOUT, stream.read(&mut buf)).await {
            // Clean EOF or error: nothing left to drain (or the peer is
            // gone, in which case there's no RST risk either way).
            Ok(Ok(0)) | Ok(Err(_)) => return,
            Ok(Ok(_)) => continue,
            Err(_) => return, // timed out — give up rather than hang
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(buf: &[u8]) -> ProbeState {
        classify(buf)
    }

    #[test]
    fn schema_get_is_schema() {
        let req = b"GET /v1/database/relay-mirror-bitcraft-14/schema?version=9 HTTP/1.1\r\n\
                   Host: relay.bitcraftsync.app:3014\r\n\
                   \r\n";
        assert_eq!(state(req), ProbeState::Schema);
        assert_eq!(classify_final(req), HttpProbe::Schema);
    }

    #[test]
    fn schema_get_without_query_is_schema() {
        let req = b"GET /v1/database/foo/schema HTTP/1.1\r\n\r\n";
        assert_eq!(classify_final(req), HttpProbe::Schema);
    }

    #[test]
    fn websocket_upgrade_is_passthrough_even_with_schema_path() {
        // A path ending in /schema but offering a WS upgrade must defer
        // to the WebSocket handshake — never answer as HTTP.
        let req = b"GET /v1/database/x/schema HTTP/1.1\r\n\
                   Upgrade: websocket\r\n\
                   Connection: Upgrade\r\n\
                   \r\n";
        assert!(is_websocket_upgrade(req));
        assert_eq!(classify_final(req), HttpProbe::Passthrough);
    }

    #[test]
    fn non_schema_get_is_passthrough() {
        let req = b"GET /v1/database/foo/subscribe HTTP/1.1\r\n\r\n";
        assert_eq!(classify_final(req), HttpProbe::Passthrough);
    }

    #[test]
    fn post_is_passthrough() {
        let req = b"POST /v1/database/foo/schema HTTP/1.1\r\n\r\n";
        assert_eq!(classify_final(req), HttpProbe::Passthrough);
    }

    #[test]
    fn partial_request_line_needs_more() {
        let req = b"GET /v1/database/foo/sche";
        assert_eq!(state(req), ProbeState::NeedMore);
    }

    #[test]
    fn path_only_no_headers_yet_needs_more() {
        // Request line present, no \r\n\r\n terminator yet.
        let req = b"GET /v1/database/foo/schema HTTP/1.1\r\nHost: x\r\n";
        assert_eq!(state(req), ProbeState::NeedMore);
    }

    #[test]
    fn garbage_bytes_are_passthrough() {
        // Externally visible contract: anything we can't make sense of
        // (including bytes with no CRLF terminator) is handed to the WS
        // path, never answered as HTTP.
        assert_eq!(
            classify_final(b"\x01\x02\x03 not http"),
            HttpProbe::Passthrough
        );
    }

    #[test]
    fn upgrade_case_insensitive() {
        let req = b"GET /v1/database/x/schema HTTP/1.1\r\nUPGRADE: WEBSOCKET\r\n\r\n";
        assert!(is_websocket_upgrade(req));
    }

    #[test]
    fn no_upgrade_header_returns_false() {
        let req = b"GET /v1/database/x/schema HTTP/1.1\r\nHost: x\r\n\r\n";
        assert!(!is_websocket_upgrade(req));
    }

    #[test]
    fn build_response_is_well_formed_http() {
        let body = br#"{"tables":[]}"#;
        let resp = build_response(body);
        let text = std::str::from_utf8(&resp).unwrap();

        // Status line + every required header, terminated by a blank
        // line, then the body verbatim.
        assert!(text.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(text.contains("Content-Type: application/json\r\n"));
        assert!(text.contains("Content-Length: 13\r\n"));
        assert!(text.contains("Access-Control-Allow-Origin: *\r\n"));
        assert!(text.contains("Connection: close\r\n"));
        // Exactly one blank line separates headers from body.
        let blank = "\r\n\r\n";
        let idx = text.find(blank).unwrap();
        assert_eq!(&text[idx + blank.len()..], r#"{"tables":[]}"#);
        // No trailing bytes after the body.
        assert_eq!(resp.len(), text.len());
    }

    // ---- regression: probe must not consume socket bytes ----
    //
    // The classify() unit tests above all run against in-memory slices,
    // so they cannot catch a probe that eats bytes off the socket before
    // handing the stream to the WS handshake. The bug behind the
    // 2026-07-17 subscriber outage was exactly that: probe_inner used
    // try_read (destructive) where every comment said peek
    // (non-destructive). A WS upgrade arrived, the probe classified it
    // Passthrough, and the bytes it had read were gone — tungstenite
    // then blocked forever on the starved handshake. These tests use a
    // real loopback socket to assert the property that actually broke:
    // after probe() returns, every byte the peer sent is still readable.

    #[tokio::test]
    async fn probe_passthrough_leaves_ws_upgrade_bytes_intact() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // A realistic WebSocket subscribe handshake: the exact shape a
        // downstream client sends to /v1/database/<db>/subscribe.
        let req = b"GET /v1/database/relay-mirror-bc13/subscribe HTTP/1.1\r\n\
                    Host: relay.bitcraftsync.app:3013\r\n\
                    Upgrade: websocket\r\n\
                    Connection: Upgrade\r\n\
                    Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
                    Sec-WebSocket-Version: 13\r\n\
                    Sec-WebSocket-Protocol: v1.json.spacetimedb\r\n\
                    \r\n";

        let peer = tokio::spawn(async move {
            let sock = tokio::net::TcpStream::connect(addr).await.unwrap();
            // Use write_all so the whole request lands in the socket
            // buffer before the server peeks.
            use tokio::io::AsyncWriteExt;
            let mut sock = sock;
            sock.write_all(req).await.unwrap();
            sock.flush().await.unwrap();
            sock
        });

        let (stream, _) = listener.accept().await.unwrap();

        // Probe classifies this as Passthrough (WS upgrade).
        let outcome = tokio::time::timeout(Duration::from_secs(2), probe(&stream))
            .await
            .expect("probe did not return within 2s");
        assert_eq!(outcome, HttpProbe::Passthrough);

        let mut peer_sock = peer.await.unwrap();

        // The crux: every byte must still be readable from the server
        // side. With the try_read bug, stream.try_read(...) had already
        // consumed req.len() bytes and this read returned far fewer /
        // blocked until the peer gave up.
        let mut got = vec![0u8; req.len()];
        tokio::time::timeout(Duration::from_secs(2), stream.readable())
            .await
            .expect("server stream never became readable after probe")
            .unwrap();
        let n = stream.try_read(&mut got).expect("read after probe");
        assert_eq!(
            &got[..n],
            &req[..],
            "probe consumed/dropped bytes: read {} of {}",
            n,
            req.len()
        );

        use tokio::io::AsyncWriteExt;
        peer_sock.shutdown().await.ok();
    }

    #[tokio::test]
    async fn probe_passthrough_leaves_non_schema_get_intact() {
        // Same property for a plain non-schema GET (e.g. /metrics).
        // Must also fall through untouched — the WS listener will reject
        // it as a 400, but that rejection must come from tungstenite, not
        // from a starved handshake.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let req = b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n";

        let peer = tokio::spawn(async move {
            let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
            use tokio::io::AsyncWriteExt;
            sock.write_all(req).await.unwrap();
            sock.flush().await.unwrap();
        });

        let (stream, _) = listener.accept().await.unwrap();
        let outcome = tokio::time::timeout(Duration::from_secs(2), probe(&stream))
            .await
            .expect("probe did not return within 2s");
        assert_eq!(outcome, HttpProbe::Passthrough);

        peer.await.unwrap();

        let mut got = vec![0u8; req.len()];
        tokio::time::timeout(Duration::from_secs(2), stream.readable())
            .await
            .expect("server stream never became readable after probe")
            .unwrap();
        let n = stream.try_read(&mut got).expect("read after probe");
        assert_eq!(&got[..n], &req[..], "probe consumed/dropped bytes");

        // classify the re-read bytes to prove the WS layer would see the
        // real request, not a truncated prefix.
        assert_eq!(classify_final(&got[..n]), HttpProbe::Passthrough);
    }

    // ---- regression: serve_schema must deliver the full body ----
    //
    // The 2026-07-17 schema-download outage: serve_schema did write_all +
    // flush and returned, leaving the client's HTTP request unread in the
    // socket's receive buffer (probe peeks non-destructively). When the
    // caller then dropped the stream, the kernel saw unread receive data
    // at close time and sent RST instead of FIN — discarding whatever was
    // still in the send buffer. The ~580 KB BitCraft schema truncated
    // every client at ~127 KB. The fix drains the receive buffer before
    // returning so the caller's drop produces a clean close. This test
    // asserts the property that broke: a body larger than a typical socket
    // send buffer is delivered in full with a clean EOF, no reset.

    #[tokio::test]
    async fn serve_schema_delivers_large_body_in_full() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Large enough to overflow the kernel send buffer (~128-256 KB on
        // most platforms) so a premature close would visibly truncate.
        // Mirrors the real BitCraft schema (~580 KB).
        let body = vec![b'{'; 600_000];
        let body_len = body.len();

        let req = b"GET /v1/database/relay-mirror-bc-global/schema?version=9 \
                    HTTP/1.1\r\nHost: x\r\n\r\n";

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            // probe peeks without consuming — the request bytes stay in the
            // receive buffer, exactly as in handle_accept.
            let outcome = tokio::time::timeout(Duration::from_secs(2), probe(&stream))
                .await
                .expect("probe did not return within 2s");
            assert_eq!(outcome, HttpProbe::Schema);
            serve_schema(&mut stream, &body)
                .await
                .expect("serve_schema should succeed");
            // Caller drops the stream here — this is where the RST used to
            // originate. The drain inside serve_schema must prevent it.
        });

        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        client.write_all(req).await.unwrap();
        client.flush().await.unwrap();

        // Read the response incrementally. Once we have the full body
        // (headers parsed for Content-Length), close our write side —
        // mirroring how a real HTTP client behaves on `Connection: close`,
        // and giving the server's drain a clean `Ok(0)` to exit on.
        let blank = b"\r\n\r\n";
        let mut got = Vec::new();
        let mut buf = [0u8; 16384];
        let read_result: Result<(), std::io::Error> = loop {
            match tokio::time::timeout(Duration::from_secs(10), client.read(&mut buf)).await {
                Err(_) => break Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "client read did not complete within 10s — likely deadlocked",
                )),
                Ok(Err(e)) => break Err(e),
                Ok(Ok(0)) => break Err(std::io::Error::new(
                    // Unexpected early EOF — with the bug we saw RST here.
                    std::io::ErrorKind::ConnectionReset,
                    "unexpected EOF before full body — connection was reset",
                )),
                Ok(Ok(n)) => {
                    got.extend_from_slice(&buf[..n]);
                    if let Some(pos) = got.windows(blank.len()).position(|w| w == blank) {
                        let header_end = pos + blank.len();
                        let cl = extract_content_length(&got[..header_end]);
                        if got.len() >= header_end + cl {
                            // Full body received — close our write side so
                            // the server's drain exits promptly.
                            client.shutdown().await.ok();
                            break Ok(());
                        }
                    }
                }
            }
        };

        // With the bug this branch hit ConnectionReset or TimedOut; with
        // the fix we get a clean Ok after the full body.
        read_result.expect("expected clean full-body read, got reset or timeout");

        let header_end = got
            .windows(blank.len())
            .position(|w| w == blank)
            .expect("missing header terminator");
        let received_body = &got[header_end + blank.len()..];
        assert_eq!(
            received_body.len(),
            body_len,
            "body truncated: received {} of {} bytes",
            received_body.len(),
            body_len
        );
        assert_eq!(received_body, &vec![b'{'; body_len][..], "body content mismatch");

        server.await.unwrap();
    }

    /// Parse the `Content-Length` value out of an HTTP response header
    /// block. Used by the schema-delivery test to know when the full body
    /// has arrived so it can close its write side.
    fn extract_content_length(headers: &[u8]) -> usize {
        let s = std::str::from_utf8(headers).unwrap_or("");
        for line in s.split("\r\n") {
            if line.to_ascii_lowercase().starts_with("content-length:") {
                return line
                    .split(':')
                    .nth(1)
                    .and_then(|v| v.trim().parse().ok())
                    .unwrap_or(0);
            }
        }
        0
    }
}
