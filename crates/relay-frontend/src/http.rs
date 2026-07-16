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

use tokio::io::{AsyncWriteExt, Interest};
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
    let mut buf = Vec::<u8>::with_capacity(2048);
    loop {
        // Wait until there's something to read, then peek it. `peek`
        // does not advance the read cursor, so the bytes remain
        // available for the WS handshake if we return Passthrough.
        if stream.ready(Interest::READABLE).await.is_err() {
            return HttpProbe::Passthrough;
        }
        let mut chunk = [0u8; 2048];
        match stream.try_read(&mut chunk) {
            Ok(0) => return HttpProbe::Passthrough, // client closed / nothing
            Ok(n) => {
                buf.extend_from_slice(&chunk[..n]);
                if buf.len() >= PROBE_MAX_BYTES {
                    // Headers too large — give up and defer to WS.
                    return HttpProbe::Passthrough;
                }
                if classify(&buf) != ProbeState::NeedMore {
                    break;
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(_) => return HttpProbe::Passthrough,
        }
    }
    classify_final(&buf)
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
    out.extend_from_slice(b"Connection: close\r\n");
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(body);
    out
}

/// Write a minimal HTTP/1.1 200 response carrying `body` as JSON, then
/// flush and leave the stream for the caller to close.
pub async fn serve_schema(stream: &mut TcpStream, body: &[u8]) -> std::io::Result<()> {
    let response = build_response(body);
    stream.write_all(&response).await?;
    stream.flush().await
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
        assert!(text.contains("Connection: close\r\n"));
        // Exactly one blank line separates headers from body.
        let blank = "\r\n\r\n";
        let idx = text.find(blank).unwrap();
        assert_eq!(&text[idx + blank.len()..], r#"{"tables":[]}"#);
        // No trailing bytes after the body.
        assert_eq!(resp.len(), text.len());
    }
}
