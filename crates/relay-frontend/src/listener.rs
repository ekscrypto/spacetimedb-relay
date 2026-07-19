// SPDX-License-Identifier: MIT

//! Public-facing WebSocket listener. Negotiates `v1.bsatn.spacetimedb`
//! or `v2.bsatn.spacetimedb` per the client's `Sec-WebSocket-Protocol`
//! offer, then hands the upgraded socket off to [`crate::client::run`].

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::handshake::server::{
    Callback, ErrorResponse, Request, Response,
};
use tokio_tungstenite::tungstenite::http::{HeaderValue, StatusCode};
use url::Url;

use crate::client::{self, ClientCtx};
use crate::metrics::FrontendMetrics;
use crate::state::ActiveClients;
use crate::Subprotocol;
use relay_mirror_driver::MetaRegistry;

#[derive(Clone)]
pub struct Config {
    pub bind: SocketAddr,
    /// Local SpacetimeDB url (loopback in production).
    pub local_url: Url,
    pub local_database: String,
    /// Optional bearer token to forward upstream-of-the-proxy. None
    /// means anonymous connections; SpacetimeDB will mint a fresh
    /// identity for each downstream client.
    pub local_token: Option<String>,
    pub max_clients: usize,
    pub idle_timeout: Duration,
    /// Shared registry of `(request_id, UpstreamReducerMeta)` the
    /// relay-mirror-driver populates for each `relay_apply_*`
    /// CallReducer. The proxy reads it to synthesise full v1
    /// TransactionUpdates from local stdb's TransactionUpdateLight
    /// broadcasts. None disables synthesis (TUL passes through).
    pub meta_registry: Option<Arc<MetaRegistry>>,
    /// Cached upstream schema bytes the relay used to codegen+publish
    /// the mirror module. When `Some`, a plain-HTTP
    /// `GET /v1/database/<local_database>/schema` (no WS upgrade) is
    /// answered inline with these bytes; everything else falls through
    /// to the WebSocket handshake. `None` disables the schema endpoint.
    pub schema: Option<Arc<[u8]>>,
}

/// Accept loop. Returns when the listener errors or is dropped.
pub async fn run(cfg: Config, metrics: Arc<FrontendMetrics>, clients: ActiveClients) -> Result<()> {
    let listener = TcpListener::bind(cfg.bind)
        .await
        .with_context(|| format!("frontend bind {}", cfg.bind))?;
    tracing::info!(
        target: "relay::frontend",
        bind = %cfg.bind,
        local_url = %cfg.local_url,
        max_clients = cfg.max_clients,
        "frontend listening"
    );

    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(target: "relay::frontend", error = %e, "accept failed");
                continue;
            }
        };
        if clients.len() >= cfg.max_clients {
            tracing::warn!(
                target: "relay::frontend",
                peer = %peer,
                active = clients.len(),
                cap = cfg.max_clients,
                "rejecting new connection: max-clients reached"
            );
            // Drop the TcpStream; the client will see RST.
            drop(stream);
            continue;
        }

        let cfg = cfg.clone();
        let metrics = metrics.clone();
        let clients = clients.clone();
        tokio::spawn(async move {
            handle_accept(stream, peer, cfg, metrics, clients).await;
        });
    }
}

async fn handle_accept(
    mut stream: TcpStream,
    peer: SocketAddr,
    cfg: Config,
    metrics: Arc<FrontendMetrics>,
    clients: ActiveClients,
) {
    // Serve the cached schema as plain HTTP on the same port as the WS
    // listener. `probe` peeks without consuming, so a WebSocket upgrade
    // (or anything we're unsure about) falls through untouched to the
    // normal handshake.
    if let Some(schema) = cfg.schema.as_deref() {
        if matches!(
            crate::http::probe(&stream).await,
            crate::http::HttpProbe::Schema
        ) {
            if let Err(e) = crate::http::serve_schema(&mut stream, schema).await {
                tracing::warn!(
                    target: "relay::frontend",
                    peer = %peer,
                    error = %e,
                    "failed to write schema HTTP response"
                );
            }
            return;
        }
    }

    let mut chosen: Option<Subprotocol> = None;
    let cb = SubprotocolNegotiator {
        chosen: &mut chosen,
    };
    // Disable tungstenite's 64 MiB default message/frame size cap. The
    // downstream-facing server is the WS reader for frames the local stdb
    // emits, and a single `SubscribeApplied` for a large public table
    // (BitCraft's `location_state` snapshot is ~1 GB as one WS message) can
    // exceed the default — observed downstream as `Connection reset without
    // closing handshake` after 0 rows. Every WS *client* path in this repo
    // already sets None/None (see relay-upstream client.rs and
    // relay-mirror-driver lib.rs); the server acceptor was the one site that
    // inherited the default, so downstream subscriptions to large tables died
    // even though the upstream path receiving the same table was fine.
    let ws_config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
        max_message_size: None,
        max_frame_size: None,
        ..Default::default()
    };
    let ws =
        match tokio_tungstenite::accept_hdr_async_with_config(stream, cb, Some(ws_config)).await {
            Ok(ws) => ws,
            Err(e) => {
                tracing::warn!(
                    target: "relay::frontend",
                    peer = %peer,
                    error = %e,
                    "ws handshake failed"
                );
                return;
            }
        };
    let Some(subprotocol) = chosen else {
        tracing::warn!(
            target: "relay::frontend",
            peer = %peer,
            "client did not offer a supported subprotocol"
        );
        return;
    };

    let ctx = ClientCtx {
        remote: peer,
        subprotocol,
        local_url: cfg.local_url,
        local_database: cfg.local_database,
        local_token: cfg.local_token,
        idle_timeout: cfg.idle_timeout,
        metrics,
        clients,
        meta_registry: cfg.meta_registry,
    };
    client::run(ws, ctx).await;
}

/// Tungstenite handshake callback. Inspects `Sec-WebSocket-Protocol`,
/// picks v2 over v1 when both are offered, rejects connections that
/// don't offer either.
struct SubprotocolNegotiator<'a> {
    chosen: &'a mut Option<Subprotocol>,
}

impl<'a> Callback for SubprotocolNegotiator<'a> {
    fn on_request(
        self,
        request: &Request,
        mut response: Response,
    ) -> Result<Response, ErrorResponse> {
        let mut want: Option<Subprotocol> = None;
        for h in request.headers().get_all("sec-websocket-protocol") {
            let s = match h.to_str() {
                Ok(s) => s,
                Err(_) => continue,
            };
            for p in s.split(',') {
                let p = p.trim();
                if let Some(sp) = Subprotocol::from_name(p) {
                    // Pick the first recognized protocol, then upgrade to a
                    // more efficient one if the client also offers it:
                    // V2 (bsatn) > V1 (bsatn) > V1Json (text). BSATN is
                    // smaller and avoids JSON encode/decode, so when a
                    // client offers both, prefer it.
                    if want.is_none() || sp.rank() > want.unwrap().rank() {
                        want = Some(sp);
                    }
                }
            }
        }
        let Some(sp) = want else {
            let mut err = ErrorResponse::new(Some("no supported subprotocol".into()));
            *err.status_mut() = StatusCode::BAD_REQUEST;
            return Err(err);
        };
        *self.chosen = Some(sp);
        response.headers_mut().insert(
            "sec-websocket-protocol",
            HeaderValue::from_static(sp.name()),
        );
        Ok(response)
    }
}
