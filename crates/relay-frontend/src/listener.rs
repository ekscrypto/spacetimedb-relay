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

#[derive(Debug, Clone)]
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
    stream: TcpStream,
    peer: SocketAddr,
    cfg: Config,
    metrics: Arc<FrontendMetrics>,
    clients: ActiveClients,
) {
    let mut chosen: Option<Subprotocol> = None;
    let cb = SubprotocolNegotiator {
        chosen: &mut chosen,
    };
    let ws = match tokio_tungstenite::accept_hdr_async(stream, cb).await {
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
        let mut want = None;
        for h in request.headers().get_all("sec-websocket-protocol") {
            let s = match h.to_str() {
                Ok(s) => s,
                Err(_) => continue,
            };
            for p in s.split(',') {
                let p = p.trim();
                if let Some(sp) = Subprotocol::from_name(p) {
                    if want.is_none() || sp == Subprotocol::V2 {
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
