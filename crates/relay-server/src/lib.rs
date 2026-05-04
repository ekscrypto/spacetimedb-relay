// SPDX-License-Identifier: MIT

//! Downstream WebSocket server.
//!
//! Mimics the SpacetimeDB v2 wire protocol so unmodified clients
//! using any official SDK can connect to the relay as if it were a
//! real SpacetimeDB instance. Each downstream connection only ever
//! receives data that was already mirrored from upstream — no
//! `CallReducer` or `OneOffQuery` is forwarded; clients that need to
//! mutate state must talk to the actual SpacetimeDB server directly.

mod bsatn_emit;
mod connection;

use std::net::SocketAddr;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use axum::extract::{ConnectInfo, Path, State, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use bytes::Bytes;
use dashmap::DashMap;
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tracing::{info, warn};

use relay_engine::{ClientId, ClientTableDiff, Engine, QuerySetId};

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("upstream database mismatch: configured {configured}, requested {requested}")]
    DatabaseMismatch {
        configured: String,
        requested: String,
    },
}

/// One filtered diff destined for one specific downstream connection.
/// The connection task encodes this into a v2 `TransactionUpdate`.
#[derive(Debug, Clone)]
pub struct ClientFrame {
    pub qset: QuerySetId,
    pub table: Arc<str>,
    pub deletes: Vec<Bytes>,
    pub inserts: Vec<Bytes>,
}

/// Bound on the per-client mpsc. Slow clients lose frames rather than
/// stalling the upstream-handling task. PR6 will revisit (close vs.
/// drop on overflow); for now, log + drop.
const CLIENT_CHANNEL_CAPACITY: usize = 256;

#[derive(Clone, Default)]
pub struct ServerHandle {
    inner: Arc<ServerHandleInner>,
}

#[derive(Default)]
struct ServerHandleInner {
    next_id: AtomicU64,
    senders: DashMap<ClientId, mpsc::Sender<ClientFrame>>,
}

impl ServerHandle {
    pub fn new() -> Self {
        Self::default()
    }

    /// Allocate a `ClientId` and return its receive end. The connection
    /// task should hold the receiver in its main loop and call
    /// `deregister(id)` when it exits (or rely on `Drop` on a guard).
    pub fn register(&self) -> (ClientId, mpsc::Receiver<ClientFrame>) {
        let id = ClientId::next(&self.inner.next_id);
        let (tx, rx) = mpsc::channel(CLIENT_CHANNEL_CAPACITY);
        self.inner.senders.insert(id, tx);
        (id, rx)
    }

    pub fn deregister(&self, id: ClientId) {
        self.inner.senders.remove(&id);
    }

    /// Fan one engine-routed diff out to its target client. Drops the
    /// frame on a full mpsc rather than blocking the caller; logs at
    /// `warn` so we don't lose the signal.
    pub fn deliver(&self, diff: ClientTableDiff) {
        let client = diff.client;
        let frame = ClientFrame {
            qset: diff.qset,
            table: diff.table,
            deletes: diff.deletes,
            inserts: diff.inserts,
        };
        let result = match self.inner.senders.get(&client) {
            Some(sender) => sender.try_send(frame),
            None => return,
        };
        match result {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                warn!(
                    target: "relay::server",
                    client = ?client,
                    "client mpsc full — dropping diff frame"
                );
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.inner.senders.remove(&client);
            }
        }
    }
}

#[derive(Clone)]
struct AppState {
    storage: Arc<relay_storage::Storage>,
    engine: Arc<Engine>,
    handle: ServerHandle,
    upstream_database: String,
}

pub async fn serve(
    bind: SocketAddr,
    storage: Arc<relay_storage::Storage>,
    engine: Arc<Engine>,
    upstream_database: String,
    handle: ServerHandle,
) -> Result<(), ServerError> {
    let state = AppState {
        storage,
        engine,
        handle,
        upstream_database,
    };
    let app = Router::new()
        .route("/v1/database/:name/subscribe", get(subscribe_handler))
        .with_state(state);
    let listener = TcpListener::bind(bind).await?;
    info!(target: "relay::server", %bind, "downstream server listening");
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;
    Ok(())
}

async fn subscribe_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    if name != state.upstream_database {
        warn!(
            target: "relay::server",
            requested = %name,
            configured = %state.upstream_database,
            "rejecting connection: database name mismatch"
        );
        return axum::http::StatusCode::NOT_FOUND.into_response();
    }
    ws.protocols(["v2.bsatn.spacetimedb"])
        .on_upgrade(move |socket| {
            connection::run(
                socket,
                addr,
                state.storage,
                state.engine,
                state.handle,
            )
        })
}
