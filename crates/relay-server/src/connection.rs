// SPDX-License-Identifier: MIT

//! Per-connection state machine for downstream WebSocket clients.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, StreamExt};
use tracing::{debug, info, warn};

use relay_engine::{CompileError, Engine, QuerySetId};
use relay_protocol::api_messages::websocket::v2::{ClientMessage, ServerMessage};
use relay_protocol::sats::bsatn;
use relay_storage::Storage;

use crate::bsatn_emit;
use crate::ServerHandle;

pub async fn run(
    socket: WebSocket,
    addr: SocketAddr,
    storage: Arc<Storage>,
    engine: Arc<Engine>,
    handle: ServerHandle,
) {
    let (mut tx, mut rx) = socket.split();
    let (identity, connection_id) = bsatn_emit::random_identity_and_connection();
    let (client_id, mut frames) = handle.register();
    let _guard = ConnectionGuard {
        engine: engine.clone(),
        handle: handle.clone(),
        client_id,
    };
    let downstream_identity = identity;

    info!(
        target: "relay::server",
        client = %addr,
        client_id = ?client_id,
        identity = %identity.to_hex().as_str(),
        connection_id = %connection_id.to_hex().as_str(),
        "downstream client connected"
    );

    let initial = bsatn_emit::initial_connection(identity, connection_id, "");
    if let Err(e) = send(&mut tx, &initial).await {
        warn!(target: "relay::server", error = %e, client = %addr, "send InitialConnection failed");
        return;
    }

    loop {
        tokio::select! {
            biased;
            msg = rx.next() => {
                let Some(msg) = msg else { break };
                let msg = match msg {
                    Ok(m) => m,
                    Err(e) => {
                        warn!(target: "relay::server", error = %e, client = %addr, "recv error");
                        break;
                    }
                };
                match msg {
                    Message::Binary(data) => {
                        if let Err(e) = handle_client_frame(
                            &data,
                            &storage,
                            &engine,
                            client_id,
                            downstream_identity,
                            &mut tx,
                        )
                        .await
                        {
                            warn!(target: "relay::server", error = %e, client = %addr, "handle frame failed");
                        }
                    }
                    Message::Close(_) => {
                        info!(target: "relay::server", client = %addr, "client closed");
                        break;
                    }
                    Message::Text(_) | Message::Ping(_) | Message::Pong(_) => {}
                }
            }
            frame = frames.recv() => {
                let Some(frame) = frame else { break };
                let msg = bsatn_emit::transaction_update_for_table(
                    frame.qset.0,
                    frame.table.as_ref(),
                    &frame.deletes,
                    &frame.inserts,
                );
                if let Err(e) = send(&mut tx, &msg).await {
                    warn!(target: "relay::server", error = %e, client = %addr, "send TransactionUpdate failed");
                    break;
                }
            }
        }
    }

    info!(target: "relay::server", client = %addr, "downstream client gone");
}

async fn handle_client_frame(
    data: &[u8],
    storage: &Arc<Storage>,
    engine: &Arc<Engine>,
    client_id: relay_engine::ClientId,
    sender: relay_engine::Identity,
    tx: &mut futures_util::stream::SplitSink<WebSocket, Message>,
) -> Result<(), HandleError> {
    if data.is_empty() {
        return Err(HandleError::Empty);
    }
    let msg: ClientMessage = bsatn::from_slice(data).map_err(|e| HandleError::Decode(e.to_string()))?;
    match msg {
        ClientMessage::Subscribe(sub) => {
            let n_queries = sub.query_strings.len();
            debug!(target: "relay::server", request_id = sub.request_id, n_queries, "got Subscribe");
            for q in sub.query_strings.iter() {
                let compiled = match engine.compile_for_sender(q, sender) {
                    Ok(c) => c,
                    Err(e) => {
                        warn!(
                            target: "relay::server",
                            request_id = sub.request_id,
                            query = %q,
                            error = %e,
                            "compile failed"
                        );
                        let err = bsatn_emit::subscription_error(
                            Some(sub.request_id),
                            sub.query_set_id.id,
                            &compile_error_message(&e),
                        );
                        send(tx, &err).await?;
                        return Ok(());
                    }
                };
                let table_name = compiled.table.clone();
                let snapshot = engine
                    .snapshot_for(storage, &compiled)
                    .await
                    .map_err(|e| HandleError::Engine(e.to_string()))?;
                let qset = QuerySetId(sub.query_set_id.id);
                engine.subscribe(client_id, qset, compiled);
                let applied = bsatn_emit::subscribe_applied(
                    sub.request_id,
                    sub.query_set_id.id,
                    table_name.as_ref(),
                    &snapshot,
                );
                send(tx, &applied).await?;
            }
        }
        ClientMessage::Unsubscribe(unsub) => {
            engine.unsubscribe(client_id, QuerySetId(unsub.query_set_id.id));
        }
        ClientMessage::OneOffQuery(_)
        | ClientMessage::CallReducer(_)
        | ClientMessage::CallProcedure(_) => {
            let err = bsatn_emit::subscription_error(
                None,
                0,
                "the relay only supports Subscribe/Unsubscribe; call reducers directly on SpacetimeDB",
            );
            send(tx, &err).await?;
        }
    }
    Ok(())
}

async fn send(
    tx: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    msg: &ServerMessage,
) -> Result<(), HandleError> {
    let bytes = bsatn_emit::frame(msg).map_err(|e| HandleError::Encode(e.to_string()))?;
    tx.send(Message::Binary(bytes))
        .await
        .map_err(|e| HandleError::Send(e.to_string()))?;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
enum HandleError {
    #[error("empty frame")]
    Empty,
    #[error("decode: {0}")]
    Decode(String),
    #[error("encode: {0}")]
    Encode(String),
    #[error("send: {0}")]
    Send(String),
    #[error("engine: {0}")]
    Engine(String),
}

fn compile_error_message(err: &CompileError) -> String {
    match err {
        CompileError::Parse(_)
        | CompileError::UnknownTable(_)
        | CompileError::UnknownColumn { .. }
        | CompileError::TypeMismatch { .. }
        | CompileError::BadLiteral { .. }
        | CompileError::UnresolvedSender => err.to_string(),
        CompileError::Unsupported(msg) => format!("unsupported: {msg}"),
    }
}

/// Drops the client's subscription registry entries and sender slot
/// when the connection task exits. Without this, an aborted task or
/// a client-side hang-up would leak engine state until restart.
struct ConnectionGuard {
    engine: Arc<Engine>,
    handle: ServerHandle,
    client_id: relay_engine::ClientId,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.engine.drop_client(self.client_id);
        self.handle.deregister(self.client_id);
    }
}

