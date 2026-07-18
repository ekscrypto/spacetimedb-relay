// SPDX-License-Identifier: MIT

//! Coordinator daemon logic — a bounded semaphore over a Unix socket,
//! plus a `/health` HTTP aggregator alongside it.
//!
//! The two responsibilities share a process (both are host-scoped fleet
//! utilities) but no state: the Unix-socket permit semaphore is wholly
//! independent of the `/health` aggregator's sources map and host
//! sampler. A failure in one cannot wedge the other.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use axum::extract::State;
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use axum::Router;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, UnixListener, UnixStream};
use tokio::sync::Semaphore;

use crate::health::HealthState;
use crate::sys_metrics::SysState;

/// Run the coordinator daemon.
///
/// Listens on `socket_path`, issuing at most `max_concurrent` permits
/// at once. When `health_bind` is `Some`, also serves `/health` and `/`
/// (the public dashboard) on that loopback address, with the sources
/// poller and host sampler running in the background. Blocks until
/// `shutdown` resolves.
pub async fn run(
    socket_path: PathBuf,
    max_concurrent: usize,
    health_bind: Option<SocketAddr>,
    unit_dir: PathBuf,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<()> {
    // Remove a stale socket from a previous run.
    if socket_path.exists() {
        std::fs::remove_file(&socket_path)?;
    }
    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let listener = UnixListener::bind(&socket_path)?;
    tracing::info!(
        target: "relay_coordinator",
        path = %socket_path.display(),
        max_concurrent,
        "coordinator listening"
    );

    let sem = Arc::new(Semaphore::new(max_concurrent));

    // Health aggregator: background poller + host sampler + HTTP server.
    // The poller runs one immediate pass at startup so /health is
    // populated within seconds of process start, then every 30s.
    let health_task = if let Some(bind) = health_bind {
        let sys = SysState::new();
        let health = HealthState::new(unit_dir.clone(), sys.clone());

        // Spawn the sources poller (drives the `sources` map) and the
        // host metrics sampler (drives `system.cpu` / `system.network`).
        // Each task owns its own shutdown future — they exit when the
        // coordinator does.
        {
            let h = health.clone();
            let shutdown = shutdown_signal_clone();
            tokio::spawn(async move {
                h.run_sources_poller(shutdown).await;
            });
        }
        {
            let s = sys.clone();
            let shutdown = shutdown_signal_clone();
            tokio::spawn(async move {
                s.run(shutdown).await;
            });
        }

        let app = Router::new()
            .route("/", get(index))
            .route("/health", get(health_json))
            .with_state(health);
        match TcpListener::bind(bind).await {
            Ok(tcp) => {
                tracing::info!(
                    target: "relay_coordinator",
                    %bind,
                    "health endpoint listening"
                );
                Some(tokio::spawn(async move {
                    if let Err(e) = axum::serve(tcp, app).await {
                        tracing::error!(
                            target: "relay_coordinator",
                            error = %e,
                            "health HTTP server exited"
                        );
                    }
                }))
            }
            Err(e) => {
                tracing::error!(
                    target: "relay_coordinator",
                    bind = %bind,
                    error = %e,
                    "health endpoint bind failed — /health disabled"
                );
                None
            }
        }
    } else {
        None
    };

    let mut shutdown = std::pin::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                tracing::info!(target: "relay_coordinator", "shutdown signal received");
                break;
            }
            accept = listener.accept() => {
                match accept {
                    Ok((stream, _addr)) => {
                        let sem = sem.clone();
                        tokio::spawn(async move {
                            handle_client(stream, sem).await;
                        });
                    }
                    Err(e) => {
                        tracing::error!(
                            target: "relay_coordinator",
                            error = %e,
                            "accept error"
                        );
                    }
                }
            }
        }
    }

    cleanup(&socket_path);
    // Drop the health task handle so it winds down with the process.
    // The spawned tasks observe their own shutdown futures firing; the
    // process is exiting regardless, so we don't await them here.
    drop(health_task);
    Ok(())
}

async fn health_json(State(state): State<HealthState>) -> impl IntoResponse {
    // no-store: index.html polls every 60s; never want a stale layer
    // (nginx, browser) masking a fresh snapshot.
    (
        [("Cache-Control", "no-store")],
        axum::Json(state.snapshot_json()),
    )
}

async fn index() -> impl IntoResponse {
    // The dashboard page is baked into the binary at build time. Path
    // is relative to this source file: crates/relay-coordinator/src/
    // → ../../www/index.html.
    Html(INDEX_HTML)
}

const INDEX_HTML: &str = include_str!("../../../www/index.html");

/// Independent shutdown future for spawned background tasks. Resolves
/// on SIGINT/SIGTERM just like the main `shutdown` arg, but each task
/// owns its own copy so they don't borrow from the main future.
fn shutdown_signal_clone() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
    Box::pin(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut term = match signal(SignalKind::terminate()) {
                Ok(s) => s,
                Err(_) => {
                    let _ = tokio::signal::ctrl_c().await;
                    return;
                }
            };
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = term.recv() => {}
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
    })
}

async fn handle_client(stream: UnixStream, sem: Arc<Semaphore>) {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    // Read the identify message (one line).
    let mut line = String::new();
    match reader.read_line(&mut line).await {
        Ok(0) | Err(_) => return, // peer closed immediately
        Ok(_) => {}
    }

    let relay_id = parse_relay_id(&line);
    let queue_depth = sem.available_permits();
    tracing::info!(
        target: "relay_coordinator",
        relay_id = %relay_id,
        queue_depth,
        "relay requesting permit"
    );

    // Acquire a slot — blocks until one is free.
    let _permit = match sem.acquire().await {
        Ok(p) => p,
        Err(_) => return, // semaphore closed (shutdown)
    };

    tracing::info!(
        target: "relay_coordinator",
        relay_id = %relay_id,
        "permit granted"
    );

    // Inform the client it may proceed.
    if writer
        .write_all(b"{\"status\":\"granted\"}\n")
        .await
        .is_err()
    {
        return; // client disconnected while waiting
    }

    // Hold the permit until the client disconnects (EOF on reader).
    // The client closes when its `ReconnectPermit` is dropped (initial
    // sync complete) or when the relay process crashes.
    let mut buf = String::new();
    loop {
        buf.clear();
        match reader.read_line(&mut buf).await {
            Ok(0) | Err(_) => break, // EOF or error → client released
            Ok(_) => {}              // future-proofing: ignore extra messages
        }
    }

    tracing::info!(
        target: "relay_coordinator",
        relay_id = %relay_id,
        "permit returned (client disconnected)"
    );
    // _permit drops here → slot back in the semaphore
}

fn parse_relay_id(line: &str) -> String {
    // Best-effort: extract the "relay_id" field value for logging.
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(line.trim()) {
        if let Some(id) = v.get("relay_id").and_then(|v| v.as_str()) {
            return id.to_string();
        }
    }
    "<unknown>".to_string()
}

fn cleanup(path: &Path) {
    if let Err(e) = std::fs::remove_file(path) {
        tracing::warn!(
            target: "relay_coordinator",
            path = %path.display(),
            error = %e,
            "failed to remove socket on shutdown"
        );
    }
}
