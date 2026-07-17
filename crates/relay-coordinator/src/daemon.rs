// SPDX-License-Identifier: MIT

//! Coordinator daemon logic — a bounded semaphore over a Unix socket.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Semaphore;

/// Run the coordinator daemon.
///
/// Listens on `socket_path`, issuing at most `max_concurrent` permits
/// at once. Blocks until `shutdown` resolves.
pub async fn run(
    socket_path: PathBuf,
    max_concurrent: usize,
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
    Ok(())
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
    if writer.write_all(b"{\"status\":\"granted\"}\n").await.is_err() {
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
