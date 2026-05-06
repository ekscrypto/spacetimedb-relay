// SPDX-License-Identifier: MIT

//! Replacement for the legacy MemStore + downstream-WS path. Mirrors
//! upstream tables into a sibling SpacetimeDB by calling per-table
//! `relay_apply_<table>` reducers on a generated mirror module.
//!
//! Three components wired together:
//! * [`relay_publisher::Publisher`] — codegen + cargo build + spacetime
//!   publish, keyed by SHA-256 of the upstream schema JSON. On drift the
//!   *whole* local database is wiped (`--delete-data`); we never trust
//!   a partial preservation across schema changes (invariant #4).
//! * [`relay_mirror_driver::MirrorDriver`] — v2 WebSocket client that
//!   pushes `relay_apply_<table>(deletes, inserts)` calls to the local
//!   stdb with bounded in-flight backpressure.
//! * Upstream subscribe loop — mostly identical to the legacy path,
//!   but the per-message handler routes `SubscribeApplied`/
//!   `TransactionUpdate` rows straight to the driver instead of
//!   updating MemStore + the engine.

use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use relay_mirror_driver::{DriverConfig, MirrorDriver};
use relay_protocol::api_messages::websocket::v2::{ServerMessage, TableUpdateRows};
use relay_protocol::MirroredSchema;
use relay_publisher::{Publisher, PublisherConfig};
use relay_upstream::{
    connect_and_run, server_tag_name, Compression, ProtocolVersion, UpstreamCommand,
    UpstreamConfig, UpstreamEvent,
};
use tokio::sync::mpsc;
use url::Url;

use crate::dashboard::Metrics;

/// CLI-derived configuration for the stdb-backed mode.
#[derive(Debug, Clone)]
pub struct StdbModeConfig {
    pub upstream_host: Url,
    pub upstream_database: String,
    pub upstream_token: Option<String>,
    pub upstream_protocol: ProtocolVersion,
    pub frame_limit: Option<u64>,
    pub subscribe_tables: Vec<String>,

    /// Local SpacetimeDB target (e.g. `ws://127.0.0.1:3000`).
    pub stdb_url: Url,
    /// Database name to publish under (e.g. `relay-mirror-bitcraft-14`).
    pub mirror_database: String,
    /// Bearer token for the writer identity. The same identity that
    /// runs `spacetime publish` should be used here so the wasm
    /// `assert_writer` gate accepts our calls.
    pub identity_token: Option<String>,
    /// Server alias known to `spacetime` CLI (e.g. `relay-local`).
    pub stdb_server_alias: String,

    /// Where to materialize the generated mirror crate.
    pub publisher_workdir: PathBuf,
    /// Source `Cargo.toml` + rust-toolchain.toml for the mirror crate.
    pub publisher_template_dir: PathBuf,
    /// Path to `tools/codegen.py`.
    pub codegen_script: PathBuf,
    /// Path to the `spacetime` binary.
    pub spacetime_bin: PathBuf,
}

pub async fn run(
    cfg: StdbModeConfig,
    raw_schema: Vec<u8>,
    schema: Arc<MirroredSchema>,
    metrics: Arc<Metrics>,
) -> Result<()> {
    // 1. Publish the mirror module if the schema has drifted (or this
    //    is the first run). Wipes the whole local database on drift.
    let publisher = Publisher::new(PublisherConfig {
        workdir: cfg.publisher_workdir.clone(),
        template_dir: cfg.publisher_template_dir.clone(),
        codegen_script: cfg.codegen_script.clone(),
        spacetime_bin: cfg.spacetime_bin.clone(),
        stdb_server: cfg.stdb_server_alias.clone(),
        database_name: cfg.mirror_database.clone(),
    });
    let outcome = publisher
        .publish_if_drifted(&raw_schema)
        .await
        .context("publish mirror module")?;
    tracing::info!(
        target: "relay::stdb_mode",
        republished = outcome.republished,
        fingerprint = %outcome.fingerprint,
        "mirror module ready"
    );
    metrics.publisher.record(&outcome.fingerprint, outcome.republished);

    // 2. Open the WS link to local stdb and bind ourselves as the
    //    writer (idempotent if init already captured us).
    let mut driver = MirrorDriver::connect(DriverConfig {
        stdb_url: cfg.stdb_url.clone(),
        database: cfg.mirror_database.clone(),
        identity_token: cfg.identity_token.clone(),
        ..Default::default()
    })
    .await
    .context("connect to local SpacetimeDB")?;
    metrics.local_stdb.mark_up();
    driver
        .bind_writer()
        .await
        .context("relay_bind_writer on local stdb")?;
    tracing::info!(target: "relay::stdb_mode", database = %cfg.mirror_database, "bound writer on local stdb");

    // 3. Resolve the table list to subscribe to upstream — same logic
    //    as the legacy path: explicit `--subscribe-table` wins,
    //    otherwise default to all user-public tables.
    let mut subscribe_tables = cfg.subscribe_tables.clone();
    if subscribe_tables.is_empty() {
        subscribe_tables = schema
            .tables
            .iter()
            .filter(|t| matches!(t.kind, relay_protocol::TableKind::User))
            .filter(|t| matches!(t.access, relay_protocol::TableAccess::Public))
            .map(|t| t.name.clone())
            .collect();
        tracing::info!(
            target: "relay::stdb_mode",
            ?subscribe_tables,
            "no --subscribe-table given; defaulting to all user-public tables"
        );
    }

    // 4. Reconnect loop, lifted from the legacy path.
    let upstream_cfg = UpstreamConfig {
        host: cfg.upstream_host.clone(),
        database: cfg.upstream_database.clone(),
        auth_token: cfg.upstream_token.clone(),
        compression: Compression::None,
        connect_timeout: Duration::from_secs(10),
        protocol: cfg.upstream_protocol,
    };

    let shutdown = std::pin::pin!(crate::shutdown_signal());
    let mut shutdown = shutdown;
    let mut frames = 0u64;

    const BACKOFF_MAX_SECS: u64 = 30;
    const STABLE_THRESHOLD: Duration = Duration::from_secs(5);
    let mut backoff_secs: u64 = 1;

    enum InnerExit {
        Shutdown,
        UpstreamGone,
        FrameLimitReached,
    }

    'reconnect: loop {
        let (event_tx, mut event_rx) = mpsc::channel(256);
        let (cmd_tx, cmd_rx) = mpsc::channel(32);
        let attempt_cfg = upstream_cfg.clone();
        let upstream_handle = tokio::spawn(async move {
            if let Err(e) = connect_and_run(attempt_cfg, event_tx, cmd_rx).await {
                tracing::error!(
                    target: "relay::stdb_mode",
                    error = %e,
                    "upstream task ended with error"
                );
            }
        });

        let mut connected_at: Option<std::time::Instant> = None;
        let exit_reason = loop {
            let event = tokio::select! {
                biased;
                _ = shutdown.as_mut() => break InnerExit::Shutdown,
                event = event_rx.recv() => match event {
                    Some(e) => e,
                    None => break InnerExit::UpstreamGone,
                },
            };
            match event {
                UpstreamEvent::Connected => {
                    connected_at = Some(std::time::Instant::now());
                    metrics.upstream.mark_up();
                    tracing::info!(target: "relay::stdb_mode", "upstream connected");
                }
                UpstreamEvent::Frame(frame) => {
                    frames += 1;
                    let frame_bytes = frame.bsatn.len() as u64;
                    metrics.upstream.record_traffic(frame_bytes, 1);
                    let tag = frame.server_tag();
                    let protocol = frame.protocol;
                    match frame.decode() {
                        Ok(message) => {
                            if let Err(e) = handle_message(
                                message,
                                &subscribe_tables,
                                &cmd_tx,
                                &mut driver,
                                &metrics,
                            )
                            .await
                            {
                                tracing::error!(
                                    target: "relay::stdb_mode",
                                    error = %e,
                                    "failed to apply upstream message"
                                );
                                // Driver/stdb link is critical; bail out
                                // of the inner loop so we reconnect both
                                // ends.
                                metrics.local_stdb.mark_down(Some(format!("{e}")));
                                break InnerExit::UpstreamGone;
                            }
                        }
                        Err(e) => tracing::warn!(
                            target: "relay::stdb_mode",
                            tag,
                            kind = server_tag_name(tag, protocol),
                            error = %e,
                            "failed to decode ServerMessage"
                        ),
                    }
                    metrics.available_permits.store(
                        driver.available_permits() as u64,
                        Ordering::Relaxed,
                    );
                    if let Some(limit) = cfg.frame_limit {
                        if frames >= limit {
                            tracing::info!(target: "relay::stdb_mode", frames, "frame limit reached");
                            let _ = cmd_tx.send(UpstreamCommand::Shutdown).await;
                            break InnerExit::FrameLimitReached;
                        }
                    }
                }
                UpstreamEvent::Ping => {}
                UpstreamEvent::Disconnected { reason } => {
                    tracing::warn!(target: "relay::stdb_mode", %reason, "upstream disconnected");
                    metrics.upstream.mark_down(Some(reason.clone()));
                    break InnerExit::UpstreamGone;
                }
            }
        };

        let _ = cmd_tx.send(UpstreamCommand::Shutdown).await;
        drop(cmd_tx);
        let _ = upstream_handle.await;

        match exit_reason {
            InnerExit::Shutdown => {
                tracing::info!(target: "relay::stdb_mode", "received shutdown signal");
                break 'reconnect;
            }
            InnerExit::FrameLimitReached => break 'reconnect,
            InnerExit::UpstreamGone => {
                let stayed_up = connected_at.map(|t| t.elapsed()).unwrap_or_default();
                if stayed_up >= STABLE_THRESHOLD {
                    backoff_secs = 1;
                }
                let sleep_for = Duration::from_secs(backoff_secs);
                backoff_secs = (backoff_secs * 2).min(BACKOFF_MAX_SECS);
                tracing::warn!(
                    target: "relay::stdb_mode",
                    backoff_secs = sleep_for.as_secs(),
                    stayed_up_ms = stayed_up.as_millis() as u64,
                    "upstream gone — reconnecting after backoff"
                );
                tokio::select! {
                    _ = shutdown.as_mut() => {
                        tracing::info!(target: "relay::stdb_mode", "shutdown during backoff");
                        break 'reconnect;
                    }
                    _ = tokio::time::sleep(sleep_for) => {}
                }
            }
        }
    }

    let _ = driver.close().await;
    Ok(())
}

async fn handle_message(
    message: ServerMessage,
    subscribe_tables: &[String],
    cmd_tx: &mpsc::Sender<UpstreamCommand>,
    driver: &mut MirrorDriver,
    metrics: &Arc<Metrics>,
) -> Result<()> {
    match message {
        ServerMessage::InitialConnection(ic) => {
            tracing::info!(
                target: "relay::stdb_mode",
                identity = %ic.identity.to_hex().as_str(),
                connection_id = %ic.connection_id.to_hex().as_str(),
                token_len = ic.token.len(),
                "InitialConnection"
            );
            if !subscribe_tables.is_empty() {
                let queries: Vec<String> = subscribe_tables
                    .iter()
                    .map(|t| format!("SELECT * FROM {t}"))
                    .collect();
                cmd_tx
                    .send(UpstreamCommand::Subscribe {
                        request_id: 1,
                        query_set_id: 1,
                        queries,
                    })
                    .await
                    .map_err(|_| anyhow!("upstream command channel closed"))?;
            }
        }
        ServerMessage::SubscribeApplied(sa) => {
            tracing::info!(
                target: "relay::stdb_mode",
                request_id = sa.request_id,
                query_set_id = sa.query_set_id.id,
                n_tables = sa.rows.tables.len(),
                "SubscribeApplied"
            );
            for table in sa.rows.tables.iter() {
                let upstream_name: &str = table.table.as_ref();
                let inserts: Vec<Bytes> = table.rows.into_iter().collect();
                if inserts.is_empty() {
                    continue;
                }
                tracing::debug!(
                    target: "relay::stdb_mode",
                    table = %upstream_name,
                    rows = inserts.len(),
                    "applying initial subscribe rows"
                );
                let stats = driver
                    .apply(upstream_name, Vec::new(), inserts)
                    .await
                    .with_context(|| format!("driver.apply for {upstream_name}"))?;
                metrics
                    .local_stdb
                    .record_traffic(stats.bytes_sent, stats.calls);
            }
        }
        ServerMessage::TransactionUpdate(tu) => {
            for set in tu.query_sets.iter() {
                for table in set.tables.iter() {
                    let upstream_name: &str = table.table_name.as_ref();
                    let mut deletes: Vec<Bytes> = Vec::new();
                    let mut inserts: Vec<Bytes> = Vec::new();
                    for rows in table.rows.iter() {
                        match rows {
                            TableUpdateRows::PersistentTable(p) => {
                                deletes.extend(p.deletes.into_iter());
                                inserts.extend(p.inserts.into_iter());
                            }
                            TableUpdateRows::EventTable(e) => {
                                inserts.extend(e.events.into_iter());
                            }
                        }
                    }
                    if deletes.is_empty() && inserts.is_empty() {
                        continue;
                    }
                    let stats = driver
                        .apply(upstream_name, deletes, inserts)
                        .await
                        .with_context(|| format!("driver.apply for {upstream_name}"))?;
                    metrics
                        .local_stdb
                        .record_traffic(stats.bytes_sent, stats.calls);
                }
            }
        }
        // Reducer/procedure results, errors, and other one-offs are
        // not propagated to the local stdb.
        _ => {}
    }
    Ok(())
}
