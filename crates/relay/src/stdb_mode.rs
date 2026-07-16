// SPDX-License-Identifier: MIT

//! Mirrors upstream tables into a sibling SpacetimeDB by calling
//! per-table `relay_apply_<table>` reducers on a generated mirror
//! module.
//!
//! Three components wired together:
//! * [`relay_publisher::Publisher`] — codegen + cargo build + spacetime
//!   publish, keyed by SHA-256 of the upstream schema JSON. On drift the
//!   *whole* local database is wiped (`--delete-data`); we never trust
//!   a partial preservation across schema changes (invariant #4).
//! * [`relay_mirror_driver::MirrorDriver`] — v2 WebSocket client that
//!   pushes `relay_apply_<table>(upstream, deletes, inserts)` calls to
//!   the local stdb with bounded in-flight backpressure.
//! * Upstream subscribe loop — opens one upstream WS, sends either a
//!   single set-replace `Subscribe` (default) OR a sequential
//!   `SubscribeMulti` per table (`--subscribe-chunk-size 1`, v1
//!   only — see CLAUDE.md "Subscribing at scale" for why this is
//!   required against BitCraft). Routes `SubscribeApplied` and
//!   `TransactionUpdate` rows into `driver.apply()`. Reconnects with
//!   exponential backoff on disconnect.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use relay_mirror_driver::{DriverConfig, MetaRegistry, MirrorDriver};
use relay_protocol::api_messages::websocket::v2::{ServerMessage, TableUpdateRows};
use relay_protocol::{MirroredSchema, UpstreamReducerMeta};
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
    /// Split the subscription into independent WS connections of this
    /// many tables each. `0` means a single connection covers all
    /// tables (legacy behavior). When non-zero, the relay opens
    /// `ceil(n_tables / chunk_size)` WS connections in parallel; each
    /// runs its own reconnect loop and applies frames into the shared
    /// `MirrorDriver`. This avoids the multi-hundred-MB single-message
    /// initial subscription that BitCraft's middlebox kills at ~90 s.
    pub subscribe_chunk_size: usize,

    /// Local SpacetimeDB target (e.g. `ws://127.0.0.1:3000`).
    pub stdb_url: Url,
    /// Database name to publish under (e.g. `relay-mirror-bitcraft-14`).
    pub mirror_database: String,
    /// Optional explicit Bearer token for the local-stdb connection.
    /// Usually `None` — the relay reads/writes
    /// `identity_token_file` instead so it captures the identity
    /// SpacetimeDB issued on the first connection. This field is only
    /// here for callers who want to override that flow.
    pub identity_token: Option<String>,
    /// File the relay persists the local-stdb identity token to.
    /// Loaded on startup; written after `connect()` if absent or stale.
    pub identity_token_file: PathBuf,
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
    raw_schema: Arc<[u8]>,
    schema: Arc<MirroredSchema>,
    metrics: Arc<Metrics>,
    meta_registry: Arc<MetaRegistry>,
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
    metrics
        .publisher
        .record(&outcome.fingerprint, outcome.republished);

    // 2. Open the WS link to local stdb. The token comes either from
    //    an explicit override on cfg.identity_token, or from a token
    //    file the relay persisted on a previous run. If neither is
    //    available we connect anonymously and capture the
    //    server-issued token from `InitialConnection`, so the next
    //    restart reconnects as the same identity.
    let identity_token = if cfg.identity_token.is_some() {
        cfg.identity_token.clone()
    } else {
        load_identity_token(&cfg.identity_token_file)
    };
    let mut driver = MirrorDriver::connect(DriverConfig {
        stdb_url: cfg.stdb_url.clone(),
        database: cfg.mirror_database.clone(),
        identity_token,
        ..Default::default()
    })
    .await
    .context("connect to local SpacetimeDB")?;
    driver.set_meta_registry(meta_registry.clone());
    metrics.local_stdb.mark_up();
    if let Some(captured) = driver.captured() {
        if let Err(e) = save_identity_token(&cfg.identity_token_file, &captured.token) {
            tracing::warn!(
                target: "relay::stdb_mode",
                path = %cfg.identity_token_file.display(),
                error = %e,
                "failed to persist local-stdb identity token"
            );
        } else {
            tracing::info!(
                target: "relay::stdb_mode",
                identity = %captured.identity_hex,
                path = %cfg.identity_token_file.display(),
                "persisted local-stdb identity token"
            );
        }
    }
    driver
        .bind_writer()
        .await
        .context("relay_bind_writer on local stdb")?;
    tracing::info!(target: "relay::stdb_mode", database = %cfg.mirror_database, "bound writer on local stdb");
    let mut module_dead = driver.module_dead_receiver();

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

    // 4. Reconnect loop. Single upstream connection. When
    //    `subscribe_chunk_size == 1` and the upstream is v1, we use
    //    sequential SubscribeMulti (one query at a time, additive)
    //    instead of one big set-replace Subscribe — avoiding
    //    BitCraft's ~90 s middlebox kill on multi-hundred-MB initial
    //    subscriptions.
    let upstream_cfg = UpstreamConfig {
        host: cfg.upstream_host.clone(),
        database: cfg.upstream_database.clone(),
        auth_token: cfg.upstream_token.clone(),
        compression: Compression::None,
        connect_timeout: Duration::from_secs(10),
        protocol: cfg.upstream_protocol,
    };

    let sequential = cfg.subscribe_chunk_size == 1 && cfg.upstream_protocol == ProtocolVersion::V1;
    if sequential {
        tracing::info!(
            target: "relay::stdb_mode",
            n_tables = subscribe_tables.len(),
            "sequential SubscribeMulti mode — one table at a time"
        );
    }

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
        ModuleDead,
    }

    // Sequential-mode state. Outside the reconnect loop so it persists
    // across reconnects — though re-subscribing from scratch on each
    // reconnect is the conservative behavior (BitCraft's per-table
    // initial dumps are inexpensive once we're past the 90 s wall).
    let mut sequential_progress = SequentialState {
        next_idx: 0,
        in_flight_query_id: None,
    };

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

        // On reconnect, start sequential subscribe over.
        if sequential {
            sequential_progress = SequentialState {
                next_idx: 0,
                in_flight_query_id: None,
            };
        }

        // --- Decoupled apply pipeline ---
        //
        // The upstream WS read path and the local-stdb apply path run on
        // separate tasks connected by an unbounded channel. The reader
        // task (the 'reconnect inner loop below) drains the upstream
        // socket immediately and pushes ApplyJobs to the channel without
        // ever blocking on the local stdb. The applier task owns the
        // MirrorDriver and drains the channel at its own pace.
        //
        // Why unbounded: if the apply channel were bounded and the
        // applier fell behind, the reader would block on channel.send(),
        // the upstream WS read buffer would fill, and the upstream would
        // kill the connection for being unresponsive — the exact failure
        // this decoupling prevents. Memory is bounded by Bytes
        // refcounts and the driver's own 8K in-flight semaphore.
        let (apply_tx, mut apply_rx) = mpsc::unbounded_channel::<ApplyJob>();
        let metrics_clone = metrics.clone();
        let mut apply_driver = driver;
        let apply_handle = tokio::spawn(async move {
            loop {
                let Some(job) = apply_rx.recv().await else {
                    break;
                };
                let stats = match apply_driver
                    .apply(&job.table, job.meta.as_ref(), job.deletes, job.inserts)
                    .await
                {
                    Ok(stats) => stats,
                    Err(e) => {
                        // Log but do NOT tear down the connection. One
                        // bad table (e.g. a missing reducer from a schema
                        // mismatch) should not kill the subscription for
                        // all other tables.
                        tracing::warn!(
                            target: "relay::stdb_mode",
                            table = %job.table,
                            error = %e,
                            "apply failed for table — skipping, connection stays up"
                        );
                        continue;
                    }
                };
                metrics_clone
                    .local_stdb
                    .record_traffic(stats.bytes_sent, stats.calls);
            }
            // Channel closed (reader dropped the sender) — return the
            // driver so the reconnect loop can reuse it.
            apply_driver
        });

        let mut connected_at: Option<std::time::Instant> = None;
        let exit_reason = loop {
            let event = tokio::select! {
                biased;
                _ = shutdown.as_mut() => break InnerExit::Shutdown,
                r = module_dead.changed() => {
                    // Fires when drain_responses detected a WASM fatal error.
                    // Also fires (Err) if the sender was dropped (driver
                    // disconnected) — only treat as ModuleDead on Ok + true.
                    if r.is_ok() && *module_dead.borrow() {
                        break InnerExit::ModuleDead;
                    }
                    continue;
                }
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
                        Ok((message, meta)) => {
                            // Lightweight: decode + extract apply jobs +
                            // advance subscribe state. Never blocks on
                            // the local stdb — all apply work goes to
                            // the channel.
                            dispatch_message(
                                message,
                                meta,
                                &subscribe_tables,
                                &cmd_tx,
                                &apply_tx,
                                &mut sequential_progress,
                                sequential,
                            )
                            .await;
                        }
                        Err(e) => tracing::warn!(
                            target: "relay::stdb_mode",
                            tag,
                            kind = server_tag_name(tag, protocol),
                            error = %e,
                            "failed to decode ServerMessage"
                        ),
                    }
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

        // Shut down the upstream connection.
        let _ = cmd_tx.send(UpstreamCommand::Shutdown).await;
        drop(cmd_tx);
        let _ = upstream_handle.await;

        // Drain the apply pipeline and recover the driver.
        drop(apply_tx);
        driver = match apply_handle.await {
            Ok(d) => d,
            Err(e) => {
                tracing::error!(
                    target: "relay::stdb_mode",
                    error = %e,
                    "apply task panicked — cannot recover driver"
                );
                return Err(anyhow!("apply task panicked: {e}"));
            }
        };

        match exit_reason {
            InnerExit::Shutdown => {
                tracing::info!(target: "relay::stdb_mode", "received shutdown signal");
                break 'reconnect;
            }
            InnerExit::FrameLimitReached => break 'reconnect,
            InnerExit::ModuleDead => {
                tracing::error!(
                    target: "relay::stdb_mode",
                    database = %cfg.mirror_database,
                    "local stdb module fatal — forcing republish and reconnect"
                );
                metrics
                    .upstream
                    .mark_down(Some("module dead — republishing".into()));
                metrics.local_stdb.mark_down(Some("module dead".into()));
                metrics.local_stdb.mark_module_dead();

                // Close the dead driver.
                let _ = driver.close().await;

                // Delete the cached fingerprint so publish_if_drifted
                // treats this as a fresh run and re-publishes unconditionally.
                let fp_path = cfg.publisher_workdir.join("fingerprint.json");
                if let Err(e) = std::fs::remove_file(&fp_path) {
                    tracing::warn!(
                        target: "relay::stdb_mode",
                        path = %fp_path.display(),
                        error = %e,
                        "could not remove fingerprint before force-republish"
                    );
                }

                let outcome = publisher
                    .publish_if_drifted(&raw_schema)
                    .await
                    .context("force republish after module death")?;
                tracing::info!(
                    target: "relay::stdb_mode",
                    republished = outcome.republished,
                    fingerprint = %outcome.fingerprint,
                    "force-republish complete"
                );
                metrics
                    .publisher
                    .record(&outcome.fingerprint, outcome.republished);

                // Reconnect driver with the same (persisted) identity token.
                let identity_token = if cfg.identity_token.is_some() {
                    cfg.identity_token.clone()
                } else {
                    load_identity_token(&cfg.identity_token_file)
                };
                driver = MirrorDriver::connect(DriverConfig {
                    stdb_url: cfg.stdb_url.clone(),
                    database: cfg.mirror_database.clone(),
                    identity_token,
                    ..Default::default()
                })
                .await
                .context("reconnect to local stdb after force-republish")?;
                driver.set_meta_registry(meta_registry.clone());
                metrics.local_stdb.mark_up();
                if let Some(captured) = driver.captured() {
                    if let Err(e) = save_identity_token(&cfg.identity_token_file, &captured.token) {
                        tracing::warn!(
                            target: "relay::stdb_mode",
                            error = %e,
                            "failed to persist identity token after reconnect"
                        );
                    }
                }
                driver
                    .bind_writer()
                    .await
                    .context("relay_bind_writer after force-republish")?;
                module_dead = driver.module_dead_receiver();

                tracing::info!(
                    target: "relay::stdb_mode",
                    database = %cfg.mirror_database,
                    "driver reconnected after force-republish — restarting upstream subscription"
                );
                backoff_secs = 1;
                continue 'reconnect;
            }
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

/// In sequential SubscribeMulti mode, drives the per-table state
/// machine: track which table index is next to subscribe and which
/// query_id we're awaiting `SubscribeApplied` for.
struct SequentialState {
    next_idx: usize,
    in_flight_query_id: Option<u32>,
}

/// One unit of work for the applier task: apply a batch of row changes
/// for a single table to the local stdb. Produced by the reader task
/// from decoded `SubscribeApplied` and `TransactionUpdate` frames.
struct ApplyJob {
    table: String,
    meta: Option<UpstreamReducerMeta>,
    deletes: Vec<Bytes>,
    inserts: Vec<Bytes>,
}

fn load_identity_token(path: &std::path::Path) -> Option<String> {
    match std::fs::read_to_string(path) {
        Ok(s) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => {
            tracing::warn!(
                target: "relay::stdb_mode",
                path = %path.display(),
                error = %e,
                "failed to read persisted identity token"
            );
            None
        }
    }
}

fn save_identity_token(path: &std::path::Path, token: &str) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    // Write to a temp file then rename so a crashed write never leaves
    // a partial token on disk.
    let tmp = path.with_extension("token.tmp");
    {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)?;
        use std::io::Write;
        f.write_all(token.as_bytes())?;
        f.write_all(b"\n")?;
        f.sync_all()?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
    }
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// Lightweight dispatch of a decoded upstream `ServerMessage`. Runs on
/// the reader task — must never block on the local stdb. Extracts row
/// data into `ApplyJob`s pushed to the apply channel, and handles
/// subscribe-state-machine advancement inline.
///
/// `apply_tx` is unbounded, so `send()` never blocks. This is what
/// keeps the upstream socket draining even when the local-stdb applier
/// is slow.
async fn dispatch_message(
    message: ServerMessage,
    meta: Option<UpstreamReducerMeta>,
    subscribe_tables: &[String],
    cmd_tx: &mpsc::Sender<UpstreamCommand>,
    apply_tx: &mpsc::UnboundedSender<ApplyJob>,
    sequential_progress: &mut SequentialState,
    sequential: bool,
) {
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
                if sequential {
                    if let Err(e) =
                        send_next_sequential(sequential_progress, subscribe_tables, cmd_tx).await
                    {
                        tracing::error!(
                            target: "relay::stdb_mode",
                            error = %e,
                            "failed to send first sequential subscribe"
                        );
                    }
                } else {
                    let queries: Vec<String> = subscribe_tables
                        .iter()
                        .map(|t| format!("SELECT * FROM {t}"))
                        .collect();
                    let _ = cmd_tx
                        .send(UpstreamCommand::Subscribe {
                            request_id: 1,
                            query_set_id: 1,
                            queries,
                        })
                        .await;
                }
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
            // Extract row data into apply jobs — no blocking apply here.
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
                    "queuing initial subscribe rows"
                );
                let _ = apply_tx.send(ApplyJob {
                    table: upstream_name.to_string(),
                    meta: None,
                    deletes: Vec::new(),
                    inserts,
                });
            }
            // Advance the sequential subscribe state machine
            // immediately — do NOT wait for the apply to finish before
            // requesting the next table. This lets the upstream send
            // the next table's data sooner and keeps the read path
            // moving.
            if sequential {
                sequential_progress.in_flight_query_id = None;
                if sequential_progress.next_idx < subscribe_tables.len() {
                    if let Err(e) =
                        send_next_sequential(sequential_progress, subscribe_tables, cmd_tx).await
                    {
                        tracing::error!(
                            target: "relay::stdb_mode",
                            error = %e,
                            "failed to send next sequential subscribe"
                        );
                    }
                } else {
                    tracing::info!(
                        target: "relay::stdb_mode",
                        n_tables = subscribe_tables.len(),
                        "all sequential subscriptions applied"
                    );
                }
            }
        }
        ServerMessage::TransactionUpdate(tu) => {
            // Extract per-table row changes into apply jobs.
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
                    let _ = apply_tx.send(ApplyJob {
                        table: upstream_name.to_string(),
                        meta: meta.clone(),
                        deletes,
                        inserts,
                    });
                }
            }
        }
        // Reducer/procedure results, errors, and other one-offs are
        // not propagated to the local stdb.
        _ => {}
    }
}

/// Send the next pending `SubscribeMulti` for the table at
/// `state.next_idx`; advance `next_idx` and arm `in_flight_query_id`.
async fn send_next_sequential(
    state: &mut SequentialState,
    subscribe_tables: &[String],
    cmd_tx: &mpsc::Sender<UpstreamCommand>,
) -> Result<()> {
    let idx = state.next_idx;
    let table = match subscribe_tables.get(idx) {
        Some(t) => t,
        None => return Ok(()),
    };
    // Use idx + 1 as both request_id and query_id — both are 1-based,
    // unique within the connection's lifetime, and trivially comparable
    // when SubscribeApplied lands.
    let id = (idx as u32) + 1;
    let query = format!("SELECT * FROM {table}");
    tracing::info!(
        target: "relay::stdb_mode",
        idx,
        n_tables = subscribe_tables.len(),
        table = %table,
        query_id = id,
        "subscribing to next table sequentially"
    );
    cmd_tx
        .send(UpstreamCommand::SubscribeOne {
            request_id: id,
            query_id: id,
            query,
        })
        .await
        .map_err(|_| anyhow!("upstream command channel closed"))?;
    state.in_flight_query_id = Some(id);
    state.next_idx = idx + 1;
    Ok(())
}
