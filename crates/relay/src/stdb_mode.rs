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
use relay_coordinator::CoordinatorClient;
use relay_mirror_driver::{DriverConfig, DriverError, MetaRegistry, MirrorDriver};
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

    /// Optional coordinator client. When present the relay acquires a
    /// permit from the coordinator before each stdb reconnect and
    /// releases it once all sequential subscriptions are applied.
    /// `None` means uncoordinated — the stdb backoff alone governs
    /// reconnect spacing.
    pub coordinator: Option<CoordinatorClient>,

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
    // Hard timeout for joining the upstream task on reconnect. The task
    // normally exits within milliseconds of event_rx being dropped (its
    // parked events_tx.send() returns Err(ChannelClosed)); this backstop
    // only fires if the task is wedged inside tungstenite mid-message
    // reassembly, where no channel state change can reach it. Without it
    // the reconnect loop would hang forever — the 2026-07-17 stall.
    const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);
    let mut backoff_secs: u64 = 1;

    // Separate backoff for local-stdb reconnects. Starts at 5s so we
    // don't immediately re-flood an overloaded shared stdb; caps at 60s.
    // Reset to 5 whenever the stdb connection was healthy long enough
    // (i.e. the inner loop exited for a non-stdb reason).
    const STDB_BACKOFF_MAX_SECS: u64 = 60;
    let mut stdb_backoff_secs: u64 = 5;

    // Shared mirror of the driver's in_flight available-permits count,
    // updated by the applier task after each apply() call so the reconnect
    // loop's watchdog can detect saturation without owning the driver.
    // Matches DriverConfig::default().max_in_flight (8000).
    const MAX_IN_FLIGHT: usize = 8000;
    let apply_in_flight_available =
        std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(MAX_IN_FLIGHT));
    // Monotonic count of apply() calls completed since connect. Lets the
    // liveness watchdog distinguish "stdb is slow but making progress"
    // (permits saturated, counter advancing) from "stdb is dead" (permits
    // saturated AND counter frozen). Without this, a CPU-bound but healthy
    // stdb is misdiagnosed as a dead connection and force-reconnected,
    // which only adds load and worsens the slowdown.
    let apply_completed =
        std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));

    enum InnerExit {
        Shutdown,
        UpstreamGone,
        FrameLimitReached,
        ModuleDead,
        StdbConnectionDead,
    }

    // Sequential-mode state. Outside the reconnect loop so it persists
    // across reconnects — though re-subscribing from scratch on each
    // reconnect is the conservative behavior (BitCraft's per-table
    // initial dumps are inexpensive once we're past the 90 s wall).
    let mut sequential_progress = SequentialState {
        next_idx: 0,
        in_flight_query_id: None,
        all_applied: false,
    };

    // Holds the coordinator permit across a reconnect cycle. Acquired in
    // StdbConnectionDead before `continue 'reconnect`; released (dropped)
    // once all sequential subscriptions are applied, or on any exit that
    // doesn't restart the subscribe loop (Shutdown, UpstreamGone, etc.).
    let mut permit_opt: Option<relay_coordinator::ReconnectPermit> = None;

    'reconnect: loop {
        let (event_tx, mut event_rx) = mpsc::channel(256);
        let (cmd_tx, cmd_rx) = mpsc::channel(32);
        // Fresh apply-progress baseline for this connection attempt — the
        // watchdog compares apply_completed against the count captured at
        // the first saturated tick to decide whether stdb is advancing.
        apply_completed.store(0, std::sync::atomic::Ordering::Relaxed);
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
                all_applied: false,
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
        // Capture the driver's WS-dropped notify before moving the driver
        // into the applier task. Both the drainer (WS read error) and the
        // applier (connection-fatal apply error) signal through this — they
        // both mean the transport is dead and we should reconnect without
        // republishing. Clone so the select loop keeps its own handle.
        let ws_dropped = driver.ws_dropped_notify().clone();
        let ws_dropped_apply = ws_dropped.clone();
        // Capture the in-flight semaphore handle before moving the driver.
        // On dead-driver exit paths (StdbConnectionDead, ModuleDead) we close
        // the semaphore so the apply task can exit from acquire_owned() even
        // when all 8 K permits are consumed and the drainer has stopped
        // returning them — without this, apply_handle.await deadlocks.
        let in_flight_cancel = driver.in_flight_semaphore();
        let mut apply_driver = driver;
        let in_flight_mirror = apply_in_flight_available.clone();
        let apply_completed_mirror = apply_completed.clone();
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
                        // Distinguish connection-fatal errors (the WS sink
                        // is dead: Send, WebSocket, Closed) from per-table
                        // logic errors (Encode). A dead sink means the
                        // local-stdb connection is gone — the drainer has
                        // already exited and no responses will ever arrive,
                        // so the in_flight semaphore saturates and apply()
                        // parks forever. Signal the reconnect loop.
                        let is_connection_fatal = matches!(
                            e,
                            DriverError::Send(_) | DriverError::WebSocket(_) | DriverError::Closed
                        );
                        if is_connection_fatal {
                            tracing::error!(
                                target: "relay::stdb_mode",
                                table = %job.table,
                                error = %e,
                                "local-stdb apply connection-fatal — signalling reconnect"
                            );
                            in_flight_mirror.store(0, std::sync::atomic::Ordering::Relaxed);
                            ws_dropped_apply.notify_one();
                            break;
                        }
                        // Per-table logic error (e.g. a missing reducer from
                        // a schema mismatch). Log but do NOT tear down the
                        // connection — one bad table should not kill the
                        // subscription for all other tables.
                        tracing::warn!(
                            target: "relay::stdb_mode",
                            table = %job.table,
                            error = %e,
                            "apply failed for table — skipping, connection stays up"
                        );
                        continue;
                    }
                };
                // Mirror the driver's available permits for the watchdog.
                in_flight_mirror.store(
                    apply_driver.available_permits(),
                    std::sync::atomic::Ordering::Relaxed,
                );
                // Bump the progress counter so the watchdog can confirm the
                // apply pipeline is advancing — not just saturated.
                apply_completed_mirror.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                metrics_clone
                    .local_stdb
                    .record_traffic(stats.bytes_sent, stats.calls);
            }
            // Channel closed (reader dropped the sender) — return the
            // driver so the reconnect loop can reuse it.
            apply_driver
        });

        let mut connected_at: Option<std::time::Instant> = None;

        // Liveness watchdog: periodically check whether the local-stdb
        // connection is silently dead. The fingerprint of a dead WS is
        // all in-flight permits held (apply() acquired them, the drainer
        // that returns them is gone) with no progress. We check every
        // 15s; if fully saturated for two consecutive checks (30s) AND
        // apply_completed hasn't advanced in that window, the connection
        // is dead. The progress check is essential: a live but CPU-bound
        // stdb can sit fully saturated for >30s while still draining, and
        // tearing it down only adds re-subscribe load and worsens the
        // slowdown.
        let mut saturation_ticks: u32 = 0;
        let mut last_apply_count: Option<u64> = None;
        let mut watchdog = tokio::time::interval(Duration::from_secs(15));
        watchdog.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let exit_reason = loop {
            // Level-triggered check: if the module_dead watch is already
            // true (e.g. set while the select was parked on another arm),
            // bail immediately rather than waiting for an edge that will
            // never come. The edge-triggered changed() alone can miss the
            // signal when the watch coalesces two sends into one.
            if *module_dead.borrow() {
                break InnerExit::ModuleDead;
            }

            let event = tokio::select! {
                biased;
                _ = shutdown.as_mut() => break InnerExit::Shutdown,
                _ = ws_dropped.notified() => {
                    // The drainer detected a WS read error, or the applier
                    // detected a connection-fatal apply error. Either way
                    // the transport is dead — reconnect WITHOUT republishing
                    // (the WASM module is likely still alive).
                    break InnerExit::StdbConnectionDead;
                }
                r = module_dead.changed() => {
                    // Edge-triggered: fires when drain_responses detected
                    // a WASM fatal error ("The instance encountered a fatal
                    // error."). WS read errors now go to ws_dropped instead.
                    // Also fires (Err) if the sender was dropped (driver
                    // disconnected) — only treat as ModuleDead on Ok + true.
                    if r.is_ok() && *module_dead.borrow() {
                        break InnerExit::ModuleDead;
                    }
                    continue;
                }
                _ = watchdog.tick() => {
                    // in_flight saturation watchdog. If every permit is
                    // held (max_in_flight == 0 available) the drainer may
                    // be gone — but only declare the connection dead when
                    // permits stayed at 0 across two ticks (30s) AND no
                    // apply() completed in between. A live-but-slow stdb
                    // keeps completing apply() calls (slowly), advancing
                    // apply_completed; that path must NOT trigger a
                    // reconnect, since tearing down and re-subscribing all
                    // tables only adds load to an already-slow stdb.
                    let avail = apply_in_flight_available.load(std::sync::atomic::Ordering::Relaxed);
                    if avail == 0 {
                        let done = apply_completed.load(std::sync::atomic::Ordering::Relaxed);
                        match last_apply_count {
                            None => {
                                // First saturated tick: capture the progress
                                // baseline. The next tick decides whether stdb
                                // advanced.
                                last_apply_count = Some(done);
                                saturation_ticks = 1;
                            }
                            Some(prev) => {
                                if done == prev {
                                    // Still saturated AND frozen — stdb is dead.
                                    saturation_ticks += 1;
                                    if saturation_ticks >= 2 {
                                        tracing::error!(
                                            target: "relay::stdb_mode",
                                            available_permits = avail,
                                            apply_completed = done,
                                            "local-stdb in_flight fully saturated for 30s \
                                             with no apply progress — connection is dead, \
                                             forcing reconnect"
                                        );
                                        break InnerExit::StdbConnectionDead;
                                    }
                                } else {
                                    // Saturated but advancing — stdb is alive,
                                    // just behind. Leave it alone; reconnecting
                                    // would only add load.
                                    tracing::debug!(
                                        target: "relay::stdb_mode",
                                        available_permits = avail,
                                        apply_completed = done,
                                        prev_apply_completed = prev,
                                        "local-stdb in_flight saturated but apply is progressing — \
                                         leaving connection alone"
                                    );
                                    last_apply_count = Some(done);
                                    saturation_ticks = 1;
                                }
                            }
                        }
                    } else {
                        saturation_ticks = 0;
                        last_apply_count = None;
                    }
                    // Yield to the event arm to process a frame this iteration.
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
                    // Release the coordinator permit once all sequential
                    // subscriptions are applied — the heavy initial-sync
                    // flood is over and the next queued relay can proceed.
                    if sequential && sequential_progress.all_applied && permit_opt.is_some() {
                        let _ = permit_opt.take();
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
        //
        // Order is critical and was the root cause of the 2026-07-17
        // fleet stall (7 of 14 sources wedged for 30+ minutes). The
        // upstream task can be parked inside `events_tx.send(Frame).await`
        // against a full 256-cap event channel whose receiver — this
        // loop's `event_rx` — is still alive. While event_rx is alive the
        // send blocks forever instead of returning Err, so awaiting the
        // task hangs, and since the OneOffQuery probe is a select arm
        // *inside* that parked task, nothing ever recovers it.
        //
        // Fix: drop event_rx FIRST. That turns the parked send into an
        // immediate Err(ChannelClosed); the task returns via its
        // EventChannelClosed path and the join completes. The Shutdown
        // command is still sent as a graceful-close best effort for tasks
        // currently polling the select (they'll process it and return Ok).
        // The final `timeout` is a backstop for a task wedged inside
        // tungstenite mid-message reassembly — no channel signal can
        // reach it there, so we don't hang the reconnect loop on it.
        let _ = cmd_tx.send(UpstreamCommand::Shutdown).await;
        drop(cmd_tx);
        drop(event_rx);
        match tokio::time::timeout(SHUTDOWN_GRACE, upstream_handle).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::warn!(
                    target: "relay::stdb_mode",
                    error = %e,
                    "upstream task join failed after shutdown"
                );
            }
            Err(_) => {
                tracing::warn!(
                    target: "relay::stdb_mode",
                    grace_secs = SHUTDOWN_GRACE.as_secs(),
                    "upstream task did not exit within grace window — leaving it detached"
                );
                // The handle drops here without awaiting; tokio detaches
                // the task. It cannot affect this connection's channels
                // (all senders/receivers are dropped), so leaving it is
                // safe — at worst it eventually errors out on its own.
            }
        }

        // On dead-driver exit paths, close the in-flight semaphore BEFORE
        // dropping apply_tx / awaiting the apply task. If the apply task is
        // parked in acquire_owned() (all 8 K permits consumed, drainer gone),
        // dropping apply_tx alone is not enough — the task is not selecting on
        // the channel, it's waiting for a permit. close() makes acquire_owned()
        // return Err → DriverError::Closed → connection-fatal → task signals
        // ws_dropped and breaks. For UpstreamGone/Shutdown/FrameLimitReached
        // the driver is still live and the apply task drains naturally.
        if matches!(exit_reason, InnerExit::StdbConnectionDead | InnerExit::ModuleDead) {
            in_flight_cancel.close();
        }
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
                stdb_backoff_secs = 5;
                continue 'reconnect;
            }
            InnerExit::UpstreamGone => {
                let stayed_up = connected_at.map(|t| t.elapsed()).unwrap_or_default();
                if stayed_up >= STABLE_THRESHOLD {
                    backoff_secs = 1;
                }
                // Upstream died while stdb was healthy — stdb backoff can reset.
                stdb_backoff_secs = 5;
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
            InnerExit::StdbConnectionDead => {
                // The local-stdb WS connection died (TCP reset, sink
                // error, or in_flight saturation with no responses), but
                // the module itself is fine — no republish needed. Just
                // close the dead driver and reconnect with the same
                // identity. Unlike ModuleDead, the fingerprint and
                // published module are untouched.
                //
                // Mark BOTH links down here. The upstream task is being
                // torn down above (event_rx dropped, task joined) and will
                // reconnect after stdb comes back. Leaving upstream="up"
                // during this window is what produced the misleading
                // "up but 0 units_1m" /health state on 2026-07-17 — the
                // www stalled pill then flagged these as a real error
                // condition. Upstream recovers via the existing mark_up()
                // on the Connected event after reconnect.
                metrics
                    .upstream
                    .mark_down(Some("local-stdb reconnect".into()));
                metrics
                    .local_stdb
                    .mark_down(Some("connection dead — reconnecting".into()));
                let sleep_for = Duration::from_secs(stdb_backoff_secs);
                stdb_backoff_secs = (stdb_backoff_secs * 2).min(STDB_BACKOFF_MAX_SECS);
                tracing::warn!(
                    target: "relay::stdb_mode",
                    database = %cfg.mirror_database,
                    backoff_secs = sleep_for.as_secs(),
                    "local-stdb connection dead — reconnecting (no republish)"
                );
                let _ = driver.close().await;
                // driver is now consumed/closed. All subsequent shutdown exits in
                // this arm use `return Ok(())` rather than `break 'reconnect` so
                // the post-loop `driver.close()` is never reached with a moved driver.

                // Sleep BEFORE the reconnect attempt so stdb has time to recover
                // under load. Without this, MirrorDriver::connect() fires immediately
                // and races with other relays that hit the same dead-connection path.
                tokio::select! {
                    _ = shutdown.as_mut() => {
                        tracing::info!(target: "relay::stdb_mode", "shutdown during stdb backoff");
                        return Ok(());
                    }
                    _ = tokio::time::sleep(sleep_for) => {}
                }

                // Retry the connect in a loop rather than crashing. If stdb is
                // temporarily overloaded (e.g. another relay's initial sync is
                // flooding it), the connection attempt itself can time out. Keep
                // retrying with growing backoff until we succeed or are shut down.
                driver = loop {
                    let identity_token = if cfg.identity_token.is_some() {
                        cfg.identity_token.clone()
                    } else {
                        load_identity_token(&cfg.identity_token_file)
                    };
                    match MirrorDriver::connect(DriverConfig {
                        stdb_url: cfg.stdb_url.clone(),
                        database: cfg.mirror_database.clone(),
                        identity_token,
                        ..Default::default()
                    })
                    .await
                    {
                        Ok(d) => break d,
                        Err(e) => {
                            let retry_secs = stdb_backoff_secs;
                            stdb_backoff_secs =
                                (stdb_backoff_secs * 2).min(STDB_BACKOFF_MAX_SECS);
                            tracing::warn!(
                                target: "relay::stdb_mode",
                                database = %cfg.mirror_database,
                                error = %e,
                                retry_secs,
                                "local-stdb reconnect failed — retrying"
                            );
                            tokio::select! {
                                _ = shutdown.as_mut() => {
                                    tracing::info!(target: "relay::stdb_mode",
                                        "shutdown during stdb reconnect retry");
                                    return Ok(());
                                }
                                _ = tokio::time::sleep(Duration::from_secs(retry_secs)) => {}
                            }
                        }
                    }
                };
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
                    .context("relay_bind_writer after stdb reconnect")?;
                module_dead = driver.module_dead_receiver();
                // Reset the saturation watchdog and in_flight mirror.
                apply_in_flight_available.store(
                    MAX_IN_FLIGHT,
                    std::sync::atomic::Ordering::Relaxed,
                );

                tracing::info!(
                    target: "relay::stdb_mode",
                    database = %cfg.mirror_database,
                    "driver reconnected — restarting upstream subscription"
                );
                backoff_secs = 1;
                // Acquire a coordinator permit (if configured). This blocks until
                // the coordinator grants a slot — i.e. until no other relay is
                // mid-initial-sync. Falls back to uncoordinated if the daemon is absent.
                if sequential {
                    if let Some(ref client) = cfg.coordinator {
                        permit_opt = client.acquire().await;
                    }
                }
                continue 'reconnect;
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
    /// Set to `true` once all tables have received `SubscribeApplied`.
    /// The reconnect loop polls this to know when to release the
    /// coordinator permit.
    all_applied: bool,
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
                    sequential_progress.all_applied = true;
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
