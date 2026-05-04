// SPDX-License-Identifier: MIT

//! End-to-end test harness for spacetimedb-relay.
//!
//! Spawns two clients side-by-side:
//!
//!  * **subscriber** — connects to the local relay (D in the
//!    architecture diagram), subscribes to `SELECT * FROM user`, and
//!    waits for a `TransactionUpdate` whose row body contains a
//!    fixed test name.
//!  * **writer** — connects directly to the upstream SpacetimeDB
//!    server (C in the diagram), waits for `InitialConnection`, then
//!    calls the `set_name` reducer with that fixed test name.
//!
//! If the subscriber sees the new name within the timeout, the
//! propagation path
//!   `S -> R -> D`
//! is verified and the harness exits with status 0. Otherwise it
//! exits with status 1.
//!
//! Note on intent: this binary deliberately uses raw
//! `tokio-tungstenite` + `spacetimedb-client-api-messages` instead of
//! the `relay-upstream` crate, because **the relay never calls
//! reducers**. Re-using `relay-upstream` for the writer would muddle
//! that invariant in the architecture.

mod stdb_client;

use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use clap::Parser;
use rand::Rng;
use tracing_subscriber::EnvFilter;
use url::Url;

use crate::stdb_client::{
    call_reducer, encode_string_arg, expect_initial_connection, expect_subscribe_applied,
    open_connection, recv_server_message, send_subscribe,
};
use spacetimedb_client_api_messages::websocket::v2::ServerMessage;

#[derive(Debug, Parser)]
#[command(name = "relay-test-harness", version)]
struct Args {
    /// Upstream SpacetimeDB server URL (where C connects).
    #[arg(
        long,
        env = "TEST_UPSTREAM",
        default_value = "wss://maincloud.spacetimedb.com"
    )]
    upstream: Url,

    /// Local relay URL (where D connects).
    #[arg(long, env = "TEST_RELAY", default_value = "ws://127.0.0.1:3001")]
    relay: Url,

    /// SpacetimeDB database name (set via --database or TEST_DATABASE).
    #[arg(long, env = "TEST_DATABASE")]
    database: String,

    /// SpacetimeDB table to assert propagation through.
    #[arg(long, env = "TEST_TABLE", default_value = "user_account")]
    table: String,

    /// Reducer to invoke on the upstream.
    #[arg(long, env = "TEST_REDUCER", default_value = "set_name")]
    reducer: String,

    /// Optional fixed test name. If omitted, generates a random one
    /// per run so concurrent harness runs don't collide.
    #[arg(long, env = "TEST_NAME")]
    name: Option<String>,

    /// How long the subscriber waits for the propagated row (seconds).
    #[arg(long, env = "TEST_TIMEOUT_SECS", default_value_t = 30)]
    timeout_secs: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,relay_test_harness=debug")),
        )
        .init();

    let args = Args::parse();
    let name = args.name.clone().unwrap_or_else(|| {
        let suffix: u32 = rand::thread_rng().gen();
        format!("RelayHarness-{suffix:08x}")
    });

    tracing::info!(
        upstream = %args.upstream,
        relay = %args.relay,
        database = %args.database,
        table = %args.table,
        reducer = %args.reducer,
        test_name = %name,
        "starting end-to-end harness"
    );

    let timeout = Duration::from_secs(args.timeout_secs);
    let database = args.database.clone();
    let relay_url = args.relay.clone();
    let upstream_url = args.upstream.clone();
    let table = args.table.clone();
    let reducer = args.reducer.clone();
    let expected = name.clone();

    let subscriber = tokio::spawn(async move {
        run_subscriber(relay_url, database.clone(), table, expected, timeout).await
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    let writer = tokio::spawn(async move {
        run_writer(upstream_url, args.database.clone(), reducer, name).await
    });

    let writer_outcome = writer
        .await
        .map_err(|e| anyhow!("writer task panicked: {e}"))?;
    writer_outcome.map_err(|e| anyhow!("writer error: {e}"))?;
    tracing::info!("writer completed; waiting on subscriber");

    let subscriber_outcome = subscriber
        .await
        .map_err(|e| anyhow!("subscriber task panicked: {e}"))?;
    match subscriber_outcome {
        Ok(true) => {
            println!("PASS: relay propagated set_name from upstream to subscriber");
            Ok(())
        }
        Ok(false) => {
            eprintln!("FAIL: subscriber did not observe the expected name within timeout");
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("FAIL: subscriber error: {e}");
            std::process::exit(1);
        }
    }
}

async fn run_writer(upstream: Url, database: String, reducer: String, name: String) -> Result<()> {
    tracing::info!(target: "harness::writer", "connecting to upstream");
    let mut conn = open_connection(&upstream, &database).await?;
    let initial = expect_initial_connection(&mut conn).await?;
    tracing::info!(
        target: "harness::writer",
        identity = %initial.identity.to_hex().as_str(),
        "writer got InitialConnection"
    );

    let args_bsatn = encode_string_arg(&name);
    tracing::info!(
        target: "harness::writer",
        reducer = %reducer,
        name = %name,
        "calling reducer"
    );
    call_reducer(&mut conn, 1, &reducer, args_bsatn).await?;

    // We don't strictly need the ReducerResult to consider the call
    // successful — the upstream may take a moment to broadcast — but
    // pulling one frame off the socket lets us surface a server-side
    // failure in the harness output.
    match tokio::time::timeout(Duration::from_secs(10), recv_server_message(&mut conn)).await {
        Ok(Ok(ServerMessage::ReducerResult(rr))) => {
            tracing::info!(
                target: "harness::writer",
                request_id = rr.request_id,
                ?rr.result,
                "writer ReducerResult received"
            );
        }
        Ok(Ok(other)) => {
            tracing::info!(target: "harness::writer", ?other, "writer received non-ReducerResult");
        }
        Ok(Err(e)) => bail!("writer recv error: {e}"),
        Err(_) => {
            tracing::warn!(
                target: "harness::writer",
                "no ReducerResult within 10s; continuing anyway"
            );
        }
    }
    Ok(())
}

async fn run_subscriber(
    relay: Url,
    database: String,
    table: String,
    expected_name: String,
    timeout: Duration,
) -> Result<bool> {
    tracing::info!(target: "harness::subscriber", "connecting to relay");
    let mut conn = open_connection(&relay, &database).await?;
    let initial = expect_initial_connection(&mut conn).await?;
    tracing::info!(
        target: "harness::subscriber",
        identity = %initial.identity.to_hex().as_str(),
        "subscriber got InitialConnection"
    );

    let query = format!("SELECT * FROM {table}");
    send_subscribe(&mut conn, 100, 100, vec![query.clone()]).await?;
    let applied = expect_subscribe_applied(&mut conn).await?;
    let baseline_rows: usize = applied
        .rows
        .tables
        .iter()
        .map(|t| {
            use spacetimedb_client_api_messages::websocket::common::RowListLen;
            t.rows.len()
        })
        .sum();
    tracing::info!(
        target: "harness::subscriber",
        baseline_rows,
        "subscriber SubscribeApplied — searching baseline for expected name"
    );

    if any_row_contains_string(&applied, &expected_name) {
        tracing::info!(
            target: "harness::subscriber",
            "expected name already present in baseline (race or stale data) — counting as PASS"
        );
        return Ok(true);
    }

    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Ok(false);
        }
        let msg = match tokio::time::timeout(remaining, recv_server_message(&mut conn)).await {
            Ok(Ok(m)) => m,
            Ok(Err(e)) => return Err(anyhow!("subscriber recv error: {e}")),
            Err(_) => return Ok(false),
        };
        match msg {
            ServerMessage::TransactionUpdate(tu) => {
                tracing::info!(
                    target: "harness::subscriber",
                    n_query_sets = tu.query_sets.len(),
                    "subscriber TransactionUpdate"
                );
                if transaction_update_contains_string(&tu, &expected_name) {
                    return Ok(true);
                }
            }
            other => {
                tracing::debug!(target: "harness::subscriber", ?other, "subscriber other frame");
            }
        }
    }
}

fn any_row_contains_string(
    applied: &spacetimedb_client_api_messages::websocket::v2::SubscribeApplied,
    needle: &str,
) -> bool {
    use spacetimedb_client_api_messages::websocket::common::RowListLen;
    for table in applied.rows.tables.iter() {
        for i in 0..table.rows.len() {
            if let Some(row) = table.rows.get(i) {
                if bytes_contain(&row, needle.as_bytes()) {
                    return true;
                }
            }
        }
    }
    false
}

fn transaction_update_contains_string(
    tu: &spacetimedb_client_api_messages::websocket::v2::TransactionUpdate,
    needle: &str,
) -> bool {
    use spacetimedb_client_api_messages::websocket::common::RowListLen;
    use spacetimedb_client_api_messages::websocket::v2::TableUpdateRows;
    for set in tu.query_sets.iter() {
        for table in set.tables.iter() {
            for rows in table.rows.iter() {
                let lists: [&spacetimedb_client_api_messages::websocket::common::BsatnRowList; 2] =
                    match rows {
                        TableUpdateRows::PersistentTable(p) => [&p.inserts, &p.deletes],
                        TableUpdateRows::EventTable(e) => [&e.events, &e.events],
                    };
                for list in &lists {
                    for i in 0..list.len() {
                        if let Some(row) = list.get(i) {
                            if bytes_contain(&row, needle.as_bytes()) {
                                return true;
                            }
                        }
                    }
                }
            }
        }
    }
    false
}

fn bytes_contain(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() || haystack.len() < needle.len() {
        return false;
    }
    haystack.windows(needle.len()).any(|w| w == needle)
}
