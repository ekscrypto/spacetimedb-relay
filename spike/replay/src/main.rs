// SPDX-License-Identifier: MIT

//! Replay relay snapshot files into a local SpacetimeDB by calling the
//! per-table relay_insert_<table>(row: Vec<u8>) reducers.
//!
//! Snapshot file format (matches relay-storage::snapshot):
//!   64 ASCII bytes  schema fingerprint hex
//!    8 LE bytes     row count (u64)
//!   repeat:
//!      4 LE bytes  pk_len (u32) + pk bytes
//!      4 LE bytes  bsatn_len (u32) + bsatn bytes (the row's BSATN encoding)

use std::fs::File;
use std::io::{BufReader, Read};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::Semaphore;
use http::header::{HeaderName, HeaderValue, SEC_WEBSOCKET_PROTOCOL};
use spacetimedb_client_api_messages::websocket::v2::{CallReducer, CallReducerFlags, ClientMessage};
use spacetimedb_sats::bsatn;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::Message;
use url::Url;

const SUBPROTOCOL: &str = "v2.bsatn.spacetimedb";
const SCHEMA_HASH_LEN: usize = 64;

#[derive(Parser)]
struct Args {
    /// Directory holding *.snapshot files written by the relay.
    #[arg(long)]
    snapshot_dir: PathBuf,

    /// Local SpacetimeDB host (ws://… or http://… — converted internally).
    #[arg(long, default_value = "ws://127.0.0.1:3010")]
    target: Url,

    /// Database name on the local SpacetimeDB (the `spacetime publish` name).
    #[arg(long, default_value = "spike-mirror")]
    database: String,

    /// Optional: restrict replay to these table names.
    #[arg(long, value_delimiter = ',')]
    only: Vec<String>,

    /// Semaphore-based cap on in-flight CallReducer messages. The server
    /// caps incoming queue at 16384, so stay comfortably under that.
    #[arg(long, default_value_t = 8000)]
    in_flight: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let args = Args::parse();

    let snapshots = collect_snapshots(&args.snapshot_dir, &args.only)?;
    if snapshots.is_empty() {
        return Err(anyhow!("no snapshot files in {}", args.snapshot_dir.display()));
    }
    tracing::info!("found {} snapshot files", snapshots.len());

    let conn = open_connection(&args.target, &args.database).await?;
    let (mut sink, mut stream) = conn.split();
    let in_flight = Arc::new(Semaphore::new(args.in_flight));
    // Drain server responses on a background task. Each binary frame from
    // the server corresponds to one ReducerResult (or InitialConnection,
    // first message); release a permit per frame so the sender can keep
    // pushing. Permit balance might drift on the very first frame
    // (InitialConnection isn't an ack of any send), but a single extra
    // permit is harmless against a 16384-deep server queue.
    let in_flight_drain = in_flight.clone();
    let drain = tokio::spawn(async move {
        let mut errors_logged = 0;
        while let Some(msg) = stream.next().await {
            match msg {
                Ok(Message::Binary(_)) | Ok(Message::Text(_)) => {
                    in_flight_drain.add_permits(1);
                }
                Ok(_) => {}
                Err(e) => {
                    if errors_logged < 5 {
                        tracing::warn!("ws recv error: {e}");
                        errors_logged += 1;
                    }
                }
            }
        }
    });

    let request_id = AtomicU32::new(1);
    let mut grand_rows: u64 = 0;
    let mut grand_bytes: u64 = 0;
    let started = Instant::now();
    for path in &snapshots {
        let table = path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| anyhow!("bad snapshot path {}", path.display()))?
            .to_string();
        let reducer = format!("relay_insert_{table}");
        let t0 = Instant::now();
        let (rows, bytes) =
            replay_file(&mut sink, &reducer, path, &request_id, &in_flight).await?;
        grand_rows += rows;
        grand_bytes += bytes;
        tracing::info!(
            "{:<48} rows={:>8} bytes={:>12} elapsed_ms={:>6}",
            table,
            rows,
            bytes,
            t0.elapsed().as_millis(),
        );
    }
    // Politely close the sink so the drain task exits.
    let _ = sink.close().await;
    let _ = drain.await;
    tracing::info!(
        "done: {} rows, {} bytes BSATN replayed in {:.1}s",
        grand_rows,
        grand_bytes,
        started.elapsed().as_secs_f64(),
    );
    Ok(())
}

fn collect_snapshots(dir: &std::path::Path, only: &[String]) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).with_context(|| format!("read_dir {}", dir.display()))? {
        let e = entry?;
        let p = e.path();
        if p.extension().and_then(|s| s.to_str()) != Some("snapshot") {
            continue;
        }
        if !only.is_empty() {
            let stem = p.file_stem().and_then(|s| s.to_str()).unwrap_or("");
            if !only.iter().any(|n| n == stem) {
                continue;
            }
        }
        out.push(p);
    }
    out.sort();
    Ok(out)
}

type Conn = tokio_tungstenite::WebSocketStream<
    tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
>;

async fn open_connection(host: &Url, database: &str) -> Result<Conn> {
    let mut url = host.clone();
    let scheme = match url.scheme() {
        "http" => "ws",
        "https" => "wss",
        s => s,
    }
    .to_string();
    url.set_scheme(&scheme).map_err(|_| anyhow!("bad scheme"))?;
    url.set_path(&format!("/v1/database/{database}/subscribe"));
    url.set_query(Some("compression=None"));
    let mut req = url.as_str().into_client_request()?;
    req.headers_mut().insert(
        HeaderName::from_static("sec-websocket-protocol"),
        HeaderValue::from_static(SUBPROTOCOL),
    );
    let _ = SEC_WEBSOCKET_PROTOCOL; // keep import alive for clarity
    let (ws, _resp) = tokio_tungstenite::connect_async(req).await?;
    Ok(ws)
}

async fn replay_file<S>(
    sink: &mut S,
    reducer: &str,
    path: &std::path::Path,
    request_id: &AtomicU32,
    in_flight: &Arc<Semaphore>,
) -> Result<(u64, u64)>
where
    S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    let mut f = BufReader::new(File::open(path)?);
    let mut hdr = [0u8; SCHEMA_HASH_LEN];
    f.read_exact(&mut hdr)?;
    let mut count_buf = [0u8; 8];
    f.read_exact(&mut count_buf)?;
    let row_count = u64::from_le_bytes(count_buf);
    let mut rows = 0u64;
    let mut bytes = 0u64;
    while rows < row_count {
        let pk_len = read_u32(&mut f)?;
        let mut pk_buf = vec![0u8; pk_len as usize];
        f.read_exact(&mut pk_buf)?;
        let bsatn_len = read_u32(&mut f)?;
        let mut row_buf = vec![0u8; bsatn_len as usize];
        f.read_exact(&mut row_buf)?;
        bytes += bsatn_len as u64;

        // Reducer args = ProductValue([Vec<u8>]). BSATN encoding of Vec<u8>
        // is `[u32 len, raw bytes]`.
        let mut args = Vec::with_capacity(4 + row_buf.len());
        args.extend_from_slice(&(row_buf.len() as u32).to_le_bytes());
        args.extend_from_slice(&row_buf);

        let rid = request_id.fetch_add(1, Ordering::Relaxed);
        let msg = ClientMessage::CallReducer(CallReducer {
            request_id: rid,
            flags: CallReducerFlags::Default,
            reducer: reducer.into(),
            args: Bytes::from(args),
        });
        let frame = bsatn::to_vec(&msg).map_err(|e| anyhow!("encode CallReducer: {e}"))?;
        let permit = in_flight
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| anyhow!("semaphore: {e}"))?;
        permit.forget();
        sink.send(Message::Binary(frame))
            .await
            .map_err(|e| anyhow!("ws send: {e}"))?;
        rows += 1;
    }
    Ok((rows, bytes))
}

fn read_u32<R: Read>(r: &mut R) -> Result<u32> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b)?;
    Ok(u32::from_le_bytes(b))
}
