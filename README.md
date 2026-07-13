# spacetimedb-relay

A relay/proxy for SpacetimeDB. One persistent subscription to an
upstream SpacetimeDB instance fans out to many downstream clients
without multiplying load on the game server.

The relay code-generates a mirror module at runtime, publishes it to a
sibling SpacetimeDB instance you run alongside the relay, and pipes
upstream inserts/deletes/updates onto that local SpacetimeDB.
**Downstream clients connect to the relay's frontend proxy** (default
`:3009`), which forwards each connection to the local SpacetimeDB on
loopback and — for v1 clients — synthesises full v1
`TransactionUpdate` frames from the local stdb's
`TransactionUpdateLight` broadcasts so subscribers see the upstream's
`reducer_call` and `caller_identity` rather than the relay's
internal `relay_apply_<table>` plumbing.

The frontend negotiates the same subprotocol the client offers
(**`v1.bsatn.spacetimedb`** or **`v2.bsatn.spacetimedb`**), and the
upstream side can speak either of those too (default `v2`, opt into
`v1` via `--upstream-protocol v1`).

> **Community project — not affiliated with Clockwork Labs.**
> SpacetimeDB™ is a trademark of [Clockwork Labs](https://clockworklabs.io/).

## Why

Public SpacetimeDB games (BitCraft, etc.) are rate-limited: studios
restrict how many third-party tools can hold subscriptions, because
every extra subscriber costs CPU on the live server. The relay holds
**one** subscription upstream and serves any number of downstream
clients from a sibling SpacetimeDB, so community tools (BitCraftMap,
BitCraftSync, BitJita, …) can share a single upstream cost.

Memory-wise this scales much better than the previous
in-process-mirror approach: SpacetimeDB stores rows ~3× their raw
BSATN size on disk; storing the same data as Rust structs in process
RAM with a struct-per-row mirror was closer to 20×.

## Architecture

```
    C ───────────► S            C calls reducers directly on S.
                   │
                   ▼
                   R ───► P ───► L         R subscribes to S; P
                                            (codegen + spacetime
                                            publish) materializes a
                                            mirror module on L (local
                                            SpacetimeDB on loopback).
                                  ▲
                                  │ loopback only
                                  F ◄──── D    D connects to F; F
                                               negotiates v1/v2 with D,
                                               proxies frames to L, and
                                               synthesises full v1 TUs
                                               from L's TULs for v1 D.
```

`R` = relay process. `S` = upstream SpacetimeDB. `L` = sibling
SpacetimeDB you run on the relay host (loopback-only). `P` =
publisher pipeline. `F` = frontend proxy (in-process with `R`,
default `0.0.0.0:3009`). `D` = downstream clients (game SDKs).

The relay never propagates `CallReducer` upstream. Clients that need
to mutate state call reducers directly on the upstream (`C → S` in
the diagram); the relay's job is read-only fan-out via `L` and `F`.

## Prerequisites

- **Rust toolchain 1.93** — pinned by `rust-toolchain.toml`. `rustup`
  picks it up automatically.
- **Python 3** — the publisher shells out to `tools/codegen.py`.
- **SpacetimeDB CLI** — install from
  <https://spacetimedb.com/install>. Used for both `spacetime start`
  (the local SpacetimeDB) and `spacetime publish` (publishing the
  mirror module).
- **Wasm target** — `rustup target add wasm32-unknown-unknown`.
- **A spacetime CLI server alias for the local SpacetimeDB.** Run
  once: `spacetime server add --url http://127.0.0.1:3010 relay-local`.
- **The same identity for both publish and runtime.** The mirror
  module's `init` reducer captures the publishing identity as the
  only authorized writer (`assert_writer` gate). Pass that identity's
  bearer token to the relay via `RELAY_STDB_IDENTITY_TOKEN`.

## Quick start

```sh
# 1. Start the local SpacetimeDB.
spacetime start --listen-addr 127.0.0.1:3010 --data-dir /var/lib/relay-stdb &

# 2. Register it with the spacetime CLI.
spacetime server add --url http://127.0.0.1:3010 relay-local

# 3. Build the relay (downloads the pinned toolchain on first run).
cargo build --release -p relay

# 4. Run the relay against your chosen upstream. The relay itself
#    runs the codegen + publish step on first connect.
RELAY_STDB_IDENTITY_TOKEN="<your spacetime CLI's logged-in token>" \
./target/release/relay \
    --upstream wss://maincloud.spacetimedb.com \
    --database <your-database-name-or-identity> \
    --stdb-url ws://127.0.0.1:3010 \
    --stdb-server-alias relay-local
```

Logs you should see, in order:
1. `schema loaded` — relay fetched the upstream schema
2. `mirror module ready republished=true` — publisher ran codegen +
   `cargo build` + `spacetime publish` (~50 s on a cold cache; no-op
   thereafter when the schema's fingerprint hasn't changed)
3. `bound writer on local stdb` — `relay_bind_writer` ack'd
4. `frontend listening bind=0.0.0.0:3009` — proxy ready for downstream
5. `upstream connected` — relay→upstream WebSocket up
6. `SubscribeApplied n_tables=…` — initial bulk-load is streaming
   `relay_apply_<table>` calls into the local SpacetimeDB

Downstream clients connect to the **frontend proxy**. In production on
`relay.bitcraftsync.app`, nginx terminates TLS in front of each loopback
relay, so the public address is
`wss://relay.bitcraftsync.app:<port>/v1/database/<mirror-database>/subscribe`
where `<port>` is `3000` (global) or `3000 + regionID`. See the next
section for v1 and v2 examples, and `PORTS.md` for the full exposure
scheme.

## Connecting downstream clients

The frontend negotiates whichever subprotocol the client offers in
`Sec-WebSocket-Protocol`. Both shapes are valid; pick based on what
your client SDK speaks. Local SpacetimeDB binds loopback-only; clients
should never address it directly.

### v2.bsatn.spacetimedb (default for current SDKs)

Plain passthrough plus per-client metrics. Subscribers see standard
v2 `TransactionUpdate` frames (rows-only — v2 broadcasts don't carry
reducer info, regardless of what's upstream).

```
GET /v1/database/<mirror-database>/subscribe?compression=None HTTP/1.1
Host: relay.bitcraftsync.app:<port>
Upgrade: websocket
Sec-WebSocket-Protocol: v2.bsatn.spacetimedb
```

Drop-in replacement for the official SpacetimeDB Rust/TS/C# SDK
configured against `wss://relay.bitcraftsync.app:<port>`. No code
changes.

### v1.bsatn.spacetimedb (full upstream-flavored TransactionUpdates)

Subscribers see full v1 `TransactionUpdate` frames whose
`reducer_call.{reducer_name,args,request_id}`, `caller_identity`,
`caller_connection_id`, and `timestamp` are sourced from the
**upstream** transaction that triggered the change — not the relay's
local `relay_apply_<table>` plumbing.

The proxy gets these by joining each local-stdb
`TransactionUpdateLight` against an in-process registry the
relay-mirror-driver populates with `(request_id, UpstreamReducerMeta)`
at the moment it sends the corresponding `CallReducer`. Effective
when the upstream is v1; against a v2 upstream the registry stores
`None` (no upstream meta available) and TULs pass through verbatim.

```
GET /v1/database/<mirror-database>/subscribe?compression=None HTTP/1.1
Host: relay.bitcraftsync.app:<port>
Upgrade: websocket
Sec-WebSocket-Protocol: v1.bsatn.spacetimedb
```

The dashboard's `frontend.lifetime_rewrites` counter increments by 1
for every TUL the proxy turns into a synthesised full TU.

### End-to-end smoke test (relay-test-harness)

The bundled harness exercises both paths against a running relay:

```sh
# v2 subscriber (passthrough) against the maincloud test database
cargo run -p relay-test-harness --release -- \
  --upstream wss://maincloud.spacetimedb.com \
  --database <upstream-db> \
  --via-frontend ws://127.0.0.1:3009 \
  --subscriber-protocol v2 \
  --table user_account --reducer set_name

# v1 subscriber (full TU synthesis) — same flags, different subprotocol
cargo run -p relay-test-harness --release -- \
  --upstream wss://maincloud.spacetimedb.com \
  --database <upstream-db> \
  --via-frontend ws://127.0.0.1:3009 \
  --subscriber-protocol v1 \
  --table user_account --reducer set_name

# v1 subscriber against a real v1 upstream — observes synthesised TUs
# carrying the upstream's actual reducer_name + caller_identity.
RELAY_UPSTREAM_TOKEN=$(cat .bitcraft-token) \
cargo run -p relay-test-harness --release -- \
  --upstream wss://bitcraft-early-access.spacetimedb.com \
  --database bitcraft-live-14 \
  --via-frontend ws://127.0.0.1:3009 \
  --subscriber-protocol v1 \
  --subscribe-only \
  --table chat_message_state \
  --timeout-secs 180
```

The last command, against BitCraft's live v1 server, prints frames
like:

```
★ FULL v1 TransactionUpdate (rewrite path lit up)
  reducer=chat_post_message
  caller_id=8c36065830b0…  args_len=28
```

— which is the upstream player's actual chat reducer, surfaced to a
v1 subscriber as if it had been talking to BitCraft directly.

## Configuration

| Flag                       | Env var                            | Default                                      | Notes |
|----------------------------|------------------------------------|----------------------------------------------|-------|
| `--upstream`               | `RELAY_UPSTREAM`                   | _required_                                   | e.g. `wss://maincloud.spacetimedb.com` |
| `--database`               | `RELAY_DATABASE`                   | _required_                                   | Database name or identity on the upstream |
| `--upstream-token`         | `RELAY_UPSTREAM_TOKEN`             | none                                         | Bearer token for upstream auth |
| `--upstream-protocol`      | `RELAY_UPSTREAM_PROTOCOL`          | `v2`                                         | `v1` for pre-2.0 SpacetimeDB |
| `--subscribe-table`        | `RELAY_SUBSCRIBE_TABLES`           | all user-public tables                       | Repeatable; comma-separated via env |
| `--subscribe-chunk-size`   | `RELAY_SUBSCRIBE_CHUNK_SIZE`       | `0`                                          | `0` = single set-replace `Subscribe` for all tables. `1` (v1 only) = sequential `SubscribeMulti` per table — required for large schemas like BitCraft, see ["Large databases"](#large-databases) below |
| `--frame-limit`            | `RELAY_FRAME_LIMIT`                | unlimited                                    | Stop after N upstream frames (smoke tests) |
| `--data-dir`               | `RELAY_DATA_DIR`                   | `data`                                       | Working directory; the publisher's mirror crate workdir lives under here |
| `--stdb-url`               | `RELAY_STDB_URL`                   | `ws://127.0.0.1:3000`                        | Local SpacetimeDB URL the relay publishes to and connects to |
| `--stdb-server-alias`      | `RELAY_STDB_SERVER_ALIAS`          | `local`                                      | spacetime CLI server alias for the local SpacetimeDB |
| `--mirror-database`        | `RELAY_MIRROR_DATABASE`            | `relay-mirror-<sanitized upstream>`          | Database name to publish under |
| `--stdb-identity-token`    | `RELAY_STDB_IDENTITY_TOKEN`        | none                                         | Bearer for the writer identity. **Required** unless this is the very first publish (init binds whoever called publish first) |
| `--publisher-workdir`      | `RELAY_PUBLISHER_WORKDIR`          | `<data-dir>/mirror-publisher`                | Where the generated mirror crate is materialized |
| `--publisher-template-dir` | `RELAY_PUBLISHER_TEMPLATE_DIR`     | `tools/mirror-template/` (relative to repo)  | Source `Cargo.toml` + `rust-toolchain.toml` |
| `--codegen-script`         | `RELAY_CODEGEN_SCRIPT`             | `tools/codegen.py` (relative to repo)        | Python codegen script |
| `--spacetime-bin`          | `RELAY_SPACETIME_BIN`              | `spacetime`                                  | Path to the SpacetimeDB CLI |
| `--frontend-bind`          | `RELAY_FRONTEND_BIND`              | `0.0.0.0:3009`                               | Public WS bind for downstream clients. Empty string disables the proxy |
| `--frontend-max-clients`   | `RELAY_FRONTEND_MAX_CLIENTS`       | `1024`                                       | Cap on concurrent downstream connections; excess are dropped at accept time |
| `--frontend-idle-secs`     | `RELAY_FRONTEND_IDLE_SECS`         | `30`                                         | Seconds between WS pings to keep idle TCP flows alive through middleboxes |
| `--dashboard-bind`         | `RELAY_DASHBOARD_BIND`             | `127.0.0.1:3001`                             | HTML + `/metrics` JSON endpoint. Empty string disables |

`RUST_LOG` controls log level (default `info`). For example,
`RUST_LOG=relay=trace,relay_publisher=debug cargo run -p relay …`.

## Workspace layout

| Crate                  | Purpose |
|------------------------|---------|
| `relay-protocol`       | Wire types; re-exports `spacetimedb-sats` and `spacetimedb-client-api-messages`. Hosts the shared `UpstreamReducerMeta` struct that the relay forwards to mirror reducers as the `original` arg. |
| `relay-upstream`       | Owns the single upstream WebSocket. Single un-split `tokio::select!` over read / 30 s idle Ping / cmd arms (matches the SpacetimeDB SDK). Emits a 2 s watchdog heartbeat with iteration counters on `relay::upstream::watchdog`. |
| `relay-publisher`      | Codegen → cargo build → `spacetime publish -y --delete-data`, keyed by SHA-256 fingerprint of the upstream schema. No-op when fingerprint unchanged. |
| `relay-mirror-driver`  | v2 WS client to local SpacetimeDB; sends `relay_apply_<table>(upstream, deletes, inserts)` calls with semaphore backpressure and chunking. Hosts the `MetaRegistry` the frontend reads to synthesise full v1 TUs. |
| `relay-frontend`       | Public-facing WS proxy: subprotocol negotiation, per-client metrics, hides `_relay_meta` traffic, and synthesises full v1 `TransactionUpdate`s from local stdb's `TransactionUpdateLight` broadcasts via the mirror-driver's meta registry. |
| `relay`                | Binary. Args, schema fetch, dashboard, dispatches to `stdb_mode`. |
| `relay-test-harness`   | Standalone v2 client; useful for end-to-end testing against either the local SpacetimeDB or a remote upstream. |
| `bc14-sdk-test`        | Standalone bin (excluded from the workspace) that vendors the v1.12.0 SpacetimeDB Rust SDK's WebSocket layer verbatim. Used to confirm large-scale subscribe issues against BitCraft are server/middlebox behavior, not relay-side regressions. See `crates/bc14-sdk-test/README.md`. |
| `tools/codegen.py`     | Schema JSON → Rust source for the mirror crate. Emits `#[table]` structs + four writer-gated reducers per table, each taking an `Option<UpstreamReducerInfo>` arg. |
| `tools/mirror-template/` | `Cargo.toml` + `rust-toolchain.toml` copied into the publisher's workdir. |
| `tools/fleet-status.sh` | Ops script for multi-instance hosts: auto-discovers every `relay-*` systemd unit, reads its dashboard port, and prints a per-instance sync-status table (upstream/stdb state, 1-min throughput). Run on the host: `./tools/fleet-status.sh` (one-shot) or `-w` to watch. |
| `tools/relay-fleet-start.sh` | Sequencer invoked by `relay-fleet-sequencer.service` at boot: waits for the shared stdb's `/v1/health`, then starts each relay one at a time (waiting for `upstream.state == "up"` before the next) so concurrent schema-drift rebuilds don't OOM the host. Idempotent — safe to re-run. |

## Large databases

For small schemas the default mode (`--subscribe-chunk-size 0`)
works: the relay sends one set-replace `Subscribe` covering every
table and the upstream replies with a single `InitialSubscription`.

For large v1 schemas — concretely BitCraft's 250 public-user tables,
about 1 GB of initial state — that single `InitialSubscription`
becomes a single multi-hundred-MB WebSocket message that the
upstream's edge consistently RSTs at ~90 s, before any client can
finish receiving it. We verified this against the official
SpacetimeDB Rust SDK; same TCP reset at the same byte mark.

Pass `--subscribe-chunk-size 1` to switch to **sequential
SubscribeMulti**: the relay subscribes to one table at a time, waits
for the per-table `SubscribeMultiApplied` (and applies its rows),
then advances. Each per-table dump fits comfortably under the 90 s
window even for the worst-case table (BitCraft's
`footprint_tile_state` is ~644 MB on its own and still completes
fine). Total time to ingest BitCraft 14's full 1 GB: ~8.5 minutes.

This mode is currently v1-only (the path that needs it). Use it
together with `--upstream-protocol v1`.

## Schema drift

When the upstream's schema changes (fingerprint differs from the last
publish recorded in `<workdir>/fingerprint.json`), the relay does a
**full wipe** of the local database and republishes from scratch:

- Codegen + cargo build with the new schema → new wasm.
- `spacetime publish -y --delete-data` drops the local database and
  reseeds it.
- Next `SubscribeApplied` from upstream gap-fills row data.

We never trust partial preservation across schema changes — the
upstream's migration semantics aren't visible to us, so any "looks
compatible" diff could leave stale rows mixed with fresh ones.
Downstream clients see a brief disconnect at the moment of republish
and a re-fill window of ~5–10 minutes on BitCraft-scale databases.

## Development

```sh
# Build everything.
cargo build

# Run all workspace tests.
cargo test

# Run a single crate's tests.
cargo test -p relay-publisher

# Lint and format (CI-equivalent).
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all -- --check

# Manually run the codegen + publish for debugging:
spacetime start --listen-addr 127.0.0.1:3010 --data-dir /tmp/stdb-data &
spacetime server add --url http://127.0.0.1:3010 relay-local
mkdir -p /tmp/mirror/src
cp tools/mirror-template/{Cargo.toml,rust-toolchain.toml} /tmp/mirror/
curl "https://maincloud.spacetimedb.com/v1/database/<db>/schema?version=9" \
    -o /tmp/upstream-schema.json
python3 tools/codegen.py /tmp/upstream-schema.json -o /tmp/mirror/src/lib.rs
(cd /tmp/mirror && cargo build --release --target wasm32-unknown-unknown)
(cd /tmp/mirror && spacetime publish -s relay-local -y relay-mirror)
```

The `spike/` directory holds the original validation work (codegen
Python, sample mirror crate, standalone replay binary). Useful as a
reference when refactoring codegen or the driver internals.

The `test-module/` directory contains a small SpacetimeDB module
useful for end-to-end testing. Publish it with:

```sh
cd test-module
spacetime publish -s <your-server> -y <your-database-name>
```

Architecture invariants and wire-protocol details live in
[`CLAUDE.md`](./CLAUDE.md).

## License

MIT. See [`LICENSE`](./LICENSE).
