# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project: spacetimedb-relay

A Rust relay/proxy for SpacetimeDB. One upstream subscription fans out
to many downstream clients by publishing a code-generated mirror
module to a sibling SpacetimeDB instance and replaying upstream
inserts/updates/deletes onto it. Downstream clients connect directly
to that local SpacetimeDB; SpacetimeDB itself handles fan-out, SQL
filtering, indexing, and on-disk persistence.

## Architecture in one diagram

```
    C ───────────► S            C calls reducers directly on S.
                   │
                   ▼
                   R ───► P ───► L         R subscribes to S; pipes
                                            rows to L (local SpacetimeDB)
                                            via P (publisher: codegen +
                                            spacetime publish).
                                  │
                                  ▼
                                  D         D subscribes to L; never
                                            reaches S.
```

`R` = relay process. `S` = upstream SpacetimeDB. `L` = sibling
SpacetimeDB on the relay host running a generated `relay-mirror-*`
module. `P` = publisher pipeline (codegen + cargo build + `spacetime
publish`). `D` = downstream clients (game SDKs etc.).

## Common commands

Toolchain pinned by `rust-toolchain.toml` (1.93, with `rustfmt` and
`clippy`). `rustup` picks it up automatically.

```sh
# Build the whole workspace
cargo build

# Run a single crate's tests
cargo test -p relay-publisher

# Lint / format (CI-equivalent)
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all -- --check

# Standing up a local SpacetimeDB and publishing the mirror module
# (the relay does this automatically on first run; manual recipe
# below for debugging):
spacetime start --listen-addr 127.0.0.1:3010 --data-dir /var/lib/relay-stdb &
spacetime server add --url http://127.0.0.1:3010 relay-local
python3 tools/codegen.py /tmp/upstream-schema.json -o /tmp/mirror/src/lib.rs
cp tools/mirror-template/{Cargo.toml,rust-toolchain.toml} /tmp/mirror/
(cd /tmp/mirror && cargo build --release --target wasm32-unknown-unknown)
(cd /tmp/mirror && spacetime publish -s relay-local -y --delete-data relay-mirror)

# Run the relay against the live BitCraft test region.
# `--subscribe-chunk-size 1` is REQUIRED for BitCraft (or any v1
# upstream with a large schema) — see "Subscribing at scale" below.
# RELAY_STDB_IDENTITY_TOKEN is optional — on first run the relay
# captures the local-stdb-issued token via InitialConnection and
# persists it under --data-dir; subsequent runs reuse it.
RELAY_UPSTREAM_TOKEN=$(cat .bitcraft-token) \
cargo run -p relay --release -- \
  --upstream wss://bitcraft-early-access.spacetimedb.com \
  --database bitcraft-live-14 \
  --upstream-protocol v1 \
  --subscribe-chunk-size 1 \
  --stdb-url ws://127.0.0.1:3010 \
  --stdb-server-alias relay-local \
  --mirror-database relay-mirror-bc14 \
  --data-dir /var/lib/relay-bc14

# Heap-profiling build: swaps the system allocator for dhat::Alloc
# and writes `dhat-heap.json` to CWD on graceful shutdown.
# Off by default.
cargo run -p relay --features profile-heap -- ...

# Speak v2 directly to either the local SpacetimeDB or the upstream
cargo run -p relay-test-harness -- <args>
```

Other relay flags worth knowing:
- `--data-dir` (env `RELAY_DATA_DIR`, default `data/`) — workdir for
  state safe to lose. Publisher workdir defaults to
  `<data-dir>/mirror-publisher`; persisted identity token defaults to
  `<data-dir>/relay-stdb-identity.token`.
- `--dashboard-bind` (env `RELAY_DASHBOARD_BIND`, default
  `127.0.0.1:3001`) — see "Dashboard" below. Empty string disables.
- `--subscribe-table` (env `RELAY_SUBSCRIBE_TABLES`, comma-delimited,
  repeatable) — restrict the upstream subscription set.
- `--subscribe-chunk-size` (env `RELAY_SUBSCRIBE_CHUNK_SIZE`,
  default `0`) — see "Subscribing at scale" below.
- `--frame-limit` (env `RELAY_FRAME_LIMIT`) — stop after N upstream
  frames; useful for smoke tests.

## Architecture invariants

These shape every change. Don't break them without explicit instruction.

0. **The relay never calls reducers on the upstream.** The R→S
   connection (in `relay-upstream`) only sends `Subscribe` /
   `Unsubscribe`. Downstream clients call upstream reducers themselves
   over their own C→S connection. Reducer calls from the relay
   process do happen — but only against the **local** SpacetimeDB,
   for the purpose of writing the mirror.
1. **One upstream subscription, many downstream clients.** There is
   exactly one R→S WebSocket per (upstream DB, table set). The whole
   architecture exists to amortize that one subscription across an
   arbitrary number of D→L connections.
2. **Relay binary is schema-agnostic at compile time.** The mirror
   module is per-database (codegen reads `RawModuleDefV9` / `V10` from
   the upstream's `/v1/database/<name>/schema` and emits a Rust crate
   to publish), but the relay binary itself never has any
   game-specific types. The codegen + publish runs at relay startup.
3. **Wire-protocol parity downstream.** Third-party clients see an
   unmodified SpacetimeDB v2 WebSocket: they're literally talking to
   SpacetimeDB. No relay-specific handshake or message extension.
4. **Schema drift = full wipe.** When the upstream schema's
   fingerprint changes, the relay republishes the mirror module with
   `spacetime publish --delete-data`, which drops the entire local
   database and reseeds from the next `SubscribeApplied`. We never
   trust partial preservation, because the upstream's migration
   semantics are opaque to us.
5. **Local SpacetimeDB is the canonical mirror state.** The relay
   process holds no row data of its own — it pipes upstream events
   straight into `relay_apply_<table>` reducers. SpacetimeDB owns
   storage, indexing, durability, fan-out, and SQL.
6. **Writer-identity auth on the mirror module.** Every codegen'd
   `relay_insert/delete/update/apply_<table>` reducer starts with
   `assert_writer(ctx)?`. The `init` lifecycle reducer captures the
   publisher's identity into the private `_relay_meta` singleton; only
   that identity may write the mirror. Downstream clients connecting
   to the local SpacetimeDB cannot forge writes.

## Upstream protocol versions

Defaults to **`v2.bsatn.spacetimedb`**. Pre-2.0 SpacetimeDB servers
(≤ v1.12.x) only accept `v1.bsatn.spacetimedb`; pass
`--upstream-protocol v1` (or `RELAY_UPSTREAM_PROTOCOL=v1`).

When v1 is selected:

- Handshake offers `v1.bsatn.spacetimedb`.
- Decoded v1 `ServerMessage`s are translated to the v2 shape inside
  `relay-upstream::v1_compat`, so `relay-mirror-driver` and
  `relay/src/stdb_mode.rs` stay v2-only.
- Outbound `Subscribe` is encoded as v1's set-replace
  `Subscribe { query_strings, request_id }` (no `QuerySetId`).
- Outbound `SubscribeMulti` (v1, additive) is also supported — used
  exclusively in sequential subscribe mode (`--subscribe-chunk-size 1`,
  see below). The single-query form is encoded by
  `v1_compat::encode_subscribe_multi`.
- Per-table compression (`CompressableQueryUpdate::Brotli`/`Gzip`) is
  ignored; we always ask for `?compression=None`.
- `IdentityToken` (v1) maps to v2's `InitialConnection`.
- `InitialSubscription` (v1) maps to v2's `SubscribeApplied` with
  synthetic `request_id = 1` and `query_set_id = 1`.
- `SubscribeMultiApplied` (v1) maps to v2's `SubscribeApplied` with
  the original `request_id` and `query_id` (in `query_set_id`).

Reference: `crates/client-api-messages/src/websocket.rs` in
clockworklabs/SpacetimeDB at tag `v1.12.0`. We pin that version of
`spacetimedb-client-api-messages` as a separately-named workspace dep.

## Subscribing at scale

For small schemas, the default mode (`--subscribe-chunk-size 0`)
sends a single set-replace `Subscribe` with all tables and the
upstream replies with one `InitialSubscription` covering the entire
working set. Simple, fast.

For large v1 schemas (e.g. BitCraft's 250 public-user tables — about
1 GB of initial state today), that single `InitialSubscription`
becomes a single multi-hundred-MB WS message that BitCraft's edge
RSTs at ~90 s — well before any client (verified including the
official SpacetimeDB Rust SDK at v1.12.0; see `crates/bc14-sdk-test/`)
can finish receiving it. We confirmed this empirically: every variant
of our client and the SDK itself hit the same TCP RST at the same
byte mark.

The fix: `--subscribe-chunk-size 1` activates **sequential
SubscribeMulti** mode. The relay sends one `SubscribeMulti` per
table, waits for `SubscribeMultiApplied`, applies the rows, then
moves to the next. Each per-table InitialSubscription fits
comfortably under the 90 s window even for the worst behemoth
(`footprint_tile_state` is ~644 MB on its own — still completes).

State machine lives in `stdb_mode.rs::SequentialState`; advances on
each `SubscribeApplied` and emits `"all sequential subscriptions
applied"` when done. On reconnect, restarts from index 0 — per-table
dumps are cheap once we're past the multi-hundred-MB single-message
wall.

Currently this mode is v1-only (the path we need; v2 callers can
add a similar `SubscribeMulti` encoding if it ever matters).

## Wire protocol — v2 message tags

```
ClientMessage (relay → local SpacetimeDB)   ServerMessage (upstream → relay)
  0x00 Subscribe                              0x00 InitialConnection
  0x01 Unsubscribe                            0x01 SubscribeApplied
  0x02 OneOffQuery                            0x02 UnsubscribeApplied
  0x03 CallReducer                            0x03 SubscriptionError
  0x04 CallProcedure                          0x04 TransactionUpdate
                                              0x05 OneOffQueryResult
                                              0x06 ReducerResult
                                              0x07 ProcedureResult
```

Source of truth: `clockworklabs/SpacetimeDB`,
`crates/client-api-messages/src/websocket/v2.rs`.

## Crate layout

| Crate                  | Purpose |
|------------------------|---------|
| `relay-protocol`       | Wraps `spacetimedb-sats` + `spacetimedb-client-api-messages`. Wire types only — no I/O. Hosts the shared `UpstreamReducerMeta` struct that gets forwarded as `relay_apply_<table>`'s `original` arg. |
| `relay-upstream`       | Owns the single upstream WebSocket. Un-split socket pattern (single `tokio::select!` on `&mut sock` for read / 30 s idle Ping / cmd arms — matches the SpacetimeDB SDK). Decodes ServerMessages and exposes them as `UpstreamEvent`. Emits a 2 s watchdog heartbeat with iteration / frame counters on `relay::upstream::watchdog`. |
| `relay-publisher`      | Codegen → cargo build → `spacetime publish -y --delete-data`, keyed by SHA-256 of the upstream schema JSON. No-op when the fingerprint hasn't changed. |
| `relay-mirror-driver`  | v2 WS client to local SpacetimeDB. Sends `relay_apply_<table>(upstream, deletes, inserts)` calls with semaphore backpressure (≤8 K in-flight) and chunking by row count + payload bytes. |
| `relay`                | Binary. Args, schema fetch, dashboard, dispatches to `stdb_mode`. The `stdb_mode.rs` run loop drives publisher → driver → `relay_bind_writer` → upstream subscribe (set-replace OR sequential SubscribeMulti) → routes `SubscribeApplied` + `TransactionUpdate` into `driver.apply()`. |
| `relay-test-harness`   | Standalone binary that plays the C/D role. Speaks v2 directly via `spacetimedb-client-api-messages`. |
| `bc14-sdk-test`        | Standalone bin (excluded from the workspace). Vendors v1.12.0 SDK's `websocket.rs` + `compression.rs` verbatim with `pub` accessors, no codegen. Used to prove that BitCraft's 90 s RST on a 250-table set-replace is server/middlebox behavior, not a client-side bug. See `crates/bc14-sdk-test/README.md`. |
| `tools/codegen.py`     | Schema JSON → Rust source for the mirror crate. Emits `#[table]` structs + four writer-gated reducers per table (`relay_insert/delete/update/apply_<table>`), each taking an `Option<UpstreamReducerInfo>` arg that downstream subscribers see in `ctx.event.reducer.args`. |
| `tools/mirror-template/` | Cargo.toml + rust-toolchain.toml copied into the runtime workdir before each codegen run. |

## Mirror module + writer auth

Every code-generated mirror module includes a fixed scaffold:

```rust
#[spacetimedb::table(name = "_relay_meta", accessor = relay_meta)]
struct RelayMetaRow { #[primary_key] id: u8, writer: Identity }

#[spacetimedb::reducer(init)]
fn relay_init(_ctx: &ReducerContext) {
    /* no-op: writer is captured by the first relay_bind_writer call,
       not by whichever identity ran `spacetime publish` */
}

#[spacetimedb::reducer]
pub fn relay_bind_writer(ctx: &ReducerContext) -> Result<(), Box<str>> {
    /* first call inserts ctx.sender() as the writer.
       subsequent calls: same identity → ok; different → "unauthorized" */
}

fn assert_writer(ctx: &ReducerContext) -> Result<(), Box<str>> {
    /* errors with "unauthorized" if ctx.sender() != _relay_meta.writer */
}
```

Identity-binding flow at runtime:

1. The relay opens its first WS to the local SpacetimeDB. The local
   stdb sends back `InitialConnection { identity, token }` — that's
   the identity the local stdb has issued for this connection.
2. The relay persists that token to
   `<data-dir>/relay-stdb-identity.token` (atomic rename, chmod 600)
   and uses it for all subsequent connections.
3. The relay calls `relay_bind_writer` over that connection. The
   freshly-issued identity is sealed into `_relay_meta.writer`.
4. On restart, the relay loads the persisted token, reconnects as the
   same identity, and `relay_bind_writer` is a no-op (already bound).

`--stdb-identity-token` / `RELAY_STDB_IDENTITY_TOKEN` is an **optional
override**: pass it when you want the relay to bind as a specific
pre-existing identity (e.g. the spacetime CLI's logged-in identity)
rather than a fresh one. Not required for first run.

## Dashboard

The relay binary serves an in-process dashboard plus a `/metrics` and
`/events` JSON endpoint at `--dashboard-bind` (default
`127.0.0.1:3001`; set to empty string to disable).

Panels:

- Upstream and local-stdb link state.
- Sliding 1m / 5m / 30m windows for inbound bytes and frame counts.
- Driver in-flight permits (used / max).
- Publisher fingerprint and timestamp of last republish.
- **Live log** — tail of every `relay::*` tracing event, filterable
  by substring. Captured by an in-process `tracing_subscriber::Layer`
  that respects its own `EnvFilter::new("relay=debug")` so the
  dashboard always shows debug-level relay events without restarting
  with verbose `RUST_LOG`. Ring buffer holds the last 50 000 events
  (~12 minutes of BitCraft traffic at ~64 events/s); browser polls
  `/events?since=N&max=200` every 1 s.

Source: `crates/relay/src/dashboard.rs` + `dashboard.html`.

## Historical / superseded docs

- `MEMORY-MIGRATION.md` — describes an abandoned in-process memstore
  plan that was replaced by the SpacetimeDB-mirror architecture
  documented here. Kept for reference only; do not treat as current.

## Reference: live test database

Identity, schema, reducers, and republish command live in
**`CLAUDE.local.md`** (gitignored). Test module source is in
`test-module/` (excluded from the workspace).

## Reference: BitCraft live game server (EA2)

Fan-research lives in **`BITCRAFT.md`**: meta host, region routing,
`api.bitcraftonline.com` auth flow, JWT extraction, etc.

## Reference: Swift SDK

`../spacetimedb-swift-sdk/` — same wire protocol, useful for
cross-checking encoding decisions.
- `Sources/SpacetimeDB/Tags.swift` — message tag values
- `Sources/SpacetimeDB/Server Messages/BsatnRowList.swift` — row list
  parser (`tag 0 = FixedSize(u16)`, `tag 1 = RowOffsets`)

## Coding conventions

- **Comments only when the WHY is non-obvious.** Reserve them for
  hidden invariants, surprising workarounds, and references that
  would not survive renaming the symbol.
- **No premature abstractions.** Three similar lines beat a new trait.
- **Errors:** `anyhow::Result` at binary boundaries, `thiserror` enums
  inside libraries. No `unwrap()` in code that runs in production.
- **Tracing:** instrument every async entry point with the `relay`
  target prefix (e.g. `target = "relay::stdb_mode"`).
- **Tests:** library crates ship unit tests for pure logic. End-to-end
  validation lives in the spike workflow (`spike/codegen.py +
  spike/replay/` against captured snapshots).
- **Workspace deps:** every external dep goes in the root `Cargo.toml`
  `[workspace.dependencies]` table. Crates pull them via
  `dep.workspace = true`.

## Common pitfalls

1. **Identity binding.** First run: the relay captures and persists a
   local-stdb-issued token under `<data-dir>`; `relay_bind_writer`
   seals that identity. On subsequent runs the persisted token must
   still be present and readable, otherwise the relay reconnects as a
   *different* identity and every reducer call fails `unauthorized`.
   Wiping `--data-dir` mid-deployment requires a corresponding
   `spacetime publish --delete-data` republish so the new identity
   can re-bind. (See "Mirror module + writer auth".)
2. **CallReducer args size.** SpacetimeDB caps incoming WS frames
   around 32 MB. The driver chunks by row count and payload bytes
   (default 16 MB / 4096 rows); raise either via `DriverConfig` if
   you change the underlying SpacetimeDB.
3. **Server incoming-queue cap.** SpacetimeDB drops the connection
   when more than 16 384 unacked CallReducers pile up. The driver's
   semaphore caps in-flight to 8 000 by default — keep headroom.
4. **`BsatnRowList` `tag 0` means *fixed-size rows*, not *empty list*.**
   Used in `relay-upstream` decoding only; the wasm module sees
   typed `Vec<u8>` args and decodes via `bsatn::from_slice`.
5. **Brotli/Gzip on v2 query updates.** We always ask for
   `?compression=None`; the relay never decompresses inbound payload.
6. **Schema fingerprint = SHA-256 of the upstream schema JSON.**
   Stored in `<workdir>/fingerprint.json`. Mismatch triggers a full
   `--delete-data` republish; the wipe is the correctness guarantee.
7. **BitCraft's ~90 s RST on big set-replace `Subscribe`s.** Subscribing
   to all 250 BitCraft tables in one shot triggers a single >1 GB
   `InitialSubscription` WS message. Some middlebox along
   BitCraft's edge resets the connection at ~90 s before any client
   can finish receiving it. Confirmed against the official SpacetimeDB
   Rust SDK (see `crates/bc14-sdk-test/`); same RST at the same byte
   mark. Workaround is `--subscribe-chunk-size 1` (sequential
   `SubscribeMulti`); see "Subscribing at scale".
8. **Allocator pressure on multi-hundred-MB fragmented frames.**
   `mimalloc` was previously the default global allocator but burns
   3-4× the RSS while tungstenite accumulates a giant fragmented
   Binary message. Removed in favor of the system allocator. Don't
   re-enable mimalloc without retesting against a BitCraft-scale
   subscribe.
9. **`tokio::select!` over a split `WebSocketStream`.** Don't.
   `WebSocketStream::split` returns a `BiLock`-shared sink/stream
   pair; tungstenite's auto-Pong replies queue on the unpolled write
   half during a long read poll. The relay uses a single un-split
   `&mut sock` driven by one select with three arms (read / 30 s
   idle Ping / cmd) — matches the SpacetimeDB SDK pattern. See
   `relay-upstream/src/client.rs`.

## Deployment

Specifics (host, accounts, service paths) live in **`CLAUDE.local.md`**
(gitignored).

General rules:

- Run `spacetimedb-standalone` as a separate systemd unit, bound to
  the public-facing address. Provision a writable data dir with at
  least ~5× the upstream's raw BSATN size for the WAL + commitlog.
- Run the relay process as a second systemd unit. It needs:
  - Network access to the upstream and to the local SpacetimeDB.
  - `python3` and `cargo` on `PATH` (the publisher shells out).
  - The same identity as the spacetime CLI used at publish time
    (`RELAY_STDB_IDENTITY_TOKEN`).
- Never push to the production host without explicit user
  authorization for that specific change. A prior approval doesn't
  carry over.
- Never log production identity tokens — they're long-lived bearer
  credentials.

## Wire framing — what we send and receive

Each WebSocket binary message is laid out as:

```
+----+-----------------------------------+
| u8 | BSATN-encoded ServerMessage/      |
|    | ClientMessage (or compressed body)|
+----+-----------------------------------+
  ^
  compression tag (0=None, 1=Brotli, 2=Gzip)
```

After the compression byte (and decompression if non-zero), the body
starts with the sum-type discriminant of the message (`Tags::*`).

Subprotocol: `Sec-WebSocket-Protocol: v2.bsatn.spacetimedb` on both
upstream and relay→local-SpacetimeDB connections.

URL query parameter: `?compression=None|Brotli|Gzip` (capitalized
exactly like that — see `Sources/BSATN/Compression.swift` in the Swift
SDK).

## When in doubt

- Check `clockworklabs/SpacetimeDB` master at the matching version
  before guessing wire details.
- Cross-reference the Swift SDK's `Sources/SpacetimeDB/Server Messages/`
  for parser implementations.
- For SpacetimeDB Rust SDK behavior, the v1.12.0 source is at
  `https://github.com/clockworklabs/SpacetimeDB/tree/v1.12.0/sdks/rust`.
  The crate's WS handling lives in `src/websocket.rs` and is
  vendored verbatim by `crates/bc14-sdk-test/` for empirical
  testing — run that bin to confirm any large-scale subscribe issue
  isn't caused by something we did vs the SDK.
- The spike under `spike/` (codegen Python + sample mirror crate +
  `spike-replay` standalone driver) was the original validation.
  Useful as a reference when refactoring codegen or driver internals.
