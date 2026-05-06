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

# Run the relay against the live BitCraft test region
RELAY_UPSTREAM_TOKEN=$(cat .bitcraft-token) \
RELAY_STDB_IDENTITY_TOKEN="<spacetime CLI's logged-in token>" \
cargo run -p relay --release -- \
  --upstream wss://bitcraft-early-access.spacetimedb.com \
  --database bitcraft-live-14 \
  --upstream-protocol v1 \
  --stdb-url ws://127.0.0.1:3010 \
  --stdb-server-alias relay-local \
  --mirror-database relay-mirror-bc14

# Speak v2 directly to either the local SpacetimeDB or the upstream
cargo run -p relay-test-harness -- <args>
```

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
- Per-table compression (`CompressableQueryUpdate::Brotli`/`Gzip`) is
  ignored; we always ask for `?compression=None`.
- `IdentityToken` (v1) maps to v2's `InitialConnection`.
- `InitialSubscription` (v1) maps to v2's `SubscribeApplied` with
  synthetic `request_id = 1` and `query_set_id = 1`.

Reference: `crates/client-api-messages/src/websocket.rs` in
clockworklabs/SpacetimeDB at tag `v1.12.0`. We pin that version of
`spacetimedb-client-api-messages` as a separately-named workspace dep.

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
| `relay-protocol`       | Wraps `spacetimedb-sats` + `spacetimedb-client-api-messages`. Wire types only — no I/O. |
| `relay-upstream`       | Owns the single upstream WebSocket. Decodes ServerMessages, exposes a stream of `UpstreamEvent`. |
| `relay-publisher`      | Codegen → cargo build → `spacetime publish -y --delete-data`, keyed by SHA-256 of the upstream schema JSON. No-op when the fingerprint hasn't changed. |
| `relay-mirror-driver`  | v2 WS client to local SpacetimeDB. Sends `relay_apply_<table>(deletes, inserts)` calls with semaphore backpressure (≤8 K in-flight) and chunking by row count + payload bytes. |
| `relay`                | Binary. Args, schema fetch, dispatches to `stdb_mode`. |
| `relay-test-harness`   | Standalone binary that plays the C/D role. Speaks v2 directly via `spacetimedb-client-api-messages`. |
| `tools/codegen.py`     | Schema JSON → Rust source for the mirror crate. Emits `#[table]` structs + four writer-gated reducers per table (`relay_insert/delete/update/apply_<table>`). |
| `tools/mirror-template/` | Cargo.toml + rust-toolchain.toml copied into the runtime workdir before each codegen run. |

## Mirror module + writer auth

Every code-generated mirror module includes a fixed scaffold:

```rust
#[spacetimedb::table(name = "_relay_meta", accessor = relay_meta)]
struct RelayMetaRow { #[primary_key] id: u8, writer: Identity }

#[spacetimedb::reducer(init)]
fn relay_init(ctx: &ReducerContext) {
    if ctx.db.relay_meta().id().find(&0u8).is_none() {
        ctx.db.relay_meta().insert(RelayMetaRow { id: 0, writer: ctx.sender() });
    }
}

#[spacetimedb::reducer]
pub fn relay_bind_writer(ctx: &ReducerContext) -> Result<(), Box<str>> {
    /* idempotent: already-bound to same identity → ok; different → error */
}

fn assert_writer(ctx: &ReducerContext) -> Result<(), Box<str>> {
    /* errors with "unauthorized" if ctx.sender() != _relay_meta.writer */
}
```

The publisher's identity (the spacetime CLI's logged-in identity) is
captured at `init` time. The relay must present that same identity at
runtime via `--stdb-identity-token` / `RELAY_STDB_IDENTITY_TOKEN`,
otherwise every reducer call fails `unauthorized`.

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

1. **Identity binding.** The publisher and the running relay must use
   the same identity, otherwise `assert_writer` rejects every call.
   Use `RELAY_STDB_IDENTITY_TOKEN`. (See "Mirror module + writer auth".)
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
- The spike under `spike/` (codegen Python + sample mirror crate +
  `spike-replay` standalone driver) was the original validation.
  Useful as a reference when refactoring codegen or driver internals.
