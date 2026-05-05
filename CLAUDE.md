# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project: spacetimedb-relay

A Rust relay/proxy for SpacetimeDB. One upstream subscription fans out
to many downstream clients while mirroring state to PostgreSQL.

## Common commands

Toolchain is pinned by `rust-toolchain.toml` (currently 1.93, with
`rustfmt` and `clippy`). No need to install separately — `rustup`
picks it up.

```sh
# Build the whole workspace
cargo build

# Run a single crate's tests
cargo test -p relay-storage

# Filter to one test by name
cargo test -p relay-upstream -- bsatn_row_list

# Lint / format (CI-equivalent)
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all -- --check

# Bring up the local Postgres mirror (creates relay/relay/relay db)
docker compose up -d postgres

# Run the binary against the test database (identity in CLAUDE.local.md)
cargo run -p relay -- \
  --upstream wss://maincloud.spacetimedb.com \
  --database <see-CLAUDE.local.md> \
  --bind 0.0.0.0:3001

# Speak v2 directly to either the relay or upstream (no relay-upstream dep)
cargo run -p relay-test-harness -- <args>
```

## Architecture invariants

These shape every change in this codebase. Don't break them without explicit instruction.

0. **The relay never calls reducers.** The R→S connection (in
   `relay-upstream`) only ever sends `ClientMessage::Subscribe` /
   `Unsubscribe`. The relay does not propagate downstream `CallReducer`
   messages to upstream either. Reducer calls are made by clients
   directly to the SpacetimeDB server.

   Architecture in one diagram:
   ```
       C ─────────────► S          C calls reducers directly on S.
                        │
                        ▼
                        R          R only subscribes; S pushes updates.
                        │
                        ▼
                        D          D subscribes to R; never reaches S.
   ```

1. **One upstream, many downstream**: there must be exactly one
   subscription per (upstream DB, table) — never one per downstream
   client. The relay's whole purpose is to amortize upstream cost.
2. **Schema-agnostic at compile time**: the relay is **not** generated
   per-database. It learns the schema at runtime from the upstream's
   `/v1/database/{name}/schema` HTTP endpoint and persists rows
   accordingly. We never codegen Rust structs from a particular game's
   tables.
3. **Wire-protocol parity downstream**: third-party clients connect
   using the unmodified SpacetimeDB v2 WebSocket protocol. Their SDK
   should not be able to tell it's talking to a relay. ServerMessage
   tags 0x00–0x07 must match `Tags.swift` / clockworklabs's
   `client-api-messages` v2.
4. **Schema drift = wipe**: on detecting that an upstream table's
   schema differs from what we have stored, drop the mirrored Postgres
   table and re-fetch from scratch. We cannot replay row migrations
   the upstream may have applied.
5. **Postgres = canonical state**: in-memory caches are derivable.
   Anything authoritative (current row state, per-client subscription
   queries, identity tokens) lives in Postgres.

## Upstream protocol versions

The relay defaults to the **`v2.bsatn.spacetimedb`** subprotocol — the
current stable. Pre-2.0 SpacetimeDB servers (≤ v1.12.x, before the
v2.0 release on 2026-02-20) only accept `v1.bsatn.spacetimedb`. Pass
`--upstream-protocol v1` (or set `RELAY_UPSTREAM_PROTOCOL=v1`) to
target one of those.

When the upstream protocol is v1:

- The handshake offers `v1.bsatn.spacetimedb`.
- Decoded v1 `ServerMessage`s are translated to the v2 shape inside
  `relay-upstream::v1_compat`, so the rest of the codebase
  (`relay-engine`, `relay-storage`, `relay-server`) stays v2-only.
- Outbound `Subscribe` is encoded as v1's set-replace
  `Subscribe { query_strings, request_id }` (no `QuerySetId`).
- Per-table compression (`CompressableQueryUpdate::Brotli`/`Gzip`) is
  ignored. The relay always asks the server for `?compression=None`.
  If a server compresses anyway, those updates are dropped with a
  warning. Don't add Brotli/Gzip handling on the v1 path unless a real
  deployment forces our hand.
- `IdentityToken` (v1) maps to v2's `InitialConnection`.
  `InitialSubscription` (v1) maps to v2's `SubscribeApplied` with
  synthetic `request_id = 1` and `query_set_id = 1`.
- v1 reducer-status fields (`status`, `caller_identity`, `timestamp`,
  `energy_quanta_used`) are dropped — the relay does not surface
  reducer outcomes to downstream.
- The test harness's writer (the C role from the architecture diagram)
  still speaks v2 directly. That's a test-scaffold limitation, not a
  relay limitation; downstream clients always see v2.

Reference: `crates/client-api-messages/src/websocket.rs` in
clockworklabs/SpacetimeDB at tag `v1.12.0` is the canonical v1 source
of truth — we pin that version of `spacetimedb-client-api-messages`
as a separately-named workspace dep.

## Wire protocol — v2 message tags

```
ClientMessage (downstream → relay)        ServerMessage (relay → downstream)
  0x00 Subscribe                           0x00 InitialConnection
  0x01 Unsubscribe                         0x01 SubscribeApplied
  0x02 OneOffQuery                         0x02 UnsubscribeApplied
  0x03 CallReducer                         0x03 SubscriptionError
  0x04 CallProcedure                       0x04 TransactionUpdate
                                           0x05 OneOffQueryResult
                                           0x06 ReducerResult
                                           0x07 ProcedureResult
```

Source of truth: `clockworklabs/SpacetimeDB`,
`crates/client-api-messages/src/websocket/v2.rs`.

## Reference: live test database

The project owns a dedicated test database on maincloud, used
exclusively for relay testing. Its identity, schema layout, reducer
list, and republish command live in **`CLAUDE.local.md`** (gitignored)
to keep the URL out of the public repo. Read that file before running
any test or republish command.

The module source itself lives in `test-module/` (excluded from the
workspace).

## Reference: BitCraft live game server (EA2)

Fan-research on connecting the relay to BitCraft Online (Early Access
2): meta host, region-routing bootstrap via `bitcraft-3 /
region_connection_info`, the `api.bitcraftonline.com` auth flow, the
on-disk JWT in Unity's PlayerPrefs, the `.bitcraft-token` convention,
and the open BSATN-vs-JSON wire-format question. See
**`BITCRAFT.md`**.

## Reference: Swift SDK

`../spacetimedb-swift-sdk/` — same wire protocol, useful for
cross-checking encoding decisions. Particularly:
- `Sources/SpacetimeDB/Tags.swift` — message tag values
- `Sources/SpacetimeDB/Server Messages/BsatnRowList.swift` — row list
  wire format (`tag 0 = FixedSize(u16)`, `tag 1 = RowOffsets`)
- `CLAUDE.md` in that repo — useful protocol notes

## Crate layout

| Crate              | Purpose                                                              |
|--------------------|----------------------------------------------------------------------|
| `relay-protocol`   | Re-exports / wraps `spacetimedb-sats` + `spacetimedb-client-api-messages`. Wire types only — no I/O. |
| `relay-upstream`   | Owns the single upstream WebSocket. Decodes ServerMessages, exposes a stream of structured events. |
| `relay-storage`    | Postgres mirror. Dynamic per-table DDL. Schema-drift detection + wipe. |
| `relay-engine`     | SpacetimeDB SQL parsing (via `spacetimedb-sql-parser`), per-client query state, diff computation. |
| `relay-server`     | Downstream `axum` WS server. Speaks v2 ClientMessage/ServerMessage. |
| `relay`            | Binary. CLI args, wires the pieces together, runs them under tokio. |
| `relay-test-harness` | Standalone binary that plays the C/D role (third-party client). Speaks v2 directly via `spacetimedb-client-api-messages` — **does not** depend on `relay-upstream`, so it can target either the relay or a real SpacetimeDB server. |

## Coding conventions

- **Don't add comments unless the WHY is non-obvious.** Well-named
  identifiers replace what-comments. Reserve comments for hidden
  invariants and surprising workarounds.
- **No premature abstractions.** Three similar lines beat a new trait
  hierarchy.
- **Error handling**: `anyhow::Result` at binary boundaries,
  `thiserror` enums inside libraries. Don't `unwrap()` in code that
  can run in production.
- **Tracing**: instrument every async entry point. Use the `relay`
  target prefix (e.g. `tracing::info!(target = "relay::upstream", …)`).
- **Tests**: each library crate gets unit tests for pure logic + an
  integration test against a Postgres test container or the live
  test database (see `CLAUDE.local.md`) for wire-format checks.
- **Workspace deps**: every external dep goes in the root `Cargo.toml`
  `[workspace.dependencies]` table. Crates pull them via
  `dep.workspace = true`. Don't pin versions in individual crates.

## Common pitfalls

1. `BsatnRowList` `tag 0` means *fixed-size rows*, not *empty list*.
   See the Swift SDK's `BsatnRowList.swift` for the canonical parser.
2. The upstream `IdentityToken` arrives **after** the WS handshake —
   downstream subscribe requests sent before that arrives will hang
   against maincloud. Buffer or reject them until we've received it
   upstream.
3. Brotli compression is on by default for v2 query updates. Decode
   eagerly when we receive them — re-compressing for downstream is
   fine but the cached row state must be uncompressed.
4. SpacetimeDB schemas use `RawModuleDefV9` or `V10`. We store the
   hash of the schema; if it changes, wipe the mirror.

## Deployment target

Production deployment specifics (host, accounts, service paths) are
recorded in **`CLAUDE.local.md`** (gitignored). Read that file before
making any change that would touch the production host.

General rules regardless of host:

- Never push to or modify the production host without explicit user
  authorization for that specific change. A prior approval doesn't
  carry over.
- Never log production identity tokens — they're long-lived bearer
  credentials.
- Provision a dedicated `relay` Postgres role/database. Don't assume
  an empty cluster — production hosts often have other services on
  the same Postgres instance.

## Wire framing — what we send and receive

Each downstream/upstream WebSocket binary message is laid out as:

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
upstream and downstream connections.

URL query parameter: `?compression=None|Brotli|Gzip` (capitalized
exactly like that — see `Sources/BSATN/Compression.swift` in the Swift
SDK).

## When in doubt

- Check `clockworklabs/SpacetimeDB` master at the matching version
  before guessing wire details.
- Cross-reference the Swift SDK's `Sources/SpacetimeDB/Server Messages/`
  for parser implementations of message types.
