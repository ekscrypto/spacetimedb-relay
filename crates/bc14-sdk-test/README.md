# bc14-sdk-test

Standalone binary that vendors the v1.12.0 SpacetimeDB Rust SDK's
WebSocket layer verbatim and uses it to subscribe to a database with
no codegen. Used to confirm that any large-scale subscribe issue
(in particular BitCraft's ~90 s middlebox kill on a 250-table
set-replace) is server/middlebox behavior rather than something our
relay's reimplementation does wrong.

## Why it's not in the workspace

`Cargo.toml` points at `/tmp/spacetimedb-v1/SpacetimeDB/...` (a
local clone of the v1.12.0 tag) for the `spacetimedb-{lib,sats,
client-api-messages}` deps. Those `1.12.0` versions conflict with
the workspace's `2.x` pins, so this crate is excluded from
`/Cargo.toml`'s `[workspace] members`.

## Setup

```sh
# Clone SpacetimeDB at v1.12.0 to /tmp.
mkdir -p /tmp/spacetimedb-v1
git -C /tmp/spacetimedb-v1 clone \
    --depth 1 --branch v1.12.0 \
    https://github.com/clockworklabs/SpacetimeDB.git
```

The crate vendors three SDK files:

- `src/sdk_websocket.rs` — `crates/sdk/src/websocket.rs` from
  v1.12.0, with `pub(crate)` widened to `pub` on `WsConnection`,
  `WsParams`, `WsConnection::connect`, `parse_response`,
  `encode_message`, `spawn_message_loop`. Otherwise byte-identical.
- `src/sdk_compression.rs` — `crates/sdk/src/compression.rs`,
  unchanged except `use crate::websocket::WsError` →
  `crate::sdk_websocket::WsError`.
- `src/sdk_metrics.rs` — replaced with a no-op stub so we don't pull
  prometheus + the SDK's `spacetimedb-metrics` proc-macro graph.

## Run

```sh
# .bitcraft-token must exist in CWD with the upstream auth token.
cargo run --release --manifest-path crates/bc14-sdk-test/Cargo.toml
```

The binary connects, sends a single 250-query `Subscribe` to
`bitcraft-live-14`, drains incoming messages, and exits when it
either receives a complete `InitialSubscription` (success) or hits
`ResetWithoutClosingHandshake` (the upstream's 90 s kill).

## What this proved

Run on 2026-05-07: connected, sent Subscribe with 250 query strings,
got `IdentityToken`, then bytes flowed in for ~90 s reaching ~258 MB
before:

```
Error reading message from read WebSocket stream:
Protocol(ResetWithoutClosingHandshake)
```

Identical RST timing and byte mark to our relay's reimplementation.
Conclusion: nothing the SDK does (un-split socket, 30 s idle Ping,
auto-Pong, etc.) survives the BitCraft edge's behavior on large
single-message InitialSubscriptions. The fix has to be on the
subscribe-shape side (sequential `SubscribeMulti`, see the relay's
`--subscribe-chunk-size 1` mode and `CLAUDE.md`'s "Subscribing at
scale" section).

Keep this crate as a regression artifact: if anyone changes the
relay's WebSocket plumbing in a way that breaks against BitCraft,
running this bin tells you immediately whether the issue is our code
or BitCraft's edge.
