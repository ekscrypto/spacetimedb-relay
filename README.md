# spacetimedb-relay

A relay/proxy for SpacetimeDB. One persistent subscription to an
upstream SpacetimeDB instance fans out to many downstream clients
without multiplying load on the game server.

The relay code-generates a mirror module at runtime, publishes it to a
sibling SpacetimeDB instance you run alongside the relay, and pipes
upstream inserts/deletes/updates onto that local SpacetimeDB.
**Downstream clients connect directly to the local SpacetimeDB**
using the unmodified v2 WebSocket protocol — they're literally talking
to a SpacetimeDB, so any SDK works without changes.

The upstream side can speak either **`v2.bsatn.spacetimedb`** (default,
SpacetimeDB ≥ 2.0) or **`v1.bsatn.spacetimedb`** (pre-2.0 deployments,
opt-in via `--upstream-protocol v1`). v1 messages are translated to v2
internally.

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
                                            SpacetimeDB).
                                  │
                                  ▼
                                  D         D subscribes to L; never
                                            reaches S.
```

`R` = relay process. `S` = upstream SpacetimeDB. `L` = sibling
SpacetimeDB you run on the relay host. `P` = publisher pipeline. `D` =
downstream clients (game SDKs).

The relay never propagates `CallReducer` upstream. Clients that need
to mutate state call reducers directly on the upstream; the relay's
job is read-only fan-out via `L`.

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
4. `upstream connected` — relay→upstream WebSocket up
5. `SubscribeApplied n_tables=…` — initial bulk-load is streaming
   `relay_apply_<table>` calls into the local SpacetimeDB

Downstream clients now connect directly to the local SpacetimeDB on
`ws://relay-host:3010/v1/database/relay-mirror-<your-database>/subscribe`.

## Configuration

| Flag                       | Env var                            | Default                                      | Notes |
|----------------------------|------------------------------------|----------------------------------------------|-------|
| `--upstream`               | `RELAY_UPSTREAM`                   | _required_                                   | e.g. `wss://maincloud.spacetimedb.com` |
| `--database`               | `RELAY_DATABASE`                   | _required_                                   | Database name or identity on the upstream |
| `--upstream-token`         | `RELAY_UPSTREAM_TOKEN`             | none                                         | Bearer token for upstream auth |
| `--upstream-protocol`      | `RELAY_UPSTREAM_PROTOCOL`          | `v2`                                         | `v1` for pre-2.0 SpacetimeDB |
| `--subscribe-table`        | `RELAY_SUBSCRIBE_TABLES`           | all user-public tables                       | Repeatable; comma-separated via env |
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

`RUST_LOG` controls log level (default `info`). For example,
`RUST_LOG=relay=trace,relay_publisher=debug cargo run -p relay …`.

## Workspace layout

| Crate                  | Purpose |
|------------------------|---------|
| `relay-protocol`       | Wire types; re-exports `spacetimedb-sats` and `spacetimedb-client-api-messages`. |
| `relay-upstream`       | Owns the single upstream WebSocket; emits decoded `ServerMessage` events. |
| `relay-publisher`      | Codegen → cargo build → `spacetime publish -y --delete-data`, keyed by SHA-256 fingerprint of the upstream schema. No-op when fingerprint unchanged. |
| `relay-mirror-driver`  | v2 WS client to local SpacetimeDB; sends `relay_apply_<table>(deletes, inserts)` calls with semaphore backpressure and chunking. |
| `relay`                | Binary. Args, schema fetch, dispatches to `stdb_mode`. |
| `relay-test-harness`   | Standalone v2 client; useful for end-to-end testing against either the local SpacetimeDB or a remote upstream. |
| `tools/codegen.py`     | Schema JSON → Rust source for the mirror crate. Emits `#[table]` structs + four writer-gated reducers per table. |
| `tools/mirror-template/` | `Cargo.toml` + `rust-toolchain.toml` copied into the publisher's workdir. |

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
