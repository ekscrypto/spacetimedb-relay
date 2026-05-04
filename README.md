# spacetimedb-relay

A relay/proxy for SpacetimeDB. One persistent subscription to an
upstream SpacetimeDB instance fans out to many downstream clients
without multiplying load on the game server.

Downstream clients speak the **unmodified** SpacetimeDB v2 WebSocket
protocol, so existing SDKs work without changes — they just point at
the relay's URL instead of the game server.

> **Community project — not affiliated with Clockwork Labs.**
> SpacetimeDB™ is a trademark of [Clockwork Labs](https://clockworklabs.io/).
> This project is an independent, community-maintained tool that speaks
> the public SpacetimeDB v2 WebSocket protocol; it is not endorsed by,
> sponsored by, or otherwise associated with Clockwork Labs.

## Why

Public SpacetimeDB games (BitCraft, etc.) are rate-limited: studios
restrict how many third-party tools can hold subscriptions, because
every extra subscriber costs CPU on the live server. A relay holds
**one** subscription upstream and serves any number of downstream
clients from its own mirrored state, so community tools (BitCraftMap,
BitCraftSync, BitJita, …) can share a single upstream cost.

## Architecture

```
    C ─────────────► S          C calls reducers directly on S.
                     │
                     ▼
                     R          R only subscribes; S pushes updates.
                     │
                     ▼
                     D          D subscribes to R; never reaches S.
```

```
SpacetimeDB game server (S)
        │   one subscription, all tables, all columns
        ▼
┌─────────────────────────────────────────┐
│  spacetimedb-relay (R)                  │
│  ─ upstream client (BSATN over WS)      │
│  ─ schema cache (HTTP /schema endpoint) │
│  ─ Postgres mirror (per-table DDL)      │
│  ─ SQL evaluator (SpacetimeDB SQL)      │
│  ─ downstream WS server (mimics v2)     │
└─────────────────────────────────────────┘
        │   per-client filtered streams
        ▼
   third-party clients (D)
```

The relay never propagates `CallReducer` upstream. Clients that need
to mutate state call reducers directly on the SpacetimeDB server; the
relay's job is read-only fan-out.

## Status

Active development. The relay runs end-to-end against maincloud:
fetches schema, mirrors rows into Postgres, and serves v2 subscribers
on a downstream port. Wire-protocol coverage of v2 is partial —
expect rough edges around less common message types.

## Prerequisites

- **Rust toolchain** — pinned to 1.93 by `rust-toolchain.toml`. If
  you have `rustup`, no install step is needed; `cargo` will fetch
  the right toolchain on first build.
- **Docker** — for the local Postgres mirror via `docker compose`.
  Alternatively, point `DATABASE_URL` at any Postgres 14+ instance
  and any writable database within it. The role/database/host in
  the URL are not hardcoded; the `relay` shown in the default URL
  (`postgres://relay:relay@localhost:5432/relay`) is just what
  `docker-compose.yml` provisions.
- **An upstream SpacetimeDB database** — any deployed SpacetimeDB
  module on a host you can reach. Pass its name or identity to the
  relay via `--database` (or `RELAY_DATABASE`). For local
  experimentation you can publish your own module with the
  `spacetime` CLI; see `test-module/` for an example.

## Quick start

```sh
# 1. Bring up the local Postgres mirror.
docker compose up -d postgres

# 2. Build the workspace (downloads the pinned toolchain on first run).
cargo build

# 3. Run the relay. Substitute your upstream host and database identity.
cargo run -p relay -- \
    --upstream wss://maincloud.spacetimedb.com \
    --database <your-database-name-or-identity> \
    --bind 0.0.0.0:3001
```

You should see log lines for `schema loaded`, one `table` line per
mirrored table, then `upstream connected` and `SubscribeApplied`
followed by `snapshot reconciled` for each table.

### Verify end-to-end propagation

In a second terminal, run the test harness. It spawns a "writer"
that calls a reducer directly on the upstream and a "subscriber"
that listens to the relay, then asserts the change makes it through
`S → R → D`:

```sh
cargo run -p relay-test-harness -- \
    --database <your-database-name-or-identity>
```

Exit code 0 = propagation verified.

## Configuration

The `relay` binary takes its configuration from CLI flags or the
matching environment variable. Flags win.

| Flag                  | Env var                  | Default                                          | Notes                                                     |
|-----------------------|--------------------------|--------------------------------------------------|-----------------------------------------------------------|
| `--upstream`          | `RELAY_UPSTREAM`         | _required_                                       | e.g. `wss://maincloud.spacetimedb.com`                    |
| `--database`          | `RELAY_DATABASE`         | _required_                                       | Database name or identity on the upstream                 |
| `--upstream-token`    | `RELAY_UPSTREAM_TOKEN`   | none                                             | Bearer token for upstream auth (private DBs)              |
| `--database-url`      | `DATABASE_URL`           | `postgres://relay:relay@localhost:5432/relay`    | Postgres connection string for the mirror                 |
| `--bind`              | `RELAY_BIND`             | `0.0.0.0:3001`                                   | Address for the downstream WS server                      |
| `--subscribe-table`   | `RELAY_SUBSCRIBE_TABLES` | all `User` tables with `Public` access           | Repeatable. Comma-separated when set via env              |
| `--frame-limit`       | `RELAY_FRAME_LIMIT`      | unlimited                                        | Stop after N upstream frames — useful for smoke tests     |

`RUST_LOG` controls log level (default `info,relay=debug`). For
example, `RUST_LOG=relay=trace,relay_storage=debug cargo run -p relay …`.

## Workspace layout

| Crate                | Purpose                                                                                  |
|----------------------|------------------------------------------------------------------------------------------|
| `relay-protocol`     | Wire types, BSATN, schema definitions (re-exports `spacetimedb-sats`).                   |
| `relay-upstream`     | Owns the single upstream WebSocket; emits decoded `ServerMessage` events.                |
| `relay-storage`      | Postgres mirror — dynamic per-table DDL, schema-drift detection.                         |
| `relay-engine`       | SpacetimeDB SQL parsing, per-client query state, diff routing.                           |
| `relay-server`       | Downstream `axum` WS server. Speaks v2 `ClientMessage`/`ServerMessage`.                  |
| `relay`              | Binary that wires the components together under tokio.                                   |
| `relay-test-harness` | Standalone end-to-end harness. Speaks v2 directly so it can target the relay or the upstream. |

## Development

```sh
# Run all workspace tests.
cargo test

# Run a single crate's tests.
cargo test -p relay-storage

# Filter to one test by name.
cargo test -p relay-upstream -- bsatn_row_list

# Lint and format (CI-equivalent).
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all -- --check
```

The `test-module/` directory contains a small SpacetimeDB module
useful for end-to-end testing the relay. It is excluded from the
workspace. Publish it with:

```sh
cd test-module
spacetime publish -s <your-server> -y <your-database-name>
```

Architecture invariants and wire-protocol details live in
[`CLAUDE.md`](./CLAUDE.md). Read it before changing anything in
`relay-protocol`, `relay-upstream`, or `relay-server`.

## License

MIT. See [`LICENSE`](./LICENSE).
