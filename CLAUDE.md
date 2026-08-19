# CLAUDE.md

Agent guide for **spacetimedb-relay** (post–2026-08 slim workspace).

## What remains

Three crates only:

| Crate | Notes |
|-------|-------|
| `relay-coordinator` | `health.rs` = `/health`; `daemon.rs` = Unix reconnect permits; `fleet_sequencer.rs` = legacy relay-bc* systemd control (disabled on green) |
| `relay-protocol` | No I/O — schema + BSATN decode shared with `relay-cache` |
| `relay-test-harness` | Standalone integrity binary; uses `relay-protocol` + SpacetimeDB wire crates |

The old **`relay` binary stack** was removed. Do not recreate it — mirroring
lives in `spacetimedb-bitcraft-mirror`.

## Commands

```sh
cargo build --release -p relay-coordinator -p relay-test-harness
cargo test -p relay-coordinator -p relay-protocol
cargo clippy --workspace --all-targets -- -D warnings
```

## relay-coordinator

- **`/health`** aggregates from `RELAY_MIRRORS_URL` (production: `http://127.0.0.1:3130/v1/mirrors`).
- Empty `RELAY_MIRRORS_URL` falls back to walking `relay-*.service` units (legacy; unused on green).
- Dashboard HTML path: `--index-html` (from `relay-coordinator.service`).

## relay-protocol

Consumed by `relay-test-harness` and by path-dep from
`spacetimedb-bitcraft-mirror/crates/relay-cache`. When bumping SpacetimeDB
wire crate versions in `Cargo.toml`, bump the fork workspace too.

## relay-test-harness

`--check-integrity` mode is what `check-integrity.sh` invokes. Intentionally
does **not** depend on the removed upstream relay crates — it speaks SpacetimeDB
client protocols directly.

## Conventions

- `anyhow::Result` at binary boundaries, `thiserror` in libraries.
- No `unwrap()` in production paths.
- Deploy: sibling workspace [`../DEPLOY.md`](../DEPLOY.md).

## History

Per-region relay architecture (R→P→L→F diagram, codegen, stdb-spawn, etc.) was
removed from the tree 2026-08. See git history before that date to recover the
old crates and docs.
