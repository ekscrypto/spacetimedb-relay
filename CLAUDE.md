# CLAUDE.md

Agent guide for **spacetimedb-relay** (post–2026-08 slim workspace).

## What remains

Two crates only:

| Crate | Notes |
|-------|-------|
| `relay-protocol` | No I/O — schema + BSATN decode shared with `relay-cache` |
| `relay-test-harness` | Standalone integrity binary; uses `relay-protocol` + SpacetimeDB wire crates |

Fleet `/health` moved to `spacetimedb-bitcraft-mirror/crates/mirror-health`
(`mirror-health` binary, `mirror-health.service`).

The old **`relay` binary stack** and **`relay-coordinator`** were removed.
Do not recreate them — mirroring lives in `spacetimedb-bitcraft-mirror`.

## Commands

```sh
cargo build --release -p relay-test-harness
cargo test -p relay-protocol
cargo clippy --workspace --all-targets -- -D warnings
```

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

Per-region relay architecture (R→P→L→F diagram, codegen, stdb-spawn, etc.) and
`relay-coordinator` were removed or relocated 2026-08. See git history before
that date to recover the old crates and docs.
