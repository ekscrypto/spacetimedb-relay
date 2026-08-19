# spacetimedb-relay

Shared Rust crates for **relay.bitcraftsync.app** ops: post-deploy integrity
checks and wire-format helpers consumed by `relay-cache` in the sibling
`spacetimedb-bitcraft-mirror` repo.

> **Community project — not affiliated with Clockwork Labs.**

The per-region **`relay` daemon stack** and **`relay-coordinator`** (`/health`
aggregator) were removed or relocated in 2026-08. Fleet `/health` now lives in
`spacetimedb-bitcraft-mirror/crates/mirror-health`. The old crates remain in
git history.

## Crates

| Crate | Binary | Role |
|-------|--------|------|
| **`relay-test-harness`** | `relay-test-harness` | v1/v2 BSATN + schema integrity gate |
| **`relay-protocol`** | (library) | Schema parse + BSATN row decode |

See [`PRODUCTION.md`](PRODUCTION.md) for what deploys from this repo.

## Build

```sh
cargo build --release -p relay-test-harness
cargo test -p relay-protocol
```

Deploy from the workspace root: [`../DEPLOY.md`](../DEPLOY.md).

## relay-test-harness

End-to-end check used by `bitcraft-relay/tools/check-integrity.sh` after every
deploy: v2.bsatn subscribe, v1.bsatn subscribe, v1.json subscribe, schema GET.

```sh
cargo run -p relay-test-harness --release -- \
  --check-integrity wss://relay.bitcraftsync.app:3014 \
  --database bitcraft-live-14
```

## relay-protocol

Wire-types-only library: `parse_schema`, `MirroredSchema`, `bsatn::decode_row`.
Path-dep of `relay-cache` in `spacetimedb-bitcraft-mirror` — keep SpacetimeDB
crate versions aligned across both workspaces.

## Layout

```
crates/
  relay-protocol/      shared decode types
  relay-test-harness/  integrity binary
```

Sibling repos under the workspace root: `spacetimedb-bitcraft-mirror`,
`bitcraft-relay`. See [`../AGENTS.md`](../AGENTS.md).
