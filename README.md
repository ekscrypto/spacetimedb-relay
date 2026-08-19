# spacetimedb-relay

Shared Rust crates for **relay.bitcraftsync.app** ops: fleet health aggregation,
post-deploy integrity checks, and wire-format helpers consumed by
`relay-cache` in the sibling `spacetimedb-bitcraft-mirror` repo.

> **Community project — not affiliated with Clockwork Labs.**

The per-region **`relay` daemon stack** (upstream subscribe → codegen mirror
module → local SpacetimeDB → frontend proxy) was removed in 2026-08. Production
mirroring runs in `spacetimedb-bitcraft-mirror` instead. The old crates remain
in git history.

## Crates

| Crate | Binary | Role |
|-------|--------|------|
| **`relay-coordinator`** | `relay-coordinator` | `/health` JSON + reconnect permit daemon |
| **`relay-test-harness`** | `relay-test-harness` | v1/v2 BSATN + schema integrity gate |
| **`relay-protocol`** | (library) | Schema parse + BSATN row decode |

See [`PRODUCTION.md`](PRODUCTION.md) for what deploys to production.

## Build

```sh
cargo build --release -p relay-coordinator -p relay-test-harness
cargo test -p relay-coordinator -p relay-protocol
```

Deploy from the workspace root: [`../DEPLOY.md`](../DEPLOY.md) (`tools/deploy.sh core`).

## relay-coordinator

Polls `GET /v1/mirrors` on the green status sidecar (`127.0.0.1:3130` by
default) and serves aggregated fleet JSON at `/health` (public via nginx).
Also listens on a Unix socket for reconnect permits (legacy relay fleet used
this; harmless on green).

Unit file: [`tools/relay-coordinator.service`](tools/relay-coordinator.service).

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
  relay-coordinator/   /health daemon
  relay-protocol/      shared decode types
  relay-test-harness/  integrity binary
tools/
  relay-coordinator.service
```

Sibling repos under the workspace root: `spacetimedb-bitcraft-mirror`,
`bitcraft-relay`. See [`../AGENTS.md`](../AGENTS.md).
