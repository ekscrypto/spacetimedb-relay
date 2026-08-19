# Production on relay.bitcraftsync.app

## Deployed crates

| Crate / binary | Role | Deploy |
|--------------|------|--------|
| **`relay-coordinator`** | Fleet `/health`; polls green `:3130/v1/mirrors` | `tools/deploy.sh core` |
| **`relay-test-harness`** | Post-deploy integrity gate | Built during deploy verify |
| **`relay-protocol`** | Library for `relay-cache` (sibling fork) | Not deployed standalone |

Mirroring + read cache: **`spacetimedb-bitcraft-mirror`**
(`bitcraft-mirror.service`).

## Build

```sh
cargo build --release -p relay-coordinator -p relay-test-harness
```

## Removed (2026-08)

The per-region relay daemon and its dependency crates (`relay`,
`relay-upstream`, `relay-publisher`, `relay-mirror-driver`, `relay-frontend`)
were deleted from this repo. Retrieve them from git history if needed.

## Docs

- [`README.md`](README.md) — crate overview
- [`CLAUDE.md`](CLAUDE.md) — agent notes for the remaining crates
- [`../AGENTS.md`](../AGENTS.md) — workspace map
- [`../DEPLOY.md`](../DEPLOY.md) — deploy runbook
