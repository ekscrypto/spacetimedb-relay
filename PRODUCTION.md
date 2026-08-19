# Production on relay.bitcraftsync.app

## Deployed from this repo

| Crate / binary | Role | Deploy |
|--------------|------|--------|
| **`relay-test-harness`** | Post-deploy integrity gate | Built during deploy verify |
| **`relay-protocol`** | Library for `relay-cache` (sibling fork) | Not deployed standalone |

Fleet `/health`: **`mirror-health`** in `spacetimedb-bitcraft-mirror`
(`mirror-health.service`). Mirroring + read cache: **`bitcraft-mirror.service`**.

## Build

```sh
cargo build --release -p relay-test-harness
```

## Removed (2026-08)

The per-region relay daemon and its dependency crates (`relay`,
`relay-upstream`, `relay-publisher`, `relay-mirror-driver`, `relay-frontend`)
were deleted from this repo. **`relay-coordinator`** moved to
`spacetimedb-bitcraft-mirror/crates/mirror-health` (2026-08). Retrieve old
crates from git history if needed.

## Docs

- [`README.md`](README.md) — crate overview
- [`CLAUDE.md`](CLAUDE.md) — agent notes for the remaining crates
- [`../AGENTS.md`](../AGENTS.md) — workspace map
- [`../DEPLOY.md`](../DEPLOY.md) — deploy runbook
