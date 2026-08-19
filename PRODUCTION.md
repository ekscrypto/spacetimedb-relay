# Production on relay.bitcraftsync.app

This workspace contains the **full relay research codebase**. On
**relay.bitcraftsync.app** only a subset is built and deployed.

## Deployed today (green topology, since 2026-08-17)

| Crate / binary | Role | Deploy |
|--------------|------|--------|
| **`relay-coordinator`** | Fleet `/health` JSON; polls green mirror sidecar `:3130` | `tools/deploy.sh core` (workspace root) |
| **`relay-test-harness`** | Post-deploy integrity gate (`check-integrity.sh`) | Built during deploy verify |
| **`relay-protocol`** | Shared types; path-dep of `relay-cache` in `spacetimedb-bitcraft-mirror` | Not deployed standalone |

The mirror + read cache run from **`spacetimedb-bitcraft-mirror`**
(`bitcraft-mirror.service`), not from this repo's `relay` binary.

## Legacy (retained for dev / reference — not production)

| Crate | Was used for |
|-------|----------------|
| `relay` | Per-region relay daemon (`relay-bc*.service`) |
| `relay-upstream` | Upstream v1/v2 subscription client |
| `relay-publisher` | Codegen + publish mirror module to local stdb |
| `relay-mirror-driver` | Mirror module registry |
| `relay-frontend` | Downstream WS proxy in front of local stdb |
| `spike/replay`, `spike/mirror` | Experiments |

These remain workspace members so local dev and tests keep compiling. Do
**not** build or restart `relay` / fleet sequencer on production — the 14×
relay fleet and blue public-mirror stack were retired 2026-08.

## Local build (production crates only)

```sh
cargo build --release -p relay-coordinator -p relay-test-harness
```

## Where to read more

- Generic relay architecture: [`CLAUDE.md`](CLAUDE.md)
- Workspace parent + URL map: [`../AGENTS.md`](../AGENTS.md)
- Deploy: [`../DEPLOY.md`](../DEPLOY.md)
