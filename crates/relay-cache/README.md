# relay-cache

Same-host in-memory read cache over the relay fleet. Subscribes to each
regional frontend on loopback (`ws://127.0.0.1:<port>`, v2), holds
`claim_state` / `building_state` / `inventory_state` in columnar memory,
and serves three HTTP/JSON queries on `127.0.0.1:8089`.

Replaces the cross-host polling model of `bitcraft-relay-sync` for
hot-path reads — no 5-minute snapshot staleness, no Postgres round-trip.

## Run

```sh
cargo run -p relay-cache --release
```

On the relay host, defaults discover regions from
`/etc/systemd/system/relay-bc*.service` and fetch the shared schema from
`http://127.0.0.1:3014` (any regional frontend). Override with:

- `--bind` / `RELAY_CACHE_BIND` (empty string → ingest-only)
- `--unit-dir` / `RELAY_CACHE_UNIT_DIR`
- `--schema-host` / `RELAY_CACHE_SCHEMA_HOST`
- `--schema-db` / `RELAY_CACHE_SCHEMA_DB`
- `--mem-ceiling-bytes` / `RELAY_CACHE_MEM_CEILING_BYTES` (default 4 GiB)

## Queries

```sh
# Claim by PK
curl -s http://127.0.0.1:8089/claim/1234567890

# Claim by name substring (case-insensitive)
curl -s 'http://127.0.0.1:8089/claim?name=concordia'

# Per-building inventory rollup, grouped by dimension (overworld +
# building interiors). Shared storage only — no Town Banks. Each interior
# group includes an `entrance` (e.g. Sturdy Storehouse).
curl -s http://127.0.0.1:8089/claim/1234567890/inventory
# → { "claim": {...}, "dimensions": [
#      { "dimension_id": 1, "kind": "overworld", "entrance": null, "buildings": [...] },
#      { "dimension_id": 1649, "kind": "building_interior",
#        "entrance": { "entity_id", "name", "nickname" }, "buildings": [...] }
#    ] }

# Health / readiness
curl -s http://127.0.0.1:8089/healthz
```

## Memory policy

The ceiling is an alarm, not a load shedder. Approaching it logs a warning
and flips `/healthz` `ready=false`, but queries keep serving with whatever
data is loaded. Projected resident is ~1 GiB across 13 regions.
