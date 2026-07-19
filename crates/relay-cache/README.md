# relay-cache

Same-host in-memory read cache over the relay fleet. Subscribes to each
regional frontend on loopback (`ws://127.0.0.1:<port>`, v2), holds claim /
building / inventory / player tables in columnar memory, and serves HTTP
queries on `127.0.0.1:8089` (JSON by default; protobuf via
`Accept: application/x-protobuf`).

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

Loopback (on the relay host):

```sh
# Claim by PK (enriched: supplies, upkeep, tier, researched_techs, …)
curl -s http://127.0.0.1:8089/claim/1234567890

# Claim by name substring (case-insensitive; includes tier + owner username)
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

# Claim member roster (permissions + usernames + last_login_timestamp)
curl -s http://127.0.0.1:8089/claim/1234567890/members

# Claim citizens: members joined to skill levels + skill_names map
# (includes last_login_timestamp when known)
curl -s http://127.0.0.1:8089/claim/1234567890/citizens

# Per-member Hex Coin totals (item_id == 1 across personal bags)
curl -s http://127.0.0.1:8089/claim/1234567890/hexcoins

# Player by name substring (case-insensitive; clients should send ≥2 chars)
curl -s 'http://127.0.0.1:8089/player?name=maple'
# → [ { "entity_id", "username", "region",
#       "last_login_timestamp"?, "signed_in"? }, ... ]

curl -s http://127.0.0.1:8089/player/1297036692699996362
# → { "entity_id", "username", "region",
#     "last_login_timestamp"?, "signed_in"? }
# last_login_timestamp is unix seconds from player_state.sign_in_timestamp
# (BitJita lastLogin); omitted when unknown / zero.

# Personal inventories (pockets / bank / wagon / cache / recovery /
# deployable). Toolbelt & Wallet omitted. Items aggregated per bag.
curl -s http://127.0.0.1:8089/player/1297036692699996362/inventory
# → { "player": {...}, "inventories": [
#      { "entity_id", "name", "nickname", "category",
#        "claim_entity_id", "claim_name",
#        "items": [ { "item_id", "item_type", "quantity" } ] }, ...
#    ] }

# First house for the player (resolved server-side from rent whitelist).
curl -s http://127.0.0.1:8089/player/1297036692699996362/housing
# → { "status": "ok"|"noHouse", "player": {...},
#     "house": { "entity_id", "name", "region" } | null,
#     "buildings": [ { "entity_id", "name", "nickname", "items": [...] } ] }

# Player skill levels (from experience_state + vendored XP thresholds)
curl -s http://127.0.0.1:8089/player/1297036692699996362/skills

# Health / readiness (always JSON)
curl -s http://127.0.0.1:8089/cache-health

# Protobuf schemas (JSON list + raw `.proto` download)
curl -s http://127.0.0.1:8089/proto
curl -sO http://127.0.0.1:8089/proto/relay_cache.proto

# Same data routes as protobuf (`Accept: application/x-protobuf`).
# Name-search wraps the array in ClaimList / PlayerList; entity IDs are
# uint64 (JSON keeps them as strings for JS safety).
curl -sH 'Accept: application/x-protobuf' \
  http://127.0.0.1:8089/claim/1234567890 -o claim.pb
curl -sH 'Accept: application/x-protobuf' \
  'http://127.0.0.1:8089/claim?name=concordia' -o claims.pb
curl -sH 'Accept: application/x-protobuf' \
  http://127.0.0.1:8089/claim/1234567890/inventory -o inventory.pb
curl -sH 'Accept: application/x-protobuf' \
  'http://127.0.0.1:8089/player?name=maple' -o players.pb
curl -sH 'Accept: application/x-protobuf' \
  http://127.0.0.1:8089/player/1297036692699996362/inventory -o player-inv.pb
curl -sH 'Accept: application/x-protobuf' \
  http://127.0.0.1:8089/player/1297036692699996362/housing -o player-housing.pb
curl -sH 'Accept: application/x-protobuf' \
  http://127.0.0.1:8089/player/1297036692699996362/skills -o player-skills.pb
```

Public (nginx on `relay.bitcraftsync.app` → loopback `:8089`; see
[`tools/nginx-relay-cache.snippet`](../../tools/nginx-relay-cache.snippet)):

```sh
curl -s https://relay.bitcraftsync.app/cache-health
curl -s https://relay.bitcraftsync.app/proto
curl -sO https://relay.bitcraftsync.app/proto/relay_cache.proto
curl -s 'https://relay.bitcraftsync.app/claim?name=concordia'
curl -s https://relay.bitcraftsync.app/claim/1234567890/inventory
curl -s https://relay.bitcraftsync.app/claim/1234567890/members
curl -s https://relay.bitcraftsync.app/claim/1234567890/citizens
curl -s https://relay.bitcraftsync.app/claim/1234567890/hexcoins
curl -s 'https://relay.bitcraftsync.app/player?name=maple'
curl -s https://relay.bitcraftsync.app/player/1297036692699996362/inventory
curl -s https://relay.bitcraftsync.app/player/1297036692699996362/housing
curl -s https://relay.bitcraftsync.app/player/1297036692699996362/skills
curl -sH 'Accept: application/x-protobuf' \
  https://relay.bitcraftsync.app/claim/1234567890/inventory -o inventory.pb
```

## Memory policy

The ceiling is an alarm, not a load shedder. Approaching it logs a warning
and flips `/cache-health` `ready=false`, but queries keep serving with whatever
data is loaded. Projected resident grows with player/deployable/rent/
`experience_state` tables on top of the prior claim/inventory set.
