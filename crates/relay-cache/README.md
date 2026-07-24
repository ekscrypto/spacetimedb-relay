# relay-cache

Same-host in-memory read cache over the relay fleet. Subscribes to each
regional frontend on loopback (`ws://127.0.0.1:<port>`, v2), holds claim /
building / inventory / player tables in columnar memory, and serves HTTP
queries plus live inventory WebSocket streams on `127.0.0.1:8089` (JSON by
default; protobuf via `Accept: application/x-protobuf` on HTTP).

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
- `--debug` / `RELAY_CACHE_DEBUG` — 5s heartbeats while waiting on
  `SubscribeApplied` (phase + elapsed), WS ping during that wait, and
  `relay_cache=debug` when `RUST_LOG` is unset. Always-on info logs already
  include per-query sequential Subscribe, wire bytes + wait time on each
  Applied, and total bulk-load duration.

Ingest uses two additive v2 `Subscribe` query sets: (1) all base table
queries in one set, (2) hexite `location_state` PK queries in a second
set — so busy regions never re-dump the full snapshot. (A prior hang on
the second Subscribe was a frontend ClientMessage framing bug, not a
local SpacetimeDB limit.)

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
#       "last_login_timestamp"?, "signed_in"?, "last_active_timestamp"? }, ... ]

curl -s http://127.0.0.1:8089/player/1297036692699996362
# → { "entity_id", "username", "region",
#     "last_login_timestamp"?, "signed_in"?, "last_active_timestamp"? }
# last_login_timestamp is unix seconds from player_state.sign_in_timestamp
# (BitJita lastLogin); omitted when unknown / zero (BitCraft clears it on
# logout). last_active_timestamp is unix seconds from
# mobile_entity_state.timestamp and survives logout.

# Personal inventories (pockets / toolbelt / wallet / bank / wagon /
# cache / boat / recovery / deployable). Items aggregated per bag.
curl -s http://127.0.0.1:8089/player/1297036692699996362/inventory
# → { "player": {...}, "inventories": [
#      { "entity_id", "name", "nickname", "category",
#        "claim_entity_id", "claim_name",
#        "items": [ { "item_id", "item_type", "quantity" } ] }, ...
#    ] }

# Live inventory streams (JSON text frames). Per-entity:
#   ws://127.0.0.1:8089/player/<id>/inventory/ws
#   ws://127.0.0.1:8089/player/<id>/housing/ws
#   ws://127.0.0.1:8089/claim/<id>/inventory/ws
# Multiplexed (one socket for a page — preferred):
#   ws://127.0.0.1:8089/inventory/ws
# After connect, send:
#   { "players": ["<id>", …], "houses": ["<id>", …], "claims": ["<id>", …],
#     "player_crafts": ["<id>", …], "claim_crafts": ["<id>", …] }
# Server replies with tagged snapshots
#   { "type": "player_inventory"|"player_housing"|"claim_inventory"
#            |"player_crafts"|"claim_crafts",
#     "entity_id": "<id>", "data": {…} }
# then `{ "type": "subscribed", "count": N }`, then further tagged
# snapshots on change. Heartbeat every 5s: { "ts": <unix ms UTC> }
# A later subscribe frame replaces the set.
# Max 64 entities per subscribe. Active stream counts under /cache-health.

# First house for the player (resolved from `player_housing_state`, whose
# `entity_id` is the player PK). `house.name` is
# "{username}'s House ({catalog})" or "{nickname} ({catalog})" when the
# entrance has a nickname. Interior storages are found by dimension.
curl -s http://127.0.0.1:8089/player/1297036692699996362/housing
# → { "status": "ok"|"noHouse", "player": {...},
#     "house": { "entity_id", "name", "region" } | null,
#     "buildings": [ { "entity_id", "name", "nickname", "items": [...] } ] }

# Player skill levels (from experience_state + vendored XP thresholds)
curl -s http://127.0.0.1:8089/player/1297036692699996362/skills

# Crafts at a claim (progressive + passive). Optional `completed=true|false`
# filter; omit to return both. Owners with inventory permission are filtered
# client-side via /claim/<id>/members.
curl -s 'http://127.0.0.1:8089/claim/1234567890/crafts'
curl -s 'http://127.0.0.1:8089/claim/1234567890/crafts?completed=false'
# → { "claim": {...}, "crafts": [
#      { "entity_id", "recipe_id", "craft_count", "progress",
#        "total_actions_required", "completed",
#        "owner_entity_id", "owner_username",
#        "building_entity_id", "building_name", "claim_entity_id",
#        "crafted_item": [ { "item_id", "quantity", "item_type" } ] }, ...
#    ], "count": N }

# Crafts owned by a player (any claim), same completed filter.
curl -s 'http://127.0.0.1:8089/player/1297036692699996362/crafts'
curl -s 'http://127.0.0.1:8089/player/1297036692699996362/crafts?completed=true'

# Hexite Deposits (global flat list). Optional `?region=N` filter.
# Unowned `claim_state` rows (`owner_player_entity_id=0`) whose name starts
# with `{0} (N: {1}, E: {2})|~Hexite Deposit|` — N/E parsed from the name.
# Respawn: `claim_local` world x/z ⋈ hexite `resource_state` location ⋈
# `growth_state.end_timestamp` on that resource entity.
# Active: omit `respawn_at` and `status`. Depleted: `respawn_at` from
# `growth_state.end_timestamp` (public; Depleted→Hexite grows 6–8 days).
# Join miss: `status: "unknown"` (do **not** treat as harvestable — the
# shard may still be attaching overworld location PKs).
curl -s 'http://127.0.0.1:8089/deposits'
curl -s 'http://127.0.0.1:8089/deposits?region=14'
# → { "deposits": [
#      { "north": 6158, "east": 8174, "entity_id": "...",
#        "name": "Hexite Deposit (N: 6158, E: 8174)", "region": 14 },
#      { "north": 7050, "east": 4609, "entity_id": "...",
#        "name": "Hexite Deposit (N: 7050, E: 4609)",
#        "respawn_at": "2026-07-26T08:43:52.011Z", "region": 13 }, ...
#    ], "count": N }

# Storage logs (upstream retention ~15–16 days via storage_log_cleanup_loop).
# Exactly one mode per request:
#   storageId=…                         — full history for one chest
#   playerId=…                          — all actions by that player
#   itemId=…&claimId=…                  — that item across a claim's buildings
#   itemId=…&playerId=…                 — that item in one player's actions
# Optional: itemType=Item|Cargo (item modes only), region=N, limit=N (newest N)
curl -s 'http://127.0.0.1:8089/storage-logs?storageId=1008806316593474517'
curl -s 'http://127.0.0.1:8089/storage-logs?storageId=1008806316593474517&limit=50'
curl -s 'http://127.0.0.1:8089/storage-logs?playerId=1297036692699996362'
curl -s 'http://127.0.0.1:8089/storage-logs?itemId=1080001&claimId=1234567890'
curl -s 'http://127.0.0.1:8089/storage-logs?itemId=1080001&playerId=1297036692699996362&itemType=Item&region=14'
# → { "logs": [
#      { "id", "region",
#        "building": { "entity_id", "name", "nickname" },
#        "claim_entity_id", "claim_name", "player_entity_id", "player_username",
#        "action": "deposit"|"withdraw"|"reserved",
#        "item_id", "item_type", "quantity", "timestamp", "days_since_epoch" }, ...
#    ], "count": N }

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
curl -sH 'Accept: application/x-protobuf' \
  'http://127.0.0.1:8089/claim/1234567890/crafts' -o claim-crafts.pb
curl -sH 'Accept: application/x-protobuf' \
  'http://127.0.0.1:8089/player/1297036692699996362/crafts' -o player-crafts.pb
curl -sH 'Accept: application/x-protobuf' \
  'http://127.0.0.1:8089/storage-logs?storageId=1008806316593474517' -o storage-logs.pb
```

Public (nginx on `relay.bitcraftsync.app` → loopback `:8089`; see
[`tools/nginx-relay-cache.snippet`](../../tools/nginx-relay-cache.snippet)):

```sh
curl -s https://relay.bitcraftsync.app/cache-health
curl -s https://relay.bitcraftsync.app/proto
curl -sO https://relay.bitcraftsync.app/proto/relay_cache.proto
curl -s 'https://relay.bitcraftsync.app/claim?name=concordia'
curl -s https://relay.bitcraftsync.app/claim/1234567890
curl -s https://relay.bitcraftsync.app/claim/1234567890/inventory
curl -s https://relay.bitcraftsync.app/claim/1234567890/members
curl -s https://relay.bitcraftsync.app/claim/1234567890/citizens
curl -s https://relay.bitcraftsync.app/claim/1234567890/hexcoins
curl -s 'https://relay.bitcraftsync.app/claim/1234567890/crafts?completed=false'
curl -s 'https://relay.bitcraftsync.app/player?name=maple'
curl -s https://relay.bitcraftsync.app/player/1297036692699996362
curl -s https://relay.bitcraftsync.app/player/1297036692699996362/inventory
curl -s https://relay.bitcraftsync.app/player/1297036692699996362/housing
curl -s https://relay.bitcraftsync.app/player/1297036692699996362/skills
curl -s 'https://relay.bitcraftsync.app/player/1297036692699996362/crafts?completed=true'
curl -s 'https://relay.bitcraftsync.app/deposits'
curl -s 'https://relay.bitcraftsync.app/deposits?region=14'
curl -s 'https://relay.bitcraftsync.app/storage-logs?storageId=1008806316593474517&limit=50'
curl -sH 'Accept: application/x-protobuf' \
  https://relay.bitcraftsync.app/claim/1234567890/inventory -o inventory.pb
```

## Memory policy

The ceiling is an alarm, not a load shedder. Approaching it logs a warning
and flips `/cache-health` `ready=false`, but queries keep serving with whatever
data is loaded. Projected resident grows with player/deployable/rent/
`experience_state` tables on top of the prior claim/inventory set.
