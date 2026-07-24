# Inventory WebSocket — agent handoff

Minimal contract for wiring a frontend to live inventory **and crafts**
updates via **relay-cache**. Prefer the multiplexed endpoint (one socket
per page).

## Endpoint

```
wss://relay.bitcraftsync.app/inventory/ws
```

(Local loopback: `ws://127.0.0.1:8089/inventory/ws`.)

Browser `WebSocket` always uses GET Upgrade — there is no POST body on
connect. Send the subscription as the **first text frame** after `onopen`.

## Subscribe (client → server)

```json
{
  "players": ["1297036692699996362"],
  "houses":  ["1297036692699996362"],
  "claims":  ["576460752317570321"],
  "player_crafts": ["1297036692699996362"],
  "claim_crafts":  ["576460752317570321"]
}
```

| Field | Meaning |
|-------|---------|
| `players` | Player entity ids → personal bags (pockets, bank, wagon, mounts, …) — same payload as `GET /player/:id/inventory` |
| `houses` | Player entity ids → housing interiors — same as `GET /player/:id/housing` |
| `claims` | Claim entity ids → shared claim storage — same as `GET /claim/:id/inventory` |
| `player_crafts` | Player entity ids → progressive + passive crafts — same as `GET /player/:id/crafts` (no `completed` filter; client filters) |
| `claim_crafts` | Claim entity ids → crafts on claim buildings — same as `GET /claim/:id/crafts` |

Rules:

- Entity ids are **u64**. Prefer **strings** in JSON (JS `Number` is unsafe above 2^53). Numbers are accepted but not recommended.
- At least one non-empty list required.
- Max **64** distinct `(type, id)` keys total (all lists combined after dedupe).
- Sending another subscribe frame **replaces** the previous set (full resync of snapshots for the new set).

Resolve ids first via HTTP if needed:

- `GET https://relay.bitcraftsync.app/player?name=Maplesugar`
- Claim ids from claim search / membership / bank bag `claim_entity_id` fields on player inventory.

## Server → client frames

### Initial burst (then ack)

One frame per subscribed key, then:

```json
{ "type": "subscribed", "count": 4 }
```

### Snapshot / update (same shape)

```json
{
  "type": "player_inventory",
  "entity_id": "1297036692699996362",
  "data": { /* identical to GET /player/:id/inventory JSON */ }
}
```

| `type` | `data` matches |
|--------|----------------|
| `player_inventory` | `GET /player/:id/inventory` |
| `player_housing` | `GET /player/:id/housing` |
| `claim_inventory` | `GET /claim/:id/inventory` |
| `player_crafts` | `GET /player/:id/crafts` (both completed + in-progress) |
| `claim_crafts` | `GET /claim/:id/crafts` (both completed + in-progress) |

Missing entity (still subscribed):

```json
{
  "type": "player_inventory",
  "entity_id": "…",
  "error": "player not found"
}
```

After `subscribed`, further frames are **live updates** for keys that changed (full snapshot per key, not a diff). Coalesced ~75 ms server-side.

### Heartbeat (connectivity)

Every **5 seconds** (after subscribe completes):

```json
{ "ts": 1753296000123 }
```

Unix time in milliseconds (UTC). No `type` field — distinguish from inventory frames by the presence of `ts` alone (snapshots always have `type` + `entity_id`). Treat ~15 s without a heartbeat as a dead connection and reconnect.

Protocol / validation errors (e.g. bad JSON, empty subscribe):

```json
{ "error": "…" }
```

(may close the socket afterward).

## Frontend sketch

```js
const PLAYER = "1297036692699996362"; // from /player?name=…
const CLAIM = "576460752317570321";

const ws = new WebSocket("wss://relay.bitcraftsync.app/inventory/ws");
let lastBeat = Date.now();

ws.onopen = () => {
  ws.send(JSON.stringify({
    players: [PLAYER],
    houses: [PLAYER],
    claims: [CLAIM],
    player_crafts: [PLAYER],
    claim_crafts: [CLAIM],
  }));
};

ws.onmessage = (ev) => {
  const msg = JSON.parse(ev.data);
  if (msg.ts != null && msg.type == null) {
    lastBeat = Date.now(); // or msg.ts
    return;
  }
  if (msg.type === "subscribed") {
    // initial burst done
    return;
  }
  if (msg.error && !msg.data) {
    console.error("inventory ws", msg.error);
    return;
  }
  switch (msg.type) {
    case "player_inventory":
      // msg.entity_id, msg.data | msg.error
      break;
    case "player_housing":
      break;
    case "claim_inventory":
      break;
    case "player_crafts":
      // msg.data.crafts — same shape as GET /player/:id/crafts
      break;
    case "claim_crafts":
      break;
  }
};

// Watchdog: if heartbeats stop, reconnect.
setInterval(() => {
  if (ws.readyState === WebSocket.OPEN && Date.now() - lastBeat > 15_000) {
    ws.close();
    // open a new WebSocket…
  }
}, 5_000);
```

## Semantics notes for UI

- **Player inventory** categories include `pockets`, `toolbelt`, `wallet`, `bank`, `wagon`, `cache`, `boat`, `recovery`, `deployable` (goats/birds/mounts are `deployable`).
- **Housing** is separate from player inventory (`status`: `ok` | `noHouse`).
- **Claim inventory** is shared storage only (no Town Banks); grouped by dimension.
- **Crafts** unify `progressive_action_state` + `passive_craft_state` with recipe outputs; WS always returns both completed and in-progress (filter client-side with `completed`).
- Updates can fire with the same aggregate item-stack counts (internal bag churn); treat each frame as authoritative for that `type`+`entity_id`.

## Caps / ops

- Soft cap ~512 interest leases fleet-wide; one page with ≤64 keys is fine.
- `/cache-health` → `streams.{active,lifetime_notifies,lifetime_pushes}` for sanity checks.
- Per-entity URLs still exist (`/player/:id/inventory/ws`, `/housing/ws`, `/claim/:id/inventory/ws`) but prefer `/inventory/ws` for multi-source pages. There is no per-entity `/crafts/ws`.

## Out of scope

- Auth (same public trust model as the HTTP cache).
- Protobuf on the WS (JSON text frames only).
- Skills / citizens live streams.
