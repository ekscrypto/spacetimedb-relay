# Port allocation

The relay deployment runs **one global instance and one regional
instance per BitCraft region** on a single host, plus a single shared
local SpacetimeDB. Every listen port is derived from the in-game region
ID so the mapping is self-describing — region 14 is always on `3014`.

## Formula

```
BASE = 3000

GLOBAL        frontend = BASE              dashboard = BASE + 100
REGIONAL      frontend = BASE + regionID   dashboard = BASE + 100 + regionID
SHARED STDB   BASE + 50   (loopback only, single instance)
```

`regionID` is the BitCraft in-game region number (1–49). The port
*is* the region ID, offset by the base. Global is the special case at
offset 0.

## Bands

| Band           | Ports         | Use                                   |
|----------------|---------------|---------------------------------------|
| Global         | `3000`        | Global/reference frontend             |
| Regional (ws)  | `3001–3049`   | Public downstream WebSocket, one per region ID |
| Shared infra   | `3050`        | Local SpacetimeDB (loopback)          |
| Dashboard      | `3100–3149`   | Per-instance `/metrics` (loopback)    |
| Reserved gap   | `3051–3099`   | Free                                  |

Each instance's frontend and dashboard share the same low byte:
region *N* → frontend `30NN`, dashboard `31NN`. Global is *N* = 0.
Region IDs that share a low byte with the stdb (`3050`) or fall in the
reserved gap do not collide because the high band differs; region 50
itself is excluded (it would land on the stdb port) but BitCraft does
not use such IDs.

## Example

| Instance | frontend | dashboard |
|----------|----------|-----------|
| global   | 3000     | 3100      |
| region 7 | 3007     | 3107      |
| region 14| 3014     | 3114      |

## Local dev

The single-instance defaults baked into the binary
(`--stdb-url ws://127.0.0.1:3000`, `--frontend-bind 0.0.0.0:3009`,
`--dashboard-bind 127.0.0.1:3001`) are for local development only and
do not follow this scheme. Multi-instance deployments override every
bind explicitly via per-unit systemd `Environment=` / flags.
