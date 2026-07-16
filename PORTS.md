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

## Public exposure

The frontend band `3000–3025` is exposed to the public internet, one
TLS listener per port, fronted by nginx. Clients connect with

```
wss://relay.bitcraftsync.app:<port>/v1/database/<mirror-database>/subscribe
```

where `<port>` is `3000` for global and `3000 + regionID` for a region
(the port *is* the region, same formula as everywhere else).

The same port also serves the upstream schema over plain HTTP (no new
nginx route — nginx already proxies HTTP on these ports):

```
https://relay.bitcraftsync.app:<port>/v1/database/<mirror-database>/schema?version=9
```

The schema bytes are the ones the relay cached at startup and used to
codegen+publish the running mirror, so they always match the served
data. See README §"Schema endpoint".

- nginx binds the public IPs on each port `3000–3025`, terminates TLS
  with the host's Let's Encrypt cert for `relay.bitcraftsync.app`, and
  proxies plain WebSocket to the loopback relay on the same port number
  (relay binds `127.0.0.1:<port>`, nginx binds `<pubip>:<port>` —
  distinct addresses, so the two listeners coexist).
- Each relay frontend is loopback-only; nginx is the only public
  listener on these ports. Dashboards (`3100–3149`) and the shared
  stdb (`3050`) remain strictly loopback.
- UFW allows `3000:3025/tcp` (plus `22/80/443`). Ports with no relay
  behind them (e.g. `3001` when region 1 isn't running) return nginx's
  `502 Bad Gateway` — the port is reachable, the upstream isn't.
  Standing up a new region in `1..25` makes it public with zero nginx
  or UFW follow-up: nginx is already listening on its port.
- Regions `26–49` (ports `3026–3049`) are **not** in the open band.
  The first time one is needed, widen the nginx `listen` directives
  and the UFW rule from `3025` up to the needed port.

## Local dev

The single-instance defaults baked into the binary
(`--stdb-url ws://127.0.0.1:3000`, `--frontend-bind 0.0.0.0:3009`,
`--dashboard-bind 127.0.0.1:3001`) are for local development only and
do not follow this scheme. Multi-instance deployments override every
bind explicitly via per-unit systemd `Environment=` / flags.
