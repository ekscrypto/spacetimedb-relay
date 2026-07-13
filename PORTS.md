# Port allocation

The relay deployment runs **one global instance and up to 25 regional
instances** on a single host, plus a single shared local SpacetimeDB.
All listen ports are derived from a single base and an instance index
so the entire fleet is predictable.

## Formula

```
BASE = 3000

GLOBAL        frontend = BASE              dashboard = BASE + 100
REGIONAL      frontend = BASE + index      dashboard = BASE + 100 + index
SHARED STDB   BASE + 50   (loopback only, single instance)
```

`index` is a 1-based sequential index **we assign** to each regional
relay (1–25). It is *not* BitCraft's in-game region ID — see
[Index mapping](#index-mapping) below.

## Bands

| Band           | Ports         | Use                                   |
|----------------|---------------|---------------------------------------|
| Frontend (ws)  | `3000–3025`   | Public downstream WebSocket           |
| Shared infra   | `3050`        | Local SpacetimeDB (loopback)          |
| Dashboard      | `3100–3125`   | Per-instance `/metrics` (loopback)    |
| Reserved gap   | `3026–3049`   | Free — frontends if the fleet grows past 25 |

Each instance's frontend and dashboard share the same low byte:
instance *N* → frontend `30NN`, dashboard `31NN`. Global is *N* = 0.

## Index mapping

Because the port encodes our index rather than the BitCraft region ID,
the index → region assignment is recorded alongside the host-specific
deployment notes (out of this public repo). The rule for assigning a
new index is: **lowest free index, stable once given** — never renumber
an existing instance, since downstream clients may have pinned the port.

## Local dev

The single-instance defaults baked into the binary
(`--stdb-url ws://127.0.0.1:3000`, `--frontend-bind 0.0.0.0:3009`,
`--dashboard-bind 127.0.0.1:3001`) are for local development only and
do not follow this scheme. Multi-instance deployments override every
bind explicitly via per-unit systemd `Environment=` / flags.
