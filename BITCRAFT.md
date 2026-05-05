# BitCraft live game server (EA2)

This is fan-research, not anything Clockwork Labs publishes. Routing,
hostnames, and table layouts can change without notice.

## Architecture

Everything — meta and gameplay — runs on the same SpacetimeDB host:

```
https://bitcraft-early-access.spacetimedb.com
```

EA2 modules use the `bitcraft-live-*` naming, **not** the EA1
`bitcraft-{N}` / `bitcraft-global` names:

- **`bitcraft-live-global`** — game description / reference data
  (items, recipes, biomes, etc.) and the meta `region_connection_info`
  table. Replaces EA1's `bitcraft-global` and `bitcraft-3`.
- **`bitcraft-live-{region_id}`** — per-region gameplay shard. The
  region ID in the module name matches the in-game region ID directly
  (e.g. region 14 → `bitcraft-live-14`).

The EA1 names (`bitcraft-3`, `bitcraft-global`, `bitcraft-1`..`9`)
still resolve a `/v1/database/<name>/schema` HTTP response out of
cache, but their WebSocket subscribe endpoint accepts the upgrade and
then immediately TCP-resets — they're effectively decommissioned. The
HTTP success is misleading; don't infer "module is alive" from it.

There are **no per-region hostnames**. The Unity client opens both
`bitcraft-live-global` and `bitcraft-live-{N}` against the same
`bitcraft-early-access.spacetimedb.com` host (verified by reading
`Player.log` while the game runs).

Active region IDs as of 2026-05 (from Bitjita's public
`https://bitjita.com/api/status`):

| ID | Name      | Module                |
|----|-----------|-----------------------|
|  7 | Virexal   | `bitcraft-live-7`     |
|  8 | Solmere   | `bitcraft-live-8`     |
|  9 | Marowik   | `bitcraft-live-9`     |
| 12 | Elyndor   | `bitcraft-live-12`    |
| 13 | Hexalis   | `bitcraft-live-13`    |
| 14 | Lumethis  | `bitcraft-live-14`    |
| 17 | Draxen    | `bitcraft-live-17`    |
| 18 | Oryxen    | `bitcraft-live-18`    |
| 19 | Zephra    | `bitcraft-live-19`    |

## Bootstrap: discovering active modules

If you don't already know which region you want, subscribe to
`bitcraft-live-global / region_connection_info` with a real
game-account JWT. Each row has `host` and `module` columns; `host` is
the SpacetimeDB URL (currently always the EA host) and `module` is
the `bitcraft-live-{N}` name.

Anonymous identities are not authorized to read that table; the JWT
must be from an actual game account.

## Auth flow

```
POST https://api.bitcraftonline.com/authentication/request-access-code?email=<email>
POST https://api.bitcraftonline.com/authentication/authenticate?email=<email>&accessCode=<code>
```

The response is a JWT (`alg=ES256`, `aud=["spacetimedb"]`,
**`exp=null`** — long-lived bearer credential bound to the account via
`hex_identity` / `sub`). Treat it as account-equivalent.

## Local token (already authed via the game)

If BitCraft is installed on macOS and you're logged in, the Unity
build caches the JWT in PlayerPrefs:

```
~/Library/Preferences/com.ClockworkLabs.BitCraft.plist
```

Key shape (the `:` separators in the URL get folded into the key, and
the `@` in the email becomes `_`):

```
EarlyAccess:https://api.bitcraftonline.com:<email-with-@-as-_>:AuthToken
```

Extract via `plistlib` (the `:` in the key prevents `plutil -extract`
from working with its colon-separated key paths):

```sh
python3 -c "
import plistlib, os
d = plistlib.load(open(os.path.expanduser(
    '~/Library/Preferences/com.ClockworkLabs.BitCraft.plist'), 'rb'))
print(next(v for k,v in d.items() if k.endswith(':AuthToken') and v))
"
```

For convenience this project keeps a working copy in **`.bitcraft-token`**
at the repo root (mode 600, gitignored alongside `CLAUDE.local.md`).
Consume it via the existing `RELAY_UPSTREAM_TOKEN` env var:

```sh
RELAY_UPSTREAM_TOKEN=$(cat .bitcraft-token) cargo run -p relay -- \
  --upstream wss://bitcraft-early-access.spacetimedb.com \
  --database bitcraft-live-14 \
  --upstream-protocol v1 \
  ...
```

## Wire format

The EA host accepts both `v1.bsatn.spacetimedb` and
`v1.json.spacetimedb` interchangeably on `bitcraft-live-*` modules.
The relay's existing v1-BSATN path (`v1_compat`) works as-is — no
v1-JSON support needed.

Public scrapers (`raffiandev/bitcraft-stdb`,
`BitCraftToolBox/automata`, Bitjita) historically negotiated
`v1.json.spacetimedb` because the JSON shape is easier to debug from
Python. That's a convention, not a server requirement.

> **Historical note.** Earlier rounds of this investigation thought
> the EA host was rejecting BSATN — really it was the EA1 module
> names (`bitcraft-3`, `bitcraft-global`) that no longer accept
> subscribes in EA2. With the right `bitcraft-live-*` module name,
> BSATN works on the first try.

## Caveats

- **Single session per identity.** `raffiandev/bitcraft-stdb`'s README
  warns the JWT may disconnect the running game client when re-used
  by another subscriber. Don't run the relay with a game-account
  token while you're logged in playing.
- **First-run schema sync is heavy.** `bitcraft-live-{N}` modules
  have hundreds of public-user tables; the relay creates a Postgres
  table per public-user table on first connect regardless of the
  `--subscribe-table` filter.
- **Tokens are long-lived.** Re-running the email/access-code flow
  invalidates earlier tokens; until then a leaked JWT is full account
  access.
