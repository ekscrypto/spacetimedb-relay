# BitCraft live game server (EA2)

This is fan-research, not anything Clockwork Labs publishes. Routing,
hostnames, and table layouts can change without notice.

## Architecture

The Early Access 1 host is *still* the meta surface in EA2:

```
https://bitcraft-early-access.spacetimedb.com
```

It exposes two long-lived modules from EA1:

- **`bitcraft-global`** (412 public-user tables) — game description /
  reference data (items, recipes, biomes, etc.). Schema is fetchable
  unauthenticated; live subscribes need a token.
- **`bitcraft-3`** (446 public-user tables) — account & region
  routing, including the `region_connection_info` table.

Actual EA2 *gameplay* shards live on per-region SpacetimeDB hosts whose
addresses aren't published anywhere. The only way to find them is to
subscribe to `bitcraft-3 / region_connection_info` with a real game
account and read the `host` + `module` columns per row. EA1 module
numbering (`bitcraft-1`..`bitcraft-9`) does **not** correspond to EA2
region IDs.

Active region IDs as of 2026-05 (from Bitjita's public
`https://bitjita.com/api/status`):

| ID | Name      |
|----|-----------|
|  7 | Virexal   |
|  8 | Solmere   |
|  9 | Marowik   |
| 12 | Elyndor   |
| 13 | Hexalis   |
| 14 | Lumethis  |
| 17 | Draxen    |
| 18 | Oryxen    |
| 19 | Zephra    |

IDs are non-contiguous and likely game-internal — module names
themselves are whatever `region_connection_info[N].module` says.

## Bootstrap: discovering a region's host + module

```python
# adapted from raffiandev/bitcraft-stdb
res = dump_tables(meta_host, 'bitcraft-3', 'region_connection_info', auth)
host, module = res['region_connection_info'][REGION_ID]
```

Anonymous identities are not authorized for that table; the JWT must
be from an actual game account.

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
  --database bitcraft-3 \
  --upstream-protocol v1 \
  --subscribe-table region_connection_info \
  ...
```

## Open: wire format

Every public BitCraft scraper (Bitjita's reference Python,
`raffiandev/bitcraft-stdb`, `BitCraftToolBox/automata`) negotiates
`v1.json.spacetimedb`. Our relay only speaks `v1.bsatn.spacetimedb`
and `v2.bsatn.spacetimedb`. With the BSATN subprotocol the EA host
returns 101 (Switching Protocols) but immediately resets the
connection. Cause is unconfirmed: could be auth (we tested with an
anonymous identity) or wire format (BitCraft's deployed SpacetimeDB
build may be JSON-only). Once a real game-account JWT is wired in, if
the reset persists we'll need a v1-JSON path in `relay-upstream`
alongside the existing v1-BSATN one in `v1_compat`.

## Caveats

- **Single session per identity.** `raffiandev/bitcraft-stdb`'s README
  warns the JWT may disconnect the running game client when re-used
  by another subscriber. Don't run the relay with a game-account
  token while you're logged in playing.
- **First-run schema sync is heavy.** `bitcraft-3` has 446 public-user
  tables; the relay creates a Postgres table per public-user table on
  first connect regardless of the `--subscribe-table` filter.
- **Tokens are long-lived.** Re-running the email/access-code flow
  invalidates earlier tokens; until then a leaked JWT is full account
  access.
