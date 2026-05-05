# In-memory store migration plan

Replace the Postgres mirror with an in-memory primary store backed by
periodic on-disk snapshots. Motivated by snapshot-reconcile latency on
large BitCraft-scale databases (~10 min vs ~10 s upstream receive) and
by the resulting reconnect cost on transient upstream drops.

## Stage 0 — Decisions to lock in

- **Memory budget per database**: accept ~3× raw BSATN size as RAM
  (2-3 GB for BitCraft EA2; smaller test DBs are KB-MB).
- **Persistence format**: per-table file
  `<data_dir>/<db_prefix>/<table>.snapshot`, layout =
  `[schema_hash || row_count || (pk_len, pk, bsatn_len, bsatn)*]`.
  One file per table — parallel writes, atomic per-table renames,
  no global lock.
- **Snapshot cadence**: every 60 s, plus on graceful shutdown
  (signal handler).
- **Crash recovery story**: on startup, load latest snapshots → on
  first upstream `SubscribeApplied`, gap-fill via the existing diff
  path. Accept losing `TransactionUpdate`s between last snapshot and
  crash; upstream resends them as part of the next snapshot.

## Stage 1 — Parallel-write mode (~1 day)

Goal: in-memory store fills alongside PG, no behaviour change.

1. New module `relay-storage::memstore` with API identical to current
   `Storage`: `sync_schema`, `apply_snapshot_diff`, `apply_diff`,
   `fetch_all_bsatn`. Backed by
   `HashMap<TableName, BTreeMap<Pk, Bytes>>`.
2. Modify `relay-storage::Storage` to hold both `pg` and `mem`;
   every mutating call writes to both.
3. Add a comparison test in `relay-storage/tests/`: after
   `apply_snapshot_diff`, dump both stores and `assert_eq!`. Run
   against the test database from `CLAUDE.local.md`.

## Stage 2 — Switch reads to in-memory (~0.5 day)

1. `fetch_all_bsatn` reads from `mem`. PG still written.
2. `relay-server`'s downstream fan-out reads from `mem` instead of
   issuing `SELECT _bsatn`.
3. Run the 250-table BitCraft test: snapshot reconcile and downstream
   propagation should both be sub-second now.
4. Keep PG writes on for one more stage as a paranoid "if mem is
   wrong, PG will tell us via comparison test".

## Stage 3 — Persistence (~1 day)

1. `Snapshotter` task: every 60 s, walk `mem`, write per-table files
   with `tempfile::persist`. Compute schema hash into the header.
2. On `Storage::connect`, scan `data_dir`, load files whose
   `schema_hash` matches the upstream's current schema; ignore the
   rest.
3. SIGTERM handler: trigger one final snapshot before exit.
4. Add an integration test that kills + restarts the relay between
   batches and confirms recovery.

## Stage 4 — Drop Postgres (~0.5 day)

1. Delete `pg` writes; remove `sqlx`, `postgres` deps from workspace.
2. Remove `docker-compose.yml`'s Postgres service.
3. Update `CLAUDE.md`: invariant 5 ("Postgres = canonical state")
   becomes "in-memory + snapshot file = canonical state".
4. Update README; `--database-url` flag goes away, replaced with
   `--data-dir`.

## Stage 5 — Optional (later)

- `/inspect/<table>?limit=N` HTTP endpoint that dumps decoded rows
  as JSONL — replaces `psql` for ad-hoc debugging.
- Snapshot compression (zstd) — `bitcraft-live-14`'s 961 MB → ~150
  MB on disk.
- WAL between snapshots if RPO matters more than the upstream
  gap-fill story.

## Risks / open questions

- **Per-client subscription state** currently lives in PG (per
  `CLAUDE.md` invariant 5). It's small; plan is to move it into a
  single tiny file alongside snapshots — confirm before Stage 4.
- **Identity tokens**: the relay does not appear to persist these
  (upstream issues them per connection); verify before Stage 4.
- **Memory exhaustion**: a misconfigured `--subscribe-table` filter
  against a hostile upstream could OOM. Add a `--max-table-bytes`
  guard in Stage 5.

## Time budget

| Stage | Estimate |
|-------|----------|
| 0 — decisions | ~1 h |
| 1 — parallel-write mode | ~1 day |
| 2 — read switch | ~0.5 day |
| 3 — persistence | ~1 day |
| 4 — drop Postgres | ~0.5 day |
| 5 — optional | as needed |
| **Total to cutover** | **~3 days** |
