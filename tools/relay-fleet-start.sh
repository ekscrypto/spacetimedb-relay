#!/bin/sh
# relay-fleet-start.sh — sequentially start the relay fleet.
#
# Prevents a thundering herd: without this, all relay-bc* instances start
# in parallel at boot or on a bulk restart, which is survivable in the
# cached (no-schema-change) path but OOMs the box on schema drift
# (every region runs codegen + cargo build at ~2.8 GiB peak each).
#
# Run as root (calls systemctl). Invoked by relay-fleet-sequencer.service
# at boot, or manually after a binary update / bulk stop.
#
# Behavior:
#   1. Wait for the shared stdb (127.0.0.1:3050/v1/health → 200).
#   2. Start relay-global, then each relay-bc* region in ascending ID
#      order, one at a time. For each: start it, then poll its dashboard
#      /metrics until upstream.state == "up" before starting the next.
#      This serializes the build peaks so they never overlap.
#   3. Idempotent: a region already "up" is skipped — safe to re-run any
#      time. A region that times out is logged and skipped; we don't
#      block the whole fleet on one stuck region.
#
# Requires: curl, python3, systemctl. No write side effects beyond
# `systemctl start`.

set -u

STDB_URL="http://127.0.0.1:3050/v1/health"
STDB_WAIT_secs=120      # stdb boot is fast; 2 min is generous
RELAY_READY_secs=600    # schema-drift path runs cargo build: ~5-7 min
POLL_interval=5

log() { echo "relay-fleet-sequencer: $*"; }

# Extract the dashboard port for a unit from its unit file.
dash_port() {
    grep -oE 'dashboard-bind 127\.0\.0\.1:[0-9]+' "/etc/systemd/system/$1.service" 2>/dev/null \
        | grep -oE '[0-9]+$' | head -1
}

# Wait for stdb /v1/health to return HTTP 200.
wait_for_stdb() {
    log "waiting for stdb health at $STDB_URL (up to ${STDB_WAIT_secs}s)…"
    elapsed=0
    while [ "$elapsed" -lt "$STDB_WAIT_secs" ]; do
        code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 4 "$STDB_URL" 2>/dev/null || echo 000)
        if [ "$code" = "200" ]; then
            log "stdb healthy after ${elapsed}s."
            return 0
        fi
        sleep "$POLL_interval"
        elapsed=$((elapsed + POLL_interval))
    done
    log "ERROR: stdb not healthy after ${STDB_WAIT_secs}s (last HTTP $code) — aborting fleet start."
    return 1
}

# Is a relay already up? Queries its dashboard /metrics.
relay_state() {
    port=$(dash_port "$1")
    [ -z "$port" ] && { echo "unknown"; return; }
    curl -s --max-time 4 "http://127.0.0.1:${port}/metrics" 2>/dev/null \
        | python3 -c "import sys,json; print(json.load(sys.stdin)['upstream']['state'])" 2>/dev/null || echo "unknown"
}

# Start one relay and block until its upstream is "up" (or timeout).
# Returns 0 on up, 1 on timeout.
start_relay() {
    unit="$1"
    state=$(relay_state "$unit")
    if [ "$state" = "up" ]; then
        log "  $unit: already up — skipping."
        return 0
    fi
    log "  $unit: starting (current state: $state)…"
    systemctl start "$unit" 2>&1 | sed 's/^/    /'
    # Poll for upstream.state == up.
    elapsed=0
    while [ "$elapsed" -lt "$RELAY_READY_secs" ]; do
        sleep "$POLL_interval"
        elapsed=$((elapsed + POLL_interval))
        state=$(relay_state "$unit")
        if [ "$state" = "up" ]; then
            log "  $unit: up after ${elapsed}s."
            return 0
        fi
    done
    log "  $unit: TIMEOUT after ${RELAY_READY_secs}s (state=$state) — moving on."
    return 1
}

# Discover relay units in start order: global first, then regions by ID.
discover_units() {
    for f in /etc/systemd/system/relay-*.service; do
        [ -e "$f" ] || continue
        name=$(basename "$f" .service)
        case "$name" in
            relay-stdb) continue ;;
            relay-fleet-sequencer) continue ;;
            relay-global) echo "0global $name" ;;
            relay-bc*)    echo "$(echo "$name" | sed 's/relay-bc//') $name" ;;
            *)            echo "999 $name" ;;
        esac
    done | sort -n | awk '{print $2}'
}

# --- main ---

wait_for_stdb || exit 1

ok=0
timed_out=0
timed_out_list=""
total=0

log "starting relays sequentially (global first, then regions by ID)…"
for unit in $(discover_units); do
    total=$((total + 1))
    if start_relay "$unit"; then
        ok=$((ok + 1))
    else
        timed_out=$((timed_out + 1))
        timed_out_list="$timed_out_list $unit"
    fi
done

log "done: $ok/$total up, $timed_out timed out.${timed_out_list:+  Timed out:}${timed_out_list}"
# Non-zero exit if anything timed out, so oneshot failure is visible to
# systemd/journald — but RemainAfterExit=yes keeps the unit "active" so
# boot still completes and the stalled regions retry via their own Restart=.
[ "$timed_out" -eq 0 ] || exit 1
