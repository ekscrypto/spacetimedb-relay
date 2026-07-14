#!/bin/sh
# relay-staleness-monitor.sh — restart relay instances that go silent.
#
# A relay's upstream WebSocket can stay open while no longer delivering
# data: the process keeps running (so systemd Restart=on-failure won't
# catch it) and upstream.state reports "up", but units_5m/bytes_5m sit at
# zero. This monitor detects that state and restarts the instance.
#
# Long-running daemon. Run as root (calls systemctl). Auto-discovers
# relay-* units the same way fleet-status.sh does, so it stays correct as
# regions are added/removed.
#
# Detection (per instance, per poll):
#   stale = upstream.state == "up"
#           AND upstream.units_5m == 0
#           AND upstream.bytes_5m == 0
# The 5m metrics are already 5-minute sliding windows computed by the
# relay process, so a single poll seeing them at zero IS the 5-min-zero
# signal. state == "initial"/"down" is left to Restart=on-failure and the
# boot sequencer — only "up but silent" is this monitor's job.
#
# Safety gates before any restart:
#   1. stdb healthy (127.0.0.1:3050/v1/health → 200). If stdb is down
#      every relay looks stale at once; restarting is pointless.
#   2. No concurrent republish anywhere in the fleet (a schema-drift
#      rebuild peaks at ~2.8 GiB; never overlap one with a restart).
#   3. Per-instance cooldown (default 10 min) — covers the relay's
#      sliding-window refill after a restart, prevents restart loops.
#   4. At most one restart per poll cycle. Multiple stale instances are
#      caught on successive cycles, ≥60s apart.
#
# Env knobs (defaults shown):
#   STALENESS_POLL_SECS=60        poll interval
#   STALENESS_COOLDOWN_SECS=600   per-instance restart cooldown
#   STALENESS_CURL_TIMEOUT=4      per-instance /metrics fetch timeout
#   STALENESS_DRY_RUN=0           set 1 to log-only (no restarts)
#   STALENESS_STATE_DIR=/var/lib/relay-staleness
#   STALENESS_UNIT_DIR=/etc/systemd/system
#
# Requires: curl, python3, systemctl.
# Unit: relay-staleness-monitor.service.
# Logs to stdout/journald under prefix "relay-staleness:".

set -u

POLL_SECS="${STALENESS_POLL_SECS:-60}"
COOLDOWN_SECS="${STALENESS_COOLDOWN_SECS:-600}"
CURL_TIMEOUT="${STALENESS_CURL_TIMEOUT:-4}"
DRY_RUN="${STALENESS_DRY_RUN:-0}"
STATE_DIR="${STALENESS_STATE_DIR:-/var/lib/relay-staleness}"
UNIT_DIR="${STALENESS_UNIT_DIR:-/etc/systemd/system}"
STDB_URL="http://127.0.0.1:3050/v1/health"

log() { echo "relay-staleness: $*"; }

mkdir -p "$STATE_DIR"

# Discover mirror units in canonical order: global first, then regions by
# ascending ID. Excludes the shared stdb and infra units (no mirror).
discover() {
    for f in "$UNIT_DIR"/relay-*.service; do
        [ -e "$f" ] || continue
        name=$(basename "$f" .service)
        case "$name" in
            relay-stdb)              continue ;;
            relay-fleet-sequencer)   continue ;;
            relay-staleness-monitor) continue ;;
            relay-global) echo "0global $name" ;;
            relay-bc*)    echo "$(echo "$name" | sed 's/relay-bc//') $name" ;;
            *)            echo "999 $name" ;;
        esac
    done | sort -n | awk '{print $2}'
}

# Dashboard port for a unit, parsed from --dashboard-bind in its unit file.
dash_port() {
    grep -oE 'dashboard-bind 127\.0\.0\.1:[0-9]+' "$UNIT_DIR/$1.service" 2>/dev/null \
        | grep -oE '[0-9]+$' | head -1
}

# Is stdb reachable and healthy? Returns 0 if HTTP 200, 1 otherwise.
stdb_healthy() {
    code=$(curl -s -o /dev/null -w '%{http_code}' --max-time "$CURL_TIMEOUT" "$STDB_URL" 2>/dev/null || echo 000)
    [ "$code" = "200" ]
}

# Fetch one instance's /metrics JSON, or empty on failure.
fetch_metrics() {
    port=$(dash_port "$1")
    [ -z "$port" ] && return
    curl -s --max-time "$CURL_TIMEOUT" "http://127.0.0.1:${port}/metrics" 2>/dev/null || true
}

# Seconds since epoch now.
now_epoch() { date +%s; }

# Is this unit within its post-restart cooldown? 0 if still cooling, 1 if clear.
cooldown_clear() {
    unit="$1"
    stamp="$STATE_DIR/$unit.restart"
    [ -e "$stamp" ] || { return 0; }
    last=$(cat "$stamp" 2>/dev/null || echo 0)
    case "$last" in *[!0-9]*) last=0 ;; esac
    elapsed=$(( $(now_epoch) - last ))
    [ "$elapsed" -ge "$COOLDOWN_SECS" ]
}

# Snapshot the whole fleet in one pass. Emits one TSV line per reachable
# instance: unit<TAB>upstream_state<TAB>units_5m<TAB>bytes_5m<TAB>republishing
# Instances with no dashboard or an unreachable one are omitted — we can't
# assess them and "initial"/disconnected is not this monitor's job.
snapshot_fleet() {
    for unit in $(discover); do
        json=$(fetch_metrics "$unit")
        [ -z "$json" ] && continue
        printf '%s\t' "$unit"
        printf '%s' "$json" | python3 -c '
import sys, json
try:
    d = json.load(sys.stdin)
except Exception:
    print("parse_error\t0\t0\t0"); raise SystemExit
u = d.get("upstream", {}) or {}
p = d.get("publisher", {}) or {}
state = u.get("state") or ""
units = u.get("units_5m"); bytes = u.get("bytes_5m")
units = 0 if units is None else int(units)
bytes = 0 if bytes is None else int(bytes)
repub = "1" if p.get("republished_this_run") else "0"
print("\t".join([str(state), str(units), str(bytes), repub]))
'
    done
}

restart_unit() {
    unit="$1"
    ts=$(now_epoch)
    echo "$ts" > "$STATE_DIR/$unit.restart"
    if [ "$DRY_RUN" = "1" ]; then
        log "DRY-RUN: would restart $unit (cooldown stamp written)"
    else
        log "RESTARTING $unit (stale: upstream up, 5m throughput zero)"
        systemctl restart "$unit" 2>&1 | sed 's/^/  /'
        log "restarted $unit at $(date -Iseconds)"
    fi
}

# --- main loop ---

log "monitor started: poll=${POLL_SECS}s cooldown=${COOLDOWN_SECS}s dry_run=$DRY_RUN state=$STATE_DIR"
log "discovered units: $(discover | tr '\n' ' ')"

while :; do
    # Gate 1: stdb must be healthy, else every relay will look stale and
    # restarts would just fail to reconnect. Wait out the outage instead.
    if ! stdb_healthy; then
        log "stdb unhealthy — skipping cycle (restarts wouldn't reconnect)"
        sleep "$POLL_SECS"
        continue
    fi

    snap=$(snapshot_fleet)

    # Gate 2: if any instance is currently republishing, hold off — a
    # schema-drift rebuild peaks at ~2.8 GiB and must never overlap another.
    repub_any=$(printf '%s\n' "$snap" | awk -F'\t' '$5 == "1"' | head -1)
    if [ -n "$repub_any" ]; then
        log "republish in progress on a fleet instance — skipping cycle"
        sleep "$POLL_SECS"
        continue
    fi

    # Detect stale instances: upstream "up" but zero 5m throughput.
    # Emit unit<TAB>units_5m for each, in discovery order (lowest region first).
    stale=$(printf '%s\n' "$snap" \
        | awk -F'\t' '$2 == "up" && $3 == "0" && $4 == "0" {print $1}')

    if [ -z "$stale" ]; then
        # Quiet cycle. Uncomment for per-cycle heartbeat logging:
        # log "cycle ok: no stale instances"
        :
    else
        for unit in $stale; do
            if cooldown_clear "$unit"; then
                restart_unit "$unit"
                # Gate 4: one restart per cycle. Remaining stale instances
                # are caught on subsequent cycles (≥60s apart).
                break
            else
                last=$(cat "$STATE_DIR/$unit.restart" 2>/dev/null || echo 0)
                log "$unit is stale but in cooldown (last restart $(date -r "$last" -Iseconds 2>/dev/null || echo "$last")) — waiting"
            fi
        done
    fi

    sleep "$POLL_SECS"
done
