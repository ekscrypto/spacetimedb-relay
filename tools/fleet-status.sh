#!/bin/sh
# fleet-status.sh — print a one-line-per-instance sync status table for
# every relay-* service on this host.
#
# Run on the relay host (`relay.bitcraftsync.app`) as any user that can
# read /etc/systemd/system/relay-*.service. Auto-discovers units, so it
# stays correct as regions are added/removed.
#
# Each instance's dashboard port is read from its `--dashboard-bind`
# flag; the JSON snapshot at `/metrics` gives the sync state. Region
# list and ports are never hardcoded.
#
# Usage:
#   ./tools/fleet-status.sh             # one-shot table
#   ./tools/fleet-status.sh -w          # repeat every 5s until Ctrl-C
#   DASH_ONLY=1 ./tools/fleet-status.sh # only units with a live dashboard
#
# Requires: curl, python3, systemctl. No write side effects.

set -eu

INTERVAL="${FLEET_INTERVAL:-5}"
UNIT_DIR="${FLEET_UNIT_DIR:-/etc/systemd/system}"
ONLY_DASH="${DASH_ONLY:-0}"

# Discover relay units (relay-global, relay-bc<N>, …) excluding the
# shared stdb unit, which has no mirror of its own. Sort global first,
# then numeric by region ID.
discover() {
    for f in "$UNIT_DIR"/relay-*.service; do
        [ -e "$f" ] || continue
        name=$(basename "$f" .service)
        case "$name" in
            relay-stdb) continue ;;  # shared infra, not a mirror
            relay-global) echo "0global $name" ;;
            relay-bc*)    echo "$(echo "$name" | sed 's/relay-bc//') $name" ;;
            *)            echo "999 $name" ;;  # unknown shape, show last
        esac
    done | sort -n | awk '{print $2}'
}

# dashboard port for a unit: parse `--dashboard-bind 127.0.0.1:PORT`
# from its unit file. Empty string if the unit has no dashboard.
dash_port() {
    grep -oE 'dashboard-bind 127\.0\.0\.1:[0-9]+' "$UNIT_DIR/$1.service" 2>/dev/null \
        | grep -oE '[0-9]+$' | head -1
}

# frontend port for a unit (for display). The bind host varies by
# deployment: `0.0.0.0` when the proxy faces the public directly,
# `127.0.0.1` when nginx terminates TLS in front of it. Match either.
frontend_port() {
    grep -oE 'frontend-bind (0\.0\.0\.0|127\.0\.0\.1):[0-9]+' "$UNIT_DIR/$1.service" 2>/dev/null \
        | grep -oE '[0-9]+$' | head -1
}

# region label from the unit name (relay-bc7 -> 7, relay-global -> global).
label() {
    case "$1" in
        relay-global) echo "global" ;;
        relay-bc*)    echo "$1" | sed 's/relay-bc//' ;;
        *)            echo "$1" | sed 's/relay-//' ;;
    esac
}

print_once() {
    # Header.
    printf '%-7s %-15s %-9s %10s %10s %9s %-9s %-6s %s\n' \
        REGION UNIT FRONTEND UPSTREAM U_BYTES_1M U_UNITS_1M STDB PUBLISH NOTES
    for unit in $(discover); do
        port=$(dash_port "$unit")
        lbl=$(label "$unit")
        fport=$(frontend_port "$unit")
        fport_disp=${fport:-"-"}
        if [ -z "$port" ]; then
            if [ "$ONLY_DASH" = "1" ]; then continue; fi
            printf '%-7s %-15s %-9s %-9s %10s %10s %-9s %-6s %s\n' \
                "$lbl" "$unit" "$fport_disp" "-" "-" "-" "-" "-" "no dashboard"
            continue
        fi
        json=$(curl -s --max-time 4 "http://127.0.0.1:${port}/metrics" 2>/dev/null || true)
        if [ -z "$json" ]; then
            printf '%-7s %-15s %-9s %-9s %10s %10s %-9s %-6s %s\n' \
                "$lbl" "$unit" "$fport_disp" "-" "-" "-" "-" "-" "dashboard unreachable"
            continue
        fi
        # Pull the fields we care about. python3 because it's already a
        # dependency on this host and JSON parsing in pure sh is painful.
        PYARGS=$(printf '%s' "$json")
        python3 - "$PYARGS" "$lbl" "$unit" "$fport_disp" <<'PY'
import json, sys, datetime
d = json.loads(sys.argv[1])
lbl, unit, fport = sys.argv[2], sys.argv[3], sys.argv[4]
u = d.get("upstream", {})
l = d.get("local_stdb", {})
p = d.get("publisher", {})
notes = []
if u.get("state") != "up":
    notes.append("upstream=%s" % u.get("state"))
if l.get("state") != "up":
    notes.append("stdb=%s" % l.get("state"))
reason = u.get("last_disconnect_reason")
if reason:
    notes.append("last_reason=%s" % reason)
def mb(b): return "%.1fM" % (b/1e6) if b is not None else "-"
pub = "repub" if p.get("republished_this_run") else "cached"
print("%-7s %-15s %-9s %-9s %10s %10s %-9s %-6s %s" % (
    lbl, unit, fport,
    u.get("state","?"),
    mb(u.get("bytes_1m")), (str(u.get("units_1m",0)) if u.get("units_1m") is not None else "-"),
    l.get("state","?"), pub, ", ".join(notes)))
PY
    done
}

if [ "${1:-}" = "-w" ] || [ "${1:-}" = "--watch" ]; then
    while :; do
        clear 2>/dev/null || true
        echo "relay fleet status — $(date '+%Y-%m-%d %H:%M:%S')  (refresh ${INTERVAL}s, Ctrl-C to quit)"
        echo
        print_once
        sleep "$INTERVAL"
    done
else
    print_once
fi
