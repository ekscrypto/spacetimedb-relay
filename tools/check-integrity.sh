#!/bin/sh
# check-integrity.sh — end-to-end integrity check for the relay fleet,
# run from anywhere (laptop, CI, or the host itself).
#
# For every live region it invokes the relay-test-harness binary in
# `--check-integrity` mode against the PUBLIC endpoint
# (wss://$HOST:<port>), which exercises the four primary client paths
# with the real SpacetimeDB SDK codec (no hand-rolled framing):
#
#   1. v2.bsatn subscribe   2. v1.bsatn subscribe
#   3. v1.json  subscribe   4. schema-over-HTTP GET .../schema
#
# A region passes only if all four succeed (exit 0 from the harness).
# This is the canonical post-deploy verification: run it after every
# binary rollout to catch a regression like the 2026-07-17 probe bug
# before clients notice.
#
# Fleet discovery is by port-probe, not a hardcoded region list: it
# GETs https://$HOST:<port>/v1/database/<db>/schema across the public
# band 3000–3025 (see PORTS.md) and treats nginx's 502 (no relay behind
# that port) as "not deployed here," not a failure. New regions appear
# automatically; removed ones stop being checked.
#
# Usage:
#   ./tools/check-integrity.sh                 # all live regions
#   ./tools/check-integrity.sh 3013 3000       # specific ports
#   HOST=relay.bitcraftsync.app ./tools/check-integrity.sh
#   HARNESS=./target/release/relay-test-harness ./tools/check-integrity.sh
#   TIMEOUT=20 TABLE=admin_broadcast ./tools/check-integrity.sh
#
# The harness binary must be built first:
#   cargo build -p relay-test-harness --release
#
# Exit codes:
#   0  every live region passed all four checks
#   1  at least one check failed on at least one region
#   2  no live regions discovered, or the harness binary is missing
#
# Requires: sh, curl (for discovery), the relay-test-harness binary.
# No write side effects — opens read-only WS/HTTPS connections.

set -u

HOST="${HOST:-relay.bitcraftsync.app}"
TIMEOUT="${TIMEOUT:-30}"
TABLE="${TABLE:-admin_broadcast}"
# Resolve the harness binary: explicit override, then repo-relative
# release build, then repo-relative debug build.
HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/.." && pwd)
HARNESS="${HARNESS:-$REPO/target/release/relay-test-harness}"
if [ ! -x "$HARNESS" ]; then
    HARNESS="$REPO/target/debug/relay-test-harness"
fi
if [ ! -x "$HARNESS" ]; then
    echo "FAIL: relay-test-harness binary not found." >&2
    echo "  Build it first: cargo build -p relay-test-harness --release" >&2
    echo "  or set HARNESS=/path/to/relay-test-harness" >&2
    exit 2
fi

# The mirror database name for a public port, per PORTS.md:
# 3000 -> relay-mirror-global, 30NN -> relay-mirror-bcNN.
database_for() {
    if [ "$1" = "3000" ]; then
        echo "relay-mirror-global"
    else
        echo "relay-mirror-bc$(( $1 - 3000 ))"
    fi
}

# Is a relay live behind this public port? GET the schema endpoint over
# HTTPS; 200 means yes, 502 (nginx: no upstream) or unreachable means no.
# Prints "yes" / "no".
#
# Note on curl's exit code: the schema endpoint sends `Connection: close`
# and may close before the body finishes, so curl returns 18
# (CURLE_PARTIAL_FILE) even on a successful 200. We therefore trust the
# http_code curl printed, not its exit status — capture them separately.
port_is_live() {
    port=$1
    db=$(database_for "$port")
    code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 6 \
        "https://${HOST}:${port}/v1/database/${db}/schema" 2>/dev/null)
    # curl printed nothing (dns/connect failure) → treat as not live.
    [ "$code" = "200" ] && echo yes || echo no
}

# Discover live ports. Either the caller's explicit list, or scan the
# public band 3000–3025.
discover() {
    if [ "$#" -gt 0 ]; then
        for p in "$@"; do echo "$p"; done
        return
    fi
    p=3000
    while [ "$p" -le 3025 ]; do
        echo "$p"
        p=$(( p + 1 ))
    done
}

GREEN=$([ -t 1 ] && printf '\033[32m' || printf '')
RED=$([ -t 1 ] && printf '\033[31m' || printf '')
YELLOW=$([ -t 1 ] && printf '\033[33m' || printf '')
BOLD=$([ -t 1 ] && printf '\033[1m' || printf '')
RESET=$([ -t 1 ] && printf '\033[0m' || printf '')

printf 'relay integrity check — %s (timeout %ss, table %s)\n\n' \
    "$HOST" "$TIMEOUT" "$TABLE"

live_ports=""
checked=0
passed=0
failed=0

for port in $(discover "$@"); do
    [ "$port" = "3000" ] && label="global" || label=$(( port - 3000 ))
    db=$(database_for "$port")

    # When the caller gave explicit ports, always check them (they want
    # to know if a specific region is broken, including "not live").
    # When scanning, skip non-live ports silently.
    if [ "$#" -eq 0 ]; then
        if [ "$(port_is_live "$port")" = "no" ]; then
            continue
        fi
    fi

    checked=$(( checked + 1 ))
    printf '%s%-7s%s ' "$BOLD" "$label" "$RESET"

    # Run the harness; capture output. The harness prints [OK]/[FAIL]
    # lines to stdout/stderr and exits 0 on all-pass, 1 on any fail.
    out=$("$HARNESS" --check-integrity \
            --via-frontend "wss://${HOST}:${port}" \
            --database "$db" \
            --table "$TABLE" \
            --timeout-secs "$TIMEOUT" 2>&1)
    rc=$?

    if [ "$rc" -eq 0 ]; then
        passed=$(( passed + 1 ))
        printf '%sPASS%s  v2.bsatn + v1.bsatn + v1.json + schema\n' \
            "$GREEN" "$RESET"
    else
        failed=$(( failed + 1 ))
        printf '%sFAIL%s\n' "$RED" "$RESET"
        # Show the four per-check lines (indented) so the specific
        # broken path is visible without re-running the harness.
        printf '%s\n' "$out" | grep -E '\[(OK|FAIL)\]' | sed 's/^/        /'
    fi
done

echo
if [ "$checked" -eq 0 ]; then
    printf '%sFAIL: no live regions discovered.%s\n' "$RED" "$RESET" >&2
    echo "  Is the host up? Is the public band reachable from here?" >&2
    exit 2
fi

if [ "$failed" -gt 0 ]; then
    printf '%s%d/%d region(s) failed.%s\n' "$RED" "$failed" "$checked" "$RESET" >&2
    exit 1
fi

printf '%sOK: all %d region(s) passed v2+v1+json+schema.%s\n' \
    "$GREEN" "$passed" "$RESET"
exit 0
