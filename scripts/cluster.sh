#!/usr/bin/env bash
# Run a pyKA cluster on one machine.
#
#   ./scripts/cluster.sh start 3     # three brokers
#   ./scripts/cluster.sh status
#   ./scripts/cluster.sh stop
#
# Broker N listens on gRPC 909N and admin 808N, with its own data directory.
# That is the whole difference between them: same binary, same config, two
# different numbers — which is exactly what a Kubernetes StatefulSet hands
# each pod (HOSTNAME=pyka-N, one PersistentVolumeClaim each).
set -euo pipefail
cd "$(dirname "$0")/.."

DATA=${PYKA_DEMO_DIR:-/tmp/pyka-cluster}

start() {
    local count=${1:-3}
    stop >/dev/null 2>&1 || true
    mkdir -p "$DATA"
    for n in $(seq 0 $((count - 1))); do
        HOSTNAME="pyka-$n" \
        PYKA_BROKERS="$count" \
        PYKA_ADDRESS_TEMPLATE='localhost:909{ordinal}' \
        PYKA_DATA_DIR="$DATA/broker-$n" \
        PYKA_PORT="909$n" \
        PYKA_ADMIN_PORT="808$n" \
        PYKA_SEGMENT_BYTES="${PYKA_SEGMENT_BYTES:-30000}" \
        uv run pyka-broker > "$DATA/broker-$n.log" 2>&1 &
        echo $! > "$DATA/broker-$n.pid"
    done

    echo -n "starting $count brokers"
    for _ in $(seq 1 30); do
        if curl -sf "localhost:808$((count - 1))/readyz" >/dev/null 2>&1; then
            echo " — ready"
            status
            return 0
        fi
        echo -n "."
        sleep 0.5
    done
    echo " — TIMED OUT; see $DATA/broker-*.log"
    return 1
}

status() {
    for pidfile in "$DATA"/broker-*.pid; do
        [ -e "$pidfile" ] || { echo "no cluster running"; return; }
        local n=${pidfile##*broker-}; n=${n%.pid}
        if kill -0 "$(cat "$pidfile")" 2>/dev/null; then
            echo "  broker $n  gRPC localhost:909$n  admin localhost:808$n  $(curl -s "localhost:808$n/")"
        else
            echo "  broker $n  DOWN"
        fi
    done
}

stop() {
    for pidfile in "$DATA"/broker-*.pid; do
        [ -e "$pidfile" ] || continue
        kill "$(cat "$pidfile")" 2>/dev/null || true
        rm -f "$pidfile"
    done
    sleep 1
    echo "stopped"
}

kill_one() {
    local n=$1
    kill "$(cat "$DATA/broker-$n.pid")" 2>/dev/null || true
    rm -f "$DATA/broker-$n.pid"
    echo "killed broker $n — its partitions are now unreachable (no replication)"
}

case "${1:-}" in
    start)  start "${2:-3}" ;;
    stop)   stop ;;
    status) status ;;
    kill)   kill_one "${2:?which broker?}" ;;
    clean)  stop; rm -rf "$DATA"; echo "wiped $DATA" ;;
    *)      sed -n '2,12p' "$0"; exit 1 ;;
esac
