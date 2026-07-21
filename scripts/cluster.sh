#!/usr/bin/env bash
# Run a pyKA cluster on one machine.
#
#   ./scripts/cluster.sh start 3     # three brokers
#   ./scripts/cluster.sh kill 2      # simulate a broker dying
#   ./scripts/cluster.sh restart 2   # bring it back
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
    # By port, not just the pid file: a stale or missing pid file would make
    # this silently do nothing, and "I killed it" turning out to be false is a
    # very confusing way to run an availability experiment.
    [ -e "$DATA/broker-$n.pid" ] && kill "$(cat "$DATA/broker-$n.pid")" 2>/dev/null || true
    lsof -ti ":909$n" 2>/dev/null | xargs kill 2>/dev/null || true
    rm -f "$DATA/broker-$n.pid"

    for _ in $(seq 1 20); do
        curl -sf --max-time 1 "localhost:808$n/healthz" >/dev/null 2>&1 || {
            echo "killed broker $n — its partitions are now unreachable (no replication)"
            return 0
        }
        sleep 0.25
    done
    echo "broker $n is STILL UP — kill failed"
    return 1
}

restart_one() {
    local n=$1
    # Count the data directories with a glob, not `ls -d`: BSD ls rejects
    # flags after operands, so `ls dir -d` fails on macOS — and under set -e
    # that aborted this function silently.
    local count=0
    for directory in "$DATA"/broker-*/; do
        [ -d "$directory" ] && count=$((count + 1))
    done
    HOSTNAME="pyka-$n" \
    PYKA_BROKERS="$count" \
    PYKA_ADDRESS_TEMPLATE='localhost:909{ordinal}' \
    PYKA_DATA_DIR="$DATA/broker-$n" \
    PYKA_PORT="909$n" \
    PYKA_ADMIN_PORT="808$n" \
    PYKA_SEGMENT_BYTES="${PYKA_SEGMENT_BYTES:-30000}" \
    uv run pyka-broker > "$DATA/broker-$n.log" 2>&1 &
    echo $! > "$DATA/broker-$n.pid"
    echo "restarting broker $n"
}

case "${1:-}" in
    start)  start "${2:-3}" ;;
    stop)   stop ;;
    status) status ;;
    kill)    kill_one "${2:?which broker?}" ;;
    restart) restart_one "${2:?which broker?}" ;;
    clean)  stop; rm -rf "$DATA"; echo "wiped $DATA" ;;
    *)      sed -n '2,12p' "$0"; exit 1 ;;
esac
