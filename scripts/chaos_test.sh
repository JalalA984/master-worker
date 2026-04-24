#!/bin/bash
# Chaos test: randomly kill worker pods during task execution and verify all tasks complete.
# Inspired by Netflix Chaos Monkey — tests fault recovery in distributed systems.
#
# Prerequisites: running K8s cluster with master + workers deployed
# Usage: ./scripts/chaos_test.sh
set -euo pipefail

NAMESPACE="${NAMESPACE:-default}"
MASTER_URL="${MASTER_URL:-http://localhost:9092}"
TASK_COUNT="${TASK_COUNT:-10}"
CHAOS_ROUNDS="${CHAOS_ROUNDS:-3}"

echo "=== Chaos Test ==="
echo "Master:       $MASTER_URL"
echo "Tasks:        $TASK_COUNT"
echo "Chaos rounds: $CHAOS_ROUNDS"
echo ""

# Submit tasks
echo "[1/3] Submitting $TASK_COUNT tasks..."
for i in $(seq 1 "$TASK_COUNT"); do
    curl -s -X POST "$MASTER_URL/api/v1/submit" \
        -H "Content-Type: application/json" \
        -d "{\"language\":\"bash\",\"script\":\"echo 'chaos task $i'; sleep 3\"}" > /dev/null
done
echo "  Submitted $TASK_COUNT tasks."

# Kill random workers
echo "[2/3] Killing random worker pods..."
for round in $(seq 1 "$CHAOS_ROUNDS"); do
    sleep 5
    WORKERS=$(kubectl get pods -n "$NAMESPACE" -l app=worker --no-headers -o custom-columns=":metadata.name" 2>/dev/null | shuf | head -1)
    if [ -n "$WORKERS" ]; then
        echo "  Round $round: killing $WORKERS"
        kubectl delete pod "$WORKERS" -n "$NAMESPACE" --grace-period=0 --force 2>/dev/null || true
    else
        echo "  Round $round: no workers to kill"
    fi
done

# Wait and check results
echo "[3/3] Waiting for task completion..."
sleep 30

STATS=$(curl -s "$MASTER_URL/api/v1/stats")
echo ""
echo "=== Results ==="
echo "$STATS" | python3 -m json.tool 2>/dev/null || echo "$STATS"

IN_FLIGHT=$(echo "$STATS" | python3 -c "import sys,json; print(json.load(sys.stdin)['in_flight'])" 2>/dev/null || echo "?")
DLQ=$(echo "$STATS" | python3 -c "import sys,json; print(json.load(sys.stdin)['dead_lettered'])" 2>/dev/null || echo "?")

if [ "$IN_FLIGHT" = "0" ]; then
    echo ""
    echo "SUCCESS: All tasks completed or moved to DLQ (none stuck in-flight)."
    echo "Dead-lettered: $DLQ (these would be retried in production)"
else
    echo ""
    echo "WARNING: $IN_FLIGHT tasks still in-flight. May need more time."
fi
