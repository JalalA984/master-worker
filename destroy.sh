#!/bin/bash
set -euo pipefail

CLUSTER_NAME="mw-cluster"

echo "=== Destroying Master-Worker Cluster ==="

# Check if cluster exists
if kind get clusters 2>/dev/null | grep -q "^$CLUSTER_NAME$"; then
    echo "Deleting Kind cluster '$CLUSTER_NAME'..."
    kind delete cluster --name $CLUSTER_NAME
    echo "Cluster deleted."
else
    echo "No cluster named '$CLUSTER_NAME' found."
fi

echo "=== Teardown Complete ==="
