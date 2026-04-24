#!/bin/bash
set -euo pipefail

CLUSTER_NAME="mw-cluster"
IMAGE_NAME="master-worker:v1"

echo "=== Master-Worker Deployment ==="

# Build the Docker Image
echo "[1/5] Building Docker image..."
docker build -t $IMAGE_NAME .

# Check if Kind cluster exists, if not create it
echo "[2/5] Checking Kind cluster..."
if ! kind get clusters 2>/dev/null | grep -q "^$CLUSTER_NAME$"; then
    echo "  Creating Kind cluster..."
    kind create cluster --name $CLUSTER_NAME --config kind-config.yaml
else
    echo "  Kind cluster already exists."
fi

# Load the image into Kind
echo "[3/5] Loading image into Kind nodes..."
kind load docker-image $IMAGE_NAME --name $CLUSTER_NAME

# Deploy/Upgrade via Helm
echo "[4/5] Deploying Helm chart..."
helm upgrade --install my-release ./charts/master-worker

# Wait for rollout
echo "[5/5] Waiting for pods to be ready..."
kubectl rollout status deployment/master --timeout=120s || true
kubectl rollout status deployment/worker --timeout=120s || true

echo ""
echo "=== Deployment Complete ==="
echo "Run 'kubectl get pods' to check status."
echo ""
echo "Access services:"
echo "  Master HTTP:  kubectl port-forward svc/master-service 9092:9092"
echo "  Prometheus:   kubectl port-forward svc/prometheus-service 9090:9090"
echo "  Grafana:      kubectl port-forward svc/grafana-service 3000:3000"
echo ""
echo "Submit tasks:"
echo "  curl -X POST 'http://localhost:9092/tasks?dir=/etc/scripts'"
echo ""
echo "Submit inline task:"
echo '  curl -X POST http://localhost:9092/api/v1/submit \'
echo '    -H "Content-Type: application/json" \'
echo '    -d '"'"'{"language":"python","script":"print(\"hello from k8s\")"}'"'"
