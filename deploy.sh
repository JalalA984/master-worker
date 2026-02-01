#!/bin/bash

CLUSTER_NAME="mw-cluster"
IMAGE_NAME="master-worker:v1"

echo "Starting Deployment Process..."

# Build the Docker Image
echo "Building Docker image..."
docker build -t $IMAGE_NAME .

# Check if Kind cluster exists, if not create it
if ! kind get clusters | grep -q "^$CLUSTER_NAME$"; then
    echo "Creating Kind cluster..."
    kind create cluster --name $CLUSTER_NAME --config kind-config.yaml
else
    echo "Kind cluster already exists."
fi

# Load the image into Kind
echo "Loading image into Kind nodes..."
kind load docker-image $IMAGE_NAME --name $CLUSTER_NAME

# Deploy/Upgrade via Helm
echo "Deploying Helm chart..."
helm upgrade --install my-release ./charts/master-worker

# Restart deployments to ensure they use the newly loaded image
echo "Refreshing Pods..."
kubectl rollout restart deployment/master
kubectl rollout restart deployment/worker

echo "Deployment Complete! Run 'kubectl get pods' to check status."