# Distributed Task Orchestrator (Go + gRPC + K8s)

Just wanted to learn distributed system with Master-Worker arch: just executes bash scripts in ConfigMap

    Master Node: Hosts a gRPC server (port 50051) for worker management and an HTTP API (port 9092) for user interaction. It manages a Tracking Map and Task Queue for fault tolerance.

    Worker Node: Connects to the Master via gRPC server-side streaming. It pulls tasks, executes bash scripts using os/exec, and reports real-time status/logs back to the Master.

    The Handshake: The Master's AssignTask function "parks" at a Go channel until an HTTP POST request enqueues tasks. This triggers the gRPC loop to stream payloads to available workers.

# Run Local

## Terminal 1: Start Master
go run main.go master

## Terminal 2: Start Worker
go run main.go worker

## Terminal 3: Trigger execution (Use absolute path for local)
curl -X POST "http://localhost:9092/tasks?dir=$(pwd)/test_scripts"

# Run Docker

## Build the Multi-stage Docker Image
docker build -t master-worker:v1 .

## Setup Networking & Storage
docker network create my-net

## Run Master (Mounting local scripts to /scripts)
docker run -d --name master-node --network my-net \
  -v $(pwd)/test_scripts:/scripts \
  -p 9092:9092 -p 50051:50051 \
  master-worker:v1 ./main master

## Run Worker (Pointing to Master's container name)
docker run -d --name worker-node --network my-net \
  -e MASTER_ADDR=master-node:50051 \
  -v $(pwd)/test_scripts:/scripts \
  master-worker:v1 ./main worker

## Trigger (Path is now relative to the container's mount)
curl -X POST "http://localhost:9092/tasks?dir=/scripts"

# Docker Cleanup

## Stop and remove containers
docker stop worker-node master-node
docker rm worker-node master-node

## Remove the custom network
docker network rm my-net

# Kubernetes & Helm

## Spin up Multi-Node Cluster
kind create cluster --name mw-cluster --config kind-config.yaml

## Sideload Image into Kind
kind load docker-image master-worker:v1 --name mw-cluster

## Deploy via Helm
helm install my-release ./charts/master-worker

## Verify Nodes and Pods
kubectl get nodes
kubectl get pods -o wide

## Create Tunnel for API access
kubectl port-forward svc/master-service 9092:9092

## Trigger (Using the ConfigMap mount path)
curl -X POST "http://localhost:9092/tasks?dir=/etc/scripts"

## Observe the Orchestration
kubectl logs -f deployment/master

# Kubernetes & Helm Cleanup

## Uninstall the Helm release
helm uninstall my-release

## Delete the Kind Cluster
kind delete cluster --name mw-cluster

## Check up any lingering port-forwards
sudo lsof -i :9092

# TODOs
    High Availability Master: Implement concensus to have a standby Master if the primary fails or shadow backup thing GPS/HDFS

    Use a Write-Ahead Log or KV store to store task results so they survive a Master reboot.

    Service Discovery