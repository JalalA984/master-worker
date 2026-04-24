#!/bin/bash
# gen-certs.sh — Generate self-signed CA + server + client certificates for mTLS.
#
# Certificate chain:
#   CA (root of trust) → Server cert (master) → Client cert (workers)
#
# Both server and client certs are signed by the same CA, so they can
# mutually verify each other. This is the same pattern used by:
#   - Kubernetes component certs (API server, kubelet, etcd)
#   - Istio Citadel for workload identity
#
# Usage: ./scripts/gen-certs.sh
# Output: certs/ directory with ca.pem, server.pem, server-key.pem, client.pem, client-key.pem

set -euo pipefail

CERT_DIR="${1:-certs}"
mkdir -p "$CERT_DIR"

echo "=== Generating mTLS Certificates ==="

# 1. Generate CA (Certificate Authority) — the root of trust
echo "[1/3] Generating CA..."
openssl req -x509 -newkey rsa:4096 -sha256 -days 365 \
    -keyout "$CERT_DIR/ca-key.pem" -out "$CERT_DIR/ca.pem" \
    -nodes -subj "/CN=master-worker-ca/O=master-worker"

# 2. Generate server certificate (for the master's gRPC server)
echo "[2/3] Generating server certificate..."
openssl req -newkey rsa:4096 -sha256 \
    -keyout "$CERT_DIR/server-key.pem" -out "$CERT_DIR/server.csr" \
    -nodes -subj "/CN=master/O=master-worker"

# Sign with CA. SAN includes localhost + K8s service names.
openssl x509 -req -in "$CERT_DIR/server.csr" \
    -CA "$CERT_DIR/ca.pem" -CAkey "$CERT_DIR/ca-key.pem" -CAcreateserial \
    -out "$CERT_DIR/server.pem" -days 365 -sha256 \
    -extfile <(echo "subjectAltName=DNS:localhost,DNS:master,DNS:master-service,DNS:master-service.default.svc.cluster.local,IP:127.0.0.1")

# 3. Generate client certificate (for workers)
echo "[3/3] Generating client certificate..."
openssl req -newkey rsa:4096 -sha256 \
    -keyout "$CERT_DIR/client-key.pem" -out "$CERT_DIR/client.csr" \
    -nodes -subj "/CN=worker/O=master-worker"

openssl x509 -req -in "$CERT_DIR/client.csr" \
    -CA "$CERT_DIR/ca.pem" -CAkey "$CERT_DIR/ca-key.pem" -CAcreateserial \
    -out "$CERT_DIR/client.pem" -days 365 -sha256

# Clean up CSR files
rm -f "$CERT_DIR"/*.csr "$CERT_DIR"/*.srl

echo ""
echo "=== Certificates Generated ==="
echo "  CA:     $CERT_DIR/ca.pem"
echo "  Server: $CERT_DIR/server.pem + $CERT_DIR/server-key.pem"
echo "  Client: $CERT_DIR/client.pem + $CERT_DIR/client-key.pem"
