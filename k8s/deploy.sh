#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="dremio"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== Creating namespace and RBAC ==="
kubectl apply -f "$SCRIPT_DIR/00-namespace.yaml"
kubectl apply -f "$SCRIPT_DIR/01-rbac.yaml"

echo "=== Creating persistent volumes ==="
kubectl apply -f "$SCRIPT_DIR/00b-coordinator-pvc.yaml"
kubectl apply -f "$SCRIPT_DIR/00c-dist-pvc.yaml"

echo "=== Creating GHCR secret ==="
if [ -n "${GHCR_TOKEN:-}" ]; then
  kubectl create secret docker-registry ghcr-secret \
    --docker-server=ghcr.io \
    --docker-username="${GHCR_USERNAME:-}" \
    --docker-password="$GHCR_TOKEN" \
    --namespace="$NAMESPACE" \
    --dry-run=client -o yaml | kubectl apply -f -
else
  echo "WARNING: GHCR_TOKEN not set — skipping image pull secret creation"
fi

echo "=== Creating host directories on target node ==="
TARGET_NODE="vmi1594378.contaboserver.net"
for dir in /mnt/dremio/coordinator /mnt/dremio/dist; do
  echo "  Ensuring $dir exists on $TARGET_NODE"
  ssh "root@$TARGET_NODE" "mkdir -p $dir" 2>/dev/null || echo "  (skipped — assume directory exists or SSH not available)"
done

echo "=== Applying services ==="
kubectl apply -f "$SCRIPT_DIR/02-service.yaml"
kubectl apply -f "$SCRIPT_DIR/08-service-executor.yaml"

echo "=== Applying configmaps ==="
kubectl apply -f "$SCRIPT_DIR/03-configmap.yaml"
kubectl apply -f "$SCRIPT_DIR/10-configmap-executor-small.yaml"
kubectl apply -f "$SCRIPT_DIR/11-configmap-executor-large.yaml"

echo "=== Applying coordinator deployment ==="
kubectl apply -f "$SCRIPT_DIR/04-coordinator.yaml"

echo "=== Applying ingress ==="
kubectl apply -f "$SCRIPT_DIR/05-service-ingress.yaml"
kubectl apply -f "$SCRIPT_DIR/05-ingress-tcp-flight.yaml"
kubectl apply -f "$SCRIPT_DIR/06-ingress.yaml"

echo "=== Applying KEDA ScaledObjects ==="
kubectl apply -f "$SCRIPT_DIR/06-keda-small.yaml"
kubectl apply -f "$SCRIPT_DIR/07-keda-large.yaml"

echo "=== Applying executor StatefulSets ==="
kubectl apply -f "$SCRIPT_DIR/12-executor-small.yaml"
kubectl apply -f "$SCRIPT_DIR/13-executor-large.yaml"

echo "=== Waiting for coordinator to be ready ==="
kubectl rollout status deployment/dremio-coordinator -n "$NAMESPACE" --timeout=300s

echo "=== Done ==="
kubectl get pods -n "$NAMESPACE"
