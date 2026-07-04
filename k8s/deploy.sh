#!/usr/bin/env bash
set -e

NAMESPACE="dremio"
REPO="${GHCR_REPO:-ghcr.io/rusha-corp/dremio-oss}"
VERSION="${DREMIO_VERSION:-26.0.5-elastic}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

export GHCR_REPO="$REPO"
export DREMIO_VERSION="$VERSION"

echo "=== Creating namespace and RBAC ==="
kubectl apply -f "$SCRIPT_DIR/00-namespace.yaml"
kubectl apply -f "$SCRIPT_DIR/01-rbac.yaml"

echo "=== Creating shared dist PVC ==="
kubectl apply -f "$SCRIPT_DIR/00c-dist-pvc.yaml"

echo "=== Creating GHCR secret ==="
if [ -n "${GHCR_TOKEN:-}" ]; then
  kubectl create secret docker-registry ghcr-secret \
    --docker-server=ghcr.io \
    --docker-username="${GHCR_USERNAME:-}" \
    --docker-password="$GHCR_TOKEN" \
    --namespace=$NAMESPACE \
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

echo "=== Applying manifests (with env substitution) ==="
envsubst < "$SCRIPT_DIR/02-service.yaml" | kubectl apply -f -
envsubst < "$SCRIPT_DIR/02b-executor-service.yaml" | kubectl apply -f -
envsubst < "$SCRIPT_DIR/03-configmap.yaml" | kubectl apply -f -
envsubst < "$SCRIPT_DIR/04-coordinator.yaml" | kubectl apply -f -
envsubst < "$SCRIPT_DIR/05-service-ingress.yaml" | kubectl apply -f -
envsubst < "$SCRIPT_DIR/06-ingress.yaml" | kubectl apply -f -
# envsubst < "$SCRIPT_DIR/07-hpa.yaml" | kubectl apply -f -  # Replaced by KEDA ScaledObject
envsubst < "$SCRIPT_DIR/10-keda-scaledobject.yaml" | kubectl apply -f -
envsubst < "$SCRIPT_DIR/11a-executor-small-stub.yaml" | kubectl apply -f -
envsubst < "$SCRIPT_DIR/11b-executor-large-stub.yaml" | kubectl apply -f -
envsubst < "$SCRIPT_DIR/11c-executor-configmaps.yaml" | kubectl apply -f -
envsubst < "$SCRIPT_DIR/11-coredns-custom.yaml" | kubectl apply -f -
envsubst < "$SCRIPT_DIR/12d-dremio-ops-credentials-secret.yaml" | kubectl apply -f -

echo "=== Waiting for coordinator to be ready ==="
kubectl rollout status deployment/dremio-coordinator -n $NAMESPACE --timeout=300s

echo "=== Done ==="
kubectl get pods -n $NAMESPACE
kubectl get hpa -n $NAMESPACE
