#!/bin/bash
set -e

NAMESPACE="dremio"
GHCR_REGISTRY="ghcr.io/rusha-corp/dremio-oss"

echo "=== Creating namespace and RBAC ==="
kubectl apply -f 00-namespace.yaml

echo "=== Creating GHCR secret ==="
kubectl create secret docker-registry ghcr-secret \
  --docker-server=ghcr.io \
  --docker-username="$GHCR_USERNAME" \
  --docker-password="$GHCR_TOKEN" \
  --namespace=$NAMESPACE \
  --dry-run=client -o yaml | kubectl apply -f -

echo "=== Applying manifests ==="
kubectl apply -f 01-rbac.yaml
kubectl apply -f 02-service.yaml
kubectl apply -f 03-configmap.yaml
kubectl apply -f 04-coordinator.yaml

echo "=== Waiting for coordinator to be ready ==="
kubectl wait --for=condition=Ready pod/dremio-coordinator-0 -n $NAMESPACE --timeout=300s

echo "=== Done ==="
kubectl get pods -n $NAMESPACE
