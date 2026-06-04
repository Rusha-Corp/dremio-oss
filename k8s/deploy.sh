#!/bin/bash
# ==============================================================================
# Dremio elastic executor deployment script.
# Review and customize storage classes, node selectors, and credentials
# for your platform before deploying. See k8s/README.md for details.
# ==============================================================================
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
kubectl apply -f 05-ingress-tcp-flight.yaml

echo "=== Applying executor manifests ==="
kubectl apply -f 08-service-executor.yaml
kubectl apply -f 10-configmap-executor-small.yaml
kubectl apply -f 11-configmap-executor-large.yaml
kubectl apply -f 12-executor-small.yaml
kubectl apply -f 13-executor-large.yaml

echo "=== Applying KEDA autoscaling ==="
kubectl apply -f 06-keda-small.yaml
kubectl apply -f 07-keda-large.yaml

echo "=== Waiting for coordinator to be ready ==="
kubectl wait --for=condition=Ready pod/dremio-coordinator-0 -n $NAMESPACE --timeout=300s

echo "=== Done ==="
kubectl get pods -n $NAMESPACE
