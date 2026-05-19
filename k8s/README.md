# Dremio K8s Elastic Scaling Deployment

## Overview
Deployment manifests for Dremio with elastic executor scaling on Kubernetes. Images are hosted on GitHub Container Registry (GHCR) under the Rusha-Corp organization.

## Prerequisites
1. Docker images built and pushed to GHCR:
   - `ghcr.io/rusha-corp/dremio-oss:coordinator-26.0.5-elastic`
   - `ghcr.io/rusha-corp/dremio-oss:executor-26.0.5-elastic-v3`

2. GitHub credentials configured for GHCR push:
   ```bash
   echo $GITHUB_TOKEN | docker login ghcr.io -u <username> --password-stdin
   ```

3. Kubernetes cluster (minikube or actual cluster)

4. Environment variables for deploy.sh:
   - `GHCR_USERNAME` - GitHub username for GHCR authentication
   - `GHCR_TOKEN` - GitHub personal access token with `read:packages` scope

## Manifests
| File | Description |
|------|-------------|
| `00-namespace.yaml` | Namespace definition |
| `01-rbac.yaml` | Service account and ClusterRoleBinding |
| `02-service.yaml` | Headless service for coordinator pod DNS |
| `03-configmap.yaml` | Dremio configuration with elastic scaling settings |
| `04-coordinator.yaml` | Coordinator pod definition |
| `deploy.sh` | Deployment script |

## Deployment

```bash
cd k8s
export GHCR_USERNAME=<your-github-username>
export GHCR_TOKEN=<your-github-token>
./deploy.sh
```

## Verification

```bash
kubectl get pods -n dremio
kubectl logs dremio-coordinator-0 -n dremio
```

## Testing Elastic Scaling

```bash
# Port forward to coordinator
kubectl port-forward -n dremio pod/dremio-coordinator-0 9047:9047 &

# Login and run a query
curl -s -c /tmp/ck.txt -L -X POST "http://localhost:9047/login" \
  -d "username=dremio&password=dremio123"
curl -s -b /tmp/ck.txt -X POST "http://localhost:9047/api/v3/sql" \
  -H "Content-Type: application/json" -d '{"sql": "SELECT 1 as num"}'

# Check if executor pods were spawned
kubectl get pods -n dremio -l role=executor
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Executor not spawning | Check coordinator logs: `kubectl logs dremio-coordinator-0 -n dremio \| grep -i elastic` |
| Executor can't connect to coordinator | Verify hostAlias and DNS: `kubectl exec <executor-pod> -n dremio -- getent hosts dremio-coordinator.dremio.svc.cluster.local` |
| ImagePullBackOff | Recreate GHCR secret: `kubectl create secret docker-registry ghcr-secret --docker-server=ghcr.io --docker-username=$GHCR_USERNAME --docker-password=$GHCR_TOKEN --namespace=dremio` |
| ConfigMap properties invalid | Use only properties supported by the base image version |
