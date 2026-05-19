# Dremio K8s Elastic Scaling Deployment Skill

## Overview
This skill provides a consistent way to deploy Dremio with elastic executor scaling on Kubernetes.

## Prerequisites
1. Docker images built and pushed to ECR:
   - `217493348668.dkr.ecr.eu-west-1.amazonaws.com/dremio-coordinator:26.0.5-elastic` (with K8sPlatform fix)
   - `217493348668.dkr.ecr.eu-west-1.amazonaws.com/dremio-executor:26.0.5-elastic-v2` (with hostAlias fix)

2. AWS credentials configured for ECR push

3. Kubernetes cluster (minikube or actual cluster)

## Manifests Location
All manifests are in `/home/dev/dremio-oss/k8s/`:
- `00-namespace.yaml` - Namespace definition
- `01-rbac.yaml` - Service account and RBAC
- `02-service.yaml` - Headless service for coordinator
- `03-configmap.yaml` - Dremio configuration with elastic settings
- `04-coordinator.yaml` - Coordinator pod definition
- `deploy.sh` - Deployment script

## Deployment Steps

### 1. Build and push Docker images (if needed)

```bash
# Build coordinator image with updated JAR
cd /home/dev/dremio-oss
./mvnw package -DskipTests -Derrorprone.skip=true -pl services/resourcescheduler -am

# Build Docker image (from dremio-elastic base, no cache)
rm -rf /tmp/direct-build && mkdir -p /tmp/direct-build
cp services/resourcescheduler/target/dremio-services-resourcescheduler-*.jar /tmp/direct-build/

cat > /tmp/direct-build/Dockerfile << 'EOF'
FROM 217493348668.dkr.ecr.eu-west-1.amazonaws.com/dremio-coordinator:26.0.5-elastic
USER root
RUN rm -f /opt/dremio/jars/dremio-services-resourcescheduler-*.jar
COPY dremio-services-resourcescheduler-*.jar /opt/dremio/jars/
USER dremio
EOF

docker build --no-cache -t 217493348668.dkr.ecr.eu-west-1.amazonaws.com/dremio-coordinator:26.0.5-elastic /tmp/direct-build
docker push 217493348668.dkr.ecr.eu-west-1.amazonaws.com/dremio-coordinator:26.0.5-elastic

# Similarly build executor image
```

### 2. Deploy to Kubernetes

```bash
cd /home/dev/dremio-oss/k8s
./deploy.sh
```

### 3. Verify deployment

```bash
kubectl get pods -n dremio
kubectl logs dremio-coordinator-0 -n dremio
```

### 4. Test elastic scaling

```bash
# Port forward to coordinator
kubectl port-forward -n dremio pod/dremio-coordinator-0 9047:9047 &

# Login and run query
curl -s -c /tmp/ck.txt -L -X POST "http://localhost:9047/login" -d "username=dremio&password=dremio123"
curl -s -b /tmp/ck.txt -X POST "http://localhost:9047/api/v3/sql" -H "Content-Type: application/json" -d '{"sql": "SELECT 1 as num"}'

# Check if executor spawned
kubectl get pods -n dremio -l role=executor
```

## Key Fixes in K8sPlatform.java

1. **coordinator.address in executor config** - Adds `coordinator.address: "dremio-coordinator.dremio.svc.cluster.local"` to executor ConfigMap

2. **hostAlias for pod name resolution** - Maps `dremio-coordinator-0` to service DNS:
```java
.addNewHostAlias()
  .withHostnames("dremio-coordinator-0")
  .withIp("dremio-coordinator.dremio.svc.cluster.local")
.endHostAlias()
```

## Troubleshooting

### Executor not spawning
- Check coordinator logs: `kubectl logs dremio-coordinator-0 -n dremio | grep -i elastic`
- Verify K8sPlatform is being loaded
- Check if ConfigMap is being created with correct executor config

### Executor can't connect to coordinator
- Check executor logs: `kubectl logs <executor-pod> -n dremio`
- Verify hostAlias is applied to executor pod
- Check DNS resolution: `kubectl exec <executor-pod> -n dremio -- getent hosts dremio-coordinator.dremio.svc.cluster.local`

### ImagePullBackOff
- Verify ECR secret exists: `kubectl get secret ecr-secret -n dremio`
- Recreate if needed:
```bash
kubectl create secret docker-registry ecr-secret \
  --docker-server=217493348668.dkr.ecr.eu-west-1.amazonaws.com \
  --docker-username=AWS \
  --docker-password=$(aws ecr get-login-password --region eu-west-1) \
  --namespace=dremio
```

## Common Issues

1. **Docker build not picking up new JAR**: Use `--no-cache` flag
2. **ConfigMap properties invalid**: Use only properties supported by the base image version
3. **Executor uses pod name instead of service DNS**: hostAlias fix required due to headless service
