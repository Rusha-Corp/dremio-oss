# Dremio K8s Elastic Scaling — k3s Playground

## Overview

This is our k3s playground for testing Dremio elastic executor scaling with KEDA autoscaling. The manifests are configured for our specific environment (k3s + Longhorn on a single Contabo node), but the **pitfalls and lessons learned documented here apply to any Dremio elastic scaling deployment on Kubernetes**.

The executor tiers (small/large) scale from zero based on query demand and scale down when idle. A metrics exporter bridges Dremio's `ElasticResourceAllocator` with KEDA to drive scaling decisions.

## What's Running

| Component | Version / Config |
|-----------|-----------------|
| Kubernetes | k3s on a single node (`vmi1594378.contaboserver.net`) |
| Storage | Longhorn (all PVCs use `storageClassName: longhorn`) |
| Dremio OSS | `ghcr.io/rusha-corp/dremio-oss:2026.05.7` |
| KEDA Metrics Exporter | `ghcr.io/rusha-corp/dremio-keda-exporter:2026.05.6` |
| KEDA | v2.x operator installed on cluster |
| Coordinator | 1 pod, 4Gi heap |
| Small executors | KEDA-managed StatefulSet, 4Gi heap, 15Gi/30Gi ephemeral-storage, 30Gi data PVC |
| Large executors | KEDA-managed StatefulSet, 14Gi heap, 30Gi/60Gi ephemeral-storage, 100Gi data PVC |

## Images

| Image | Registry URI | Source |
|---|---|---|
| Dremio OSS (coordinator + executor) | `ghcr.io/rusha-corp/dremio-oss:2026.05.7` | Single binary; role determined by ConfigMap |
| KEDA Metrics Exporter | `ghcr.io/rusha-corp/dremio-keda-exporter:2026.05.6` | [github.com/Rusha-Corp/dremio-keda-exporter](https://github.com/Rusha-Corp/dremio-keda-exporter) |

## Manifests

| File | Description |
|------|-------------|
| `00-namespace.yaml` | Namespace definition |
| `00b-coordinator-pvc.yaml` | Coordinator data PVC (Longhorn) |
| `00c-dist-pvc.yaml` | Shared distribution PVC (Longhorn, ReadWriteMany) |
| `01-rbac.yaml` | Service accounts, Roles, ClusterRoleBindings |
| `02-service.yaml` | Headless service for coordinator pod DNS |
| `02b-executor-service.yaml` | Headless service for executor pod DNS |
| `03-configmap.yaml` | Dremio coordinator configuration |
| `04-coordinator.yaml` | Coordinator deployment |
| `05-ingress-tcp-flight.yaml` | TCP ingress for Flight SQL |
| `05-service-ingress.yaml` | ClusterIP service for coordinator UI |
| `06-ingress.yaml` | HTTP ingress for coordinator UI |
| `06-keda-small.yaml` | KEDA ScaledObject for small executor tier |
| `07-keda-large.yaml` | KEDA ScaledObject for large executor tier |
| `08-service-executor.yaml` | Headless service for executor StatefulSet DNS |
| `09-metrics-exporter-deployment.yaml` | Metrics exporter deployment + service |
| `10-configmap-executor-small.yaml` | Small executor Dremio config |
| `10-configmap-executor-large.yaml` | Large executor Dremio config |
| `12-executor-small.yaml` | Small executor StatefulSet (30Gi data PVC) |
| `13-executor-large.yaml` | Large executor StatefulSet (100Gi data PVC) |
| `build-and-push.sh` | Build and push Docker image to GHCR |
| `Dockerfile` | Dockerfile for Dremio OSS image |
| `deploy.sh` | Deployment script with envsubst |
| `services/nessie-token-rotator/` | Nessie catalog token rotation CronJob |

## KEDA Metrics Exporter Integration

KEDA scales executors based on metrics from the `dremio-keda-exporter` sidecar. The flow is:

```
┌─────────────┐  polls /apiv2/jobs   ┌──────────────────────┐
│   Dremio     │◄──────────────────────│  metrics-exporter    │
│  Coordinator │                        │  (:5001/json)        │
└──────┬──────┘                        └──────────┬───────────┘
       │                                          │
       │ reads StatefulSet annotations             │ exposes metrics
       │ set by ElasticResourceAllocator           │ executor_desired_small
       │                                          │ executor_desired_large
       │                                          │
┌──────┴──────┐                           ┌──────┴───────────┐
│   K8s API   │                           │  KEDA Controller  │
│  StatefulSets│                          │  polls :5001/json │
└─────────────┘                           └──────┬───────────┘
                                                  │
                                           scales StatefulSets
                                                  │
                                          ┌───────┴────────┐
                                          │  Executor Pods  │
                                          │  (small / large)│
                                          └────────────────┘
```

1. The **metrics exporter** pod polls Dremio's `/apiv2/jobs` API for active/queued jobs
2. It reads `dremio.io/scale-requested-count` annotations on StatefulSets (set by Dremio's `ElasticResourceAllocator`)
3. It computes `executor_desired_small` and `executor_desired_large` from job requirements vs current capacity
4. **KEDA** polls `:5001/json` every 10s and scales the StatefulSets up/down accordingly
5. A **120s grace period** prevents premature scale-down of executors with in-flight queries

The metrics exporter requires `dremio-ops-credentials` secret:
```bash
kubectl create secret generic dremio-ops-credentials \
  --namespace=dremio \
  --from-literal=DREMIO_USERNAME=<admin-username> \
  --from-literal=DREMIO_PASSWORD=<admin-password>
```

## Deployment

These manifests are configured for our k3s/Longhorn environment. If you're deploying on a different platform, see [Adapting for Other Platforms](#adapting-for-other-platforms).

```bash
cd k8s
export GHCR_USERNAME=<your-registry-username>
export GHCR_TOKEN=<your-registry-token>
./deploy.sh
```

## Verification

```bash
kubectl get pods -n dremio
kubectl logs dremio-coordinator-0 -n dremio
```

## Adapting for Other Platforms

These manifests are configured for k3s with Longhorn storage. If you want to deploy Dremio elastic scaling on EKS, GKE, AKS, or another K8s platform, here's what to change:

### Storage Class

All PVCs and `volumeClaimTemplates` use `storageClassName: longhorn`. Replace this with your platform's storage class:

```bash
kubectl get storageclass
```

| Platform | Storage Class | Notes |
|----------|--------------|-------|
| EKS (AWS) | `gp3` | Requires [EBS CSI driver](https://docs.aws.amazon.com/eks/latest/userguide/ebs-csi.html) |
| GKE (GCP) | `standard` or `premium-rwo` | `premium-rwo` uses SSD-backed PDs |
| AKS (Azure) | `azurefile` or `managed-premium` | `azurefile` supports ReadWriteMany |
| k3s with Longhorn | `longhorn` | Default if Longhorn is installed |
| On-prem / bare-metal | Depends on provisioner | May be `local-path`, `openebs-hostpath`, etc. |

Files that contain `storageClassName`:

| File | Resource | Current Value |
|------|----------|---------------|
| `00b-coordinator-pvc.yaml` | Coordinator data PVC | `longhorn` |
| `00c-dist-pvc.yaml` | Shared distribution PVC | `longhorn` |
| `12-executor-small.yaml` | Small executor `volumeClaimTemplates` | `longhorn` |
| `13-executor-large.yaml` | Large executor `volumeClaimTemplates` | `longhorn` |

> **Note:** The distribution PVC (`00c-dist-pvc.yaml`) uses `ReadWriteMany` access mode. Make sure your storage class supports it (Longhorn, EFS, Azure Files, NFS). EBS `gp3` only supports `ReadWriteOnce` — use EFS or an S3 dist path instead.

### S3 / Object Storage Credentials

Executors need access to your distribution bucket. Configure `core-site.xml` in executor ConfigMaps:

| Environment | Credential Provider |
|-------------|-------------------|
| EKS with IAM roles | `InstanceProfileCredentialsProvider` (no secret needed) |
| k3s / on-prem | `EnvironmentVariableCredentialsProvider` with K8s secret |
| GKE with Workload Identity | Use GCS or HMAC keys |

### Distribution Path

| Storage Type | Config Value |
|-------------|-------------|
| PVC (pre-loaded) | `pdfs:///opt/dremio/dist` |
| S3 bucket | `dremioS3://<bucket>/dist` |
| GCS bucket | `dremioGCS://<bucket>/dist` |

### Node Placement

Our manifests use a hardcoded `nodeSelector` for the single-node playground:
```yaml
nodeSelector:
  kubernetes.io/hostname: vmi1594378.contaboserver.net
```

Replace with role-based labels for a multi-node cluster:
```yaml
nodeSelector:
  role: dremio-executor
```

### Spill and Cache Paths

| Tier | Spill Path | Cache Path | PVC Size |
|------|-----------|------------|-----------|
| Small | `/opt/dremio/data/spill` | `/opt/dremio/data/cm/fs/*` | 30Gi minimum |
| Large | `/opt/dremio/data/spill` | `/opt/dremio/data/cm/fs/*` | 100Gi minimum |
| Large (NVMe) | `/mnt/nvme/spill` | `/mnt/nvme/cache/*` | `hostPath` or local PV |

> **Sizing guidance:** The column cache (`/opt/dremio/data/cm/fs/`) grows with the amount of data queried. 30Gi is a reasonable starting point for small executors, but monitor PVC usage and increase if cache eviction appears in logs. Large executors working on bigger datasets may need 100Gi or more. On our playground, we observed the large executor cache reaching 29GB.

## Pitfalls & Lessons Learned

These are real issues we hit during testing on our k3s playground. They apply broadly to any Dremio elastic scaling deployment on Kubernetes — especially the ephemeral storage eviction and probe pitfalls, which are easy to miss.

### 1. KEDA `maxReplicaCount` Too Low Causes Query Stampedes

**Symptom:** Repeated `Scale cap: requested N replicas exceeds max M, capping at M` in coordinator logs. Queries queue up, then all pile onto the single available executor.

**Root cause:** The KEDA ScaledObject for the small tier had `maxReplicaCount: 1`. When multiple concurrent queries each request 2 executors, only 1 is allowed. Every query sees "have 0, need 2, capped to 1" and waits for the same single executor.

**Fix:** Set `maxReplicaCount` high enough for your peak concurrency:
- Small: `4` (handles ~4 concurrent small queries)
- Large: `5`–`8` (depending on node pool size)

### 2. Executor Readiness Probes: Use `tcpSocket` on Port 45678

**Symptom:** Pods stuck at `0/1 Running` indefinitely. Startup probe fails with `connection refused`.

**Root cause:** Port 9047 (`/apiv2/server_status`) is only served by the **coordinator**, not by executors. Executors only listen on port 45678 (Fabric RPC). An `httpGet` probe on port 9047 will never succeed on an executor pod.

**Correct configuration:**
```yaml
startupProbe:
  tcpSocket:
    port: 45678
  failureThreshold: 300    # Up to 10 min for JVM startup (300 × 2s)
  periodSeconds: 2
readinessProbe:
  tcpSocket:
    port: 45678
  failureThreshold: 3
  initialDelaySeconds: 60
  periodSeconds: 10
  timeoutSeconds: 5
```

### 3. Cold-Start Latency Is Significant

Expect **30–90 seconds** from pod scheduling to executor readiness:
- **5–15s**: Pod scheduling, image pull (if not cached), init containers
- **20–60s**: JVM startup, classpath scanning, ZK registration
- **10–15s**: Dremio engine initialization

### 4. `preStop: sleep 120` Is Intentional

The executor StatefulSets include a `preStop` hook with `sleep 120`. This gives in-flight queries 2 minutes to complete before the pod terminates. Combined with `terminationGracePeriodSeconds: 1800` (30 min), this ensures graceful draining during scale-down.

### 5. KEDA Default Timings Are Too Conservative

The default KEDA `cooldownPeriod` of 1800s (30 min) and `stabilizationWindowSeconds` of 900s (15 min) for scale-down are too slow for interactive workloads. Recommended:
- `cooldownPeriod: 600` (10 min idle before scale-down)
- `scaleDown.stabilizationWindowSeconds: 300` (5 min stability window)
- `scaleUp.stabilizationWindowSeconds: 0` (scale up immediately)

### 6. Ephemeral Storage Eviction Kills Executors

**Symptom:** `ExecutionSetupException: One or more nodes lost connectivity during query. Identified nodes were [10.42.x.x:0]`. Executor pods are repeatedly evicted with `Pod ephemeral local storage usage exceeds the total limit of containers`.

**Root cause:** Dremio executors write working data (JAR extractions, native libraries in `/tmp`, query spill, logs) to the container's writable overlay layer. Even though `/opt/dremio/data` is on a PVC (and therefore excluded from ephemeral storage accounting), the overlay layer accumulates ~1GB+ of runtime artifacts. When the container's `ephemeral-storage` limit is set too low (e.g. the original `10Gi` for small executors), the Kubelet evicts the pod mid-query.

On our playground we observed:
- K8s events: `Pod ephemeral local storage usage exceeds the total limit of containers 10Gi`
- `preStop: sleep 120` hook fails during eviction — no graceful shutdown
- Executors enter a crash loop: evicted → recreated → eviction repeats

**Fix:** Increase `ephemeral-storage` limits in the executor StatefulSet to provide adequate headroom:

| Tier | Requests | Limits |
|------|----------|--------|
| Small | `15Gi` | `30Gi` |
| Large | `30Gi` | `60Gi` |

```yaml
resources:
  limits:
    ephemeral-storage: 30Gi   # was 10Gi
  requests:
    ephemeral-storage: 15Gi   # was 5Gi
```

### 7. PVC Retention Policy Matters for Elastic Scaling

**Symptom:** Orphaned PVCs accumulate in the cluster after KEDA scales executors down. Old cache data (8GB+) persists on PVCs that are never reused, wasting storage.

**Root cause:** The default StatefulSet `persistentVolumeClaimRetentionPolicy` is `whenScaled: Retain`. When KEDA scales an executor from N replicas to 0, the PVCs are kept. When a new pod is later created, it gets a brand-new PVC anyway (because the pod ordinal may differ or the StatefulSet was scaled to 0 first). The old PVCs become orphans consuming hundreds of GB of storage.

**Fix:** Set `whenScaled: Delete` so PVCs are cleaned up when replicas are reduced:

```yaml
spec:
  persistentVolumeClaimRetentionPolicy:
    whenDeleted: Retain   # keep if StatefulSet is deleted (safety net)
    whenScaled: Delete     # clean up PVCs when KEDA scales down
```

> **Note:** With `whenScaled: Delete`, PVCs are deleted when the replica count decreases. This is correct for elastic executors because the cache rebuilds from scratch on scale-up anyway. Do NOT use this for the coordinator or any stateful workload where data persistence matters.

### 8. `minReplicaCount: 0` vs `1`

| Setting | Pros | Cons |
|---------|------|------|
| `minReplicaCount: 0` | No cost when idle | 30–90s cold start on first query |
| `minReplicaCount: 1` | Always warm, instant response | Continuous cost even when idle |

## Executor JVM Memory Configuration

Executors **must** have `DREMIO_MAX_MEMORY_SIZE_MB` set to prevent OOMKills. Dremio's startup script calculates `-Xmx` and `-XX:MaxDirectMemorySize` from this value.

> **Warning:** Do NOT use `DREMIO_JAVA_OPTS` for heap settings. The Dremio startup script overrides `-Xmx` and `-XX:MaxDirectMemorySize` based on `DREMIO_MAX_MEMORY_SIZE_MB`.

| Tier | Memory Request | Memory Limit | DREMIO_MAX_MEMORY_SIZE_MB | Ephemeral-Storage Request | Ephemeral-Storage Limit |
|------|----------------|--------------|---------------------------|--------------------------|------------------------|
| small | 4Gi | 8Gi | 6144 | 15Gi | 30Gi |
| large | 8Gi | 16Gi | 14336 | 30Gi | 60Gi |

## Nessie Token Rotation

The `services/nessie-token-rotator/` directory contains a CronJob that refreshes the Cognito OAuth2 bearer token for the Nessie catalog source in Dremio.

Required secrets:
```bash
kubectl create secret generic nessie-token-rotator-secret \
  --namespace=dremio \
  --from-literal=DREMIO_PASSWORD=<dremio-admin-password> \
  --from-literal=OAUTH2_CLIENT_SECRET=<cognito-client-secret>
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Executor not spawning | Check coordinator logs: `kubectl logs dremio-coordinator-0 -n dremio \| grep -i elastic` |
| Executor can't connect to coordinator | Verify DNS: `kubectl exec <executor-pod> -n dremio -c executor -- nslookup dremio-coordinator.dremio.svc.cluster.local` |
| ImagePullBackOff | Recreate registry secret: `kubectl create secret docker-registry ghcr-secret --docker-server=ghcr.io --docker-username=$GHCR_USERNAME --docker-password=$GHCR_TOKEN --namespace=dremio` |
| ConfigMap properties invalid | Use only properties supported by the base image version |
| Executor OOMKilled | Ensure `DREMIO_MAX_MEMORY_SIZE_MB` env var is set and pod memory limit is sufficient |
| Metrics exporter 401 Unauthorized | Verify `dremio-ops-credentials` secret has valid Dremio admin credentials, then restart the pod |
| KEDA not scaling executors | Check KEDA scaler status: `kubectl get scaledobject -n dremio` and metrics exporter logs |
| Scale cap throttling (`requested N exceeds max M`) | Increase `maxReplicaCount` in KEDA ScaledObject |
| Pods stuck at `0/1 Running` | Ensure probes use `tcpSocket` on port 45678, NOT `httpGet` on port 9047 |
| Cold-start delays (30–90s) | Expected; reduce `cooldownPeriod` and `stabilizationWindowSeconds` for faster scale-up |
| S3/object storage access denied | Configure `core-site.xml` in executor ConfigMaps with appropriate credential provider |
| PVC provision failures | Verify `storageClassName` matches your platform's available storage classes |
| Executor pod evicted for ephemeral storage | Increase `ephemeral-storage` limits (15Gi/30Gi for small, 30Gi/60Gi for large); set `whenScaled: Delete` on PVC retention policy (see Pitfalls #6 and #7) |
