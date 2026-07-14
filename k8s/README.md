# Dremio K8s Elastic Scaling — k3s Playground

## Overview

This is our k3s playground for testing Dremio elastic executor scaling with KEDA autoscaling. The manifests are configured for our specific environment (k3s + Longhorn on a single Contabo node), but the **pitfalls and lessons learned documented here apply to any Dremio elastic scaling deployment on Kubernetes**.

The executor tiers (small/large) scale from zero based on query demand and scale down when idle. KEDA reads scaling metrics directly from the coordinator's Prometheus endpoint — no sidecar exporter required.

## What's Running

| Component | Version / Config |
|-----------|-----------------|
| Kubernetes | k3s on a multi-node cluster (4 workers + 1 control-plane) |
| Storage | Longhorn (all PVCs use `storageClassName: longhorn`) |
| Dremio OSS | `ghcr.io/rusha-corp/dremio-oss:2026.07.2` |
| KEDA | v2.x operator installed on cluster |
| Prometheus | Deployed in `rusha` namespace, scrapes coordinator every 5s |
| Coordinator | 1 pod, 8 GB heap / 4 GB direct (on vmi1594378) |
| Small executors | KEDA-managed StatefulSet, 4 vCPU / 15Gi (t3a.xlarge-equiv), anti-affinity, 30Gi spill PVC |
| Large executors | KEDA-managed StatefulSet, 8 vCPU / 62Gi (i4i.2xlarge-equiv), anti-affinity, 100Gi spill PVC |

## Images

| Image | Registry URI | Source |
|---|---|---|
| Dremio OSS (coordinator + executor) | `ghcr.io/rusha-corp/dremio-oss:2026.07.2` | Single binary; role determined by ConfigMap |

## Manifests

| File | Description |
|------|-------------|
| `00-namespace.yaml` | Namespace definition |
| `00b-coordinator-pvc.yaml` | Coordinator data PVC (Longhorn) |
| `00c-dist-pvc.yaml` | Shared distribution PVC (Longhorn, ReadWriteMany) |
| `01-rbac.yaml` | Service accounts, Roles, ClusterRoleBindings |
| `02-service.yaml` | Headless service for coordinator pod DNS (includes liveness port 45679) |
| `02b-service-liveness.yaml` | Headless service for coordinator liveness/metrics endpoint (port 45679, for Prometheus) |
| `03-configmap.yaml` | Dremio coordinator configuration |
| `04-coordinator.yaml` | Coordinator deployment |
| `05-ingress-tcp-flight.yaml` | TCP ingress for Flight SQL |
| `05-service-ingress.yaml` | ClusterIP service for coordinator UI |
| `06-ingress.yaml` | HTTP ingress for coordinator UI |
| `06-keda-small.yaml` | KEDA ScaledObject for small executor tier |
| `07-keda-large.yaml` | KEDA ScaledObject for large executor tier |
| `08-service-executor.yaml` | Headless service for executor StatefulSet DNS |
| `10-configmap-executor-small.yaml` | Small executor Dremio config |
| `11-configmap-executor-large.yaml` | Large executor Dremio config |
| `12-executor-small.yaml` | Small executor StatefulSet (30Gi data PVC) |
| `13-executor-large.yaml` | Large executor StatefulSet (100Gi data PVC) |
| `build-and-push.sh` | Build and push Docker image to GHCR |
| `Dockerfile` | Dockerfile for Dremio OSS image |
| `deploy.sh` | Deployment script with envsubst |
| `services/nessie-token-rotator/` | Nessie catalog token rotation CronJob |

## KEDA Prometheus Integration

KEDA scales executors using native `prometheus` triggers that read metrics published directly by the coordinator. No sidecar exporter is required.

```
┌──────────────────────────────────────────────┐
│           Dremio Coordinator                  │
│                                               │
│  ElasticResourceAllocator                     │
│  - query arrives → compute required executors │
│  - scaleExecutors(delta, tier)                │
│  - sets elastic_desired_small/large gauge     │
│                                               │
│  K8sPlatform idle-reset thread (every 10s)   │
│  - reads jobs_active + maestro_active         │
│    from Micrometer globalRegistry (in-process)│
│  - after 30 min idle → resets gauges to 0     │
│    (cold-start guard suppresses during         │
│     executor startup window; see below)       │
│                                               │
│  Liveness endpoint: :45679/metrics            │
└──────────────────┬───────────────────────────┘
                   │ Prometheus scrape (5s)
                   ▼
        ┌────────────────────┐
        │  Prometheus        │  (rusha namespace)
        └──────────┬─────────┘
                   │ native prometheus trigger (10s)
                   ▼
        ┌────────────────────┐
        │   KEDA Controller  │
        └──────────┬─────────┘
                   │ sets .spec.replicas
                   ▼
        ┌────────────────────┐
        │ Executor StatefulSets│
        │  (small / large)   │
        └────────────────────┘
```

### Scale-up path

1. Query arrives at the coordinator → `ElasticResourceAllocator` computes tier and delta
2. `K8sPlatform.scaleDeployment()` sets `elastic_desired_small` or `elastic_desired_large` gauge to the new desired count and arms the cold-start guard (`jobSeenSinceScaleUp = false`)
3. Prometheus scrapes the gauge within 5s → KEDA reads it within 10s → sets `spec.replicas`
4. Executor pods start and register with ZooKeeper → `waitForExecutors()` returns → query executes

### Scale-down path

The coordinator's `checkAndResetIfIdle()` thread (10s poll interval) reads `jobs_active` and `maestro_active` directly from the Micrometer `globalRegistry` (in-process, no HTTP). After 180 consecutive idle polls (30 minutes), it resets `elastic_desired_small` and `elastic_desired_large` to 0. The 30-minute window accommodates long-running queries (4+ hour dbt MERGE operations) where `jobs_active` can briefly dip to 0 between planning phases.

When both triggers read 0, KEDA becomes INACTIVE and begins its `cooldownPeriod` (600s). After the cooldown, `spec.replicas` is set to 0 and pods terminate gracefully.

### Cold-start guard

When `scaleDeployment()` is called with `newReplicas > 0`, it sets `jobSeenSinceScaleUp = false`. The idle countdown is suppressed while this flag is false — preventing the 30-minute reset from firing during the executor cold-start window (~60–90s) when `jobs_active=0` even though the coordinator is actively waiting for executors.

The flag is set back to `true` by the idle-reset thread as soon as it sees `jobs_active > 0` for the first time, after which the 30-minute countdown runs normally.

**Important:** The `jobSeenSinceScaleUp` flag is the sole guard for the idle-reset countdown. An earlier version also checked `desiredSmall > 0 || desiredLarge > 0`, but this created a deadlock: the reset could only clear the gauges to 0, but the guard prevented the reset whenever they were > 0. The deadlock was fixed in v2026.07.2 by removing the gauge guard. The threshold was subsequently increased from 5 minutes to 30 minutes to protect long-running queries from premature scale-down.

### Dual triggers per ScaledObject

Each ScaledObject has two `prometheus` triggers. KEDA takes the maximum across all triggers:

| Trigger | Metric | Purpose |
|---|---|---|
| Primary | `elastic_desired_small` / `elastic_desired_large` | Drives the desired replica count |
| Guard | `clamp_max(jobs_active + maestro_active + reflections_active + reflections_refreshing, 1)` | Keeps executors alive while work is running, even during the 30-min idle-reset window |

**Note:** The guard expression includes `reflections_active` and `reflections_refreshing` because they are published by Dremio's reflection subsystem. These metrics keep executors alive during reflection refreshes, which can be long-running operations.

```yaml
- job_name: dremio-coordinator
  scrape_interval: 5s
  static_configs:
    - targets:
        - dremio-coordinator-liveness.dremio:45679
```

Replace `dremio-coordinator-liveness.dremio:45679` with the coordinator's liveness service DNS and port for your namespace. The liveness service is defined in `02b-service-liveness.yaml`.

## Building the Image

The Dremio image is built from the existing distribution tarball with freshly compiled JARs overlaid on top. This avoids a full Maven build (~15 min) for each change.

**Workflow for Java changes** (e.g., `K8sPlatform.java`, `dremio-reference.conf`):

```bash
# 1. Compile only the changed modules
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
./mvnw package -pl common/legacy,services/resourcescheduler \
  -DskipTests -Denforcer.skip=true -Derrorprone.skip=true -q

# 2. Stage the JARs into the Docker build context
cp common/legacy/target/dremio-common-*.jar k8s/
cp services/resourcescheduler/target/dremio-services-resourcescheduler-*.jar k8s/

# 3. Build and push
cd k8s && bash build-and-push.sh
```

The `Dockerfile` copies the staged JARs on top of the tarball at image build time:
```dockerfile
COPY dremio-common-26.0.5-...jar /opt/dremio/jars/
COPY dremio-services-execselector-26.0.5-...jar /opt/dremio/jars/
COPY dremio-services-resourcescheduler-26.0.5-...jar /opt/dremio/jars/
```

> **Note:** `dremio-common` must be copied whenever `dremio-reference.conf` is changed — it is bundled inside that JAR.

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

Check that the coordinator is publishing metrics (after the first scale-up event triggers gauge initialization):

```bash
kubectl run curl-test --image=curlimages/curl --rm -it --restart=Never -n dremio -- \
  curl -s http://dremio-coordinator-liveness.dremio.svc.cluster.local:45679/metrics \
  | grep elastic_desired
```

Verify KEDA ScaledObjects are healthy:
```bash
kubectl get scaledobject -n dremio
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

> **Sizing guidance:** The column cache (`/opt/dremio/data/cm/fs/`) grows with the amount of data queried. 30Gi is a reasonable starting point for small executors, but monitor PVC usage and increase if cache eviction appears in logs. Large executors working on bigger datasets may need 100Gi or more.

## Pitfalls & Lessons Learned

These are real issues we hit during testing on our k3s playground. They apply broadly to any Dremio elastic scaling deployment on Kubernetes.

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

### 8. Executor Log Flood from Netty DEBUG Hex Dumps

**Symptom:** Executor evicted for `Pod ephemeral local storage usage exceeds the total limit of containers 30Gi`, but `df` inside the pod shows only ~1GB used. Kubelet stats show `logs usedBytes: 10+ GB` while `rootfs usedBytes: ~124MB`.

**Root cause:** The ConfigMap volume mount for `/opt/dremio/conf` hides the distribution's bundled `logback.xml`. The fallback classpath config (from `sabot-kernel.jar`) leaves `io.netty` at DEBUG level and sends all wire traffic to stdout. With S3-backed queries, every 8KB HTTP response chunk is hex-dumped as ~512 lines of text, generating ~1 GB/min of container log output. The kubelet counts stdout/stderr (captured to `/var/log/pods/`) toward the pod's ephemeral-storage limit separately from the overlay writable layer.

**Fix:** Add a `logback.xml` key directly to the executor ConfigMaps that explicitly sets `io.netty` to `WARN`:
```xml
<logger name="io.netty" level="WARN"/>
<logger name="software.amazon.awssdk" level="WARN"/>
<logger name="com.amazonaws" level="WARN"/>
```
This is already present in `10-configmap-executor-small.yaml` and `11-configmap-executor-large.yaml`.

### 9. Cold-Start Guard Prevents Premature Idle-Reset During Executor Startup

**Symptom:** Query submitted from a cold state (0 executors) → executors start → query times out in `waitForExecutors()`. Coordinator logs show `elastic_desired_*` was reset to 0 during executor startup.

**Root cause:** Race condition in the coordinator's idle-reset thread. When a query arrives with 0 executors:

1. `scaleExecutors(+N)` sets `elastic_desired_large=N` → KEDA provisions pods
2. `waitForExecutors()` blocks for up to `scale_timeout_minutes` (default 20) while pods start
3. During this window, `jobs_active=0` and `maestro_active=0` (no executors registered yet)
4. Without the guard, after 5 minutes the idle-reset thread resets `elastic_desired_large` back to 0
5. KEDA sees 0 → scales back to 0 → pods terminate before Ready → `waitForExecutors()` times out → query fails

**Fix:** The `jobSeenSinceScaleUp` volatile flag acts as a guard:
- `scaleDeployment()` sets it to `false` when `newReplicas > 0` (arms the guard)
- The idle countdown is skipped entirely while it is `false`
- The flag is set back to `true` when `checkAndResetIfIdle()` sees `jobs_active > 0` for the first time (confirms executors are working)
- After each idle-reset fires, the flag is reset to `false` to guard the next scale-up cycle

### 10. Idle-Reset Deadlock: Executors Stuck After Query Completion

**Symptom:** After queries complete, `elastic_desired_small` and `elastic_desired_large` remain stuck at their scale-up values (e.g., 3). KEDA never scales executors down. Executors run for hours with no active queries.

**Root cause:** A guard condition `desiredSmall > 0 || desiredLarge > 0` in `checkAndResetIfIdle()` prevented the idle-reset from firing whenever the gauges were above 0. Since the reset is the only mechanism that clears the gauges to 0, this created a circular dependency: the reset could only fire when `desiredSmall=0 && desiredLarge=0`, but they could only reach 0 through the reset.

**Fix (v2026.07.2):** Removed the `desiredSmall/desiredLarge > 0` guard entirely. The `jobSeenSinceScaleUp` flag alone provides sufficient cold-start protection. Increased `IDLE_RESET_THRESHOLD` from 6 (60s) to 30 (5 minutes) to accommodate executor registration and PVC provisioning delays without the deadlock-prone gauge guard.

### 11. New `DremioConfig` Keys Must Be Added to `dremio-reference.conf`

**Symptom:** Coordinator crashes at startup with:
```
java.lang.RuntimeException: Failure reading configuration file. The following properties were invalid:
    services.executor.elastic.kubernetes.pod_template_large
    services.executor.elastic.max_executors_large
```

**Root cause:** `DremioConfig.checkForInvalidPaths()` validates every config key used in code against `dremio-reference.conf`. Any key not present in the reference conf causes a hard startup failure — even if the key has a default value.

**Fix:** Add the new key with a default value to `dremio-reference.conf` in the correct HOCON path, then rebuild the image. Because `dremio-reference.conf` is bundled inside `dremio-common-*.jar`, the Docker build must also copy a freshly compiled `dremio-common-*.jar` on top of the distribution tarball:

```dockerfile
# In k8s/Dockerfile — after extracting the distribution tarball:
COPY dremio-common-26.0.5-...jar /opt/dremio/jars/
COPY dremio-services-resourcescheduler-26.0.5-...jar /opt/dremio/jars/
```

### 11. `minReplicaCount: 0` vs `1`

| Setting | Pros | Cons |
|---------|------|------|
| `minReplicaCount: 0` | No cost when idle | 30–90s cold start on first query |
| `minReplicaCount: 1` | Always warm, instant response | Continuous cost even when idle |

## Executor JVM Memory Configuration

Executors **must** set `DREMIO_MAX_HEAP_MEMORY_SIZE_MB` and `DREMIO_MAX_DIRECT_MEMORY_SIZE_MB` explicitly. Dremio's startup script (`bin/dremio-config`) resets `DREMIO_JAVA_OPTS` and then appends `-Xmx` and `-XX:MaxDirectMemorySize` derived from these two env vars. Setting `-Xmx` in `DREMIO_JAVA_OPTS` is **silently discarded** — the launcher's defaults (4 GB heap / 8 GB direct) will apply instead.

> **Warning:** Do NOT use `DREMIO_JAVA_OPTS` for heap/direct memory settings. The Dremio startup script resets this variable and re-appends `-Xmx${DREMIO_MAX_HEAP_MEMORY_SIZE_MB}m -XX:MaxDirectMemorySize=${DREMIO_MAX_DIRECT_MEMORY_SIZE_MB}m` from the dedicated env vars. Always use `DREMIO_MAX_HEAP_MEMORY_SIZE_MB` and `DREMIO_MAX_DIRECT_MEMORY_SIZE_MB` (or `DREMIO_MAX_MEMORY_SIZE_MB` which auto-splits) instead.

> **Per official Dremio docs:** leave 1-2 GB for the OS. Direct memory is what query operators (hash join, hash agg, external sort, TopN) actually consume; the Memory Arbiter tracks and manages direct memory for these operators. Heap is used for DML writers, metadata, and planner state. For ETL-heavy workloads (MERGE, COPY INTO), favor direct memory.

| Tier | Memory Request | Memory Limit | Heap (MB) | Direct (MB) | Total JVM (MB) | OS Headroom | Ephemeral-Storage Request | Ephemeral-Storage Limit | PVC | vCPU | AWS Equiv |
|------|----------------|--------------|-----------|-------------|----------------|-------------|--------------------------|------------------------|-----|------|-----------|
| small | 15Gi | 15Gi | 4096 | 11264 | 15360 | ~1 Gi | 5Gi | 10Gi | 20Gi | 4 | t3a.xlarge |
| large | 62Gi | 62Gi | 8192 | 53248 | 61440 | ~4 Gi | 50Gi | 100Gi | 500Gi | 8 | i4i.2xlarge |

## Nessie Token Rotation

The `services/nessie-token-rotator/` directory contains a CronJob that refreshes the Cognito OAuth2 bearer token for the Nessie catalog source in Dremio.

Required secrets:
```bash
kubectl create secret generic nessie-token-rotator-secret \
  --namespace=dremio \
  --from-literal=DREMIO_PASSWORD=<dremio-admin-password> \
  --from-literal=OAUTH2_CLIENT_SECRET=<cognito-client-secret>
```

## Kubeconfig Setup (Cognito OIDC)

The k3s API server authenticates via AWS Cognito OIDC. To create a kubeconfig from any machine:

### Prerequisites

- `kubectl` installed
- `openssl` installed
- Python 3 with standard library (for HMAC calculation)
- Cognito credentials (client ID, client secret, username, password)

### Environment Variables

```bash
export RUSHA_K8S_SERVER=https://213.199.60.26:6443
export RUSHA_K8S_COGNITO_REGION=eu-west-2
export RUSHA_K8S_COGNITO_CLIENT_ID=73r8agt15158to5n7ei7uukvql
export RUSHA_K8S_COGNITO_CLIENT_SECRET=<your-client-secret>
export RUSHA_K8S_COGNITO_USERNAME=<your-username>
export RUSHA_K8S_COGNITO_PASSWORD=<your-password>
```

### One-Command Kubeconfig Generation

This script authenticates with Cognito, fetches OIDC tokens, extracts the k3s CA certificate, and writes a kubeconfig to `~/.kube/k3s.yaml`:

```bash
#!/bin/bash
set -euo pipefail

# --- Config ---
K3S_SERVER="https://213.199.60.26:6443"
COGNITO_REGION="eu-west-2"
COGNITO_CLIENT_ID="${RUSHA_K8S_COGNITO_CLIENT_ID}"
COGNITO_CLIENT_SECRET="${RUSHA_K8S_COGNITO_CLIENT_SECRET}"
COGNITO_USERNAME="${RUSHA_K8S_COGNITO_USERNAME}"
COGNITO_PASSWORD="${RUSHA_K8S_COGNITO_PASSWORD}"
KUBECONFIG_FILE="${HOME}/.kube/k3s.yaml"

# Cognito User Pool ID (from the OIDC issuer URL)
COGNITO_USER_POOL_ID="eu-west-2_YcLwmCOqe"
OIDC_ISSUER="https://cognito-idp.${COGNITO_REGION}.amazonaws.com/${COGNITO_USER_POOL_ID}"

# --- 1. Calculate SECRET_HASH ---
SECRET_HASH=$(echo -n "${COGNITO_USERNAME}${COGNITO_CLIENT_ID}" \
  | openssl dgst -sha256 -hmac "${COGNITO_CLIENT_SECRET}" -binary \
  | base64)

# --- 2. Authenticate with Cognito (USER_PASSWORD_AUTH) ---
AUTH_RESPONSE=$(python3 -c "
import json, urllib.request, ssl, base64, hashlib, hmac

client_id = '${COGNITO_CLIENT_ID}'
client_secret = '${COGNITO_CLIENT_SECRET}'
username = '${COGNITO_USERNAME}'
password = '${COGNITO_PASSWORD}'
secret_hash = '${SECRET_HASH}'

message = username + client_id
computed_hash = base64.b64encode(
    hmac.new(client_secret.encode('utf-8'), message.encode('utf-8'), hashlib.sha256).digest()
).decode('utf-8')

payload = json.dumps({
    'ClientId': client_id,
    'AuthFlow': 'USER_PASSWORD_AUTH',
    'AuthParameters': {
        'USERNAME': username,
        'PASSWORD': password,
        'SECRET_HASH': computed_hash
    }
}).encode('utf-8')

ctx = ssl.create_default_context()
req = urllib.request.Request(
    'https://cognito-idp.${COGNITO_REGION}.amazonaws.com/',
    data=payload,
    headers={
        'Content-Type': 'application/x-amz-json-1.1',
        'X-Amz-Target': 'AWSCognitoIdentityProviderService.InitiateAuth'
    }
)

resp = urllib.request.urlopen(req, context=ctx)
result = json.loads(resp.read().decode())
tokens = result['AuthenticationResult']
print(json.dumps(tokens))
")

ID_TOKEN=$(echo "$AUTH_RESPONSE" | python3 -c "import json,sys; print(json.load(sys.stdin)['IdToken'])")
REFRESH_TOKEN=$(echo "$AUTH_RESPONSE" | python3 -c "import json,sys; print(json.load(sys.stdin)['RefreshToken'])")

# --- 3. Extract k3s API server CA certificate ---
CA_DATA=$(echo -n | openssl s_client -connect 213.199.60.26:6443 -showcerts 2>/dev/null \
  | openssl x509 -outform PEM 2>/dev/null \
  | base64 -w0)

# --- 4. Write kubeconfig ---
mkdir -p "$(dirname "$KUBECONFIG_FILE")"

cat > "$KUBECONFIG_FILE" <<EOF
apiVersion: v1
kind: Config
clusters:
- cluster:
    certificate-authority-data: ${CA_DATA}
    server: ${K3S_SERVER}
  name: k3s-contabo62
contexts:
- context:
    cluster: k3s-contabo62
    user: cognito-user
  name: k3s-contabo62
current-context: k3s-contabo62
preferences: {}
users:
- name: cognito-user
  user:
    auth-provider:
      name: oidc
      config:
        client-id: ${COGNITO_CLIENT_ID}
        client-secret: ${COGNITO_CLIENT_SECRET}
        id-token: ${ID_TOKEN}
        idp-issuer-url: ${OIDC_ISSUER}
        refresh-token: ${REFRESH_TOKEN}
EOF

echo "Kubeconfig written to $KUBECONFIG_FILE"
echo "Test with: KUBECONFIG=$KUBECONFIG_FILE kubectl get nodes"
```

### Token Refresh

Cognito id_tokens expire after the configured TTL (default 8 hours per the dashboard setup). When the token expires, kubectl will prompt for re-authentication or you can re-run the script above to get fresh tokens.

### Notes

- The Cognito app client (`73r8agt15158to5n7ei7uukvql`) must have **USER_PASSWORD_AUTH** enabled in its allowed auth flows for this script to work.
- The SECRET_HASH is computed as `Base64(HMAC-SHA256(client_secret, username + client_id))` per the [AWS Cognito spec](https://docs.aws.amazon.com/cognito/latest/developerguide/signing-up-users-in-your-app.html#cognito-user-pools-computing-secret-hash).
- RBAC is configured via a ClusterRoleBinding (`cognito-dashboard-admins`) that maps Cognito users to cluster admin permissions.
- The k3s API server OIDC configuration (`--oidc-issuer-url`, `--oidc-client-id`, etc.) must match the Cognito User Pool settings.

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Executor not spawning | Check coordinator logs: `kubectl logs -n dremio -l role=coordinator \| grep -i elastic` |
| Executor can't connect to coordinator | Verify DNS: `kubectl exec <executor-pod> -n dremio -c executor -- nslookup dremio-coordinator.dremio.svc.cluster.local` |
| ImagePullBackOff | Recreate registry secret: `kubectl create secret docker-registry ghcr-secret --docker-server=ghcr.io --docker-username=$GHCR_USERNAME --docker-password=$GHCR_TOKEN --namespace=dremio` |
| ConfigMap properties invalid | Use only properties supported by the base image version |
| Executor OOMKilled | Ensure `DREMIO_MAX_HEAP_MEMORY_SIZE_MB` and `DREMIO_MAX_DIRECT_MEMORY_SIZE_MB` env vars are set (NOT `DREMIO_JAVA_OPTS` — it is silently reset by the launcher). Pod memory limit must exceed the sum of heap + direct by 1-2 GB for OS overhead |
| KEDA not scaling executors | Check ScaledObject status: `kubectl get scaledobject -n dremio`; verify Prometheus is scraping coordinator |
| `elastic_desired_*` stuck at 0 | Gauges only appear after first scale event; submit a query to trigger initialization |
| `elastic_desired_*` never resets after query | Idle-reset threshold may be too high or the `jobSeenSinceScaleUp` guard may not be arming — check coordinator logs for `Idle reset` messages. If no reset messages appear, the gauge guard deadlock may be present — ensure image is `2026.07.2`+ (see Pitfall #10) |
| Scale cap throttling (`requested N exceeds max M`) | Increase `maxReplicaCount` in KEDA ScaledObject |
| Pods stuck at `0/1 Running` | Ensure probes use `tcpSocket` on port 45678, NOT `httpGet` on port 9047 |
| Cold-start delays (30–90s) | Expected; reduce `cooldownPeriod` and `stabilizationWindowSeconds` for faster scale-up |
| S3/object storage access denied | Configure `core-site.xml` in executor ConfigMaps with appropriate credential provider |
| PVC provision failures | Verify `storageClassName` matches your platform's available storage classes |
| Executor pod evicted for ephemeral storage | Increase `ephemeral-storage` limits (15Gi/30Gi for small, 30Gi/60Gi for large); set `whenScaled: Delete` on PVC retention policy (see Pitfalls #6 and #7) |
| Executor evicted but `df` shows only ~1GB used | Eviction is from log flood, not disk writes — check kubelet stats for `logs usedBytes`; ensure executor ConfigMaps include `logback.xml` with `io.netty` at WARN (see Pitfall #8) |
| KEDA sees 0, executors not scaling up | Verify Prometheus is scraping `:45679/metrics`; check `kubectl get scaledobject -n dremio` shows `ACTIVE: True` after query submitted |
| Coordinator crashes: `properties were invalid` | Config key missing from `dremio-reference.conf` — ensure image is `2026.07.2`+ (see Pitfall #11) |
| Query fails immediately after scale-up on cold start | Cold-start guard may be missing — ensure coordinator image is `2026.07.2`+ (see Pitfall #9) |
| Executors stuck running after all queries complete | Idle-reset deadlock — `elastic_desired_*` gauges never reset to 0. Ensure coordinator image is `2026.07.2`+ (see Pitfall #10) |

## Dev vs Prod Node Scheduling

### Dev (k3s playground)

- **No nodeSelectors** on executor pods — any non-tainted worker can run any executor
- **Pod anti-affinity** (preferred): spreads executor pods across different nodes when possible
- **Node labels**: all workers have `dremio-large=true` and `dremio-small=true` (separate keys, not mutually exclusive)
- **Control-plane node** (`vmi2936997`) has taint `node-role.kubernetes.io/control-plane:NoSchedule` — no executor pods can schedule there

### Prod (AWS)

- **Add nodeSelectors** to match instance types:
  - Large executor: `nodeSelector: { dremio-large: "true", storage: nvme }`
  - Small executor: `nodeSelector: { dremio-small: "true" }`
- **Instance sizing enforces 1 executor per node**: i4i.2xlarge (64 GiB) fits exactly 1 large executor (62 GiB pod); t3a.xlarge (16 GiB) fits exactly 1 small executor (15 GiB pod)
- **No anti-affinity needed** in prod — instance sizing prevents co-location
- **KEDA `maxReplicaCount`** should match the ASG max capacity

### Switching from dev to prod

1. Add `nodeSelector` blocks to both executor StatefulSets
2. Adjust `maxReplicaCount` in KEDA ScaledObjects to match ASG capacity
3. Adjust `max_executors_small` / `max_executors_large` in coordinator configmap
4. No other changes needed — the anti-affinity is `preferredDuringSchedulingIgnoredDuringExecution` so it degrades gracefully when nodeSelectors restrict placement
