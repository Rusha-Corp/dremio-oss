# Elastic Executor Scaling — Implementation Summary

## Overview

This implementation adds elastic (auto-scaling) executor support to Dremio with **tiered scaling** for SMALL and LARGE query executors. The system dynamically scales executor pods based on query workload using KEDA-driven autoscaling with three independent signals:

1. **Job history signal** — Recent completed jobs within the grace period
2. **Ready-pod window** — Executor became ready within the grace period  
3. **Scale-request annotation** — Coordinator requested scale-up within grace period

**Node provisioning is handled by the Kubernetes Cluster Autoscaler.** KEDA manages pod replica counts. Scaling is driven by metrics exported by a Python Flask service that scrapes Dremio's REST API.

The feature is **disabled by default** and does not alter existing Dremio behavior when turned off.

### Two-Tier Architecture

| Tier | Target Workload | Typical Resources | Queue Routing |
|------|-----------------|-------------------|----------------|
| SMALL | Interactive queries | 500m-2GB CPU, 2-4GB memory | `SMALL`, `LOW_COST` |
| LARGE | Analytics/ETL | 2-4GB CPU, 8-16GB memory | `LARGE` |

Executors are tagged via `node-tag: "small"` or `node-tag: "large"` in their ConfigMap. `ExecutorSelectionServiceImpl` routes queries by `queueName` to only executors with matching `node_tag`.

**Important:** Dremio's planner reports `planCost = 1.0` for most queries (metadata queries, simple SELECTs, reflections). The `ElasticAdmissionCalculator.getTier()` method uses `routingQueue` as the **primary signal** for tier detection — if the queue contains `"large"` (case-insensitive), queries are classified as LARGE regardless of plan cost. Plan cost acts only as a secondary fallback when no queue is configured.

### Drain Window Configuration

All three layers of the scaling pipeline are aligned to a 30-minute drain window:

| Layer | Setting | Value | Purpose |
|-------|---------|-------|---------|
| KEDA ScaledObject | `cooldownPeriod` | 1800s (30min) | Prevent rapid scale-down |
| KEDA HPA | `stabilizationWindowSeconds` | 1800s (30min) | Smooth replica changes |
| Exporter | `SCALE_DOWN_GRACE_SECS` | 1800s (30min) | Hold desired ≥ 1 during drain |
| Executor pod | `terminationGracePeriodSeconds` | 1800s (30min) | Allow in-flight query completion |

**Observed scale-down sequence:** In production, when no activity occurs for 30+ minutes:
1. Signal 3 (annotation) expires first, then Signal 2 (ready-replica window) takes over
2. Exporter logs `\"large tier idle Xs (past grace), scaling to 0\"` when both expire
3. KEDA exits `ScalerCooldown` after its own 30-min cooldown
4. HPA `stabilizationWindowSeconds=1800` provides a final 30-min buffer
5. Total cold-start-to-zero time: ~30-60 min depending on which signal fired last

---

## Architecture

### Two-Actor Scaling Model

```
┌─────────────────────────────────────────────────────────────────────┐
│                      Dremio Coordinator                              │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                    ElasticResourceAllocator                    │  │
│  │  (scales StatefulSet via K8s API)                             │  │
│  │  1. Query arrives → determine tier (SMALL/LARGE)              │  │
│  │  2. Check available executors for tier                        │  │
│  │  3. If insufficient: scale delta → spec.replicas              │  │
│  │  4. Write dremio.io/scale-requested-at annotation             │  │
│  │  5. Wait for executors → proceed with query                   │  │
│  └───────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
                    ┌─────────────────────┐
                    │  K8s API (spec.repl)│
                    │  + annotation       │
                    └─────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
┌───────────────┐  ┌──────────────────┐  ┌────────────────┐
│  KEDA Scaled- │  │  dremio-exporter │  │  Executor Pods   │
│  Object       │  │  (Python Flask)  │  │  (StatefulSet)   │
│  (polls metric│  │  - Job history   │  │  - SMALL tier    │
│   every 10s)  │  │  - Ready pod     │  │  - LARGE tier    │
└───────────────┘  │  - Annotation    │  └────────────────┘
                   └──────────────────┘
        │
        ▼
┌──────────────────────────────────────────────────────────────────────┐
│  KEDA ScaledObjects (per tier)                                       │
│  - dremio-executor-small: minReplicaCount: 1 (always-on small tier) │
│  - dremio-executor-large: minReplicaCount: 0 (on-demand large tier) │
│                                                                      │
│  Triggers: metrics-api polling exporter:                              │
│    - executor_desired_small → dremio-executor-small                 │
│    - executor_desired_large → dremio-executor-large                 │
└──────────────────────────────────────────────────────────────────────┘
```

### Three-Signal Grace Timer (Exporter)

The exporter maintains a 30-minute `SCALE_DOWN_GRACE_SECS` window per tier. If **any** of these three signals fires, `desired = spec.replicas` (or 1 if spec=0) to prevent premature scale-down:

1. **Job history signal** — `/apiv2/jobs` shows a job's `endTime` within the last 1800s
2. **Ready-pod window** — `readyReplicas` was > 0 within the last 1800s
3. **Scale-request annotation** — `dremio.io/scale-requested-at` within the last 1800s

### Flow Diagram

```
Query Arrives (e.g., 7-way cross join)
        │
        ▼
ElasticAdmissionCalculator
        │
        ├─► routingQueue = \"query.large\"  ← primary signal
        ├─► tier = LARGE  (queue contains \"large\")
        └─► planCost = 1.0  (ignored when queue set)
                │
                ▼
        ElasticResourceAllocator.allocate()
                │
                ├─► scaleDelta = 3 - 0 = 3
                │
                ▼
        scaleDeployment(dremio-executor-large, 3, 8)
                │
                ├─► Read StatefulSet → spec.replicas = 0
                ├─► Write annotation: dremio.io/scale-requested-at = <now>
                ├─► scale(0+3) → spec.replicas = 3
                │
                ▼
        waitForExecutors(3, LARGE, 5min)
                │
                ├─► Poll K8s: readyReplicas >= 3?
                ├─► Poll ZK: executorSet.size() >= 3?
                │
                ▼
        super.allocate() → BasicResourceAllocator
                │
                ├─► getAllActiveExecutors() with requiredTag = "large"
                │        │
                │        ▼
                │   Filter endpoints by node_tag == "large"
                │   (fails fast if no large executors — throws UserException)
                │
                ▼
        Query executes on 3 large executors
                │
                ▼
        ResourceSet.close() → no more demand gauge
                │
                ▼
        Exporter sees no job history, no ready pod, no annotation
                │
                ├─► If idle >= 1800s: desired_large = 0 → KEDA scales to 0
                └─► Else: desired_large = spec.replicas (hold)
```

---

## Component Details

### 1. ElasticAdmissionCalculator

**Package:** `com.dremio.resource.elastic`

Platform-agnostic admission logic that determines how many executors a query needs based on its estimated cost.

| Method | Description |
|--------|-------------|
| `calculateRequiredExecutors(planCost)` | Returns 1, 2, or 3 executors based on cost thresholds |
| `calculateScaleDelta(requiredExecutors, currentExecutors)` | Returns how many executors to add (0 if sufficient) |
| `getTier(planCost)` | **Deprecated.** Cost-only: SMALL if cost ≤ 10M, else LARGE |
| `getTier(planCost, routingQueue)` | **Primary method.** Returns LARGE if `routingQueue` contains \"large\" (case-insensitive), otherwise falls back to cost-based detection |

**Routing queue as primary signal:**

| Queue Name | Required Tag | Result |
|------------|--------------|--------|
| `LARGE`, `REFLECTION_LARGE`, `WS.large` | `"large"` | LARGE tier regardless of plan cost |
| `SMALL`, `REFLECTION_SMALL`, `LOW_COST` | `"small"` | Falls back to cost |
| `null` / unrecognised | (none) | Falls back to cost |

**Cost Thresholds (configurable):**

| Query Size | Cost Range | Executors | Tier |
|-----------|-----------|----------|------|
| Small | cost <= 10,000,000 | 1 | SMALL |
| Medium | 10M < cost <= 30,000,000 | 2 | SMALL |
| Large | cost > 30,000,000 | 3 | LARGE |

> **Important:** In practice Dremio reports `planCost = 1.0` for most queries (metadata queries, simple SELECTs, reflections). The `routingQueue` parameter (e.g. `"query.large"`) is therefore the primary signal for tier assignment. The plan cost acts only as a secondary fallback when no queue is configured.

### 2. ExecutorSelectionServiceImpl

**Package:** `com.dremio.service.execselector`

Routes queries to executors with matching `node_tag` based on `queueName`.

| Method | Description |
|--------|-------------|
| `getAllActiveExecutors(context)` | Returns endpoints filtered by `node_tag`, fails fast if no matching executors |
| `getExecutors(desiredNum, context)` | Returns filtered endpoints for affinity-based assignment |

**Routing logic:**

| Queue Name | Required Tag | Fallback Behavior |
|------------|--------------|-------------------|
| `LARGE`, `REFLECTION_LARGE` | `"large"` | **FAILS FAST** with clear error message |
| `SMALL`, `REFLECTION_SMALL`, `LOW_COST` | `"small"` | Fallback to all executors |
| `null` / unrecognised | `"small"` (default) | Fallback to all executors |

**Error message example:**
```
RuntimeException: No 'large'-tagged executors are available for queue 'LARGE'.
The large executor pool is scaling up — please retry in a moment.
```

This prevents silent OOM by rejecting LARGE queries when no large executors are available instead of routing to the small tier.

### 3. ElasticResourceAllocator

**Package:** `com.dremio.resource.elastic`
**Extends:** `BasicResourceAllocator`

The main entry point for elastic scaling. Overrides the `allocate()` method to add scaling logic before delegating to the base allocator.

**Flow:**
1. Get the query cost from `ResourceSchedulingProperties`
2. Calculate required executors via `ElasticAdmissionCalculator`
3. Check available executors for the specific tier
4. If insufficient, call `scaleDeployment()` which:
   - Reads current StatefulSet replicas
   - Writes `dremio.io/scale-requested-at = <now>` annotation
   - Calls `scale()` to update `spec.replicas`
5. Calls `waitForExecutors()` which polls K8s and ZooKeeper
6. Delegate to `BasicResourceAllocator.allocate()` for standard allocation

**Scale-up timing:**
- `scale_timeout_minutes` (default 5) — timeout for `waitForExecutors()`
- After timeout, proceeds with available executors (may be 0)
- If 0 executors, `getAllActiveExecutors()` throws `RuntimeException` with clear message

**No demand tracking** — the exporter tracks demand via its three-signal logic (job history, ready-pod window, annotation freshness).

### 6. ResourcePlatform (Interface)

**Package:** `com.dremio.resource.elastic`

Interface for Kubernetes resource management for elastic executor scaling.

| Method | Returns | Description |
|--------|---------|-------------|
| `getReadyPodCount()` | `int` | Number of ready executor pods (all tiers) |
| `getReadyPodCount(tier)` | `int` | Number of ready executor pods for a specific tier |
| `getAvailableExecutors()` | `int` | Number of executors registered with Dremio coordinator (all tiers) |
| `getAvailableExecutors(tier)` | `int` | Number of executors registered for a specific tier |
| `waitForExecutors(count, timeout, unit)` | `boolean` | Blocks until executors are ready or timeout |
| `waitForExecutors(count, tier, timeout, unit)` | `boolean` | Blocks until tier-specific executors are ready |
| `scaleExecutors(scaleDelta)` | `boolean` | Scale small tier (KEDA handles scaling); returns success |
| `scaleExecutors(scaleDelta, tier)` | `boolean` | Scale specific tier (KEDA handles scaling); returns success |

### 7. DremioConfig Constants

Added to `com.dremio.config.DremioConfig`:

| Constant | Config Key | Default |
|----------|-----------|----------|
| `ELASTIC_ENABLED` | `services.executor.elastic.enabled` | `false` |
| `ELASTIC_MIN_EXECUTORS` | `services.executor.elastic.min_executors` | 0 |
| `ELASTIC_MAX_EXECUTORS` | `services.executor.elastic.max_executors` | 10 |
| `ELASTIC_SCALE_TIMEOUT` | `services.executor.elastic.scale_timeout_minutes` | 5 |
| `ELASTIC_K8S_NAMESPACE` | `services.executor.elastic.kubernetes.namespace` | `dremio` |
| `ELASTIC_K8S_POD_TEMPLATE` | `services.executor.elastic.kubernetes.pod_template` | `dremio-executor-small` |
| `ELASTIC_K8S_POD_TEMPLATE_LARGE` | `services.executor.elastic.kubernetes.pod_template_large` | `dremio-executor-large` |
| `ELASTIC_SMALL_QUERY_THRESHOLD` | `services.executor.elastic.small_query_threshold` | 10000000 |
| `ELASTIC_MEDIUM_QUERY_THRESHOLD` | `services.executor.elastic.medium_query_threshold` | 30000000 |

**Note:** The exporter (`SCALE_DOWN_GRACE_SECS`) defaults to 1800 (30 minutes) and should match `scale_timeout_minutes`.

### 4. K8sPlatform

**Package:** `com.dremio.resource.elastic`
**Dependency:** `io.fabric8:kubernetes-client:6.0.0`
**Implements:** `ResourcePlatform`, `Closeable`

Read-only Kubernetes implementation using the Fabric8 K8s client. **Scaling is handled entirely by KEDA ScaledObjects.** This class only observes executor state and writes scale-request annotations.

**Observation methods:**
- `getReadyPodCount(tier)` — lists pods matching tier labels, filters by `phase=Running` and all containers ready
- `getAvailableExecutors(tier)` — reads from ZooKeeper `ListenableSet`, filtering by `node_tag`

**Scale methods:**
- `scaleDeployment(deploymentName, scaleDelta, maxReplicas)`:
  - Reads current StatefulSet `spec.replicas`
  - Computes `newReplicas = min(maxReplicas, current + scaleDelta)`
  - **Writes annotation:** `dremio.io/scale-requested-at = <now>`
  - Calls `scale(newReplicas)` → updates `spec.replicas`
  - Returns `true` on success, `false` on error

**Wait methods:**
- `waitForExecutors(count, tier, timeout, unit)` — polls `getReadyPodCount()` and `getAvailableExecutors()` until both meet the requirement or timeout
- Poll interval: 2 seconds

**Cleanup:** Implements `Closeable` to close the `KubernetesClient` on coordinator shutdown.

### 5. NoOpResourcePlatform

Singleton no-op implementation used when elastic scaling is disabled. Returns 0 for all counts and `false` for wait/scale operations.

### 6. ResourcePlatformProvider

**Package:** `com.dremio.resource.elastic`
**Implements:** `javax.inject.Provider<ResourcePlatform>`, `Closeable`

Factory that creates a `K8sPlatform` when elastic scaling is enabled. The resolved platform is **lazily initialized and cached** on first access. Implements `Closeable` to clean up the `KubernetesClient` on coordinator shutdown.

Performs validation:
- Throws if elastic is enabled but required K8s config is missing (e.g., namespace)
- For Kubernetes: validates connectivity by listing namespaces before returning

---

## Configuration

All configuration lives under `services.executor.elastic` in `dremio.conf` (or `dremio-reference.conf`):

```hocon
services.executor.elastic {
  # Master switch
  enabled: true

  # Scaling bounds
  min_executors: 0
  max_executors: 10
  max_executors_large: 8
  scale_timeout_minutes: 5

  # Kubernetes-specific
  kubernetes {
    namespace: "dremio"
    pod_template: "dremio-executor-small"
    pod_template_large: "dremio-executor-large"
  }

  # Admission thresholds
  small_query_threshold: 10000000
  medium_query_threshold: 30000000
}
```

**Note:** The exporter's `SCALE_DOWN_GRACE_SECS` (default 1800 = 30min) should match `scale_timeout_minutes` to ensure consistent behavior.

---

## Metrics Exporter

The metrics exporter is a Python Flask service (`/tmp/dremio-keda-exporter/app.py`) that runs in the `dremio-metrics-exporter` deployment. It exposes two endpoints:

| Endpoint | Purpose |
|----------|---------|
| `/json` | KEDA metrics-api endpoint — `executor_desired_small`, `executor_desired_large` |
| `/health` | Health check — returns `{"status": "ok"}` |

### Three-Signal Grace Logic

The exporter maintains a 30-minute `SCALE_DOWN_GRACE_SECS` window per tier. `desired = spec.replicas` (or 1 if spec=0) if **any** signal fires:

| Signal | Check | Updates |
|--------|-------|---------|
| Job history | `/apiv2/jobs` shows `endTime` within 1800s | `_last_active_* = now` |
| Ready-pod window | `readyReplicas > 0` within 1800s from first-ready | `_last_active_* = now` |
| Scale-request annotation | `dremio.io/scale-requested-at` within 1800s | `_last_active_* = now` |

If **no** signal fires for 1800s → `desired = 0` → KEDA scales to 0.

### Signal Details

**Signal 1: Job History**
- Polls `/apiv2/jobs` for recent jobs
- For jobs with `endTime` within 1800s, queries `/api/v3/job/{id}` to get `queueName`
- Updates `_last_active_small/large` if SMALL/LARGE jobs found
- Caches queueName to avoid duplicate API calls

**Signal 2: Ready-Pod Window**
- Reads StatefulSet `readyReplicas` from K8s API
- Tracks `first_ready_at` timestamp when `readyReplicas` first goes > 0
- Updates `_last_active_* = now` while within 1800s of `first_ready_at`

**Signal 3: Scale-Request Annotation**
- Reads `dremio.io/scale-requested-at` annotation from StatefulSet
- Updates `_last_active_* = now` if annotation age < 1800s
- Also bumps `spec_replicas = max(spec_replicas, 1)` to ensure KEDA sees desired ≥ 1

### Exporter Output

```json
{
  "active_small_jobs": 0,
  "active_large_jobs": 0,
  "active_user_jobs": 0,
  "active_reflection_jobs": 0,
  "registered_executors": 1,
  "executor_desired_small": 1,
  "executor_desired_large": 4
}
```

### KEDA Configuration

```yaml
# k8s/10-keda-scaledobject.yaml
- name: dremio-executor-small
  minReplicaCount: 1    # Always have 1 small executor (interactive queries)
  maxReplicaCount: 2
  triggers:
    - metadata:
        valueLocation: executor_desired_small

- name: dremio-executor-large
  minReplicaCount: 0    # Scale large on-demand
  maxReplicaCount: 8
  triggers:
    - metadata:
        valueLocation: executor_desired_large
```

---

## DremioConfig Constants

Added to `com.dremio.config.DremioConfig`:

| Constant | Config Key | Default |
|----------|-----------|----------|
| `ELASTIC_ENABLED` | `services.executor.elastic.enabled` | `false` |
| `ELASTIC_MIN_EXECUTORS` | `services.executor.elastic.min_executors` | 0 |
| `ELASTIC_MAX_EXECUTORS` | `services.executor.elastic.max_executors` | 10 |
| `ELASTIC_MAX_EXECUTORS_LARGE` | `services.executor.elastic.max_executors_large` | 8 |
| `ELASTIC_SCALE_TIMEOUT` | `services.executor.elastic.scale_timeout_minutes` | 5 |
| `ELASTIC_K8S_NAMESPACE` | `services.executor.elastic.kubernetes.namespace` | `dremio` |
| `ELASTIC_K8S_POD_TEMPLATE` | `services.executor.elastic.kubernetes.pod_template` | `dremio-executor-small` |
| `ELASTIC_K8S_POD_TEMPLATE_LARGE` | `services.executor.elastic.kubernetes.pod_template_large` | `dremio-executor-large` |
| `ELASTIC_SMALL_QUERY_THRESHOLD` | `services.executor.elastic.small_query_threshold` | 10000000 |
| `ELASTIC_MEDIUM_QUERY_THRESHOLD` | `services.executor.elastic.medium_query_threshold` | 30000000 |

---

## Wiring (DACDaemonModule)

In `DACDaemonModule.java`, the `ResourceAllocator` binding is now conditional:

```java
boolean elasticEnabled = config.getBoolean(DremioConfig.ELASTIC_ENABLED);

if (elasticEnabled) {
  registry.bind(ResourceAllocator.class,
    new ElasticResourceAllocator(
      registry.provider(ClusterCoordinator.class),
      registry.provider(GroupResourceInformation.class),
      new ResourcePlatformProvider(config, registry.provider(ClusterCoordinator.class)),
      config));
} else {
  registry.bind(ResourceAllocator.class,
    new BasicResourceAllocator(
      registry.provider(ClusterCoordinator.class),
      registry.provider(GroupResourceInformation.class)));
}
```

The `dac/backend/pom.xml` was updated to add a dependency on `dremio-services-resourcescheduler`.

---

## Maven Dependencies

Added to `services/resourcescheduler/pom.xml`:

| Dependency | Version | Scope |
|-----------|---------|-------|
| `io.fabric8:kubernetes-client` | 6.0.0 | compile |
| `org.mockito:mockito-core` | 5.15.2 | test |

---

## Kubernetes Deployment

### Manifests (`k8s/`)

| File | Purpose |
|------|---------|
| `00b-coordinator-pvc.yaml` | 20Gi longhorn PVC for coordinator data |
| `00c-dist-pvc.yaml` | 500Gi longhorn PVC for dist (shared by all nodes) |
| `01-rbac.yaml` | ServiceAccount `dremio-elastic` + Role (pod/statefulset CRUD, node list) + RoleBinding |
| `02-service.yaml` | Headless service for coordinator pod DNS resolution |
| `02b-executor-service.yaml` | Headless service for executor pod DNS resolution |
| `03-configmap.yaml` | Coordinator config with `executor.elastic.enabled: true` (no hardcoded secrets) |
| `04-coordinator.yaml` | Coordinator pod (GHCR image: `2026.05.4`, 4GB heap, configmap volume, PVC) |
| `05-service-ingress.yaml` | Service + Ingress for external access |
| `06-ingress.yaml` | Nginx ingress configuration |
| `07-hpa.yaml` | HorizontalPodAutoscaler for coordinator (min 1, max 2) |
| `09-metrics-exporter-deployment.yaml` | Exporter deployment (image: `2026.05.4`, sidecar for KEDA metrics) |
| `10-keda-scaledobject.yaml` | KEDA ScaledObjects for small (minReplica: 1) and large (minReplica: 0) tiers |
| `11a-executor-small-stub.yaml` | StatefulSet for small executors (image: `2026.05.6`, replicas: 0, KEDA-controlled) |
| `11b-executor-large-stub.yaml` | StatefulSet for large executors (image: `2026.05.6`, replicas: 0, KEDA-controlled) |
| `11c-executor-configmaps.yaml` | ConfigMaps: `dremio-executor-config-small` (node-tag: "small") and `dremio-executor-config-large` (node-tag: "large") |
| `11-coredns-custom.yaml` | Custom coreDNS config for pod DNS resolution |
| `12-minio.yaml` | MinIO deployment for test data |
| `12b-minio-init-job.yaml` | Job to create Dremio bucket |
| `12c-minio-credentials-secret.yaml` | MinIO credentials (secrets redacted) |
| `12d-dremio-ops-credentials-secret.yaml` | Dremio ops credentials |
| `Dockerfile` | Docker image for Dremio coordinator |
| `build-and-push.sh` | Build and push script |
| `deploy.sh` | Full deployment script (creates GHCR image pull secret, applies all manifests) |

### Images (GHCR)

- **Coordinator:** `ghcr.io/rusha-corp/dremio-oss:2026.05.6`
- **Executor:** `ghcr.io/rusha-corp/dremio-oss:2026.05.6`
- **Metrics Exporter:** `ghcr.io/rusha-corp/dremio-keda-exporter:2026.05.5`

### Drain Window Alignment

All three layers of the scaling pipeline are aligned to a 30-minute drain window:

| Layer | Setting | Value |
|-------|---------|-------|
| KEDA ScaledObject | `cooldownPeriod` | 1800s (30min) |
| KEDA HPA | `stabilizationWindowSeconds` | 1800s (30min) |
| Exporter | `SCALE_DOWN_GRACE_SECS` | 1800s (30min) |
| Executor pod | `terminationGracePeriodSeconds` | 1800s (30min) |

### Deployment

```bash
export GHCR_USERNAME=<github-username>
export GHCR_TOKEN=<github-token>
cd k8s && ./deploy.sh
```

---

## Tests

### ElasticAdmissionCalculatorTest

Unit tests for the admission calculator:

| Test | Validates |
|------|----------|
| `testSmallQueryRequiresOneExecutor` | cost <= 10M → 1 executor |
| `testMediumQueryRequiresTwoExecutors` | 10M < cost <= 30M → 2 executors |
| `testLargeQueryRequiresThreeExecutors` | cost > 30M → 3 executors |
| `testZeroCostQueryRequiresOneExecutor` | Edge case: zero cost |
| `testNoScaleWhenAlreadyEnough` | Delta = 0 when capacity sufficient |
| `testScaleDeltaWhenNotEnough` | Delta = required - current |
| `testNoScaleWhenExactMatch` | Delta = 0 at exact capacity |
| `testScaleFromZero` | Scaling from 0 executors |
| `testGetTierWithLargeQueueName` | queue \"query.large\" → LARGE regardless of cost (primary signal) |
| `testGetTierWithSmallOrNullQueue` | null/small queue → falls back to cost-based detection |

### K8sPlatformKubeconfigIntegrationTest

Integration test that validates K8s connectivity using kubeconfig:
- Creates a `KubernetesClient` from kubeconfig
- Lists namespaces to verify connectivity
- Queries pods and nodes in the target namespace
- Constructs a `K8sPlatform` and exercises `getReadyPodCount()`

---

## Implementation Status

| Component | Status |
|-----------|--------|
| ElasticAdmissionCalculator | Complete |
| ElasticResourceAllocator | Complete (annotation-driven, scaleDeployment writes `dremio.io/scale-requested-at`) |
| ExecutorSelectionServiceImpl | Complete (tier-aware filtering, no cross-tier fallback for LARGE) |
| ResourcePlatform interface | Complete |
| K8sPlatform | Complete (read-only, Closeable, writes annotation) |
| NoOpResourcePlatform | Complete |
| ResourcePlatformProvider | Complete (cached, Closeable) |
| DremioConfig constants | Complete |
| dremio-reference.conf | Complete |
| DACDaemonModule wiring | Complete |
| Exporter metrics | Complete (3-signal grace: job history, ready-pod, annotation) |
| Unit tests | Complete |
| K8s manifests | Complete (KEDA ScaledObjects with minReplica: 1 for small, 0 for large) |
| KEDA ScaledObjects | Complete (pollingInterval: 10s, per-tier, cooldown: 1800s) |

---

## Observed Scale-Down Behavior

The three-signal grace logic was verified in production on 2026-05-29:

1. **Signal 3 (annotation)** — Coordinator writes `dremio.io/scale-requested-at` when scaling up. Exporter holds `desired = spec.replicas` for 30 min.
2. **Signal 2 (ready-replica window)** — When annotation expires, exporter tracks `first_ready_at` and holds for 30 min from when last executor became Ready.
3. **Signal 1 (job history)** — If no executors were cold-started, any recent job with `endTime` within 1800s resets the timer.

**Observed timeline (large tier):**
- Annotation expired at ~06:50 (520s after initial cold-start)
- Ready-replica window expired at ~06:50+605s = ~07:00
- KEDA `ScalerCooldown` expired at ~07:19 (30 min after first `desired=0`)
- HPA `stabilizationWindowSeconds=1800` expired
- `large-1` began Terminating at ~07:19
- Both large executors gone by ~07:24
- Total cold-start-to-zero time: ~45 minutes

---

## Backward Compatibility

- **Default off:** `services.executor.elastic.enabled` defaults to `false`
- When disabled, `BasicResourceAllocator` is used exactly as before
- No changes to existing query planning or execution paths
- The feature is additive — no existing classes are modified in their default behavior
- Scaling is metric-driven (KEDA) — no imperative K8s API writes from Dremio

## Known Issues and Limitations

### Executor Drain Window

The 30-minute `SCALE_DOWN_GRACE_SECS` is designed to protect in-flight queries and recently-started executors. This means:

- **Large executors may not scale down immediately** — they hold at `spec.replicas` until 30 minutes after the last activity
- The drain window applies per-tier independently (small and large have separate timers)
- Queries that complete before the drain window expire will see executors remain idle but available

### Scale-Up Latency

- First query after idle period triggers scale-up → 2-3 minutes for executors to start and register
- `waitForExecutors()` timeout (default 5min) provides fail-safe fallback
- If no executors available after 5min, query fails with clear error message

### Plan Cost Reliability

Dremio's planner reports `planCost = 1.0` for most queries (metadata queries, simple SELECTs, reflections). The tier detection was updated to use `routingQueue` as the primary signal and plan cost only as a fallback. This is set via Dremio's queue policies (WLM rules).

### KEDA Race Handling

The exporter's three-signal approach prevents race conditions:
1. **Annotation signal** — Coordinator writes `dremio.io/scale-requested-at` before scaling, exporter holds `desired ≥ 1`
2. **Ready-pod window** — When executor becomes ready, exporter holds until drain window expires
3. **Job history signal** — Any recent completed job extends the grace period

This design ensures KEDA never overrides a scale-up request and executors don't terminate prematurely.

---

## Recent Fixes

### 2026-05-29: Maven Build Regression Fix

**Problem:** A regression occurred where the Maven build used `-rf :dremio-dac-daemon`, which skips `exec-selector` and `resourcescheduler` modules. The deployed Docker image contained stale JARs with the old `RuntimeException` in `ExecutorSelectionServiceImpl` instead of the fallback logic introduced in commit `91257d713`.

**Impact:** Large queries routed to the `LARGE` queue failed with:
```
RuntimeException: No 'large'-tagged executors are available for queue 'LARGE'.
The large executor pool is scaling up — please retry in a moment.
```

**Fix:** Updated `k8s/Dockerfile` to explicitly copy the fixed JARs after tarball extraction:
```dockerfile
# Copy fixed exec-selector and resourcescheduler JARs
COPY dremio-services-execselector-26.0.5-202509091642240013-f5051a07.jar /opt/dremio/jars/
COPY dremio-services-resourcescheduler-26.0.5-202509091642240013-f5051a07.jar /opt/dremio/jars/
```

This ensures the latest elastic scaling code (annotation-driven scaling, tier-aware executor wait, ResourceUnavailableException on timeout) is always deployed, even if the full Maven distribution build skips these modules.

**Commit:** `0292980a1` - `k8s: Copy fixed exec-selector and resourcescheduler JARs`

---

## Images (GHCR)

| Service | Image | Latest Tag | Build Date |
|---------|-------|------------|------------|
| Coordinator/Executor | `ghcr.io/rusha-corp/dremio-oss` | `2026.05.7` | 2026-05-29 |
| Metrics Exporter | `ghcr.io/rusha-corp/dremio-keda-exporter` | `2026.05.6` | 2026-05-29 |
