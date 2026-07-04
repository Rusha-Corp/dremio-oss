# Elastic Executor Scaling — Implementation Summary

## Overview

This implementation adds elastic (auto-scaling) executor support to Dremio with **tiered scaling** for SMALL and LARGE query executors. The system dynamically scales executor pods based on query workload using KEDA-driven autoscaling with native Prometheus triggers.

**Scaling signals come directly from the coordinator:**
1. `elastic_desired_small` / `elastic_desired_large` — Prometheus gauges set by `ElasticResourceAllocator` when a query requires more executors
2. `jobs_active` + `maestro_active` — activity gauges used by KEDA's guard trigger to protect executors from premature scale-down while work is running

Scale-down is handled by a background idle-reset thread in `K8sPlatform` — it resets `elastic_desired_*` to 0 after 30 seconds of inactivity. No sidecar exporter is required.

The feature is **disabled by default** and does not alter existing Dremio behavior when turned off.

### Two-Tier Architecture

| Tier | Target Workload | Typical Resources | Queue Routing |
|------|-----------------|-------------------|----------------|
| SMALL | Interactive queries | 4GB heap, 8GB pod limit | Any queue without `"large"` |
| LARGE | Analytics/ETL | 20GB heap, 24GB pod limit | Queues containing `"large"` |

Executors are tagged via `node-tag: "small"` or `node-tag: "large"` in their ConfigMap. `ExecutorSelectionServiceImpl` routes queries by `queueName` to only executors with matching `node_tag`.

**Important:** Dremio's planner reports `planCost = 1.0` for most queries (metadata queries, simple SELECTs, reflections). The `ElasticAdmissionCalculator.getTier()` method uses `routingQueue` as the **primary signal** for tier detection — if the queue contains `"large"` (case-insensitive), queries are classified as LARGE regardless of plan cost. Plan cost acts only as a secondary fallback when no queue is configured.

### Scale-Down Timing

| Layer | Setting | Value | Purpose |
|-------|---------|-------|---------|
| K8sPlatform idle-reset | poll interval | 10s | Check `jobs.active` + `maestro.active` via Micrometer globalRegistry |
| K8sPlatform idle-reset | threshold | 6 polls = 60s | Reset `elastic_desired_*` to 0 after idle; uses total metrics (`jobs.active` + `maestro.active`) |
| KEDA ScaledObject | `cooldownPeriod` | 600s | Hold pods after INACTIVE signal |
| KEDA HPA | `stabilizationWindowSeconds` (scale-down) | 300s | Smooth replica changes |
| Executor pod | `terminationGracePeriodSeconds` | 1800s | Allow in-flight query completion |

**Observed scale-down sequence from a fully loaded state:**
1. Last query completes → `jobs.active=0`, `maestro.active=0`
2. After 60s: idle-reset fires → `elastic_desired_small=0`, `elastic_desired_large=0`
3. KEDA guard trigger also drops to 0 → ScaledObject becomes INACTIVE
4. After 600s cooldown: KEDA sets `spec.replicas=0`
5. Pods enter `preStop` sleep (120s) then terminate
6. Total query-complete-to-zero: ~12 minutes

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                      Dremio Coordinator                              │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                    ElasticResourceAllocator                    │  │
│  │  1. Query arrives → determine tier (SMALL/LARGE)              │  │
│  │  2. Check available executors for tier                        │  │
│  │  3. If insufficient: scaleExecutors(delta, tier)              │  │
│  │     → sets elastic_desired_small/large gauge                  │  │
│  │     → arms cold-start guard (jobSeenSinceScaleUp=false)       │  │
│  │  4. waitForExecutors() — blocks until ZK registration         │  │
│  │  5. BasicResourceAllocator.allocate() → query executes        │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │  K8sPlatform.checkAndResetIfIdle() [every 10s]                │  │
│  │  - reads jobs.active, maestro.active from Micrometer           │  │
│  │    globalRegistry (in-process, no HTTP)                       │  │
│  │  - if both=0 and jobSeenSinceScaleUp=true:                    │  │
│  │      idlePollCount++ → after 6 polls (60s): reset gauges to 0 │  │
│  │  - if both=0 and jobSeenSinceScaleUp=false: skip countdown    │  │
│  │    (cold-start guard: executors provisioning, not yet ready)   │  │
│  │  - if either>0: jobSeenSinceScaleUp=true, reset countdown     │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  Liveness endpoint: :45679/metrics                                   │
│  - elastic_desired_small  (AtomicInteger gauge)                     │
│  - elastic_desired_large  (AtomicInteger gauge)                     │
│  - jobs.active, maestro.active, reflections_active                  │
│  (also accessible via Micrometer globalRegistry in-process)
└─────────────────────────────────────────────────────────────────────┘
                    │
                    │ Prometheus scrape (5s interval)
                    ▼
         ┌────────────────────┐
         │     Prometheus     │
         └──────────┬─────────┘
                    │ native prometheus trigger (10s poll)
                    ▼
         ┌──────────────────────────────────────┐
         │          KEDA Controller             │
         │                                      │
         │  Per-tier ScaledObject:              │
         │  trigger 1 (primary):                │
         │    elastic_desired_small/large        │
         │    → drives replica count            │
         │  trigger 2 (guard):                  │
         │    jobs_active + maestro_active       │
         │    (+ reflections_active for small)   │
         │    → keeps executors alive during     │
         │      active work                     │
         │  KEDA takes max across triggers       │
         └──────────┬───────────────────────────┘
                    │ sets .spec.replicas
                    ▼
         ┌────────────────────┐
         │ Executor StatefulSets│
         │  (small / large)   │
         └────────────────────┘
```

### Scale-up flow

```
Query Arrives (e.g., routed to query.large queue)
        │
        ▼
ElasticAdmissionCalculator
        │
        ├─► routingQueue = "query.large"  ← primary signal
        ├─► tier = LARGE  (queue contains "large")
        └─► requiredExecutors = 3  (based on planCost)
                │
                ▼
        ElasticResourceAllocator.allocate()
                │
                ├─► availableExecutors(LARGE) = 0
                ├─► scaleDelta = max(0, 3-0) = 3
                │
                ▼
        K8sPlatform.scaleDeployment("dremio-executor-large", 3, 8)
                │
                ├─► desiredLarge.set(3)  → elastic_desired_large gauge = 3
                ├─► jobSeenSinceScaleUp = false  (arms cold-start guard)
                │
                ▼
        Prometheus scrapes :45679/metrics within 5s
        KEDA reads elastic_desired_large=3 within 10s
        KEDA sets spec.replicas=3 on dremio-executor-large
                │
                ▼
        waitForExecutors(3, LARGE, 5min)
                │
                ├─► polls ZK: executorSet.size() >= 3?
                ├─► polls every 2s until ready or timeout
                │
                ▼
        checkAndResetIfIdle() sees jobs_active>0 → jobSeenSinceScaleUp=true
                │
                ▼
        super.allocate() → BasicResourceAllocator
                │
                └─► Query executes on 3 large executors
```

---

## Component Details

### 1. ElasticAdmissionCalculator

**Package:** `com.dremio.resource.elastic`

Platform-agnostic admission logic that determines how many executors a query needs based on its estimated cost.

| Method | Description |
|--------|-------------|
| `calculateRequiredExecutors(planCost)` | Returns 1, 2, or 3 executors based on cost thresholds |
| `calculateScaleDelta(requiredExecutors, currentExecutors)` | Returns `max(0, required - current)` |
| `getTier(planCost)` | Cost-only fallback: SMALL if cost ≤ 10M, else LARGE |
| `getTier(planCost, routingQueue)` | **Primary method.** Returns LARGE if `routingQueue` contains `"large"` (case-insensitive), otherwise falls back to cost |

**Routing queue as primary signal:**

| Queue Name | Result |
|------------|--------|
| `LARGE`, `REFLECTION_LARGE`, `query.large` | LARGE tier regardless of plan cost |
| `SMALL`, `REFLECTION_SMALL`, `LOW_COST`, `null` | Falls back to cost-based detection |

**Cost Thresholds (configurable):**

| Query Size | Cost Range | Executors | Tier |
|-----------|-----------|----------|------|
| Small | cost ≤ 10,000,000 | 1 | SMALL |
| Medium | 10M < cost ≤ 30,000,000 | 2 | SMALL |
| Large | cost > 30,000,000 | 3 | LARGE |

> **Important:** In practice Dremio reports `planCost = 1.0` for most queries. The `routingQueue` parameter is therefore the primary tier signal. Plan cost acts only as a secondary fallback.

### 2. ExecutorSelectionServiceImpl

**Package:** `com.dremio.service.execselector`

Routes queries to executors with matching `node_tag` based on `queueName`.

| Queue Name | Required Tag | Fallback Behavior |
|------------|--------------|-------------------|
| `LARGE`, `REFLECTION_LARGE` | `"large"` | **FAILS FAST** with clear error if no large executors available |
| `SMALL`, `REFLECTION_SMALL`, `LOW_COST` | `"small"` | Fallback to all executors |
| `null` / unrecognised | `"small"` (default) | Fallback to all executors |

**Error message example:**
```
RuntimeException: No 'large'-tagged executors are available for queue 'LARGE'.
The large executor pool is scaling up — please retry in a moment.
```

### 3. ElasticResourceAllocator

**Package:** `com.dremio.resource.elastic`
**Extends:** `BasicResourceAllocator`

The main entry point for elastic scaling. Overrides `allocate()` to add scaling logic before delegating to the base allocator.

**Flow:**
1. Get the query cost from `ResourceSchedulingProperties`
2. Calculate required executors and tier via `ElasticAdmissionCalculator`
3. Check available executors for the tier (ZooKeeper-registered)
4. If `scaleDelta > 0`: call `scaleExecutors(delta, tier)` → sets Prometheus gauge, arms cold-start guard
5. Call `waitForExecutors()` — polls ZK every 2s until count met or timeout
6. Delegate to `BasicResourceAllocator.allocate()` for standard allocation

**Scale-up timing:**
- `scale_timeout_minutes` (default 5) — timeout for `waitForExecutors()`
- If timeout expires with 0 executors, `getAllActiveExecutors()` throws `RuntimeException` with clear message

**When `scaleDelta = 0` (executors already available):**
- `scaleExecutors()` is NOT called; gauges are NOT updated; cold-start guard is NOT armed
- Query goes directly to `super.allocate()` and runs with existing executors
- The guard trigger (`jobs_active > 0`) keeps KEDA from scaling down during the query

### 4. K8sPlatform

**Package:** `com.dremio.resource.elastic`
**Dependency:** `io.fabric8:kubernetes-client:6.0.0`
**Implements:** `ResourcePlatform`, `Closeable`

Kubernetes implementation of `ResourcePlatform`. Uses Fabric8 to read StatefulSet state and ZooKeeper to count registered executors.

**Prometheus gauges (published to liveness endpoint via Micrometer):**
- `elastic_desired_small` — desired small executor replica count
- `elastic_desired_large` — desired large executor replica count

**`scaleDeployment()` flow:**
1. Read current `spec.replicas` from StatefulSet
2. Compute `newReplicas = min(maxReplicas, current + delta)`
3. Set `desiredSmall.set(newReplicas)` or `desiredLarge.set(newReplicas)` → updates Prometheus gauge
4. If `newReplicas > 0`: set `jobSeenSinceScaleUp = false` (arm cold-start guard)

> Note: The StatefulSet `spec.replicas` is set by KEDA, not by this class. This class only publishes the desired count as a metric; KEDA reads it from Prometheus and adjusts `spec.replicas`.

**`checkAndResetIfIdle()` — background thread (every 10s):**

```
Read jobs.active, maestro.active from Micrometer globalRegistry

if jobs.active > 0 OR maestro.active > 0:
    jobSeenSinceScaleUp = true
    idlePollCount = 0

else if jobSeenSinceScaleUp = false:  (cold-start guard)
    idlePollCount = 0
    return  // suppress countdown during executor startup window

else:
    idlePollCount++
    if idlePollCount >= 6:  (60s threshold)
        desiredSmall = 0
        desiredLarge = 0
        jobSeenSinceScaleUp = false  // re-arm for next cycle
        idlePollCount = 0
        log "Idle reset: resetting elastic_desired_* to 0"
```

**Cold-start guard rationale:**

When a query arrives from a cold state (0 executors), `waitForExecutors()` blocks for up to 5 minutes while executor pods start. During this window, `jobs_active=0` because no executors are registered yet. Without the guard, the idle-reset thread would fire after 30s, reset `elastic_desired_*` to 0, KEDA would scale back to 0, and the query would time out.

The guard suppresses the countdown until the first `jobs_active > 0` poll confirms executors are working. After that, the countdown runs normally.

**`getAvailableExecutors(tier)`:** Reads from ZooKeeper `ListenableSet`, filtering by `node_tag`.

**`waitForExecutors(count, tier, timeout, unit)`:** Polls `getAvailableExecutors(tier)` every 2s until count met or timeout.

**Cleanup:** `close()` shuts down the idle-reset scheduler and closes the `KubernetesClient`.

### 5. ResourcePlatform (Interface)

**Package:** `com.dremio.resource.elastic`

| Method | Returns | Description |
|--------|---------|-------------|
| `getAvailableExecutors()` | `int` | Executors registered with coordinator (all tiers) |
| `getAvailableExecutors(tier)` | `int` | Executors registered for specific tier |
| `waitForExecutors(count, timeout, unit)` | `boolean` | Blocks until executors ready or timeout |
| `waitForExecutors(count, tier, timeout, unit)` | `boolean` | Tier-specific wait |
| `scaleExecutors(scaleDelta)` | `boolean` | Scale small tier |
| `scaleExecutors(scaleDelta, tier)` | `boolean` | Scale specific tier |

### 6. NoOpResourcePlatform

Singleton no-op used when elastic scaling is disabled. Returns 0 for all counts.

### 7. ResourcePlatformProvider

**Package:** `com.dremio.resource.elastic`

Factory that creates a `K8sPlatform` when elastic scaling is enabled. Lazily initialized and cached on first access. Validates K8s connectivity before returning.

---

## Configuration

All configuration lives under `services.executor.elastic` in `dremio.conf`:

```hocon
services.executor.elastic {
  enabled: true

  min_executors: 0
  max_executors: 4               # small-tier cap
  max_executors_large: 8         # large-tier cap
  scale_timeout_minutes: 5

  kubernetes {
    namespace: "dremio"
    pod_template: "dremio-executor-small"
    pod_template_large: "dremio-executor-large"
  }

  small_query_threshold: 10000000
  medium_query_threshold: 30000000
}
```

### DremioConfig Constants

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

## KEDA ScaledObjects

Each tier has a ScaledObject with two triggers. KEDA takes the maximum:

```yaml
triggers:
  # Primary: coordinator's desired count — drives .spec.replicas
  - type: prometheus
    metadata:
      serverAddress: http://prometheus.<namespace>.svc.cluster.local:9090
      metricName: elastic_desired_small
      query: "elastic_desired_small"
      threshold: "1"
      activationThreshold: "0"

  # Guard: keeps executors alive while work is running,
  # even during the 60s idle-reset window after elastic_desired drops to 0.
  # Uses total metrics (jobs_active + maestro_active + reflections_active +
  # reflections_refreshing) because small-tier queries can fall back to
  # large executors, so keeping both tiers alive is the correct behavior.
  - type: prometheus
    metadata:
      serverAddress: http://prometheus.<namespace>.svc.cluster.local:9090
      metricName: dremio_work_active_small
      query: "clamp_max(jobs_active + maestro_active + reflections_active + reflections_refreshing, 1)"
      threshold: "1"
      activationThreshold: "0"
```

Key ScaledObject parameters:

| Parameter | Small | Large | Notes |
|---|---|---|---|
| `minReplicaCount` | 0 | 0 | Full scale-to-zero |
| `maxReplicaCount` | 4 | 8 | Must match `max_executors` in `dremio.conf` |
| `cooldownPeriod` | 600 | 600 | Seconds after INACTIVE before scaling to 0 |
| `pollingInterval` | 10 | 10 | Seconds between metric reads |
| `scaleUp.stabilizationWindowSeconds` | 0 | 0 | Scale up immediately |
| `scaleDown.stabilizationWindowSeconds` | 300 | 300 | 5-min stability window before scaling down |

---

## Kubernetes Manifests (`k8s/`)

| File | Purpose |
|------|---------|
| `00b-coordinator-pvc.yaml` | 20Gi Longhorn PVC for coordinator data |
| `00c-dist-pvc.yaml` | 500Gi Longhorn PVC for dist (shared across nodes) |
| `01-rbac.yaml` | ServiceAccount `dremio-elastic` + Role (StatefulSet read/patch, node list) + RoleBinding |
| `02-service.yaml` | Headless service for coordinator pod DNS |
| `02b-executor-service.yaml` | Headless service for executor pod DNS |
| `03-configmap.yaml` | Coordinator config with `executor.elastic.enabled: true` |
| `04-coordinator.yaml` | Coordinator deployment |
| `05-service-ingress.yaml` | Service + Ingress for external access |
| `06-ingress.yaml` | Nginx ingress configuration |
| `06-keda-small.yaml` | KEDA ScaledObject for small tier (dual prometheus triggers) |
| `07-keda-large.yaml` | KEDA ScaledObject for large tier (dual prometheus triggers) |
| `08-service-executor.yaml` | Headless service for executor StatefulSet DNS |
| `10-configmap-executor-small.yaml` | Small executor config (node-tag: small, logback.xml) |
| `11-configmap-executor-large.yaml` | Large executor config (node-tag: large, logback.xml) |
| `12-executor-small.yaml` | Small executor StatefulSet (replicas: 0, KEDA-controlled) |
| `13-executor-large.yaml` | Large executor StatefulSet (replicas: 0, KEDA-controlled) |
| `Dockerfile` | Coordinator/executor image build; overlays 6 custom JARs |
| `build-and-push.sh` | Build and push script |
| `deploy.sh` | Full deployment script |

### Images (GHCR)

| Service | Image | Tag |
|---------|-------|-----|
| Coordinator / Executor | `ghcr.io/rusha-corp/dremio-oss` | `2026.05.7` |

---

## Wiring (DACDaemonModule)

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

---

## Maven Dependencies

Added to `services/resourcescheduler/pom.xml`:

| Dependency | Version | Scope |
|-----------|---------|-------|
| `io.fabric8:kubernetes-client` | 6.0.0 | compile |
| `org.mockito:mockito-core` | 5.15.2 | test |

---

## Tests

### ElasticAdmissionCalculatorTest

| Test | Validates |
|------|----------|
| `testSmallQueryRequiresOneExecutor` | cost ≤ 10M → 1 executor |
| `testMediumQueryRequiresTwoExecutors` | 10M < cost ≤ 30M → 2 executors |
| `testLargeQueryRequiresThreeExecutors` | cost > 30M → 3 executors |
| `testZeroCostQueryRequiresOneExecutor` | Edge case: zero cost |
| `testNoScaleWhenAlreadyEnough` | Delta = 0 when capacity sufficient |
| `testScaleDeltaWhenNotEnough` | Delta = required - current |
| `testNoScaleWhenExactMatch` | Delta = 0 at exact capacity |
| `testScaleFromZero` | Scaling from 0 executors |
| `testGetTierWithLargeQueueName` | queue `"query.large"` → LARGE regardless of cost |
| `testGetTierWithSmallOrNullQueue` | null/small queue → falls back to cost-based detection |

---

## Implementation Status

| Component | Status |
|-----------|--------|
| ElasticAdmissionCalculator | Complete |
| ElasticResourceAllocator | Complete |
| ExecutorSelectionServiceImpl | Complete (tier-aware filtering, no cross-tier fallback for LARGE) |
| ResourcePlatform interface | Complete |
| K8sPlatform | Complete (Prometheus gauges, idle-reset thread, cold-start guard, Closeable) |
| NoOpResourcePlatform | Complete |
| ResourcePlatformProvider | Complete (cached, Closeable) |
| DremioConfig constants | Complete |
| dremio-reference.conf | Complete |
| DACDaemonModule wiring | Complete |
| Idle-reset thread | Complete (30s threshold, cold-start guard) |
| Unit tests | Complete |
| K8s manifests | Complete (dual prometheus triggers, minReplicaCount: 0 both tiers) |
| KEDA ScaledObjects | Complete (prometheus triggers, cooldownPeriod: 600s) |
| Metrics exporter (deprecated) | Removed from cluster — replaced by native Prometheus triggers |

---

## Known Issues and Limitations

### Scale-Up Latency

- First query after idle period triggers scale-up → 60–90 seconds for executors to start and register
- `waitForExecutors()` timeout (default 5min) provides fail-safe fallback
- If no executors available after 5min, query fails with a clear error message

### `scaleDelta = 0` Behavior

When executors are already available (`scaleDelta = 0`), `scaleExecutors()` is skipped entirely:
- `elastic_desired_*` gauges are NOT updated
- Cold-start guard is NOT armed
- Query runs directly with existing executors

If the idle-reset has already fired (`elastic_desired_*=0`) but executors are still alive within KEDA's cooldown window, this is safe: the guard trigger (`jobs_active > 0`) keeps KEDA from scaling down during the query. Idle-reset fires after query completion as a no-op (`0→0`), and KEDA scales to 0 via the guard trigger dropping to 0.

### Plan Cost Reliability

Dremio's planner reports `planCost = 1.0` for most queries. The tier detection uses `routingQueue` as the primary signal. Queue policies must be configured in Dremio's Workload Manager (WLM) to correctly route analytics queries to the `LARGE` queue.

---

## Recent Changes

### 2026-06-05: Replace Custom Metrics Exporter with Native Prometheus Triggers

**Problem:** The `dremio-keda-exporter` sidecar had a fundamental circular dependency: it counted RUNNING jobs to determine if executors should stay alive, but with 0 executors all jobs are PENDING → it saw 0 active jobs → KEDA held at 0 → executors never scaled up.

Additional issues identified over development:
- Pagination URL bug: `/next` field pointed to wrong path prefix
- OOMKill when paginating 830k+ historical jobs
- Exporter counted `sys.nodes` queries (~17,000/day) via SQL, causing excessive coordinator load
- Sticky drain loop: after drain completed, KEDA cooldown kept replicas > 0, causing infinite re-drain

**Fix:** Architectural redesign — removed the exporter entirely and moved all scaling logic into the coordinator:

1. `K8sPlatform` publishes `elastic_desired_*` Prometheus gauges directly (no exporter middleman)
2. KEDA uses native `prometheus` triggers reading from the coordinator via Prometheus
3. A dual-trigger design (primary sizing + guard for scale-down protection) replaces the exporter's drain logic
4. Background idle-reset thread in `K8sPlatform` resets gauges to 0 after 30s of inactivity
5. Cold-start guard (`jobSeenSinceScaleUp` flag) prevents the idle-reset from firing during executor startup

**Files changed:**
- `services/resourcescheduler/src/main/java/com/dremio/resource/elastic/K8sPlatform.java` — added idle-reset thread and cold-start guard
- `k8s/06-keda-small.yaml` — replaced `metrics-api` trigger with dual `prometheus` triggers
- `k8s/07-keda-large.yaml` — same
- `k8s/09-metrics-exporter-deployment.yaml` — **deleted**
- `rusha-web-application/k8s/prometheus/configmap.yaml` — added coordinator scrape job

### 2026-05-29: Maven Build Regression Fix

**Problem:** A regression occurred where the Maven build used `-rf :dremio-dac-daemon`, which skipped `exec-selector` and `resourcescheduler` modules. The deployed Docker image contained stale JARs.

**Fix:** Updated `k8s/Dockerfile` to explicitly copy the fixed JARs after tarball extraction.

### 2026-05-29: New DremioConfig Keys Require dremio-reference.conf Entry

**Problem:** Coordinator crashed at startup — `DremioConfig.checkForInvalidPaths()` rejected keys not in `dremio-reference.conf`.

**Fix:** Added `pod_template_large` and `max_executors_large` to `dremio-reference.conf`; updated `Dockerfile` to copy the rebuilt `dremio-common-*.jar`.
