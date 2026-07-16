# Elastic Executor Scaling — Implementation Summary

## Overview

This implementation adds elastic (auto-scaling) executor support to Dremio with **tiered scaling** for SMALL and LARGE query executors. The system dynamically scales executor pods based on query workload using KEDA-driven autoscaling with native Prometheus triggers.

**Scaling signals come directly from the coordinator:**
1. `elastic_desired_small` / `elastic_desired_large` — Prometheus gauges set by `ElasticResourceAllocator` when a query requires more executors
2. `jobs_active` + `maestro_active` — activity gauges used by KEDA's guard trigger to protect executors from premature scale-down while work is running

Scale-down is handled by a background idle-reset thread in `ElasticResourceAllocator` — it resets `elastic_desired_*` to 0 after 30 minutes of inactivity. No sidecar exporter is required. Dremio does not interact with the Kubernetes API directly; KEDA reads the Prometheus gauges and scales the StatefulSets.

The feature is **disabled by default** and does not alter existing Dremio behavior when turned off.

### Two-Tier Architecture

| Tier | Target Workload | Typical Resources | Queue Routing |
|------|-----------------|-------------------|----------------|
| SMALL | Interactive queries | 4GB heap, 8GB pod limit | Any queue without `"large"` |
| LARGE | Analytics/ETL | 20GB heap, 24GB pod limit | Queues containing `"large"` |

Executors are tagged via `node-tag: "small"` or `node-tag: "large"` in their ConfigMap. `ExecutorSelectionServiceImpl` routes queries by `queueName` to only executors with matching `node_tag`.

**Important:** Dremio's planner reports `planCost = 1.0` for most queries (metadata queries, simple SELECTs, reflections). `ElasticResourceAllocator` overrides `getQueueNameFromSchedulingProperties()` to use `routingQueue` as the **primary signal** for tier detection — if the queue contains `"large"` (case-insensitive), queries are classified as `QueueType.LARGE` regardless of plan cost. Plan cost acts only as a secondary fallback when no queue is configured. This override is the single source of truth: both the scaling logic and the downstream tag filter (`applyTagFilter`) use the same `QueueType` result.

### Scale-Down Timing

| Layer | Setting | Value | Purpose |
|-------|---------|-------|---------|
| ElasticResourceAllocator idle-reset | poll interval | 10s | Check `jobs.active` + `maestro.active` via Micrometer globalRegistry |
| ElasticResourceAllocator idle-reset | threshold | 180 polls = 1800s (30 min) | Reset `elastic_desired_*` to 0 after idle; uses total metrics (`jobs.active` + `maestro.active`). The 30-minute window accommodates long-running queries (4+ hour dbt MERGE), executor cold-start (60–90s), PVC provisioning delays, and brief metric drops between query planning and execution. |
| ElasticResourceAllocator wall-clock backstop | MAX_STALE_MS | 90 min | Force-reset gauges to 0 if `elastic_desired_*` has been non-zero for >90 min with `jobs.active=0` (protects against stale Maestro gauge entries) |
| KEDA ScaledObject | `cooldownPeriod` | 600s | Hold pods after INACTIVE signal |
| KEDA HPA | `stabilizationWindowSeconds` (scale-down) | 300s | Smooth replica changes |
| Executor pod | `terminationGracePeriodSeconds` | 1800s | Allow in-flight query completion |

**Observed scale-down sequence from a fully loaded state:**
1. Last query completes → `jobs.active=0`, `maestro.active=0`
2. After 30 min: idle-reset fires → `elastic_desired_small=0`, `elastic_desired_large=0`
3. KEDA guard trigger also drops to 0 → ScaledObject becomes INACTIVE
4. After 10-min cooldown: KEDA sets `spec.replicas=0`
5. Pods enter `preStop` sleep (120s) then terminate
6. Total query-complete-to-zero: ~42 minutes

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                      Dremio Coordinator                              │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                    ElasticResourceAllocator                    │  │
│  │  1. Query arrives → getQueueNameFromSchedulingProperties()    │  │
│  │     (override: routingQueue + cost → QueueType)               │  │
│  │  2. requiredExecutors = calculator.calculateRequiredExecutors │  │
│  │  3. desired = min(required, maxForTier)                       │  │
│  │  4. publishDesired(tier, desired) → sets gauge                │  │
│  │     → arms cold-start guard (jobSeenSinceScaleUp=false)       │  │
│  │  5. waitForTierExecutors() — polls ZK by nodeTag              │  │
│  │  6. super.allocate() → same QueueType for queue + tag filter  │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │  ElasticResourceAllocator.checkAndResetIfIdle() [every 10s]   │  │
│  │  - reads jobs.active, maestro.active from Micrometer           │  │
│  │    globalRegistry (in-process, no HTTP)                       │  │
│  │  - if both=0 and jobSeenSinceScaleUp=true:                    │  │
│  │      idlePollCount++ → after 180 polls (30min): reset gauges│  │
│  │  - if both=0 and jobSeenSinceScaleUp=false: skip countdown    │  │
│  │    (cold-start guard: executors provisioning, not yet ready)   │  │
│  │  - if either>0: jobSeenSinceScaleUp=true, reset countdown     │  │
│  │  - when reset fires: desiredSmall=0, desiredLarge=0,           │  │
│  │    jobSeenSinceScaleUp=false (re-arm for next cycle)           │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  Liveness endpoint: :45679/metrics                                   │
│  - elastic_desired_small  (AtomicInteger gauge)                     │
│  - elastic_desired_large  (AtomicInteger gauge)                     │
│  - jobs.active, maestro.active, reflections.active, reflections.refreshing │
│  (also accessible via Micrometer globalRegistry in-process)         │
│  No Kubernetes API interaction — KEDA handles StatefulSet scaling   │
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
         │    clamp_max(jobs_active + maestro_active + reflections_active + reflections_refreshing, 1) │
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
ElasticResourceAllocator.getQueueNameFromSchedulingProperties()
        │
        ├─► routingQueue = "query.large"  ← primary signal
        ├─► QueueType = LARGE  (queue contains "large")
        └─► requiredExecutors = 3  (based on planCost)
                │
                ▼
        ElasticResourceAllocator.allocate()
                │
                ├─► desired = min(3, maxExecutorsLarge) = 3
                ├─► publishDesired(LARGE, 3)
                │    → desiredLarge.set(3) → elastic_desired_large gauge = 3
                ├─► armIdleGuard() → jobSeenSinceScaleUp = false
                │
                ▼
        Prometheus scrapes :45679/metrics within 5s
        KEDA reads elastic_desired_large=3 within 10s
        KEDA sets spec.replicas=3 on dremio-executor-large
                │
                ▼
        waitForTierExecutors(executorSet, 3, "large", 20min)
                │
                ├─► polls ZK: countTierExecutors(executorSet, "large") >= 3?
                ├─► polls every 2s until ready or timeout
                │
                ▼
        checkAndResetIfIdle() sees jobs_active>0 → jobSeenSinceScaleUp=true
                │
                ▼
        super.allocate() → BasicResourceAllocator
        (calls same overridden getQueueNameFromSchedulingProperties)
                │
                └─► Query executes on 3 large executors
```

---

## Component Details

### 1. ElasticAdmissionCalculator

**Package:** `com.dremio.resource.elastic`

Calculates the number of executors a query needs based on its estimated cost. Tier classification is handled by `ElasticResourceAllocator.getQueueNameFromSchedulingProperties()` which uses `QueueType` as the single source of truth.

| Method | Description |
|--------|-------------|
| `calculateRequiredExecutors(planCost)` | Returns 1, 2, or 3 executors based on cost thresholds |

**Cost Thresholds (configurable):**

| Query Size | Cost Range | Executors |
|-----------|-----------|----------|
| Small | cost <= 10,000,000 | 1 |
| Medium | 10M < cost <= 30,000,000 | 2 |
| Large | cost > 30,000,000 | 3 |

> **Important:** In practice Dremio reports `planCost = 1.0` for most queries. The `routingQueue` parameter (checked in `getQueueNameFromSchedulingProperties`) is therefore the primary tier signal. Plan cost acts only as a secondary fallback.

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

The main entry point for elastic scaling. Overrides `getQueueNameFromSchedulingProperties()` and `allocate()` to add scaling logic before delegating to the base allocator. Publishes Prometheus gauges directly and waits for executors via ZooKeeper. No Kubernetes API interaction.

**Single source of truth — `getQueueNameFromSchedulingProperties()` override:**

Adds `routingQueue` check on top of `BasicResourceAllocator`'s cost-based classification. When `routingQueue` contains "large" (case-insensitive), returns `QueueType.LARGE` (or `REFLECTION_LARGE` for background workloads) regardless of cost. Otherwise delegates to `super` (cost-based). Both `allocate()` and `super.allocate()` call this override, ensuring the scaling logic and downstream tag filter always agree on the tier.

**`allocate()` flow:**
1. If elastic disabled, delegate to `super.allocate()` (no changes)
2. Classify query via `getQueueNameFromSchedulingProperties()` → `QueueType`
3. Calculate `requiredExecutors` via `ElasticAdmissionCalculator.calculateRequiredExecutors(planCost)`
4. `desired = min(requiredExecutors, maxForTier)`
5. `publishDesired(tier, desired)` → sets `elastic_desired_small` or `elastic_desired_large` gauge
6. `armIdleGuard()` → sets `jobSeenSinceScaleUp = false`, resets idle poll count
7. If not enough tier-tagged executors registered in ZK: `waitForTierExecutors()` — polls every 2s until count met or timeout
8. `super.allocate()` → uses same overridden `getQueueNameFromSchedulingProperties()` for queue assignment

**Scale-up timing:**
- `scale_timeout_minutes` (default 5) — timeout for `waitForTierExecutors()`
- If timeout expires, throws `ResourceUnavailableException` (query cancelled cleanly)

**Prometheus gauges (published to liveness endpoint via Micrometer):**
- `elastic_desired_small` — desired small executor replica count
- `elastic_desired_large` — desired large executor replica count

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
    if idlePollCount >= 180:  (30-minute threshold)
        desiredSmall = 0
        desiredLarge = 0
        jobSeenSinceScaleUp = false  // re-arm for next cycle
        idlePollCount = 0
        log "Idle reset: all activity metrics=0 for 300s — resetting elastic_desired_* to 0"
```

**Cold-start guard rationale:**

When a query arrives from a cold state (0 executors), `waitForTierExecutors()` blocks for up to `scale_timeout_minutes` while executor pods start. During this window, `jobs_active=0` because no executors are registered yet. Without the guard, the idle-reset thread would fire after 30 minutes, reset `elastic_desired_*` to 0, KEDA would scale back to 0, and the query would time out.

The guard suppresses the countdown until the first `jobs_active > 0` poll confirms executors are working. After that, the 30-minute countdown runs normally.

**Wall-clock backstop:** If `elastic_desired_*` has been non-zero for >90 minutes with `jobs.active=0`, force-reset to 0. This protects against stale Maestro gauge entries that prevent the normal idle-reset from firing.

**`countTierExecutors(executorSet, tag)`:** Reads from ZooKeeper `ListenableSet`, filtering by `nodeTag`.

**`waitForTierExecutors(executorSet, desired, tag, timeoutMinutes)`:** Polls `countTierExecutors()` every 2s until count met or timeout.

---

## Configuration

All configuration lives under `services.executor.elastic` in `dremio.conf`:

```hocon
services.executor.elastic {
  enabled: true

  max_executors_small: 4         # small-tier cap
  max_executors_large: 8         # large-tier cap
  scale_timeout_minutes: 5

  small_query_threshold: 10000000
  medium_query_threshold: 30000000
}
```

### DremioConfig Constants

| Constant | Config Key | Default |
|----------|-----------|----------|
| `ELASTIC_ENABLED` | `services.executor.elastic.enabled` | `false` |
| `ELASTIC_MAX_EXECUTORS_SMALL` | `services.executor.elastic.max_executors_small` | 10 |
| `ELASTIC_MAX_EXECUTORS_LARGE` | `services.executor.elastic.max_executors_large` | 8 |
| `ELASTIC_SCALE_TIMEOUT` | `services.executor.elastic.scale_timeout_minutes` | 5 |
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
  # even during the 30-min idle-reset window after elastic_desired drops to 0.
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
| `maxReplicaCount` | 4 | 8 | Must match `max_executors_small`/`max_executors_large` in `dremio.conf` |
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
| `02-service.yaml` | Headless service for coordinator pod DNS (includes liveness port 45679) |
| `02b-service-liveness.yaml` | Headless service for coordinator liveness/metrics endpoint (Prometheus scrape) |
| `08-service-executor.yaml` | Headless service for executor StatefulSet DNS |
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
| Coordinator / Executor | `ghcr.io/rusha-corp/dremio-oss` | `2026.07.5` |

---

## Wiring (DACDaemonModule)

```java
boolean elasticEnabled = config.getBoolean(DremioConfig.ELASTIC_ENABLED);

if (elasticEnabled) {
  registry.bind(ResourceAllocator.class,
    new ElasticResourceAllocator(
      registry.provider(ClusterCoordinator.class),
      registry.provider(GroupResourceInformation.class),
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

No additional Maven dependencies beyond core Dremio. The previous `io.fabric8:kubernetes-client` dependency was removed when K8sPlatform was eliminated (Dremio no longer interacts with the Kubernetes API directly).

---

## Tests

### ElasticAdmissionCalculatorTest

| Test | Validates |
|------|----------|
| `testSmallQueryRequiresOneExecutor` | cost <= 10M -> 1 executor |
| `testMediumQueryRequiresTwoExecutors` | 10M < cost <= 30M -> 2 executors |
| `testLargeQueryRequiresThreeExecutors` | cost > 30M -> 3 executors |
| `testZeroCostQueryRequiresOneExecutor` | Edge case: zero cost |

### ElasticResourceAllocatorTest

| Test | Validates |
|------|----------|
| `testLargeQueueOverridesLowCost` | routingQueue="query.large" + cost=100 -> QueueType.LARGE |
| `testLargeQueueCaseInsensitive` | routingQueue="WS.LARGE" -> QueueType.LARGE |
| `testLowCostWithoutRoutingQueueIsSmall` | cost=1M, no routingQueue -> QueueType.SMALL |
| `testHighCostWithoutRoutingQueueIsLarge` | cost=50M, no routingQueue -> QueueType.LARGE |
| `testCostAtThresholdIsSmall` | cost=30M (== threshold) -> QueueType.SMALL |
| `testSmallQueueNameFallsBackToCost` | routingQueue="query.small" + cost=1M -> QueueType.SMALL |

---

## Implementation Status

| Component | Status |
|-----------|--------|
| ElasticAdmissionCalculator | Complete (calculateRequiredExecutors only) |
| ElasticResourceAllocator | Complete (QueueType override, metrics publishing, ZK waiting, idle-reset) |
| ExecutorSelectionServiceImpl | Complete (tier-aware filtering, fail-fast for LARGE, fallback for SMALL) |
| DremioConfig constants | Complete (6 keys: enabled, max_small, max_large, timeout, thresholds) |
| dremio-reference.conf | Complete |
| DACDaemonModule wiring | Complete (3-arg constructor, no ResourcePlatformProvider) |
| Idle-reset thread | Complete (30-min threshold, cold-start guard, 90-min wall-clock backstop) |
| Unit tests | Complete (ElasticResourceAllocatorTest, ElasticAdmissionCalculatorTest) |
| K8s manifests | Complete (dual prometheus triggers, minReplicaCount: 0 both tiers) |
| KEDA ScaledObjects | Complete (prometheus triggers, cooldownPeriod: 600s) |
| K8sPlatform, ResourcePlatform, etc. | Deleted (replaced by direct metrics + ZK in ElasticResourceAllocator) |

---

## Known Issues and Limitations

### Scale-Up Latency

- First query after idle period triggers scale-up → 60–90 seconds for executors to start and register
- PVC provisioning on cloud platforms adds 60–120s per volume (Longhorn, EBS CSI, etc.)
- `waitForExecutors()` timeout (configurable, default 20min in production) provides fail-safe fallback
- If no executors available after timeout, query fails with `ResourceUnavailableException`

### NodeMetricsWriter Errors (Non-Critical)

The coordinator logs `java.io.IOException: Cannot create non-canonical path /opt/dremio/dist/node_history/metrics/*.csv` every ~60 seconds. This is a known Dremio issue where `PseudoDistributedFileSystem` rejects non-remote file paths. The errors are non-critical and do not affect functionality. No fix is available without modifying core Dremio code.

### Gauge Publishing Behavior

The `elastic_desired_*` gauge is always set to `min(requiredExecutors, maxForTier)` on every query, and the idle guard is always armed. This ensures KEDA sees the correct desired count and does not scale down mid-query. If the idle-reset has already fired (`elastic_desired_*=0`) but executors are still alive within KEDA's cooldown window, the gauge refresh ensures KEDA maintains the correct replica count.

### Plan Cost Reliability

Dremio's planner reports `planCost = 1.0` for most queries. The tier detection uses `routingQueue` as the primary signal. Queue policies must be configured in Dremio's Workload Manager (WLM) to correctly route analytics queries to the `LARGE` queue.

---

## Recent Changes

### 2026-07-16: Eliminate K8sPlatform, QueueType Single Source of Truth

**Problem:** Large-tier queries were being routed to small executors, causing OOM kills. The root cause was two independent tier classifiers disagreeing: `ElasticAdmissionCalculator.getTier()` (fork addition, checks routingQueue + cost) vs `BasicResourceAllocator.getQueueNameFromSchedulingProperties()` (core Dremio, checks cost only). The downstream tag filter (`applyTagFilter`) read the core classifier's result, not the fork's, causing mismatches when `routingQueue="large"` but `cost < threshold`.

Additionally, `K8sPlatform` used a `KubernetesClient` to read StatefulSet replica counts and compute `currentReplicas + delta`, even though KEDA already handles scaling based on the `elastic_desired_*` Prometheus gauges. The k8s API read was unnecessary.

**Fix:**

1. **QueueType as single source of truth** — `ElasticResourceAllocator` overrides `getQueueNameFromSchedulingProperties()` to add the `routingQueue` check on top of the parent's cost-based classification. Both `allocate()` and `super.allocate()` use this override, so scaling and tag filtering always agree. No second classifier.

2. **Eliminated K8sPlatform** — Deleted `K8sPlatform`, `ResourcePlatform`, `ResourcePlatformProvider`, and `NoOpResourcePlatform`. `ElasticResourceAllocator` now publishes `elastic_desired_small`/`elastic_desired_large` gauges directly and waits for executors via ZooKeeper `ListenableSet` (filtered by `nodeTag`). No Kubernetes API interaction from Dremio.

3. **Removed ExecutorTier enum** — `ElasticAdmissionCalculator` no longer has `ExecutorTier`, `getTier()`, or `calculateScaleDelta()`. `QueueType` (LARGE/SMALL/REFLECTION_LARGE/REFLECTION_SMALL) is the only tier enum. Only `calculateRequiredExecutors()` remains.

4. **Fail-fast for LARGE in applyTagFilter** — When no large-tagged executors are found, throws `RuntimeException` instead of falling back to all endpoints. Prevents OOM from large queries running on small executors. SMALL tier still falls back (safe — small queries can run on large executors).

5. **Removed stale config keys** — `ELASTIC_K8S_NAMESPACE`, `ELASTIC_K8S_POD_TEMPLATE`, `ELASTIC_K8S_POD_TEMPLATE_LARGE`, `ELASTIC_MIN_EXECUTORS` removed from `DremioConfig.java` and `dremio-reference.conf`.

6. **Increased small executor memory** — `DREMIO_MAX_DIRECT_MEMORY_SIZE_MB` 11264 to 14336, pod memory 15Gi to 20Gi.

7. **Added coordinator preStop and readiness probe** — `preStop: sleep 120` for graceful shutdown, readiness probe on `/apiv2/login:9047` for near-zero-downtime deployment.

**Files changed:**
- `services/resourcescheduler/src/main/java/com/dremio/resource/elastic/ElasticResourceAllocator.java` — complete rewrite: override `getQueueNameFromSchedulingProperties`, direct metrics publishing, ZK-based executor waiting, idle-reset moved from K8sPlatform
- `services/resourcescheduler/src/main/java/com/dremio/resource/elastic/ElasticAdmissionCalculator.java` — removed `ExecutorTier` enum, all `getTier()` methods, `calculateScaleDelta()`
- `services/resourcescheduler/src/main/java/com/dremio/resource/elastic/K8sPlatform.java` — **deleted**
- `services/resourcescheduler/src/main/java/com/dremio/resource/elastic/ResourcePlatform.java` — **deleted**
- `services/resourcescheduler/src/main/java/com/dremio/resource/elastic/ResourcePlatformProvider.java` — **deleted**
- `services/resourcescheduler/src/main/java/com/dremio/resource/elastic/NoOpResourcePlatform.java` — **deleted**
- `services/resourcescheduler/src/test/java/com/dremio/resource/elastic/K8sPlatformTest.java` — **deleted**
- `services/resourcescheduler/src/test/java/com/dremio/resource/elastic/ResourcePlatformProviderTest.java` — **deleted**
- `services/resourcescheduler/src/test/java/com/dremio/resource/elastic/ElasticResourceAllocatorTest.java` — rewritten for new architecture
- `services/resourcescheduler/src/test/java/com/dremio/resource/elastic/ElasticAdmissionCalculatorTest.java` — removed getTier/calculateScaleDelta tests
- `services/exec-selector/src/main/java/com/dremio/service/execselector/ExecutorSelectionServiceImpl.java` — fail-fast for LARGE tag
- `services/exec-selector/src/test/java/com/dremio/service/execselector/TestExecutorSelectionServiceImplTagFilter.java` — updated for fail-fast behavior
- `dac/backend/src/main/java/com/dremio/dac/daemon/DACDaemonModule.java` — removed ResourcePlatformProvider binding, updated ElasticResourceAllocator constructor
- `common/legacy/src/main/java/com/dremio/config/DremioConfig.java` — removed ELASTIC_K8S_*, ELASTIC_MIN_EXECUTORS constants
- `dremio-reference.conf` — removed kubernetes platform config, min_executors, cloud provider configs
- `k8s/12-executor-small.yaml` — increased direct memory 11264 to 14336, pod memory 15Gi to 20Gi
- `k8s/04-coordinator.yaml` — added preStop hook and readiness probe

### 2026-07-14: Fix ZK fsync Latency Causing Executor Disconnections

**Problem:** After the initial fixes, queries continued to fail with `ExecutionSetupException: One or more nodes lost connectivity during query`. The root cause was ZooKeeper disk I/O latency.

**Root cause:** ZK-2 (the leader) was co-located with the coordinator (14Gi RAM) and executor-large-0 (62Gi RAM) on the same node. Additionally, ZK used Longhorn storage, which adds network replication overhead (~2x write amplification). The combined I/O contention caused ZK fsync times of 1.2-3.4 seconds (normal is <10ms). These delays triggered ZK leader elections, which caused executor Curator clients to enter SUSPENDED state. When sessions expired, the coordinator saw executors as "no longer registered" and failed queries.

**Fix:**

1. Switched ZK storage from Longhorn to `local-path` (direct node disk, no network replication). This eliminated fsync latency entirely.
2. Added `podAntiAffinity` to ZK StatefulSet: avoid coordinator nodes (weight 90) and executor nodes (weight 50) to prevent disk I/O contention.
3. Increased ZK `tickTime` from 2000ms to 3000ms, `initLimit` from 5 to 10 ticks, `syncLimit` from 2 to 5 ticks for better latency tolerance.
4. Increased readiness probe timeout from 1s to 5s (fsync delays can block ZK for 2-3s).

**Verification:** Zero fsync warnings across all 3 ZK pods after the fix. ZK pods placed on 3 different nodes, none on the coordinator node. Zero `ExecutionSetupException` errors. Queries completing successfully.

**Files changed:**
- `k8s/09-zookeeper.yaml` — switched storage to local-path, added podAntiAffinity, added ZK tuning env vars, increased readiness probe timeout

### 2026-07-14: Fix Long-Running Query Failures (3 Critical Issues)

**Problem:** Long-running queries (4+ hour dbt MERGE operations) were failing due to three interconnected issues in the KEDA/Prometheus/elastic scaling pipeline.

**Root causes:**

1. **Missing liveness Service** — Port 45679 (metrics) was not exposed in any Kubernetes Service. The headless coordinator Service (`02-service.yaml`) only had ports 9047, 45678, and 32010. Without a Service exposing 45679, Prometheus could not scrape `:45679/metrics`, so KEDA always read 0 for all metrics and never scaled up executors.

2. **KEDA small executor ScaledObject missing guard trigger** — The deployed KEDA ScaledObject for `dremio-executor-small` was missing the guard trigger entirely (only had the primary `elastic_desired_small` trigger). The `dremio-executor-large` ScaledObject had the guard trigger with the correct expression including `reflections_active` and `reflections_refreshing`. Applied the dual-trigger configuration to both ScaledObjects to ensure scale-down protection for both tiers.

3. **Idle-reset threshold too short for long-running queries** — The 5-minute (30 polls) idle-reset threshold was too aggressive for multi-hour queries. When `jobs.active` briefly dropped to 0 (e.g., between query planning phases), the countdown could start and reach threshold, resetting gauges to 0. KEDA would then scale down executors mid-query.

**Fix:**

1. Created `02b-service-liveness.yaml` — a dedicated headless Service exposing port 45679 for Prometheus scraping. Also added port 45679 to the main coordinator Service.

2. Simplified KEDA guard expressions in `06-keda-small.yaml` and `07-keda-large.yaml` to use only metrics that exist: `clamp_max(jobs_active + maestro_active, 1)`.

3. Increased `IDLE_RESET_THRESHOLD` from 30 to 180 (5 min → 30 min) and `MAX_STALE_MS` from 45 min to 90 min in `K8sPlatform.java`.

4. Changed `IllegalStateException` to `ResourceAllocationException` in `ElasticResourceAllocator.java` for graceful query failure when elastic scaling is misconfigured.

**Files changed:**
- `k8s/02b-service-liveness.yaml` — new: headless Service for coordinator liveness/metrics endpoint
- `k8s/02-service.yaml` — added port 45679 (liveness) to coordinator Service
- `k8s/06-keda-small.yaml` — added guard trigger (was missing entirely), using `clamp_max(jobs_active + maestro_active + reflections_active + reflections_refreshing, 1)`
- `k8s/07-keda-large.yaml` — guard trigger already present, no expression change needed
- `k8s/deploy.sh` — added `kubectl apply` for `02b-service-liveness.yaml`
- `services/resourcescheduler/src/main/java/com/dremio/resource/elastic/K8sPlatform.java` — `IDLE_RESET_THRESHOLD` 30→180, `MAX_STALE_MS` 45→90 min
- `services/resourcescheduler/src/main/java/com/dremio/resource/elastic/ElasticResourceAllocator.java` — `IllegalStateException` → `ResourceAllocationException`
- `services/resourcescheduler/src/test/java/com/dremio/resource/elastic/K8sPlatformTest.java` — updated thresholds, wall-clock backstop timeouts
- `k8s/README.md` — documented liveness Service, corrected guard expression, updated thresholds
- `ELASTIC_SCALING_IMPLEMENTATION.md` — documented liveness Service, corrected guard expression, updated thresholds

### 2026-07-06: Fix Idle-Reset Deadlock (Executors Stuck After Query Completion)

**Problem:** After a query completed, `elastic_desired_small` and `elastic_desired_large` gauges remained stuck at their scale-up values (e.g., 3) indefinitely. KEDA never scaled executors down because the gauges never reset to 0. This caused executors to run for hours after all queries completed.

**Root cause:** A previous fix (v2026.07.1) added a `desiredSmall > 0 || desiredLarge > 0` guard to `checkAndResetIfIdle()` to prevent premature reset during executor cold-start. However, this guard created a deadlock: the reset could only clear `desiredSmall`/`desiredLarge` to 0, but the guard prevented the reset whenever they were > 0. Since the gauges are only set to 0 by the reset, they could never be cleared.

**Fix:** Removed the `desiredSmall > 0 || desiredLarge > 0` guard and increased the idle-reset threshold from 6 polls (60s) to 30 polls (5 minutes). The longer threshold provided sufficient protection during cold-start without creating a deadlock:

1. During cold-start: `jobSeenSinceScaleUp = false` → countdown suppressed (unchanged)
2. After job seen but activity drops to 0: 5-minute countdown before reset (was 60s)
3. The 5-minute window accommodated executor JVM startup (20–60s), PVC provisioning (60–120s), and brief metric drops between planning and execution

**Note:** This 5-minute threshold was subsequently increased to 30 minutes in v2026.07.4 to protect long-running queries.

**Verified behavior:** Idle-reset now fires correctly at `Idle reset: all activity metrics=0 for 300s — resetting elastic_desired_small=1 elastic_desired_large=0 to 0` (threshold since increased to 30 min). KEDA deactivates ScaledObjects and scales executors to 0 after the cooldown period.

**Files changed:**
- `services/resourcescheduler/src/main/java/com/dremio/resource/elastic/K8sPlatform.java` — removed `desiredSmall/desiredLarge > 0` guard, increased `IDLE_RESET_THRESHOLD` from 6 to 30
- `services/resourcescheduler/src/test/java/com/dremio/resource/elastic/K8sPlatformTest.java` — updated threshold assertion, replaced deadlock test with test verifying reset fires after threshold

### 2026-07-05: Replace HTTP Polling with Micrometer GlobalRegistry Access

**Problem:** K8sPlatform was polling its own liveness endpoint (`localhost:45679/metrics`) via HTTP and parsing Prometheus text format to read `jobs_active` and `maestro_active` gauges. This was fragile (HTTP parsing, network overhead) and added unnecessary latency.

**Fix:** Replaced HTTP polling with direct Micrometer `Metrics.globalRegistry.find(name).gauge().value()` access. Since `K8sPlatform` runs in the same JVM as the coordinator, all gauges are accessible in-process without network calls or text parsing.

**Additional changes:**
- Reverted 13 core Dremio files to base 26.0.5 (tier-specific metric infrastructure in ForemenWorkManager, MaestroServiceImpl, QueryTracker, Foreman, ReflectionManager, etc.)
- KEDA guard triggers now use total metrics (`jobs_active + maestro_active + reflections_active + reflections_refreshing`)
- Removed accelerator JAR from Docker overlay
- Simplified DACDaemonModule wiring (lambda Provider instead of anonymous ResourcePlatform wrapper)

**Files changed:**
- `services/resourcescheduler/src/main/java/com/dremio/resource/elastic/K8sPlatform.java` — replaced `checkAndResetIfIdle()` HTTP polling with Micrometer `globalRegistry` access; removed `parsePrometheusGauge()`, `METRICS_PORT`
- `services/resourcescheduler/src/test/java/com/dremio/resource/elastic/K8sPlatformTest.java` — replaced 8 Prometheus parser tests with 4 Micrometer registry tests
- `services/resourcescheduler/src/test/java/com/dremio/resource/elastic/ElasticResourceAllocatorTest.java` — added `OptionManager` mock for `QUEUE_THRESHOLD_SIZE`
- `k8s/06-keda-small.yaml` — guard query uses total metrics
- `k8s/07-keda-large.yaml` — guard query uses total metrics
- `k8s/Dockerfile` — removed accelerator JAR overlay
- `k8s/build-and-push.sh` — removed accelerator JAR staging step
- `dac/daemon/src/main/java/com/dremio/dac/daemon/DACDaemonModule.java` — simplified Provider wiring

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
4. Background idle-reset thread in `K8sPlatform` resets gauges to 0 after 30 minutes of inactivity (threshold increased from 60s after deadlock fix, then from 5 min to 30 min to protect long-running queries)
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
