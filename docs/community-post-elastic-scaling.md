<!--

  POSTING INSTRUCTIONS
  ====================
  1. Go to: https://community.dremio.com/c/product-feedback/
  2. Click "New Topic"
  3. Title:  RFC: Elastic executor auto-scaling for Kubernetes deployments
  4. Category: Product Feedback
  5. Paste everything below this comment block (not this block itself)

-->

# RFC: Elastic executor auto-scaling for Kubernetes deployments

## What this does

Elastic executor auto-scaling enables a Dremio coordinator to dynamically provision Kubernetes executor pods when queries require more compute. The coordinator publishes desired replica counts as Prometheus gauges; **KEDA is the sole authority** on StatefulSet `spec.replicas`, reading those gauges via native `prometheus` triggers.

**Implemented and verified in production (July 2026).** Two tiers: SMALL (interactive) and LARGE (analytics/ETL).

---

## The problem

In a Kubernetes deployment today, you must pre-provision executors statically. A cluster serving both lightweight dashboard queries and heavy analytical workloads must either:

- **Over-provision permanently** — paying for idle executors, or
- **Under-provision** — and let heavy queries queue or degrade

Dremio OSS had no mechanism to say "spin up N executors when a large query arrives."

---

## Relationship to Dremio Cloud Engines

Dremio Cloud offers engine-based compute pools with autoscaling managed by the control plane. This RFC provides an analogous capability for self-managed OSS Kubernetes deployments:

| Dimension | Dremio Cloud | This RFC |
|---|---|---|
| Tier selection | SQL routing rules, pre-planning | `routingQueue` contains "large" → LARGE; else plan cost fallback |
| Scaling unit | Engine replica (multi-node group) | Individual executor pod |
| Scale-up | Async; query queues | Synchronous; query blocks in `waitForExecutors()` |
| Scale-down | Control plane | KEDA + idle-reset thread (30-min threshold) |
| Admin surface | Full UI + REST API | `dremio.conf` only |

---

## Design: Coordinator-Published Gauges + KEDA

### Architecture

```
Layer 1 — Coordinator (publishes intent as Prometheus gauges)
  ElasticResourceAllocator: classify via getQueueNameFromSchedulingProperties
    (routingQueue + cost → QueueType — single source of truth)
  publishDesired(tier, desired):
    - Sets elastic_desired_small/large AtomicInteger gauges
    - Published via Micrometer globalRegistry → :45679/metrics
    - Arms cold-start guard (jobSeenSinceScaleUp = false)
  NEVER writes spec.replicas — only publishes metrics.
  No Kubernetes API interaction at all.

  ElasticResourceAllocator.checkAndResetIfIdle() (background thread, every 10s):
    - Reads jobs.active, maestro.active from Micrometer globalRegistry
    - After 180 idle polls (30 min): resets gauges to 0
    - Cold-start guard: suppresses countdown while jobSeenSinceScaleUp=false

Layer 2 — KEDA (acts on intent via Prometheus triggers)
  Primary trigger: elastic_desired_small/large → drives replica count
  Guard trigger: jobs_active + maestro_active → keeps executors alive during work
  KEDA reads metrics every 10s → sets spec.replicas

Layer 3 — Cluster Autoscaler (node-level, optional)
  Transparent provisioning when K8s can't schedule pods
```

### Why This Design

1. **No sidecar exporter**: The coordinator publishes gauges directly via Micrometer (in-process, no HTTP polling). No external metrics exporter is needed.

2. **No dual-write conflict**: Coordinator publishes gauges; KEDA reads metrics and sets `spec.replicas`. The coordinator never writes `spec.replicas` directly.

3. **Synchronous contract**: The `allocate()` contract is synchronous — it must not return until resources are assigned or denied. `waitForExecutors()` polls ZooKeeper every 2s until executors register or timeout.

4. **Clean failure handling**: If executors don't become available within timeout (default 20 min), throws `ResourceUnavailableException`. Query is cancelled cleanly, not degraded to wrong tier.

5. **Idle-reset prevents stale gauges**: A background thread resets `elastic_desired_*` to 0 after 5 minutes of inactivity, ensuring KEDA scales down even if the coordinator doesn't explicitly set gauges to 0 (e.g., after query completion or cancellation).

---

## How it works

### 1. Tier Detection

**Primary signal:** `routingQueue` (from session/ALTER SESSION). If queue name contains "large" (case-insensitive), queries route to LARGE tier regardless of plan cost.

**Fallback:** Plan cost threshold (10M -> SMALL, >30M -> LARGE). Note: Dremio's planner often reports `planCost = 1.0`, so `routingQueue` is the reliable signal.

```java
@Override
protected QueueType getQueueNameFromSchedulingProperties(ctx, props) {
    String routingQueue = props.getRoutingQueue();
    if (routingQueue != null && routingQueue.toLowerCase().contains("large")) {
        return isBackground ? QueueType.REFLECTION_LARGE : QueueType.LARGE;
    }
    return super.getQueueNameFromSchedulingProperties(ctx, props);
}
```

### 2. Scale-Up Flow

```
Query arrives with routingQueue="query.large"
    │
    ▼
ElasticResourceAllocator.getQueueNameFromSchedulingProperties()
    │
    ├─► routingQueue contains "large" → QueueType = LARGE
    └─► planCost = 1.0 (ignored)
            │
            ▼
    ElasticResourceAllocator.allocate()
            │
            ├─► requiredExecutors = 3
            ├─► desired = min(3, maxExecutorsLarge) = 3
            ├─► publishDesired(LARGE, 3)
            │    → desiredLarge.set(3) → Micrometer gauge elastic_desired_large=3
            │    → armIdleGuard() → jobSeenSinceScaleUp = false
            │
            ▼
    waitForTierExecutors(executorSet, 3, "large", 20min)
            │
            └─► Poll ZooKeeper endpoints filtered by nodeTag="large"
               until count >= 3 or timeout
            │
            ▼
    checkAndResetIfIdle() sees jobs_active>0 → jobSeenSinceScaleUp=true
            │
            ▼
    super.allocate() → BasicResourceAllocator (uses same override)
            → query executes
```

### 3. Scale-Down Flow

```
Last query completes → jobs_active=0, maestro_active=0
    │
    ▼ (30 minutes of inactivity)
checkAndResetIfIdle() fires:
    desiredSmall = 0, desiredLarge = 0
    jobSeenSinceScaleUp = false (re-arm for next cycle)
    │
    ▼
KEDA reads elastic_desired_* = 0
KEDA guard trigger also = 0 → ScaledObject becomes INACTIVE
    │
    ▼ (10-minute cooldown period)
KEDA sets spec.replicas = 0
Pods terminate gracefully (preStop sleep 120s + terminationGracePeriodSeconds 1800s)
```

**Total query-complete-to-zero: ~42 minutes** (30 min idle-reset + 10 min KEDA cooldown + 2 min preStop)

---

## Component Details

### ElasticAdmissionCalculator

- `calculateRequiredExecutors(planCost)`: 1 (<=10M), 2 (10M-30M), 3 (>30M)

### ElasticResourceAllocator

- `getQueueNameFromSchedulingProperties()` (override): routingQueue primary, cost fallback → QueueType (single source of truth)
- `allocate()`: classifies tier, publishes gauges, waits for ZK registration, delegates to BasicResourceAllocator
- On timeout: throws `ResourceUnavailableException` (query cancelled cleanly)
- Publishes `elastic_desired_small`/`elastic_desired_large` Prometheus gauges directly via Micrometer
- Idle-reset thread: every 10s, checks `jobs.active` + `maestro.active` from Micrometer globalRegistry
- Cold-start guard: `jobSeenSinceScaleUp` volatile boolean suppresses idle countdown during executor startup
- **No Kubernetes API interaction** — KEDA reads gauges and scales StatefulSets

### ExecutorSelectionServiceImpl

- Routes queries to executors matching `nodeTag` ("small" or "large")
- LARGE queue with no available executors: **fails fast** with clear error message
- SMALL queue with no available executors: **falls back** to all available endpoints

### No Metrics Exporter Needed

The previous architecture used a Python Flask sidecar (`dremio-keda-exporter`) that polled the Dremio REST API. This has been removed. The coordinator now publishes gauges directly via Micrometer, and KEDA reads them via native `prometheus` triggers. This eliminates:
- The circular dependency where the exporter couldn't see PENDING jobs (no executors → no running jobs → exporter reports 0 → KEDA stays at 0)
- OOM kills from paginating 830k+ historical jobs
- Excessive coordinator load from SQL-based job counting

---

## Configuration

```hocon
services.executor.elastic {
  enabled: true
  max_executors_small: 4         # small-tier cap
  max_executors_large: 3         # large-tier cap (must match KEDA maxReplicaCount)
  scale_timeout_minutes: 20       # max wait for executors to register

  small_query_threshold: 10000000    # plan cost ≤ 10M → 1 executor (SMALL)
  medium_query_threshold: 30000000   # plan cost > 30M → 3 executors (LARGE)
}
```

No exporter configuration is needed. The coordinator's idle-reset thread is built into `ElasticResourceAllocator` and requires no external deployment.

---

## Scale-Down Timing

| Layer | Setting | Value | Purpose |
|-------|---------|-------|---------|
| ElasticResourceAllocator idle-reset | poll interval | 10s | Check `jobs.active` + `maestro.active` via Micrometer globalRegistry |
| ElasticResourceAllocator idle-reset | threshold | 180 polls = 1800s (30 min) | Reset `elastic_desired_*` to 0 after inactivity |
| KEDA ScaledObject | `cooldownPeriod` | 600s | Hold pods after INACTIVE signal |
| KEDA HPA | `stabilizationWindowSeconds` (scale-down) | 300s | Smooth replica changes |
| Executor pod | `terminationGracePeriodSeconds` | 1800s | Allow in-flight query completion |

---

## Known Issues

### Executors Stuck After Query Completion (Fixed in v2026.07.2)

An earlier version (v2026.07.1) added a guard condition `desiredSmall > 0 || desiredLarge > 0` to the idle-reset logic, intending to prevent premature reset during executor cold-start. This created a deadlock: the reset could only clear the gauges to 0, but the guard prevented the reset whenever they were > 0. As a result, `elastic_desired_small` and `elastic_desired_large` remained stuck at their scale-up values indefinitely, keeping executors running hours after all queries completed.

**Fix (v2026.07.2):** Removed the `desiredSmall/desiredLarge > 0` guard. The `jobSeenSinceScaleUp` flag alone provides sufficient cold-start protection. Increased the idle-reset threshold from 60s to 5 minutes to accommodate executor registration and PVC provisioning delays.

### NodeMetricsWriter Errors (Non-Critical)

The coordinator logs `java.io.IOException: Cannot create non-canonical path /opt/dremio/dist/node_history/metrics/*.csv` every ~60 seconds. This is a known Dremio issue where `PseudoDistributedFileSystem` rejects non-remote file paths. The errors are non-critical and do not affect functionality.

---

## Image Registry

| Service | Image | Tag |
|---------|-------|-----|
| Coordinator/Executor | `ghcr.io/rusha-corp/dremio-oss` | `2026.07.5` |

No metrics exporter sidecar is required.

---

## Questions for the Dremio team

1. **OSS appetite** — Is there interest in elastic scaling in OSS, or is this planned as an Enterprise-only feature?
2. **Tier routing** — Should we expose a `RoutingPolicy` SPI for rule-based routing (user, group, query type)?
3. **Extension point** — Any concerns about conditionally binding `ElasticResourceAllocator` vs `BasicResourceAllocator` in `DACDaemonModule`?

---

**Verified in production on multi-node k3s cluster (July 2026).**
