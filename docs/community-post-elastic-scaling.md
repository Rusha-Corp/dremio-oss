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

Elastic executor auto-scaling enables a Dremio coordinator to dynamically provision Kubernetes executor pods when queries require more compute. The coordinator signals desired replica counts via Kubernetes annotations; **KEDA is the sole authority** on StatefulSet `spec.replicas`. This eliminates the race condition where coordinator and KEDA fight over the replica count.

**Implemented and verified in production (May 2026).** Two tiers: SMALL (interactive) and LARGE (analytics/ETL).

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
| Scale-down | Control plane | KEDA + metrics-exporter (30min grace window) |
| Admin surface | Full UI + REST API | `dremio.conf` only |

---

## Design: KEDA is the Sole Authority

### Three-Layer Architecture

```
Layer 1 — Coordinator (annotates intent)
  ElasticResourceAllocator: determine tier → calculate scaleDelta
  K8sPlatform.scaleExecutors(delta, tier): writes annotations ONLY
    - dremio.io/scale-requested-at = <epoch-ms>
    - dremio.io/scale-requested-count = <newReplicas>
  NEVER writes spec.replicas — only signals via annotations.

Layer 2 — Exporter + KEDA (acts on intent)
  Exporter polls: annotations + ZK endpoints + job history
  Computes executor_desired_small/large from 3 signals (30min grace each)
  KEDA reads metrics → sets spec.replicas on each poll cycle (every 10s)

Layer 3 — Cluster Autoscaler (node-level)
  Transparent provisioning when K8s can't schedule pods
```

### Why This Design

1. **Avoids race condition**: Coordinator writes annotations; KEDA reads metrics and sets spec.replicas. No dual-write conflict.

2. **Synchronous contract**: The `allocate()` contract is synchronous — it must not return until resources are assigned or denied. Writing annotations and polling ZK (via `waitForExecutors()`) starts pod creation immediately while honoring this contract.

3. **Clean failure handling**: If executors don't become available within timeout (5 min), throws `ResourceUnavailableException`. Query is cancelled cleanly, not degraded to wrong tier.

---

## How it works

### 1. Tier Detection

**Primary signal:** `routingQueue` (from session/ALTER SESSION). If queue name contains "large" (case-insensitive), queries route to LARGE tier regardless of plan cost.

**Fallback:** Plan cost threshold (10M → SMALL, >30M → LARGE). Note: Dremio's planner often reports `planCost = 1.0`, so `routingQueue` is the reliable signal.

```java
public ExecutorTier getTier(double planCost, String routingQueue) {
    if (routingQueue != null && routingQueue.toLowerCase().contains("large")) {
        return ExecutorTier.LARGE;
    }
    return planCost <= smallQueryThreshold ? ExecutorTier.SMALL : ExecutorTier.LARGE;
}
```

### 2. Scale-Up Flow

```
Query arrives with routingQueue="query.large"
    │
    ▼
ElasticAdmissionCalculator.getTier(planCost, routingQueue)
    │
    ├─► routingQueue contains "large" → tier = LARGE
    └─► planCost = 1.0 (ignored)
            │
            ▼
    ElasticResourceAllocator.allocate()
            │
            ├─► requiredExecutors = 3
            ├─► getAvailableExecutors(LARGE) = 0 (from ZooKeeper nodeTag filter)
            └─► scaleDelta = 3 - 0 = 3
                    │
                    ▼
            K8sPlatform.scaleExecutors(3, LARGE)
                    │
                    ├─► Write: dremio.io/scale-requested-at = <now>
                    ├─► Write: dremio.io/scale-requested-count = 3
                    └─► Return (NO spec.replicas write!)
                    │
                    ▼
            waitForExecutors(3, LARGE, 5min)
                    │
                    └─► Poll ZooKeeper endpoints filtered by nodeTag=large
                       until count >= 3 or timeout
                    │
                    ▼
            Query executes on LARGE executors
```

### 3. Scale-Down Flow (30-minute grace window)

Three independent signals, each with 1800s grace:

| Signal | Source | Behavior |
|--------|--------|----------|
| Job history | `/apiv2/jobs` | endTime within 1800s → reset timer |
| Ready-pod window | K8s StatefulSet | readyReplicas > 0 within 1800s → reset timer |
| Annotation | K8s StatefulSet | scale-requested-at within 1800s → reset timer |

When **all** signals expire: exporter returns `desired = 0`, KEDA sets `spec.replicas = 0`, HPA stabilization window (1800s) provides final buffer.

---

## Component Details

### ElasticAdmissionCalculator

- `calculateRequiredExecutors(planCost)`: 1 (≤10M), 2 (10M-30M), 3 (>30M)
- `getTier(planCost, routingQueue)`: routingQueue primary, cost fallback

### ElasticResourceAllocator

- `allocate()`: determines tier, checks ZK-available executors, writes annotations, waits for ZK registration, delegates to BasicResourceAllocator
- On timeout: throws `ResourceUnavailableException` (query cancelled cleanly)

### K8sPlatform

- **Never writes spec.replicas** — only annotations
- **Never reads K8s readyReplicas** — reads ZooKeeper endpoints filtered by nodeTag
- Avoids K8s/ZK registration race where pod is K8s-ready but not yet ZK-registered

### ResourcePlatform interface

```java
public interface ResourcePlatform extends AutoCloseable {
    int getAvailableExecutors();
    int getAvailableExecutors(ExecutorTier tier);  // ZK nodeTag filter
    boolean waitForExecutors(int required, long timeout, TimeUnit unit);
    boolean waitForExecutors(int required, ExecutorTier tier, long timeout, TimeUnit unit);
    boolean scaleExecutors(int delta);
    boolean scaleExecutors(int delta, ExecutorTier tier);  // annotation-only
}
```

### ExecutorSelectionServiceImpl

- Routes queries to executors matching `nodeTag` ("small" or "large")
- If no tagged executors found: **falls back** to all available endpoints with a warning
- Does NOT throw RuntimeException

### Metrics Exporter

Python Flask service that:
1. Polls `/apiv2/jobs` every 15s for recent jobs
2. Reads StatefulSet annotations (`scale-requested-at`, `scale-requested-count`)
3. Tracks `readyReplicas` timestamps per tier
4. Computes `executor_desired_small` and `executor_desired_large`

Three-signal grace logic (1800s each):
- Signal 1: job history (endTime within grace)
- Signal 2: ready-pod window (readyReplicas > 0 within grace)
- Signal 3: annotation (scale-requested-at within grace) — also carries desired count

---

## Configuration

```hocon
services.executor.elastic {
  enabled: true
  min_executors: 0
  max_executors: 2        // SMALL tier
  max_executors_large: 8  // LARGE tier
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

Exporter env vars:
- `SCALE_DOWN_GRACE_SECS`: 1800 (matches KEDA cooldown)
- `DREMIO_URL`: coordinator service
- `DREMIO_USERNAME` / `DREMIO_PASSWORD`: for REST API

---

## Verified Behavior (May 2026)

### Scale-Up Timeline
1. Query submitted with LARGE queue
2. Coordinator writes annotations: `scale-requested-at`, `scale-requested-count=3`
3. Exporter picks up annotation → `desired_large = 3`
4. KEDA polls every 10s → sets `spec.replicas = 3`
5. Pods start, register with ZooKeeper with `nodeTag=large`
6. `waitForExecutors()` returns true → query runs
7. Query completes successfully (state: COMPLETED)

### Scale-Down Timeline
1. Query completes, no activity for 30+ minutes
2. Signal 3 (annotation) expires first
3. Signal 2 (ready-pod window) takes over for ~30 min
4. Both expire → exporter logs "scaling to 0"
5. KEDA cooldown (30 min) + HPA stabilization (30 min)
6. Pods terminate cleanly via preStop hook

**Total cold-start-to-zero**: ~45-60 minutes depending on signal timing

---

## Image Registry

| Service | Image | Tag |
|---------|-------|-----|
| Coordinator/Executor | `ghcr.io/rusha-corp/dremio-oss` | `2026.05.7` |
| Metrics Exporter | `ghcr.io/rusha-corp/dremio-keda-exporter` | `2026.05.6` |

---

## Files Changed

```
services/resourcescheduler/src/main/java/com/dremio/resource/elastic/
  ElasticAdmissionCalculator.java
  ElasticResourceAllocator.java
  ResourcePlatform.java (interface, AutoCloseable)
  K8sPlatform.java (annotation-only, ZK-based wait)
  NoOpResourcePlatform.java
  ResourcePlatformProvider.java

services/execselector/src/main/java/com/dremio/service/execselector/
  ExecutorSelectionServiceImpl.java (fallback, no RuntimeException)

docs/elastic-scaling-deployment.md
k8s/10-keda-scaledobject.yaml
k8s/04-coordinator.yaml
k8s/11a-executor-small-stub.yaml
k8s/11b-executor-large-stub.yaml
```

---

## Questions for the Dremio team

1. **OSS appetite** — Is there interest in elastic scaling in OSS, or is this planned as an Enterprise-only feature?
2. **Tier routing** — Should we expose a `RoutingPolicy` SPI for rule-based routing (user, group, query type)?
3. **Extension point** — Any concerns about conditionally binding `ElasticResourceAllocator` vs `BasicResourceAllocator` in `DACDaemonModule`?

---

**Verified in production on 3-node k3s cluster (May 2026).**
