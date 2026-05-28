# Elastic Executor Scaling — Implementation Summary

## Overview

This implementation adds elastic (auto-scaling) executor support to Dremio, allowing the coordinator to dynamically scale executor pods based on query workload. When a query arrives that requires more executor capacity than currently available, the system publishes a demand metric, KEDA scales the executor Deployment up, the allocator waits for new executors to register with the coordinator, and then proceeds with query allocation. When capacity exceeds demand and queries complete, the demand metric drops and KEDA scales the Deployment down after a cooldown period.

**Node provisioning is handled by the Kubernetes Cluster Autoscaler.** This implementation only signals demand via metrics — KEDA manages pod replica counts.

The feature is **disabled by default** and does not alter existing Dremio behavior when turned off.

---

## Architecture

```
                    ┌─────────────────────────────────────────┐
                    │           DACDaemonModule                │
                    │  (reads elastic.enabled from config)     │
                    │                                         │
                    │  if elastic.enabled:                     │
                    │    bind ElasticResourceAllocator         │
                    │  else:                                   │
                    │    bind BasicResourceAllocator            │
                    └──────────────┬──────────────────────────┘
                                   │
                    ┌──────────────▼──────────────────────────┐
                    │      ElasticResourceAllocator            │
                    │  (extends BasicResourceAllocator)        │
                    │                                         │
                    │  1. Get query cost from plan              │
                    │  2. Calculate required executors          │
                    │  3. Register demand in active-jobs map   │
                    │  4. Wait for executors to register        │
                    │  5. Delegate to BasicResourceAllocator     │
                    │  6. On ResourceSet.close() → remove demand│
                    └──────────────┬──────────────────────────┘
                                   │
                    ┌──────────────▼──────────────────────────┐
                    │    ElasticAdmissionCalculator            │
                    │                                         │
                    │  Cost-based tier assignment:              │
                    │    <= 10M  → 1 executor  (small)         │
                    │    <= 30M  → 2 executors (medium)        │
                    │     > 30M  → 3 executors (large)         │
                    │                                         │
                    │  Also calculates:                        │
                    │  - Scale delta (required - current)      │
                    └─────────────────────────────────────────┘
                                   │
                    ┌──────────────▼──────────────────────────┐
                    │       ResourcePlatformProvider           │
                    │  (creates K8sPlatform from config, cached) │
                    │                                         │
                    │  Config: elastic.enabled = true          │
                    │    → K8sPlatform (read-only)             │
                    │    (disabled)  → NoOpResourcePlatform     │
                    └──────────────┬──────────────────────────┘
                                   │
              ┌────────────────────┐
              ▼                    ▼
        ┌──────────┐        ┌─────────┐
        │K8sPlatform│        │  NoOp    │
        │(read-only)│        │(disabled)│
        │pod counts │        │          │
        │+ ZK state │        └─────────┘
        └─────┬──────┘
              │
        ┌─────▼──────────┐
        │  Micrometer     │
        │  Gauges         │
        │  desired.small  │──────▶ metrics-exporter ──▶ KEDA ScaledObject
        │  desired.large  │──────▶ metrics-exporter ──▶ KEDA ScaledObject
        └────────────────┘
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

**Cost Thresholds (configurable):**

| Query Size | Cost Range | Executors |
|-----------|-----------|----------|
| Small | cost <= 10,000,000 | 1 |
| Medium | 10M < cost <= 30,000,000 | 2 |
| Large | cost > 30,000,000 | 3 |

### 2. ElasticResourceAllocator

**Package:** `com.dremio.resource.elastic`
**Extends:** `BasicResourceAllocator`

The main entry point for elastic scaling. Overrides the `allocate()` method to add scaling logic before delegating to the base allocator.

**Flow:**
1. Get the query cost from `ResourceSchedulingProperties`
2. Calculate required executors via `ElasticAdmissionCalculator`
3. Register demand in per-tier active-jobs map (feeds Micrometer gauges)
4. Wait for executors to become ready (KEDA scales Deployment, timeout from config)
5. Delegate to `BasicResourceAllocator.allocate()` for standard allocation
6. When the query completes and `ResourceSet.close()` is called, remove the demand entry so the gauge drops and KEDA can scale down

**Demand tracking:** Uses `ConcurrentHashMap<Long, Integer>` per tier. Each query's demand is keyed by a unique job ID. The Micrometer gauge computes `max(values())` — this correctly tracks peak demand across concurrent queries and never under-counts.

**Fail-fast behavior:** If elastic is enabled but the platform returns a `NoOpResourcePlatform`, an `IllegalStateException` is thrown immediately.

### 3. ResourcePlatform (Interface)

**Package:** `com.dremio.resource.elastic`

Interface for Kubernetes resource management for elastic executor scaling.

| Method | Returns | Description |
|--------|---------|-------------|
| `getReadyPodCount()` | `int` | Number of ready executor pods |
| `getReadyPodCount(tier)` | `int` | Number of ready executor pods for a specific tier |
| `getAvailableExecutors()` | `int` | Number of executors registered with Dremio coordinator |
| `getAvailableExecutors(tier)` | `int` | Number of executors registered for a specific tier |
| `waitForExecutors(count, timeout, unit)` | `boolean` | Blocks until executors are ready or timeout |
| `waitForExecutors(count, tier, timeout, unit)` | `boolean` | Blocks until tier-specific executors are ready |
| `scaleExecutors(scaleDelta)` | `boolean` | No-op (KEDA handles scaling); returns `false` |
| `scaleExecutors(scaleDelta, tier)` | `boolean` | No-op (KEDA handles scaling); returns `false` |

### 4. K8sPlatform (Read-Only)

**Package:** `com.dremio.resource.elastic`
**Dependency:** `io.fabric8:kubernetes-client:6.0.0`
**Implements:** `ResourcePlatform`, `Closeable`

Read-only Kubernetes implementation using the Fabric8 K8s client. Scaling is handled entirely by KEDA ScaledObjects. This class only observes executor state.

**Observation methods:**
- `getReadyPodCount(tier)` — lists pods matching tier labels, filters by `phase=Running` and all containers ready
- `getAvailableExecutors(tier)` — reads from ZooKeeper `ListenableSet`, filtering by `node_tag`
- `waitForExecutors(count, tier, timeout, unit)` — polls ready pod count and ZK registration until both meet the requirement or timeout

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
  enabled: false

  # Scaling bounds
  min_executors: 0
  max_executors: 10
  scale_timeout_minutes: 5

  # Kubernetes-specific
  kubernetes {
    namespace: "dremio"
    pod_template: "dremio-executor"
    image: "dremio/dremio:latest"
    zookeeper_address: "localhost:2181"
  }

  # Admission thresholds
  small_query_threshold: 10000000
  medium_query_threshold: 30000000
}
```

---

## DremioConfig Constants

Added to `com.dremio.config.DremioConfig`:

| Constant | Config Key |
|----------|-----------|
| `ELASTIC_ENABLED` | `services.executor.elastic.enabled` |
| `ELASTIC_MIN_EXECUTORS` | `services.executor.elastic.min_executors` |
| `ELASTIC_MAX_EXECUTORS` | `services.executor.elastic.max_executors` |
| `ELASTIC_SCALE_TIMEOUT` | `services.executor.elastic.scale_timeout_minutes` |
| `ELASTIC_K8S_NAMESPACE` | `services.executor.elastic.kubernetes.namespace` |
| `ELASTIC_K8S_POD_TEMPLATE` | `services.executor.elastic.kubernetes.pod_template` |
| `ELASTIC_K8S_IMAGE` | `services.executor.elastic.kubernetes.image` |
| `ELASTIC_K8S_ZOOKEEPER_ADDRESS` | `services.executor.elastic.kubernetes.zookeeper_address` |
| `ELASTIC_SMALL_QUERY_THRESHOLD` | `services.executor.elastic.small_query_threshold` |
| `ELASTIC_MEDIUM_QUERY_THRESHOLD` | `services.executor.elastic.medium_query_threshold` |

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
| `00-namespace.yaml` | Creates `dremio` namespace |
| `01-rbac.yaml` | ServiceAccount `dremio-elastic` + Role (pod/configmap CRUD, node list) + RoleBinding |
| `02-service.yaml` | Headless service for coordinator pod DNS resolution |
| `03-configmap.yaml` | Dremio coordinator config with `executor.elastic.enabled: true` (no hardcoded secrets) |
| `04-coordinator.yaml` | Coordinator pod (GHCR image, 4GB heap, configmap volume) |
| `08-liveness-service.yaml` | Liveness service for coordinator metrics endpoint |
| `09-metrics-exporter-deployment.yaml` | Sidecar that exposes Micrometer gauges for KEDA |
| `10-keda-scaledobject.yaml` | KEDA ScaledObjects for small/large tiers (pollingInterval: 10s) |
| `11a-executor-small-stub.yaml` | Stub Deployment for small-tier executors (replicas: 0) |
| `11b-executor-large-stub.yaml` | Stub Deployment for large-tier executors (replicas: 0) |
| `11c-executor-configmaps.yaml` | ConfigMaps for both tiers (env-var credential provider, no hardcoded secrets) |
| `deploy.sh` | Deployment script (creates GHCR image pull secret, applies all manifests) |

### Images (GHCR)

- **Coordinator:** `ghcr.io/rusha-corp/dremio-oss:coordinator-26.0.5-elastic`
- **Executor:** `ghcr.io/rusha-corp/dremio-oss:executor-26.0.5-elastic-v3`

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
| ElasticResourceAllocator | Complete (metric-driven, ConcurrentHashMap demand tracking) |
| ResourcePlatform interface | Complete |
| K8sPlatform | Complete (read-only, Closeable) |
| NoOpResourcePlatform | Complete |
| ResourcePlatformProvider | Complete (cached, Closeable) |
| DremioConfig constants | Complete |
| dremio-reference.conf | Complete |
| DACDaemonModule wiring | Complete |
| Unit tests | Complete |
| K8s integration test | Complete |
| K8s manifests | Complete (KEDA ScaledObjects, stub Deployments, static ConfigMaps) |
| KEDA ScaledObjects | Complete (pollingInterval: 10s, per-tier) |
| Metrics exporter | Complete (sidecar exposing Micrometer gauges) |

---

## Backward Compatibility

- **Default off:** `services.executor.elastic.enabled` defaults to `false`
- When disabled, `BasicResourceAllocator` is used exactly as before
- No changes to existing query planning or execution paths
- The feature is additive — no existing classes are modified in their default behavior
- Scaling is metric-driven (KEDA) — no imperative K8s API writes from Dremio
