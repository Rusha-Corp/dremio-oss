# Elastic Executor Scaling — Implementation Summary

## Overview

This implementation adds elastic (auto-scaling) executor support to Dremio, allowing the coordinator to dynamically provision and de-provision executor pods based on query workload. When a query arrives that requires more executor capacity than currently available, the system automatically scales up by creating new executor pods, waits for them to register with the coordinator, and then proceeds with query allocation. When capacity exceeds demand, executors can be scaled down.

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
                    │  3. Check current available executors     │
                    │  4. Scale up if needed via ResourcePlatform│
                    │  5. Wait for executors to register        │
                    │  6. Delegate to BasicResourceAllocator     │
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
                    │  - Required nodes (memory-based packing)  │
                    └─────────────────────────────────────────┘
                                   │
                    ┌──────────────▼──────────────────────────┐
                    │       ResourcePlatformProvider           │
                    │  (creates platform from config)          │
                    │                                         │
                    │  Config: elastic.platform =              │
                    │    "kubernetes" → K8sPlatform            │
                    │    "aws-ec2"    → AwsEc2Platform         │
                    │    "gcp-compute"→ GcpComputePlatform     │
                    │    "azure-vm"   → AzureVmPlatform        │
                    │    (disabled)   → NoOpResourcePlatform    │
                    └──────────────┬──────────────────────────┘
                                   │
              ┌────────────────────┬────────┬────────┬─────────┐
              ▼                    ▼        ▼        ▼         ▼
        ┌──────────┐  ┌────────┐ ┌──────┐ ┌─────┐ ┌─────────┐
        │K8sPlatform│  │AWS EC2 │ │GCP   │ │Azure│ │  NoOp    │
        │(fabric8)  │  │        │ │Compute│ │ VM  │ │(disabled)│
        │  FULL     │  │ STUB   │ │ STUB  │ │STUB │ │          │
        └──────────┘  └────────┘ └──────┘ └─────┘ └─────────┘
```

---

## Component Details

### 1. ElasticAdmissionCalculator

**Package:** `com.dremio.resource.elastic`

Platform-agnostic admission logic that determines how many executors a query needs based on its estimated cost.

| Method | Description |
|--------|-------------|
| `calculateRequiredExecutors(planCost, avgExecutorMemoryBytes)` | Returns 1, 2, or 3 executors based on cost thresholds |
| `calculateScaleDelta(requiredExecutors, currentExecutors)` | Returns how many executors to add (0 if sufficient) |
| `calculateRequiredNodes(executors, nodeMemoryBytes, executorMemoryBytes)` | Calculates node count based on memory packing |

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
1. Check if elastic scaling is enabled in config
2. Get the query cost from `ResourceSchedulingProperties`
3. Calculate required executors via `ElasticAdmissionCalculator`
4. Get current available executors from the `ResourcePlatform`
5. If more executors are needed, call `scaleExecutors()` on the platform
6. Wait up to 5 minutes for executors to become ready
7. Delegate to `BasicResourceAllocator.allocate()` for standard allocation

**Fail-fast behavior:** If elastic is enabled but the platform returns a `NoOpResourcePlatform`, an `IllegalStateException` is thrown immediately.

### 3. ResourcePlatform (Interface)

**Package:** `com.dremio.resource.elastic`

Platform-agnostic abstraction for resource management operations.

| Method | Returns | Description |
|--------|---------|-------------|
| `getReadyNodeCount()` | `int` | Number of ready worker nodes |
| `getReadyPodCount()` | `int` | Number of ready executor pods |
| `getAvailableExecutors()` | `int` | Number of executors registered with Dremio coordinator |
| `waitForExecutors(count, timeout, unit)` | `boolean` | Blocks until executors are ready or timeout |
| `scaleExecutors(scaleDelta)` | `boolean` | Scale up (positive) or down (negative) |

### 4. K8sPlatform (Production-Ready)

**Package:** `com.dremio.resource.elastic`  
**Dependency:** `io.fabric8:kubernetes-client:6.0.0`

Full Kubernetes implementation using the Fabric8 K8s client.

**Scale Up:**
1. Creates a ConfigMap with executor configuration (ZK address, coordinator address, disabled coordinator mode)
2. Creates N executor Pods with:
   - Image from `services.executor.elastic.kubernetes.image`
   - ConfigMap volume mount at `/opt/dremio/conf`
   - `hostAlias` mapping `dremio-coordinator-0` to the headless service DNS
   - Image pull secret for GHCR authentication
   - Labels: `app=dremio`, `role=executor`

**Scale Down:**
1. Lists executor pods matching the label selector
2. Deletes pods up to the scale-down count

**Health Checks:**
- `isReadyNode()` — checks `Ready=True` condition on K8s nodes
- `isReadyPod()` — checks `phase=Running` and all containers `ready=true`

**Key Design Decisions:**
- Each scale-up creates a fresh ConfigMap with the current ZooKeeper address, ensuring executors always connect to the right coordinator
- The `hostAlias` workaround resolves a DNS issue where Dremio uses the pod hostname instead of the service FQDN for internal communication
- Pods are labeled with both `role=executor` and the configurable `executorLabel` for discovery

### 5. Cloud Platform Stubs

**AwsEc2Platform, GcpComputePlatform, AzureVmPlatform**

These are structural implementations with TODO placeholders for cloud SDK integration. They implement the `ResourcePlatform` interface and wire up configuration, but the actual provisioning calls (`launchInstances`, `createVM`, etc.) are logged as warnings and not yet implemented.

### 6. NoOpResourcePlatform

Singleton no-op implementation used when elastic scaling is disabled. Returns 0 for all counts and `false` for wait/scale operations.

### 7. ResourcePlatformProvider

**Package:** `com.dremio.resource.elastic`  
**Implements:** `javax.inject.Provider<ResourcePlatform>`

Factory that reads `services.executor.elastic.platform` from config and instantiates the correct platform. Performs validation:
- Throws if elastic is enabled but platform type is not set
- Throws if the platform type is unknown
- Throws if required platform-specific config is missing (e.g., K8s namespace)
- For Kubernetes: validates connectivity by listing namespaces before returning

---

## Configuration

All configuration lives under `services.executor.elastic` in `dremio.conf` (or `dremio-reference.conf`):

```hocon
services.executor.elastic {
  # Master switch
  enabled: false

  # Platform: kubernetes | aws-ec2 | gcp-compute | azure-vm
  platform: "kubernetes"

  # Scaling bounds
  min_executors: 0
  max_executors: 10
  scale_timeout_minutes: 5

  # Memory sizing
  executor_memory_gb: 8
  node_memory_gb: 32

  # Kubernetes-specific
  kubernetes {
    namespace: "dremio"
    pod_template: "dremio-executor"
    image: "ghcr.io/rusha-corp/dremio-oss:executor-26.0.5-elastic-v3"
    zookeeper_address: "localhost:2181"
  }

  # AWS EC2-specific
  aws-ec2 {
    instance_type: "m5.large"
    ami_id: ""
    region: "eu-west-2"
    iam_instance_profile: ""
    key_name: ""
    security_group_ids: []
    subnet_id: ""
  }

  # GCP Compute-specific
  gcp-compute {
    instance_type: "n2-standard-4"
    zone: "europe-west1-b"
    project_id: ""
    service_account: ""
    network: "default"
  }

  # Azure VM-specific
  azure-vm {
    instance_type: "Standard_D4s_v3"
    location: "westeurope"
    subscription_id: ""
    managed_identity: ""
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
| `ELASTIC_PLATFORM` | `services.executor.elastic.platform` |
| `ELASTIC_MIN_EXECUTORS` | `services.executor.elastic.min_executors` |
| `ELASTIC_MAX_EXECUTORS` | `services.executor.elastic.max_executors` |
| `ELASTIC_SCALE_TIMEOUT` | `services.executor.elastic.scale_timeout_minutes` |
| `ELASTIC_EXECUTOR_MEMORY_GB` | `services.executor.elastic.executor_memory_gb` |
| `ELASTIC_NODE_MEMORY_GB` | `services.executor.elastic.node_memory_gb` |
| `ELASTIC_K8S_NAMESPACE` | `services.executor.elastic.kubernetes.namespace` |
| `ELASTIC_K8S_POD_TEMPLATE` | `services.executor.elastic.kubernetes.pod_template` |
| `ELASTIC_K8S_IMAGE` | `services.executor.elastic.kubernetes.image` |
| `ELASTIC_K8S_ZOOKEEPER_ADDRESS` | `services.executor.elastic.kubernetes.zookeeper_address` |
| `ELASTIC_AWS_EC2_INSTANCE_TYPE` | `services.executor.elastic.aws-ec2.instance_type` |
| `ELASTIC_AWS_EC2_AMI_ID` | `services.executor.elastic.aws-ec2.ami_id` |
| `ELASTIC_AWS_EC2_REGION` | `services.executor.elastic.aws-ec2.region` |
| `ELASTIC_GCP_INSTANCE_TYPE` | `services.executor.elastic.gcp-compute.instance_type` |
| `ELASTIC_GCP_ZONE` | `services.executor.elastic.gcp-compute.zone` |
| `ELASTIC_GCP_PROJECT_ID` | `services.executor.elastic.gcp-compute.project_id` |
| `ELASTIC_AZURE_VM_INSTANCE_TYPE` | `services.executor.elastic.azure-vm.instance_type` |
| `ELASTIC_AZURE_LOCATION` | `services.executor.elastic.azure-vm.location` |
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
| `01-rbac.yaml` | ServiceAccount `dremio-elastic` + ClusterRoleBinding (cluster-admin) |
| `02-service.yaml` | Headless service for coordinator pod DNS resolution |
| `03-configmap.yaml` | Dremio config with `executor.elastic.enabled: true` and K8s platform settings |
| `04-coordinator.yaml` | Coordinator pod (GHCR image, 4GB heap, configmap volume) |
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
| `testSmallExecutorMemory` | Small memory doesn't affect tier assignment |
| `testNoScaleWhenAlreadyEnough` | Delta = 0 when capacity sufficient |
| `testScaleDeltaWhenNotEnough` | Delta = required - current |
| `testNoScaleWhenExactMatch` | Delta = 0 at exact capacity |
| `testScaleFromZero` | Scaling from 0 executors |
| `testCalculateNodesForExecutors` | Memory-based node packing |
| `testSingleExecutorFitsInOneNode` | 1 executor / 1 node |
| `testExactFitNoWastedNodes` | 4 x 8GB = 32GB fits 1 node |
| `testZeroExecutorsRequiresZeroNodes` | 0 executors → 0 nodes |
| `testManyExecutorsNeedManyNodes` | 10 executors → 3 nodes |

### K8sPlatformKubeconfigIntegrationTest

Integration test that validates K8s connectivity using kubeconfig:
- Creates a `KubernetesClient` from kubeconfig
- Lists namespaces to verify connectivity
- Queries pods and nodes in the target namespace
- Constructs a `K8sPlatform` and exercises `getReadyNodeCount()` and `getReadyPodCount()`

---

## Implementation Status

| Component | Status |
|-----------|--------|
| ElasticAdmissionCalculator | Complete |
| ElasticResourceAllocator | Complete |
| ResourcePlatform interface | Complete |
| K8sPlatform | Complete (Fabric8 client, pod/ConfigMap creation, scale up/down) |
| AwsEc2Platform | Stub (structure only, SDK integration TODO) |
| GcpComputePlatform | Stub (structure only, SDK integration TODO) |
| AzureVmPlatform | Stub (structure only, SDK integration TODO) |
| NoOpResourcePlatform | Complete |
| ResourcePlatformProvider | Complete |
| DremioConfig constants | Complete |
| dremio-reference.conf | Complete |
| DACDaemonModule wiring | Complete |
| Unit tests | Complete |
| K8s integration test | Complete |
| K8s manifests | Complete (GHCR) |

---

## Backward Compatibility

- **Default off:** `services.executor.elastic.enabled` defaults to `false`
- When disabled, `BasicResourceAllocator` is used exactly as before
- No changes to existing query planning or execution paths
- The feature is additive — no existing classes are modified in their default behavior
