# Dremio Elastic Executor Scaling — Deployment Guide

This guide walks through deploying Dremio OSS with KEDA-driven elastic executor scaling on any Kubernetes platform (EKS, GKE, AKS, k3s, on-prem). Two executor tiers — **SMALL** (interactive queries) and **LARGE** (analytics/ETL) — scale from zero on demand and scale down after a configurable idle period.

---

## How It Works

```
┌──────────────────────────────────────────────┐
│           Dremio Coordinator                  │
│  ElasticResourceAllocator                     │
│  - query arrives → classify tier via           │
│    getQueueNameFromSchedulingProperties        │
│    (routingQueue + cost → QueueType)           │
│  - publishes elastic_desired_small/large      │
│    as Prometheus gauges via Micrometer         │
│    globalRegistry (in-process, no HTTP)       │
│  - no Kubernetes API interaction              │
│                                               │
│  ElasticResourceAllocator idle-reset          │
│  thread (10s poll)                            │
│  - reads jobs.active, maestro.active from     │
│    Micrometer globalRegistry (in-process)    │
│  - after 30 min idle: resets gauges to 0      │
│  - cold-start guard: suppressed while         │
│    jobSeenSinceScaleUp=false (executor        │
│    startup window ~60-90s)                   │
│  - when reset fires: desiredSmall=0,           │
│    desiredLarge=0, jobSeenSinceScaleUp=false │
│                                               │
│  Liveness endpoint: :45679/metrics           │
│  - elastic_desired_small  (AtomicInteger)    │
│  - elastic_desired_large  (AtomicInteger)    │
│  - jobs.active, maestro.active               │
└──────────────────┬──────────────────────────┘
                    │ Prometheus scrape (5s)
                    ▼
         ┌──────────────────────┐
         │  Prometheus          │
         └──────────┬───────────┘
                    │ native prometheus trigger (10s)
                    ▼
         ┌──────────────────────┐
         │   KEDA Controller    │
         └──────────┬───────────┘
                    │ sets .spec.replicas
                    ▼
         ┌──────────────────────┐
         │ Executor StatefulSets│
         │  small / large       │
         └──────────────────────┘
```

**Scale-up path:** Query arrives → coordinator computes required executors → publishes `elastic_desired_large=3` via Micrometer gauge → Prometheus scrapes it within 5s → KEDA reads it within 10s → sets `spec.replicas=3` → pods start and register → query executes.

**Scale-down path:** `jobs_active=0` and `maestro_active=0` for 30 minutes → coordinator resets `elastic_desired_*` to 0 → KEDA becomes INACTIVE → after `cooldownPeriod` (600s) → scales to zero.

**Cold-start guard:** When `publishDesired()` sets `desired > 0`, the `jobSeenSinceScaleUp` flag is set to `false`, suppressing the idle countdown until the first job is seen running (`jobs_active > 0`). This prevents the 30-minute idle-reset from firing during the executor startup window, which would cancel the scale-up before executors become ready.

Each KEDA ScaledObject has two `prometheus` triggers — KEDA takes the maximum:
- **Primary**: `elastic_desired_small/large` — drives the desired replica count
- **Guard**: `jobs_active + maestro_active + reflections_active + reflections_refreshing` — keeps executors alive while work is running

---

## Prerequisites

| Requirement | Notes |
|---|---|
| Kubernetes 1.24+ | EKS, GKE, AKS, k3s, or any conformant cluster |
| KEDA v2.x | [Installation](https://keda.sh/docs/latest/deploy/) |
| Prometheus | Must be able to scrape coordinator `:45679/metrics` (5s interval) and be reachable from KEDA |
| Container registry | GHCR, ECR, GCR, or any OCI-compatible registry |
| Persistent storage | Storage class supporting `ReadWriteOnce`; `ReadWriteMany` for the shared dist PVC |

---

## Images

| Image | Registry | Notes |
|---|---|---|
| Dremio OSS (coordinator + executor) | `ghcr.io/rusha-corp/dremio-oss:2026.07.5` | Same binary; role set by ConfigMap |

---

## Step 1 — Namespace and RBAC

```bash
kubectl apply -f k8s/00-namespace.yaml
kubectl apply -f k8s/01-rbac.yaml
```

The RBAC creates a `dremio-elastic` service account. Dremio does not interact with the Kubernetes API directly — KEDA handles StatefulSet scaling. The service account is used for pod identity and any read-only cluster introspection.

---

## Step 2 — Storage

All PVCs default to `storageClassName: longhorn`. Replace with your platform's storage class before applying:

```bash
# Find available storage classes
kubectl get storageclass

# Replace longhorn with your class (e.g., gp3, standard, azurefile)
sed -i 's/storageClassName: longhorn/storageClassName: gp3/g' k8s/*.yaml
```

| Platform | Recommended Class | RWX Support |
|---|---|---|
| EKS (AWS) | `gp3` (EBS) for RWO; `efs-sc` for RWX | EFS for the dist PVC |
| GKE (GCP) | `standard-rwo` or `premium-rwo` | `filestore-sc` for RWX |
| AKS (Azure) | `managed-premium` for RWO; `azurefile` for RWX | Azure Files for the dist PVC |
| k3s + Longhorn | `longhorn` | Longhorn supports RWX |
| On-prem | `local-path` or NFS provisioner | NFS for RWX |

> **RWX requirement:** `00c-dist-pvc.yaml` (shared Dremio distribution) uses `ReadWriteMany`. If your storage class only supports `ReadWriteOnce`, use an object store path (`paths.dist = "s3a://bucket/dremio-dist"`) in the coordinator ConfigMap and remove the dist PVC entirely.

```bash
kubectl apply -f k8s/00-namespace.yaml
kubectl apply -f k8s/00b-coordinator-pvc.yaml
kubectl apply -f k8s/00c-dist-pvc.yaml
```

---

## Step 3 — Registry Secret

```bash
kubectl create secret docker-registry ghcr-secret \
  --docker-server=ghcr.io \
  --docker-username=<your-github-username> \
  --docker-password=<your-ghcr-token> \
  --namespace=dremio
```

If pulling from ECR, GCR, or a private registry, adjust `--docker-server` and credentials accordingly.

---

## Step 4 — Coordinator Configuration

Edit `k8s/03-configmap.yaml` — the key elastic scaling section:

```hocon
services.executor.elastic {
  enabled: true
  scale_timeout_minutes: 20      # max wait for executors to register

  max_executors_small: 4         # small-tier cap
  max_executors_large: 8         # large-tier cap
}
```

Tier routing — queries are classified LARGE when the routing queue name contains `"large"` (case-insensitive), otherwise by cost threshold:

```hocon
services.executor.elastic.small_query_threshold: 10000000   # plan cost below this → 1 executor
services.executor.elastic.medium_query_threshold: 30000000  # plan cost above this → 3 executors
```

Apply:

```bash
kubectl apply -f k8s/03-configmap.yaml
```

---

## Step 5 — Coordinator Deployment

```bash
kubectl apply -f k8s/02-service.yaml
kubectl apply -f k8s/04-coordinator.yaml

# Wait for coordinator to be ready (allow up to 5 min for JVM startup)
kubectl wait --for=condition=Ready pod -l role=coordinator -n dremio --timeout=300s
```

Verify there are no startup errors:

```bash
kubectl logs -n dremio -l role=coordinator | grep -E "ERROR|elastic"
```

---

## Step 6 — Executor ConfigMaps and StatefulSets

Each executor tier has its own ConfigMap (`dremio.conf`) and StatefulSet.

**Key settings to review before applying:**

| File | What to change for your platform |
|---|---|
| `10-configmap-executor-small.yaml` | `paths.dist`, S3/GCS credentials, `node-tag: small` |
| `11-configmap-executor-large.yaml` | Same as above but `node-tag: large`, larger heap |
| `12-executor-small.yaml` | `storageClassName`, resource limits, `nodeSelector` |
| `13-executor-large.yaml` | Same but larger resources |

```bash
kubectl apply -f k8s/08-service-executor.yaml
kubectl apply -f k8s/10-configmap-executor-small.yaml
kubectl apply -f k8s/11-configmap-executor-large.yaml
kubectl apply -f k8s/12-executor-small.yaml
kubectl apply -f k8s/13-executor-large.yaml
```

Both StatefulSets start with `replicas: 0` — KEDA controls the replica count.

---

## Step 7 — Prometheus Scrape Job

KEDA's native `prometheus` trigger requires that Prometheus is actively scraping the coordinator's liveness endpoint before scaling can work.

Add a scrape job to your Prometheus configuration:

```yaml
- job_name: dremio-coordinator
  scrape_interval: 5s
  static_configs:
    - targets:
        - dremio-coordinator-liveness.<namespace>:<port>
```

Replace `<namespace>` with the namespace where the coordinator runs (e.g., `dremio`) and `<port>` with the liveness port (default `45679`). The liveness service DNS follows the pattern `<service-name>.<namespace>.svc.cluster.local`.

For example, if coordinator is in the `dremio` namespace and the liveness service is `dremio-coordinator-liveness`:
```yaml
- job_name: dremio-coordinator
  scrape_interval: 5s
  static_configs:
    - targets:
        - dremio-coordinator-liveness.dremio:45679
```

If Prometheus is managed by a ConfigMap (e.g., ArgoCD or Helm), add this job to the `scrape_configs` section and trigger a reload:

```bash
# If using prometheus-operator, add a ServiceMonitor instead
# If using a ConfigMap, patch it and send SIGHUP or reload via /-/reload
kubectl rollout restart deployment/prometheus -n <prometheus-namespace>
```

Verify Prometheus is collecting the metric:
```bash
kubectl exec -n <prometheus-namespace> deploy/prometheus -- \
  wget -qO- "http://localhost:9090/api/v1/query?query=elastic_desired_small" \
  | python3 -c "import json,sys; r=json.load(sys.stdin); print(r['data']['result'])"
```

> **Note:** `elastic_desired_small` and `elastic_desired_large` are only published after the first scale-up event (lazy initialization). Submit a test query first, then check.

---

## Step 8 — KEDA ScaledObjects

```bash
kubectl apply -f k8s/06-keda-small.yaml
kubectl apply -f k8s/07-keda-large.yaml
```

Each ScaledObject uses two `prometheus` triggers. Key parameters:

| Parameter | Value | Notes |
|---|---|---|
| `pollingInterval` | `10s` | How often KEDA reads the metric |
| `cooldownPeriod` | `600s` | Hold period after last INACTIVE signal before scaling to 0 |
| `minReplicaCount` | `0` | Full scale-to-zero when idle |
| `maxReplicaCount` | `4` (small) / `8` (large) | Hard cap — must match `dremio.conf` |

The `prometheus` serverAddress must match your Prometheus service DNS:
```yaml
triggers:
  # Primary: coordinator's desired count drives replica count
  - type: prometheus
    metadata:
      serverAddress: http://prometheus.<namespace>.svc.cluster.local:9090
      metricName: elastic_desired_small
      query: "elastic_desired_small"
      threshold: "1"
      activationThreshold: "0"
  # Guard: keep at least 1 while any work is active
  - type: prometheus
    metadata:
      serverAddress: http://prometheus.<namespace>.svc.cluster.local:9090
      metricName: dremio_work_active_small
      query: "clamp_max(jobs_active + maestro_active + reflections_active + reflections_refreshing, 1)"
      threshold: "1"
      activationThreshold: "0"
```

Update the `serverAddress` in `06-keda-small.yaml` and `07-keda-large.yaml` before applying.

---

## Step 9 — Ingress (Optional)

```bash
kubectl apply -f k8s/05-service-ingress.yaml   # ClusterIP for coordinator UI
kubectl apply -f k8s/06-ingress.yaml            # HTTP ingress
kubectl apply -f k8s/05-ingress-tcp-flight.yaml # TCP ingress for Flight SQL (port 32010)
```

Adjust `spec.ingressClassName` and annotations for your ingress controller (nginx, ALB, Traefik, etc.).

---

## Verification

```bash
# All pods running
kubectl get pods -n dremio

# KEDA ScaledObjects are active
kubectl get scaledobject -n dremio

# Coordinator is publishing metrics (after first scale event)
kubectl run curl-test --image=curlimages/curl --rm -it --restart=Never -n dremio -- \
  curl -s http://dremio-coordinator-liveness.dremio.svc.cluster.local:45679/metrics \
  | grep elastic_desired

# Prometheus has the metric
kubectl exec -n <prometheus-namespace> deploy/prometheus -- \
  wget -qO- "http://localhost:9090/api/v1/query?query=elastic_desired_small"
```

Expected coordinator log when a query triggers scale-up:
```
INFO Elastic scaling: query cost 1.0 classified as LARGE (large), desired 3 executors
```

Expected coordinator log when idle-reset fires after query completes:
```
INFO Idle reset: all activity metrics=0 for 1800s — resetting elastic_desired_small=2 elastic_desired_large=0 to 0
```

---

## Platform-Specific Notes

### AWS EKS

- Use `gp3` (EBS) for executor data PVCs and the coordinator PVC.
- Use **Amazon EFS** (with the EFS CSI driver) for the `ReadWriteMany` distribution PVC, or configure `paths.dist = "s3a://bucket/dremio-dist"` and remove `00c-dist-pvc.yaml`.
- The coordinator pod needs IAM permissions to call the Kubernetes API. Either attach an IAM role via IRSA or use a standard `ServiceAccount` with `kubectl` RBAC only.
- For ECR images, replace `imagePullSecrets` with an ECR pull-through cache or IAM-based credential helper.

### GKE

- Use `premium-rwo` for data PVCs and Filestore for the dist PVC.
- Enable Workload Identity on the coordinator service account if accessing GCS.

### AKS

- Use `managed-premium` for data PVCs and Azure Files for the dist PVC.
- Use Azure Managed Identity on the pod to access Azure Blob Storage.

### On-Prem / Bare-Metal

- Use NFS or OpenEBS for `ReadWriteMany`.
- Remove `imagePullSecrets` if pulling from an internal registry with no auth.
- KEDA handles all StatefulSet scaling. Dremio does not interact with the Kubernetes API.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Coordinator crashes: `properties were invalid: ...pod_template_large` | Config key missing from `dremio-reference.conf` | Ensure you're on image `2026.07.5`+ |
| `elastic_desired_*` missing from Prometheus | Gauges only appear after first scale event (lazy init) | Submit a test query; check coordinator logs for `elastic-idle-reset` thread |
| KEDA ScaledObject `ACTIVE: False` even after query | Prometheus not scraping coordinator, or `serverAddress` wrong in ScaledObject | Verify scrape job; check Prometheus targets UI |
| Executors not scaling up | `elastic_desired_*` not published or KEDA trigger misconfigured | Check `kubectl get scaledobject -n dremio`; verify Prometheus has the metric |
| `elastic_desired_*` never resets to 0 after query | Idle-reset deadlock — gauges stuck above 0 | Ensure coordinator image is `2026.07.5`+. Check logs for `Idle reset` messages |
| Query fails from cold state with timeout | Cold-start guard missing — idle-reset fired during executor startup | Ensure coordinator image is `2026.07.5`+ |
| Executors stuck running after all queries complete | `elastic_desired_*` gauges never dropped to 0 — idle-reset deadlock | Ensure image is `2026.07.5`+. The v2026.07.1 release had a bug where `desiredSmall/desiredLarge > 0` blocked the idle-reset indefinitely |
| Executor evicted for ephemeral storage | Storage limits too low | Set small: 15Gi/30Gi, large: 30Gi/60Gi for request/limit |
| Executor evicted but disk appears empty | Log flood from Netty DEBUG hex dumps — check `logs usedBytes` in kubelet stats | Add `logback.xml` to executor ConfigMaps with `io.netty` at `WARN` |
| Orphaned executor PVCs after scale-down | `persistentVolumeClaimRetentionPolicy.whenScaled` is `Retain` | Set to `Delete` in the executor StatefulSet spec |
| Queries interrupted during scale-down | `preStop` sleep too short or `terminationGracePeriodSeconds` too low | Set `preStop: sleep 120` and `terminationGracePeriodSeconds: 1800` on executor pods |
