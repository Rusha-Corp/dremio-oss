# Dremio Elastic Executor Scaling — Deployment Guide

This guide walks through deploying Dremio OSS with KEDA-driven elastic executor scaling on any Kubernetes platform (EKS, GKE, AKS, k3s, on-prem). Two executor tiers — **SMALL** (interactive queries) and **LARGE** (analytics/ETL) — scale from zero on demand and scale down after a configurable idle period.

---

## How It Works

```
┌──────────────────────────────────────────────┐
│           Dremio Coordinator                  │
│  ElasticResourceAllocator                     │
│  - query arrives → determine tier & delta     │
│  - publishes elastic_desired_small/large      │
│    as Prometheus gauges on :45679/metrics     │
└───────────────────┬──────────────────────────┘
                    │ :45679/metrics
                    ▼
         ┌──────────────────────┐
         │  dremio-keda-exporter│  polls /apiv2/jobs every 5s
         │  - reads             │◄──────────────────────────
         │    elastic_desired_* │
         │  - applies scale-    │
         │    gate + drain guard│
         │  - exposes           │
         │    executor_desired_*│
         └──────────┬───────────┘
                    │ :5001/json
                    ▼
         ┌──────────────────────┐
         │   KEDA Controller    │  polls every 10s
         └──────────┬───────────┘
                    │ sets .spec.replicas
                    ▼
         ┌──────────────────────┐
         │ Executor StatefulSets│
         │  small / large       │
         └──────────────────────┘
```

**Scale-up path:** Query arrives → coordinator computes required executors → publishes `elastic_desired_large=3` on liveness metrics → exporter reads it → KEDA sets `spec.replicas=3` → pods start and register → query executes.

**Scale-down path:** No queries for 30 min → exporter enters terminal drain window (2 min) → KEDA scales to zero.

---

## Prerequisites

| Requirement | Notes |
|---|---|
| Kubernetes 1.24+ | EKS, GKE, AKS, k3s, or any conformant cluster |
| KEDA v2.x | [Installation](https://keda.sh/docs/latest/deploy/) |
| Container registry | GHCR, ECR, GCR, or any OCI-compatible registry |
| Persistent storage | Storage class supporting `ReadWriteOnce`; `ReadWriteMany` for the shared dist PVC |
| Dremio admin account | Required by the metrics exporter to poll `/apiv2/jobs` |

---

## Images

| Image | Registry | Notes |
|---|---|---|
| Dremio OSS (coordinator + executor) | `ghcr.io/rusha-corp/dremio-oss:2026.05.7` | Same binary; role set by ConfigMap |
| KEDA metrics exporter | `ghcr.io/rusha-corp/dremio-keda-exporter:2026.05.8` | Source: [Rusha-Corp/dremio-keda-exporter](https://github.com/Rusha-Corp/dremio-keda-exporter) |

---

## Step 1 — Namespace and RBAC

```bash
kubectl apply -f k8s/00-namespace.yaml
kubectl apply -f k8s/01-rbac.yaml
```

The RBAC creates a `dremio-elastic` service account with permission to read/patch StatefulSets in the `dremio` namespace (required by the coordinator's `ElasticResourceAllocator`).

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
  scale_timeout_minutes: 5       # max wait for executors to register

  max_executors: 4               # small-tier cap
  max_executors_large: 8         # large-tier cap

  kubernetes {
    namespace: "dremio"
    pod_template: "dremio-executor-small"
    pod_template_large: "dremio-executor-large"
  }
}
```

Tier routing — queries are classified LARGE when the routing queue name contains `"large"` (case-insensitive), otherwise SMALL:

```hocon
services.executor.elastic.small_query_threshold: 100   # plan cost below this → SMALL
services.executor.elastic.medium_query_threshold: 1000 # plan cost above this → LARGE
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
kubectl logs -n dremio -l role=coordinator | grep -E "ERROR|elastic|K8sPlatform"
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

## Step 7 — Metrics Exporter

The exporter needs a Dremio admin account to poll `/apiv2/jobs`:

```bash
kubectl create secret generic dremio-ops-credentials \
  --namespace=dremio \
  --from-literal=DREMIO_USERNAME=<admin-username> \
  --from-literal=DREMIO_PASSWORD=<admin-password>
```

Apply the exporter deployment:

```bash
kubectl apply -f k8s/09-metrics-exporter-deployment.yaml
```

Key environment variables (configurable in the deployment manifest):

| Variable | Default | Description |
|---|---|---|
| `DREMIO_URL` | `http://dremio-coordinator.dremio.svc.cluster.local:9047` | Coordinator REST API URL |
| `DREMIO_LIVENESS_URL` | `http://dremio-coordinator-liveness.dremio.svc.cluster.local:45679/metrics` | Prometheus metrics endpoint for `elastic_desired_*` gauges |
| `SCALE_DOWN_GRACE_SECS` | `1800` | Idle time before scaling to zero (seconds) |
| `TERMINAL_DRAIN_SECS` | `120` | Final drain buffer after grace period (matches executor `preStop: sleep 120`) |
| `MAX_JOB_PAGES` | `10` | Hard cap on `/apiv2/jobs` pages fetched per cycle (100 jobs/page); prevents OOM when Dremio has large job history |
| `JOB_LOOKBACK_SECS` | `7200` | Stop pagination when jobs are older than this many seconds; any active job must have started within this window |

---

## Step 8 — KEDA ScaledObjects

```bash
kubectl apply -f k8s/06-keda-small.yaml
kubectl apply -f k8s/07-keda-large.yaml
```

Each ScaledObject polls the exporter's `/json` endpoint for `executor_desired_small` / `executor_desired_large`. Key parameters:

| Parameter | Value | Notes |
|---|---|---|
| `pollingInterval` | `10s` | How often KEDA reads the metric |
| `cooldownPeriod` | `1800s` | KEDA's own scale-down stabilisation window |
| `minReplicaCount` | `0` | Full scale-to-zero when idle |
| `maxReplicaCount` | `4` (small) / `8` (large) | Hard cap — must match `dremio.conf` |

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

# Exporter is collecting metrics
kubectl logs -n dremio deploy/dremio-metrics-exporter --tail=20

# KEDA ScaledObjects are active
kubectl get scaledobject -n dremio

# Liveness metrics include elastic_desired gauges (after first query triggers scaling)
kubectl run curl-test --image=curlimages/curl --rm -it --restart=Never -n dremio -- \
  curl -s http://dremio-coordinator-liveness.dremio.svc.cluster.local:45679/metrics \
  | grep elastic_desired
```

Expected exporter log when idle:
```
INFO small tier idle for 45s/1800s, holding at 2
INFO Metrics: {'active_user_jobs': 0, 'registered_executors': 4, 'executor_desired_small': 2, ...}
```

Expected exporter log when a large query arrives:
```
INFO Metrics: {'active_user_jobs': 1, 'active_large_jobs': 1, 'executor_desired_large': 3, ...}
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
- Ensure the k8s API server is reachable from coordinator pods (the `ElasticResourceAllocator` calls the k8s API to read StatefulSet replica counts).

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Coordinator crashes: `properties were invalid: ...pod_template_large` | Config key missing from `dremio-reference.conf` | Ensure you're on image `2026.05.7`+ which includes the fix |
| `elastic_desired_*` not on liveness endpoint | `K8sPlatform` is lazy-initialized — gauges only appear after the first scale event | Submit a query; they will appear |
| Exporter: `Dremio unavailable: Expecting value: line 1 column 1` | `/apiv2/jobs` pagination URL bug — `next` points to web UI | Ensure exporter image is `2026.05.7`+ |
| Exporter OOMKilled | Exporter paginating through all historical jobs — 830k+ jobs overflow 512Mi limit | Upgrade to `2026.05.8`+; set `MAX_JOB_PAGES=10` and `JOB_LOOKBACK_SECS=7200` |
| KEDA sees `executor_desired=0`, executors won't scale up | Exporter blocked mid-pagination, returning stale cached zeros | Upgrade to `2026.05.8`+; check exporter pod memory and logs |
| Executors not scaling up | `elastic_desired_*` not being published (old coordinator image) or exporter not reading liveness URL | Verify coordinator version; check `DREMIO_LIVENESS_URL` env var on exporter |
| Executor evicted but disk appears nearly empty | Log flood from Netty DEBUG hex dumps filling container log quota — check kubelet stats for `logs usedBytes` | Add `logback.xml` to executor ConfigMaps with `io.netty` at `WARN` |
| Queries interrupted during scale-down | `TERMINAL_DRAIN_SECS` too low or executor `preStop` sleep mismatch | Set `TERMINAL_DRAIN_SECS` ≥ `preStop` sleep value (default: both 120s) |
| Metrics exporter 401 Unauthorized | `dremio-ops-credentials` secret missing or has wrong password | Recreate secret with valid Dremio admin credentials |
| KEDA not scaling | ScaledObject `externalMetricNames` mismatch or exporter not reachable | Check `kubectl get scaledobject -n dremio` and exporter logs |
| Orphaned executor PVCs after scale-down | `persistentVolumeClaimRetentionPolicy.whenScaled` is `Retain` | Set to `Delete` in the executor StatefulSet spec |
