# KEDA Metrics Exporter for Dremio OSS

**This repository is now at: [github.com/Rusha-Corp/dremio-keda-exporter](https://github.com/Rusha-Corp/dremio-keda-exporter)**

The KEDA metrics exporter source code has moved to a separate public repository to enable independent development and versioning.

---

## What it does

The metrics-exporter is a lightweight Flask sidecar that:
- Polls Dremio's `/apiv2/jobs` REST API for active jobs
- Reads StatefulSet replica counts via the Kubernetes API
- Exposes `executor_desired_small` and `executor_desired_large` for KEDA's metrics-api scaler
- Implements a 120s grace period for graceful scale-down

## Deployment

```bash
kubectl apply -f 09-metrics-exporter-deployment.yaml
```

## Image

| Registry | URI |
|---|---|
| GHCR | `ghcr.io/rusha-corp/dremio-keda-exporter:2026.05.0` |

## Source code

[github.com/Rusha-Corp/dremio-keda-exporter](https://github.com/Rusha-Corp/dremio-keda-exporter)
