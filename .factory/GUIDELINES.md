# Factory Droid Project Guidelines

## Repository Overview

Fork of dremio/dremio-oss with elastic executor auto-scaling on Kubernetes. The custom code lives primarily in `services/resourcescheduler/src/main/java/com/dremio/resource/elastic/`.

## Cleanup Standards

- **Remove dead code proactively.** If a method, constant, or config key has no callers, remove it. Do not leave fallbacks "for backward compatibility" when there are no legacy consumers.
- **Rename exhaustively.** When renaming a config key, constant, or method, update every reference: Java constants, reference config files, k8s manifests, and all docs. A partial rename is a bug.
- **No phantom bridge methods.** Interface overloads that exist only to delegate with a hardcoded default should be removed when every real caller uses the more specific signature.

## Key Files

| Area | Files |
|------|-------|
| Config constants | `common/legacy/src/main/java/com/dremio/config/DremioConfig.java` |
| Reference config | `common/legacy/src/main/resources/dremio-reference.conf`, `dremio-reference.conf` |
| Elastic scaling Java | `services/resourcescheduler/src/main/java/com/dremio/resource/elastic/` |
| Elastic scaling tests | `services/resourcescheduler/src/test/java/com/dremio/resource/elastic/` |
| K8s manifests | `k8s/03-configmap.yaml`, `k8s/04-coordinator.yaml`, `k8s/06-keda-small.yaml` |
| Docs | `docs/elastic-scaling-deployment.md`, `docs/community-post-elastic-scaling.md`, `ELASTIC_SCALING_IMPLEMENTATION.md`, `k8s/README.md` |

## Test Command

```bash
mvn test -pl services/resourcescheduler -am
```

## Git Conventions

- Commit messages follow conventional commits: `feat:`, `fix:`, `refactor:`, `docs:`.
- Always check `git diff --cached` before committing to catch missed renames or leftover dead references.
