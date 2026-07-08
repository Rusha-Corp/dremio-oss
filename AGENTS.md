# Agent Guidelines

This file provides instructions for AI agents (Codex, Jules, Factory Droid, etc.) working on this repository.

## Dead Code and Redundancy

- **Remove unreachable methods.** If a public or interface method has zero call sites (excluding tests that exercise the method itself), remove it rather than keeping it "just in case." Interface methods that exist only to satisfy a contract but are never called should be deleted along with their implementations.
- **Remove stale config keys.** When a config key is renamed (e.g., `max_executors` to `max_executors_small`), remove the old key from all config files, Java constants, and documentation. Do not leave the old key as a fallback unless there is a documented migration path.
- **Eliminate phantom bridge methods.** Do not add or keep overloads that simply delegate to another overload with a hardcoded default argument (e.g., `scaleExecutors(int delta)` delegating to `scaleExecutors(int delta, ExecutorTier.SMALL)`) when every real caller uses the more specific signature. Prefer removing the bridge entirely.
- **Remove unused imports and constants.** After a refactor, clean up any imports, constants, or fields that are no longer referenced.

## Interface Hygiene

- Abstract methods should match the actual call-site contract. If every caller passes a tier parameter, the interface should require a tier parameter, not provide a tier-less overload.
- Default methods on interfaces should provide meaningful behavior, not just delegate to a less-specific abstract method that nobody calls.

## Code Style

- Match the existing style of the file you are editing. This repo uses Java with Google-style formatting (4-space indent, 100-char line limit).
- Prefer composition and explicit wiring over speculative abstraction. Do not introduce wrapper interfaces or factory methods unless there is a concrete second implementation.
- Keep Javadoc factual. Remove javadoc that merely restates the method name or describes behavior that no longer exists.

## Testing

- Run `mvn test -pl services/resourcescheduler -am` before committing changes to the elastic scaling subsystem.
- If you remove a method, also remove any tests that only exercise that method.
- Do not add tests for methods you plan to remove.

## Elastic Scaling Subsystem

The elastic scaling code lives under `services/resourcescheduler/src/main/java/com/dremio/resource/elastic/`. Key classes:

- `ResourcePlatform` — interface for platform-agnostic scaling (tier-aware)
- `K8sPlatform` — Kubernetes implementation (manages two StatefulSets: small and large)
- `ElasticResourceAllocator` — entry point; classifies queries by cost, calls tier-aware scaling
- `ElasticAdmissionCalculator` — cost thresholds and tier classification
- `ResourcePlatformProvider` — DI provider, reads `DremioConfig` constants

When renaming or removing config keys, update all of: `DremioConfig.java` constants, `dremio-reference.conf` files, k8s configmap, and all documentation under `docs/` and `k8s/README.md`.
