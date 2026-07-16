#!/usr/bin/env bash
set -euo pipefail

REPO="${GHCR_REPO:-ghcr.io/rusha-corp/dremio-oss}"
export DREMIO_VERSION="2026.07.5"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TARBALL_GLOB="$REPO_ROOT/distribution/server/target/dremio-community-*.tar.gz"

export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/temurin-21-jdk-amd64}"
export PATH="$JAVA_HOME/bin:$PATH"

# 1. Build Maven distribution (always rebuild to pick up code changes)
echo "=== Building Dremio distribution with JDK 21 ==="
cd "$REPO_ROOT"
./mvnw package -pl distribution/server -am -DskipTests -Denforcer.skip=true -q 2>&1 || {
  echo "=== Maven build failed, using existing tarball ==="
}

TARBALL=$(ls $TARBALL_GLOB | head -1)
echo "=== Using tarball: $TARBALL ==="

# 2. Stage tarball into k8s build context (remove old one first)
rm -f "$SCRIPT_DIR/dremio-distribution.tar.gz"
cp "$TARBALL" "$SCRIPT_DIR/dremio-distribution.tar.gz"

# 2b. Stage rebuilt JARs into overlay/ directory for Dockerfile.
# Clean old staged JARs first so only fresh ones are present.
echo "=== Staging rebuilt JARs ==="
rm -rf "$SCRIPT_DIR/overlay"
mkdir -p "$SCRIPT_DIR/overlay"
COMMON_JAR="$(find "$REPO_ROOT/common/legacy/target" -name 'dremio-common-*.jar' ! -name '*-tests.jar' 2>/dev/null | head -1)" || true
EXECSEL_JAR="$(find "$REPO_ROOT/services/exec-selector/target" -name 'dremio-services-execselector-*.jar' ! -name '*-tests.jar' 2>/dev/null | head -1)" || true
RSCHED_JAR="$(find "$REPO_ROOT/services/resourcescheduler/target" -name 'dremio-services-resourcescheduler-*.jar' ! -name '*-tests.jar' 2>/dev/null | head -1)" || true
TELEMETRY_JAR="$(find "$REPO_ROOT/services/telemetry-api/target" -name 'dremio-services-telemetry-api-*.jar' ! -name '*-tests.jar' 2>/dev/null | head -1)" || true
SABOT_JAR="$(find "$REPO_ROOT/sabot/kernel/target" -name 'dremio-sabot-kernel-*.jar' ! -name '*-tests.jar' 2>/dev/null | head -1)" || true
DAC_BACKEND_JAR="$(find "$REPO_ROOT/dac/backend/target" -name 'dremio-dac-backend-*.jar' ! -name '*-tests.jar' 2>/dev/null | head -1)" || true
[ -n "$COMMON_JAR" ] && cp "$COMMON_JAR" "$SCRIPT_DIR/overlay/" || true
[ -n "$EXECSEL_JAR" ] && cp "$EXECSEL_JAR" "$SCRIPT_DIR/overlay/" || true
[ -n "$RSCHED_JAR" ] && cp "$RSCHED_JAR" "$SCRIPT_DIR/overlay/" || true
[ -n "$TELEMETRY_JAR" ] && cp "$TELEMETRY_JAR" "$SCRIPT_DIR/overlay/" || true
[ -n "$SABOT_JAR" ] && cp "$SABOT_JAR" "$SCRIPT_DIR/overlay/" || true
[ -n "$DAC_BACKEND_JAR" ] && cp "$DAC_BACKEND_JAR" "$SCRIPT_DIR/overlay/" || true
echo "  Staged $(ls "$SCRIPT_DIR/overlay/"*.jar 2>/dev/null | wc -l) JAR(s)"

# 3. Login to GHCR
echo "=== Logging in to GHCR ==="
gh auth token | docker login ghcr.io -u "$(gh api user --jq .login)" --password-stdin

# 4. Build and push (single image, tagged for both roles)
echo "=== Building image ==="
docker build \
  -t "${REPO}:${DREMIO_VERSION}" \
  -t "${REPO}:latest" \
  "$SCRIPT_DIR"

echo "=== Pushing images ==="
docker push "${REPO}:${DREMIO_VERSION}"
docker push "${REPO}:latest"

# 5. Cleanup staged files
rm -f "$SCRIPT_DIR/dremio-distribution.tar.gz"
rm -rf "$SCRIPT_DIR/overlay"
echo "=== Done. Images pushed: ${REPO}:${DREMIO_VERSION}, ${REPO}:latest ==="
