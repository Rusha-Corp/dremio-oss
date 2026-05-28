#!/usr/bin/env bash
set -euo pipefail

REPO="${GHCR_REPO:-ghcr.io/rusha-corp/dremio-oss}"
VERSION="${DREMIO_VERSION:-26.0.5-elastic}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TARBALL_GLOB="$REPO_ROOT/distribution/server/target/dremio-community-*.tar.gz"

export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/temurin-21-jdk-amd64}"
export PATH="$JAVA_HOME/bin:$PATH"

# 1. Build Maven distribution (always rebuild to pick up code changes)
echo "=== Building Dremio distribution with JDK 21 ==="
cd "$REPO_ROOT"
./mvnw package -pl distribution/server -am -DskipTests -Denforcer.skip=true -q

TARBALL=$(ls $TARBALL_GLOB | head -1)
echo "=== Using tarball: $TARBALL ==="

# 2. Stage tarball into k8s build context (remove old one first)
rm -f "$SCRIPT_DIR/dremio-distribution.tar.gz"
cp "$TARBALL" "$SCRIPT_DIR/dremio-distribution.tar.gz"

# 3. Login to GHCR
echo "=== Logging in to GHCR ==="
gh auth token | docker login ghcr.io -u "$(gh api user --jq .login)" --password-stdin

# 4. Build and push (single image, tagged for both roles)
echo "=== Building image ==="
docker build \
  -t "${REPO}:coordinator-${VERSION}" \
  -t "${REPO}:executor-${VERSION}" \
  "$SCRIPT_DIR"

echo "=== Pushing images ==="
docker push "${REPO}:coordinator-${VERSION}"
docker push "${REPO}:executor-${VERSION}"

# 5. Cleanup staged tarball
rm "$SCRIPT_DIR/dremio-distribution.tar.gz"
echo "=== Done. Images pushed: ${REPO}:coordinator-${VERSION}, ${REPO}:executor-${VERSION} ==="
