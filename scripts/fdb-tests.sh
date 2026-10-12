#!/usr/bin/env bash
#
# Runs Catalyst's FoundationDB tests against a real cluster, in Docker, the way CI's `fdb` job does.
#
#   scripts/fdb-tests.sh                 # the FoundationDB*Test classes in catalyst-gumbo and catalyst-api
#   scripts/fdb-tests.sh --keep          # leave the cluster running afterwards
#   scripts/fdb-tests.sh --keep -Dtest=FoundationDBEventLogTest   # extra args go to mvn
#
# Adapted from Gumbo's scripts/fdb-tests.sh. Two containers: the foundationdb/foundationdb image as
# the server, in host networking mode so it advertises 127.0.0.1:4500, and a JDK+Maven image with the
# FoundationDB client library, sharing the server's network namespace so that address is right from
# inside it — the same shape as CI (a service container plus the client package on the runner).
#
# The tests run with CATALYST_REQUIRE_FDB=true, so an unreachable cluster fails rather than skips.
# ~/.m2 is mounted as the dependency cache and the checkout as the working directory, so target/ is
# written by the container.
set -euo pipefail

FDB_VERSION="${FDB_VERSION:-7.3.43}"   # matches fdb.version in the root pom
SERVER="catalyst-fdb"
RUNNER_IMAGE="catalyst-fdb-runner:${FDB_VERSION}"
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

KEEP=false
MVN_ARGS=()
for arg in "$@"; do
  case "$arg" in
    --keep) KEEP=true ;;
    *) MVN_ARGS+=("$arg") ;;
  esac
done
if [ "${#MVN_ARGS[@]}" -eq 0 ]; then
  MVN_ARGS=(-Dtest='FoundationDB*Test')
fi

cleanup() {
  if [ "$KEEP" = false ]; then
    docker rm -f "$SERVER" >/dev/null 2>&1 || true
  else
    echo "Leaving $SERVER running (docker rm -f $SERVER to stop it)."
  fi
}
trap cleanup EXIT

if ! docker image inspect "$RUNNER_IMAGE" >/dev/null 2>&1; then
  echo "Building $RUNNER_IMAGE ..."
  docker build --platform linux/amd64 -t "$RUNNER_IMAGE" --build-arg "FDB_VERSION=$FDB_VERSION" - <<'DOCKERFILE'
FROM eclipse-temurin:21-jdk-jammy
ARG FDB_VERSION
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates maven \
 && curl -fsSL "https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/foundationdb-clients_${FDB_VERSION}-1_amd64.deb" -o /tmp/fdb-clients.deb \
 && dpkg -i /tmp/fdb-clients.deb && rm /tmp/fdb-clients.deb \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
WORKDIR /work
DOCKERFILE
fi

if ! docker inspect -f '{{.State.Running}}' "$SERVER" 2>/dev/null | grep -q true; then
  docker rm -f "$SERVER" >/dev/null 2>&1 || true
  echo "Starting FoundationDB $FDB_VERSION ..."
  docker run -d --name "$SERVER" --platform linux/amd64 \
    -e FDB_NETWORKING_MODE=host -p 4500:4500 \
    "foundationdb/foundationdb:${FDB_VERSION}" >/dev/null
  # The image starts fdbserver but does not create a database; that is a one-off fdbcli call.
  for _ in $(seq 1 30); do
    if docker exec "$SERVER" fdbcli --exec 'configure new single memory' 2>/dev/null | grep -q 'Database created'; then
      break
    fi
    sleep 2
  done
fi
for _ in $(seq 1 30); do
  if docker exec "$SERVER" fdbcli --exec 'status minimal' 2>/dev/null | grep -q 'The database is available'; then
    break
  fi
  sleep 2
done
docker exec "$SERVER" fdbcli --exec 'status minimal'

docker run --rm --platform linux/amd64 --network "container:$SERVER" \
  -v "$REPO:/work" -v "$HOME/.m2:/root/.m2" \
  -e FDB_CLUSTER_FILE=/tmp/fdb.cluster -e CATALYST_REQUIRE_FDB=true \
  "$RUNNER_IMAGE" bash -c '
    echo "docker:docker@127.0.0.1:4500" > /tmp/fdb.cluster
    status=0
    mvn -B --no-transfer-progress -pl catalyst-gumbo,catalyst-api -am -Dsurefire.failIfNoSpecifiedTests=false test "$@" || status=$?
    echo "--- surefire ---"
    grep -h "Tests run" */target/surefire-reports/*FoundationDB*.txt || true
    exit "$status"
  ' _ "${MVN_ARGS[@]}"
