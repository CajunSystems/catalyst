#!/usr/bin/env bash
# Runs one of Catalyst's exit demos (com.cajunsystems.catalyst.api.Demo).
#
#   scripts/demo.sh replay                 # single-process demos: replay, branch, snapshot, cancel,
#                                          #   retry, resumeid, tools, collections, blob, schema,
#                                          #   streaming, timeline, otel, autocapture, shutdown, signal, recover, eval
#   scripts/demo.sh /tmp/demo record       # two-process crash demo: record, then (new process) resume
#   scripts/demo.sh /tmp/demo resume
#
# Build first with `mvn install -DskipTests`; the script resolves the classpath the same way CI does.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ ! -d catalyst-api/target/classes ]; then
  echo "catalyst-api is not built yet — run: mvn install -DskipTests" >&2
  exit 1
fi
# Rebuild the cached classpath when any pom changed: dependency versions live in the root pom, and a
# stale classpath would silently run the demo against the previous jars still in ~/.m2.
if [ ! -f catalyst-api/target/cp.txt ] \
   || [ -n "$(find . -name pom.xml -not -path '*/target/*' -newer catalyst-api/target/cp.txt)" ]; then
  mvn -q -pl catalyst-api org.apache.maven.plugins:maven-dependency-plugin:3.6.1:build-classpath \
      -Dmdep.outputFile=target/cp.txt
fi

# EnableDynamicAgentLoading lets the autocapture demo attach its agent; it is inert for the others.
exec java -XX:+EnableDynamicAgentLoading \
     -cp "catalyst-api/target/classes:$(cat catalyst-api/target/cp.txt)" \
     com.cajunsystems.catalyst.api.Demo "$@"
