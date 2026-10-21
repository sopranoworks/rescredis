#!/bin/bash
#
# Starts and stops the Redis servers the test suite expects, in Docker containers only
# (nothing is installed on the host), and runs the tests against them:
#
#   * 6379          plain server, no password
#   * 6380          server configured by src/test/resources/redis-6380.conf (requirepass foobar,
#                   user tester/secretpass)
#   * 7000 - 7005   Redis cluster (3 masters, 3 replicas), no password
#
# All containers share one network namespace (owned by the first one), so every server is
# reachable as 127.0.0.1 from every other one and from the test container. The suite relies on
# that: it MIGRATEs keys from 6379 to 127.0.0.1:6380, the cluster announces 127.0.0.1, and the
# SLOWLOG test expects the client address 127.0.0.1. The ports are also published to the host.
#
# Usage:
#   ./docker-redis.sh start            start the servers
#   ./docker-redis.sh test [sbt args]  run sbt in a container next to the servers (default: +test)
#   ./docker-redis.sh stop             stop the servers
#   ./docker-redis.sh status
#
# Images can be overridden with REDIS_IMAGE, REDIS_CLUSTER_IMAGE and SBT_IMAGE. Redis 6.0.x is
# what the suite is written for: from Redis 6.2 on, AUTH with an empty password is refused when no
# password is set, which PubSubCommandsSpec relies on.

set -euo pipefail

REDIS_IMAGE="${REDIS_IMAGE:-redis:6.0.5}"
REDIS_CLUSTER_IMAGE="${REDIS_CLUSTER_IMAGE:-grokzen/redis-cluster:6.0.1}"
SBT_IMAGE="${SBT_IMAGE:-sbtscala/scala-sbt:eclipse-temurin-17.0.20_8_1.13.0_3.3.8}"
SINGLE_NAME=scredis-test-redis
CLUSTER_NAME=scredis-test-redis-cluster
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

start() {
  docker run -d --rm --name "$SINGLE_NAME" \
    -p 6379:6379 -p 6380:6380 \
    -p 7000:7000 -p 7001:7001 -p 7002:7002 -p 7003:7003 -p 7004:7004 -p 7005:7005 \
    -v "$ROOT/src/test/resources/redis-6380.conf:/conf/redis-6380.conf:ro" \
    "$REDIS_IMAGE" \
    sh -c 'redis-server /conf/redis-6380.conf --bind 0.0.0.0 & exec redis-server --bind 0.0.0.0 --protected-mode no' \
    >/dev/null

  docker run -d --rm --name "$CLUSTER_NAME" --network "container:$SINGLE_NAME" \
    -e IP=127.0.0.1 \
    "$REDIS_CLUSTER_IMAGE" >/dev/null

  echo "waiting for Redis on 6379 and 6380 ..."
  for i in $(seq 1 60); do
    if docker exec "$SINGLE_NAME" redis-cli -p 6379 ping >/dev/null 2>&1 \
      && docker exec "$SINGLE_NAME" redis-cli -p 6380 -a foobar ping >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done

  echo "waiting for the cluster on 7000-7005 ..."
  for i in $(seq 1 120); do
    if docker exec "$CLUSTER_NAME" redis-cli -p 7000 cluster info 2>/dev/null | grep -q 'cluster_state:ok'; then
      break
    fi
    sleep 1
  done
  status
}

stop() {
  docker stop "$CLUSTER_NAME" "$SINGLE_NAME" 2>/dev/null || true
}

status() {
  docker ps --filter "name=$SINGLE_NAME" --filter "name=$CLUSTER_NAME" \
    --format '{{.ID}}  {{.Names}}  {{.Image}}  {{.Status}}'
}

run_tests() {
  if [ "$#" -eq 0 ]; then set -- "+test"; fi
  docker run --rm --network "container:$SINGLE_NAME" \
    -v "$ROOT:/work" -w /work \
    -v scredis-sbt-ivy:/root/.ivy2 -v scredis-sbt-coursier:/root/.cache -v scredis-sbt-home:/root/.sbt \
    "$SBT_IMAGE" sbt -batch "$@"
}

case "${1:-}" in
  start) start ;;
  test) shift; run_tests "$@" ;;
  stop) stop ;;
  status) status ;;
  *) echo "usage: $0 start|test [sbt args]|stop|status" >&2; exit 1 ;;
esac
