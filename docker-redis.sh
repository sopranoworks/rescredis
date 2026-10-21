#!/bin/bash
#
# Starts and stops the Redis servers the test suite expects, in Docker containers only
# (nothing is installed on the host), and runs the tests against them. Every server is
# Redis 7.4.2 from the Redis Stack image redis/redis-stack-server:7.4.0-v2 (production runs
# Redis Stack with Redis 7.4.2; 7.4.0-v3 carries the same server):
#
#   * 6379          plain server, no password, Redis Stack modules loaded
#   * 6380          server configured by src/test/resources/redis-6380.conf (requirepass foobar,
#                   user tester/secretpass), same modules
#   * 7000 - 7005   Redis cluster (3 masters, 3 replicas), no password. Same image and same
#                   server binary, started without the modules (the suite does not use them
#                   and the modules are not meant for cluster nodes started by hand)
#
# All containers share one network namespace (owned by the first one), so every server is
# reachable as 127.0.0.1 from every other one and from the test container. The suite relies on
# that: it MIGRATEs keys from 6379 to 127.0.0.1:6380, the cluster announces 127.0.0.1, and the
# SLOWLOG test expects the client address 127.0.0.1 (with Docker Desktop's NAT the server would
# see 192.168.65.1). The ports are also published to the host.
#
# Usage:
#   ./docker-redis.sh start            start the servers
#   ./docker-redis.sh test [sbt args]  run sbt in a container next to the servers (default: +test)
#   ./docker-redis.sh stop             stop the servers
#   ./docker-redis.sh status           containers, images and the redis_version of every server
#
# Images can be overridden with REDIS_IMAGE and SBT_IMAGE.

set -euo pipefail

REDIS_IMAGE="${REDIS_IMAGE:-redis/redis-stack-server:7.4.0-v2}"
SBT_IMAGE="${SBT_IMAGE:-sbtscala/scala-sbt:eclipse-temurin-17.0.20_8_1.13.0_3.3.8}"
SINGLE_NAME=scredis-test-redis
CLUSTER_NAME=scredis-test-redis-cluster
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Redis Stack modules, as loaded by the image's own entrypoint
MODULES="--loadmodule /opt/redis-stack/lib/rediscompat.so \
--loadmodule /opt/redis-stack/lib/redisearch.so \
--loadmodule /opt/redis-stack/lib/redistimeseries.so \
--loadmodule /opt/redis-stack/lib/rejson.so \
--loadmodule /opt/redis-stack/lib/redisbloom.so \
--loadmodule /opt/redis-stack/lib/redisgears.so v8-plugin-path /opt/redis-stack/lib/libredisgears_v8_plugin.so"

start() {
  # 6380 first (background), then 6379 in the foreground
  docker run -d --rm --name "$SINGLE_NAME" \
    -p 6379:6379 -p 6380:6380 \
    -p 7000:7000 -p 7001:7001 -p 7002:7002 -p 7003:7003 -p 7004:7004 -p 7005:7005 \
    -v "$ROOT/src/test/resources/redis-6380.conf:/conf/redis-6380.conf:ro" \
    --entrypoint sh "$REDIS_IMAGE" -c "
      mkdir -p /data/6380 && cd /opt/redis-stack
      /opt/redis-stack/bin/redis-server /conf/redis-6380.conf --dir /data/6380 --bind 0.0.0.0 --daemonize no $MODULES &
      mkdir -p /data/6379
      exec /opt/redis-stack/bin/redis-server --dir /data/6379 --bind 0.0.0.0 --protected-mode no --daemonize no $MODULES" \
    >/dev/null

  docker run -d --rm --name "$CLUSTER_NAME" --network "container:$SINGLE_NAME" \
    --entrypoint sh "$REDIS_IMAGE" -c '
      for p in 7000 7001 7002 7003 7004 7005; do
        mkdir -p /data/$p
        /opt/redis-stack/bin/redis-server --port $p --dir /data/$p --bind 0.0.0.0 --protected-mode no \
          --cluster-enabled yes --cluster-config-file nodes.conf --cluster-node-timeout 5000 \
          --appendonly yes --daemonize yes >/dev/null
      done
      until redis-cli -p 7005 ping >/dev/null 2>&1; do sleep 0.2; done
      redis-cli --cluster create 127.0.0.1:7000 127.0.0.1:7001 127.0.0.1:7002 \
        127.0.0.1:7003 127.0.0.1:7004 127.0.0.1:7005 --cluster-replicas 1 --cluster-yes
      exec sleep infinity' \
    >/dev/null

  echo "waiting for Redis on 6379 and 6380 ..."
  for i in $(seq 1 60); do
    if docker exec "$SINGLE_NAME" redis-cli -p 6379 ping >/dev/null 2>&1 \
      && docker exec "$SINGLE_NAME" redis-cli -p 6380 -a foobar --no-auth-warning ping >/dev/null 2>&1; then
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
  docker image inspect "$REDIS_IMAGE" --format 'image {{index .RepoDigests 0}}' 2>/dev/null || true
  for spec in "$SINGLE_NAME 6379" "$SINGLE_NAME 6380" "$CLUSTER_NAME 7000" "$CLUSTER_NAME 7001" \
              "$CLUSTER_NAME 7002" "$CLUSTER_NAME 7003" "$CLUSTER_NAME 7004" "$CLUSTER_NAME 7005"; do
    set -- $spec
    auth=()
    if [ "$2" = 6380 ]; then auth=(-a foobar --no-auth-warning); fi
    echo "$1 :$2 $(docker exec "$1" redis-cli -p "$2" ${auth[@]+"${auth[@]}"} info server 2>/dev/null | grep -m1 redis_version | tr -d '\r')"
  done
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
