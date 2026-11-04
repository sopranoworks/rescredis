# Testing setup

The tests need real Redis servers. They all run in Docker containers (nothing is installed on the
host) and are Redis 7.4.2 from the Redis Stack image `redis/redis-stack-server:7.4.0-v2`, the version
production runs:

* `6379`: plain server, no password
* `6380`: configured by `src/test/resources/redis-6380.conf` (password `foobar`, ACL user `tester`)
* `7000`-`7005`: Redis cluster (3 masters, 3 replicas)
* `6390`, `6391`: a master and its replica, no password, no modules (`WaitCommandSpec`)
* `7100`-`7105`: a second Redis cluster (3 masters, 3 replicas, `cluster-node-timeout 2000`) for the failover tests
  (`ReplyPairingSpec`, `ClusterScriptingSpec`, `ClusterBlockingSpec`). They crash masters with `DEBUG SEGFAULT`
  (`enable-debug-command local`); every node runs in a loop that starts it again 6 s after it died, so it rejoins
  as a replica of the node that took over. A test waits until the cluster is whole again (every replica in sync)
  before it ends.

6379 and 6380 have the Redis Stack modules loaded; every cluster node has the JSON module (`ReJSON`) loaded,
because `JsonCommandsSpec` runs through `RedisCluster`. `./docker-redis.sh status` prints `MODULE LIST` of every port.
The JSON tests are tagged `RedisJson` (`-l RedisJson` skips them on a server without the module).
The circe version the build uses can be changed for a run: `./docker-redis.sh test -Dcirce.version=0.14.17 "+test"`.

Docker must be running. From the repository root:

    ./docker-redis.sh start             # start the servers
    ./docker-redis.sh test "+test"      # run sbt in a container next to the servers (both artifacts, all Scala versions)
    ./docker-redis.sh status            # containers, image digest, redis_version of every server
    ./docker-redis.sh stop              # stop the servers

The test container shares the servers' network namespace, so every server is `127.0.0.1` for the
suite, as it expects (the failover tests also listen on a killed node's port for a few seconds, as a forwarder in
front of a dead server does, and run a proxy between a client and 6379 that cuts, refuses or closes connections) (MIGRATE between 6379 and 6380, the cluster's announced addresses, the SLOWLOG
client address). Restart the servers before a new run: some specs change the state of 6380.
Other sbt commands work the same way, e.g. `./docker-redis.sh test "testOnly scredis.commands.StringCommandsSpec"`.

The build has two projects, `rescredis` (Akka) and `rescredisPekko` (Pekko, generated from the same
source, see docs/pekko-generation.md); `test` at the root runs both. One artifact:
`./docker-redis.sh test "rescredis/test"` or `"rescredisPekko/test"`.
