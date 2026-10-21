# Testing setup

The tests need real Redis servers. They all run in Docker containers (nothing is installed on the
host) and are Redis 7.4.2 from the Redis Stack image `redis/redis-stack-server:7.4.0-v2`, the version
production runs:

* `6379`: plain server, no password
* `6380`: configured by `src/test/resources/redis-6380.conf` (password `foobar`, ACL user `tester`)
* `7000`-`7005`: Redis cluster (3 masters, 3 replicas)

Docker must be running. From the repository root:

    ./docker-redis.sh start             # start the servers
    ./docker-redis.sh test "+test"      # run sbt in a container next to the servers (all Scala versions)
    ./docker-redis.sh status            # containers, image digest, redis_version of every server
    ./docker-redis.sh stop              # stop the servers

The test container shares the servers' network namespace, so every server is `127.0.0.1` for the
suite, as it expects (MIGRATE between 6379 and 6380, the cluster's announced addresses, the SLOWLOG
client address). Restart the servers before a new run: some specs change the state of 6380.
Other sbt commands work the same way, e.g. `./docker-redis.sh test "testOnly scredis.commands.StringCommandsSpec"`.
