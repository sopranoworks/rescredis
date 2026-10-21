# scredis-pekko

scredis-pekko is a Pekko port of [scredis](https://github.com/scredis/scredis), a reactive, non-blocking
and fast Scala [Redis](http://redis.io) client. It replaces Akka with [Apache Pekko](https://pekko.apache.org/)
and adds Scala 3 support. It is derived from scredis 2.4.3 (upstream tag `v2.4.3`, commit
`2e1928d7a961856d2bb060ccec2a92dac9745343`).

This is an independent fork. It is not affiliated with or endorsed by the scredis maintainers, Livestream LLC,
or the Apache Software Foundation. "Apache Pekko" is the name of a project of the Apache Software Foundation.

## Versioning

scredis-pekko X has the API of scredis X, ported to Pekko, with upstream bug fixes made after X backported.
A fix to the port alone is released as X.1, X.2, …

### Backported upstream fixes (relative to scredis 2.4.3)

* `05592f8` — `RedisCluster.quit()` blocked (upstream #169).
* `5debcf4` — the Redis remote address is created unresolved, so a reconnect resolves the host again (failover).
* `52dfd27` — removes a stray comment line in the `KeyCommands` documentation (upstream #191).

Upstream features added after 2.4.3 (`SMISMEMBER`, stream commands) are not included.

## Features
* Supports all Redis commands up to v3.0.0
* Built on top of Pekko non-blocking IO
* Automatic reconnection
* Automatic pipelining
* Transactions
* Pub/Sub
  * Subscribe selectively with partial functions
  * Tracked Subscribe and Unsubscribe commands (they return a Future as any other commands)
  * Automatically resubscribes to previously subscribed channels/patterns upon reconnection
* Cluster support via `RedisCluster`
* Customizable serialization and deserialization of command inputs and outputs
* Fully configurable
  * Pekko dispatchers
  * Pipelined write batch size
  * Receive timeout
  * TCP buffer size hints
  * Request encoding buffer pool
  * Concurrent requests cap (bounded memory consumption)

## Getting started

### Binaries

```scala
libraryDependencies += "com.sopranoworks" %% "scredis-pekko" % "2.4.3"
```

Built for Scala 2.12, 2.13 and 3, against Apache Pekko 1.7.1.

scredis-pekko is a drop-in replacement for code: the Scala package is still `scredis`.
For the same reason it cannot be on the same classpath as `com.github.scredis:scredis`.

### Configuration

The default configuration is in [reference.conf](src/main/resources/reference.conf). The canonical keys are
`pekko` and `scredis.io.pekko`.

For configuration written for Akka, `akka` and `scredis.io.akka` are read as well, both in the configuration
scredis reads and in the `ActorSystem` that scredis creates itself (an `ActorSystem` that you pass in
with `withActorSystem` or as an implicit is yours and is not touched). Write a key under one of them,
not both: if the same key is set under both, the `akka` value is used. Values are not translated and
must be valid for Pekko; for example, a mailbox type must be `org.apache.pekko.dispatch.BoundedMailbox`.

### Quick example
```scala
import scredis._
import scala.util.{ Success, Failure }

// Creates a Redis instance with default configuration.
// See reference.conf for the complete list of configurable parameters.
val redis = Redis()

// Import internal ActorSystem's dispatcher (execution context) to register callbacks
import redis.dispatcher

// Executing a non-blocking command and registering callbacks on the returned Future
redis.hGetAll("my-hash") onComplete {
  case Success(content) => println(content)
  case Failure(e) => e.printStackTrace()
}

// Executes a blocking command using the internal, lazily initialized BlockingClient
redis.blocking.blPop(0, "queue")

// Shutdown all initialized internal clients along with the ActorSystem
redis.quit()
```

```scala
import scredis.PubSubMessage
import scredis.stringReader
private val subscriptionHandler: Function[PubSubMessage, Unit] = {
  case m: PubSubMessage.Subscribe => println(s"Subscribed to channel ${m.channel}")
  case m: PubSubMessage.Message => println(s"Received message for channel ${m.channel} with data ${m.readAs[String]()}")
  case m: PubSubMessage.Unsubscribe => println(s"Unsubscribed from channel ${m.channelOpt}")
  case m: PubSubMessage.PSubscribe => println(s"Subscribed to channels matching pattern ${m.pattern}")
  case m: PubSubMessage.PMessage => println(s"Received message for pattern ${m.pattern} on channel ${m.channel} with data ${m.readAs[String]()}")
  case m: PubSubMessage.PUnsubscribe => println(s"Unsubscribed from pattern matching ${m.patternOpt}")
  case e: PubSubMessage.Error => println(s"Scredis received error $e")
}

// Creates a Redis instance with default configuration.
// Provide custom function handling pub/sub related events
val redis = scredis.Redis(subscription = subscriptionHandler)

// Subscribes to a Pub/Sub channel using the internal, lazily initialized SubscriberClient
redis.subscriber.subscribe("My Channel")

// Later unsubscribe from channel
redis.subscriber.unsubscribe("My Channel")

// Shutdown all initialized internal clients along with the ActorSystem
redis.quit()
```

## Tests and Redis versions

scredis-pekko is tested against Redis 7.4.2 (Redis Stack, image `redis/redis-stack-server:7.4.0-v2`), on
Scala 2.12, 2.13 and 3.

The tests need Redis servers on ports 6379, 6380 (configured by `src/test/resources/redis-6380.conf`)
and a Redis cluster on ports 7000-7005. They run in Docker containers only; see [testing.md](testing.md):

```
./docker-redis.sh start
./docker-redis.sh test "+test"
./docker-redis.sh stop
```

## API notes

* `auth("")` means "no password", for `Redis`, `Client`, `BlockingClient` and `SubscriberClient`.
  Redis 6.2 and later refuse `AUTH ""` when the server has no password, so it is sent as
  `AUTH default ""`: it succeeds when the server has no password set and fails with an error when it has
  one. The stored credentials are cleared, so a reconnect does not send `AUTH`, and subscriptions are kept.
* Replies of Redis 7 are supported: `CLUSTER SLOTS` (node metadata), `COMMAND` / `COMMAND INFO` (extra
  fields) and `ROLE` (replication states `handshake`, `none` and `unknown` in `Role.ReplicationState`).

## Documentation

The upstream documentation is at [https://scredis.github.io/scredis/](https://scredis.github.io/scredis/);
it describes scredis on Akka.

Documentation sources are in `src/main/paradox` and are converted with the sbt plugin
[paradox](https://developer.lightbend.com/docs/paradox/current/index.html): `sbt paradox` writes the
html files to `target/paradox/site/main`. `gen-doc.sh` generates the documentation and copies it to `./docs`.

All snippets should be added to `Examples.scala` in test scope and included from the documentation to
ensure snippets always compile with the latest version.

## License

Copyright (c) 2013 Livestream LLC. All rights reserved.

Modifications (port from Akka to Apache Pekko, Scala 3 support) Copyright 2023-2026 Sopranoworks.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. See accompanying [LICENSE](LICENSE) file and [NOTICE](NOTICE).
