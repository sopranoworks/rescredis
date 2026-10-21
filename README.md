# rescredis

rescredis is [scredis](https://github.com/scredis/scredis) (a reactive, non-blocking and fast Scala
[Redis](http://redis.io) client), maintained: rescredis 1.0.0 is scredis 2.4.3 plus the fixes listed
below. It comes in two artifacts built from one source, which always have the same version and the same
behaviour:

| Artifact | Built on | Dependency |
|---|---|---|
| `com.sopranoworks %% rescredis` | Akka 2.6 (the Apache-2.0 line) | `akka-actor` 2.6.21 |
| `com.sopranoworks %% rescredis-pekko` | Apache Pekko | `pekko-actor` 1.7.1 |

Both are built for Scala 2.12, 2.13 and 3.

This is an independent project. It is not affiliated with or endorsed by the scredis maintainers,
Livestream LLC, or the Apache Software Foundation. "Apache Pekko" is the name of a project of the
Apache Software Foundation.

## Versioning

SemVer: fixes are patch releases, features are minor releases. The Akka and the Pekko artifact always have
the same version; the number says nothing about upstream. The relation to scredis is stated here:
rescredis 1.0.0 = scredis 2.4.3 (upstream tag `v2.4.3`, commit `2e1928d7a961856d2bb060ccec2a92dac9745343`) plus

* `05592f8` — `RedisCluster.quit()` blocked (upstream #169).
* `5debcf4` — the Redis remote address is created unresolved, so a reconnect resolves the host again (failover).
* `52dfd27` — removes a stray comment line in the `KeyCommands` documentation (upstream #191).
* Compatibility with Redis 7.4 (see "API notes").
* `Redis.auth(password, username)` passes the user name to every client it owns.
* Scala 3 support.
* The Apache Pekko artifact.

Upstream features added after 2.4.3 (`SMISMEMBER`, stream commands) are not included.

## Features
* Supports all Redis commands up to v3.0.0
* Built on top of Akka (rescredis) or Apache Pekko (rescredis-pekko) non-blocking IO
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
  * Akka or Pekko dispatchers
  * Pipelined write batch size
  * Receive timeout
  * TCP buffer size hints
  * Request encoding buffer pool
  * Concurrent requests cap (bounded memory consumption)

## Getting started

### Binaries

```scala
libraryDependencies += "com.sopranoworks" %% "rescredis" % "1.0.0"        // Akka
libraryDependencies += "com.sopranoworks" %% "rescredis-pekko" % "1.0.0"  // Apache Pekko
```

### Migrating from scredis

The Scala package is still `scredis`, so code does not change: replace
`"com.github.scredis" %% "scredis" % "2.4.3"` with one of the dependencies above. For the same reason
rescredis cannot be on the same classpath as `com.github.scredis:scredis`, and the two artifacts of rescredis
cannot be on the same classpath either.

### Configuration

The default configuration is in `reference.conf` (`src/main/resources-akka` and `src/main/resources-pekko`).
The configuration keys differ per artifact:

* `rescredis` (Akka): `akka` and `scredis.io.akka`, exactly as in scredis.
* `rescredis-pekko`: `pekko` and `scredis.io.pekko`. Configuration written for Akka (`akka` and
  `scredis.io.akka`) is read as well, both in the configuration scredis reads and in the `ActorSystem` that
  scredis creates itself (an `ActorSystem` that you pass in with `withActorSystem` or as an implicit is yours
  and is not touched). Write a key under one of them, not both: if the same key is set under both, the `akka`
  value is used. Values are not translated and must be valid for Pekko; for example, a mailbox type must be
  `org.apache.pekko.dispatch.BoundedMailbox`.

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

rescredis and rescredis-pekko are tested against Redis 7.4.2 (Redis Stack, image
`redis/redis-stack-server:7.4.0-v2`), on Scala 2.12, 2.13 and 3.

The tests need Redis servers on ports 6379, 6380 (configured by `src/test/resources/redis-6380.conf`)
and a Redis cluster on ports 7000-7005. They run in Docker containers only; see [testing.md](testing.md):

```
./docker-redis.sh start
./docker-redis.sh test "+test"      # both artifacts, every Scala version
./docker-redis.sh stop
```

## API notes

* `auth("")` means "no password", for `Redis`, `Client`, `BlockingClient` and `SubscriberClient`.
  Redis 6.2 and later refuse `AUTH ""` when the server has no password, so it is sent as
  `AUTH default ""`: it succeeds when the server has no password set and fails with an error when it has
  one. The stored credentials are cleared, so a reconnect does not send `AUTH`, and subscriptions are kept.
* `Redis.auth(password, username)` authenticates the non-blocking, the blocking and the subscriber client with
  the same user name.
* Replies of Redis 7 are supported: `CLUSTER SLOTS` (node metadata), `COMMAND` / `COMMAND INFO` (extra
  fields) and `ROLE` (replication states `handshake`, `none` and `unknown` in `Role.ReplicationState`).

## How the Pekko artifact is generated

There is one source, written for Akka (`src/main`, `src/test`). `rescredis-pekko` is built from it: its
sources are generated at build time (`akka.*` becomes `org.apache.pekko.*`) into `target/`, plus a few files
that exist only in the Akka or only in the Pekko variant. Fixes go into the Akka source. See
[docs/pekko-generation.md](docs/pekko-generation.md): what is rewritten, how to run it, how to add a rule.

## Documentation

The upstream documentation is at [https://scredis.github.io/scredis/](https://scredis.github.io/scredis/);
it describes scredis 2.4.3, whose API rescredis has.

Documentation sources are in `src/main/paradox` and are converted with the sbt plugin
[paradox](https://developer.lightbend.com/docs/paradox/current/index.html): `sbt paradox` writes the
html files to `target/paradox/site/main`. `gen-doc.sh` generates the documentation and copies it to `./docs`.

All snippets should be added to `Examples.scala` in test scope and included from the documentation to
ensure snippets always compile with the latest version.

## License

Copyright (c) 2013 Livestream LLC. All rights reserved.

Modifications (rescredis: fixes, Redis 7 compatibility, Apache Pekko variant, Scala 3 support)
Copyright 2023-2026 Sopranoworks.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. See accompanying [LICENSE](LICENSE) file and [NOTICE](NOTICE).
