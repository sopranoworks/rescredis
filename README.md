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
* JSON commands for Redis Stack (the code of scredis-json 1.0.1, see "JSON").

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
* JSON commands (Redis Stack / RedisJSON) on `Client`, `Redis` and `RedisCluster`: `client.Json.set(...)`
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

## JSON

The JSON commands of Redis Stack (the RedisJSON module) are part of `Client`, `Redis` and `RedisCluster`, under
`Json`: `client.Json.set(key, path, value)`, `client.Json.get[T](key)`, ... All of these exist: `set`, `setNX`,
`setXX`, `get`, `merge`, `del`, `toggle`, `clear`, `types`, `strLen`, `strAppend`, `numIncBy`, `mSet`, `mGet`,
`objLen`, `objKeys`, `arrAppend`, `arrIndex`, `arrInsert`, `arrLen`, `arrPop`, `arrTrim` and `debugMemory`, plus the
variants whose names end in `Raw` (`setRaw`, `setRawNX`, `setRawXX`, `getRaw`, `mergeRaw`, `mSetRaw`, `mGetRaw`,
`arrAppendRaw`, `arrIndexRaw`, `arrInsertRaw`, `arrPopRaw`), which take a `Writer` or return the circe `Json`.
They need a server with the JSON module (Redis Stack; RedisJSON 2.6 or later for `merge` and `mSet`); on a plain
Redis they fail with an error reply.

```scala
import io.circe.generic.auto._   // or your own circe Encoder / Decoder (circe-generic is not a dependency of rescredis)
import scala.concurrent.ExecutionContext.Implicits.global

case class User(name: String, score: Int)

val client = Client()                                  // or Redis(), or RedisCluster(Server("localhost", 7000))
for {
  _    <- client.Json.set("user:1", "$", User("alice", 10))   // Future[Boolean]
  user <- client.Json.get[User]("user:1")                     // Future[Option[User]]
  _    <- client.Json.numIncBy("user:1", "$.score", 5)        // Future[List[Option[Long]]]
} yield user
```

* The values are converted with [circe](https://circe.github.io/circe/): the methods without `Raw` take an
  `io.circe.Encoder` or `io.circe.Decoder`; the `Raw` ones take a `scredis.serialization.Writer` (for circe's `Json`
  use `import scredis.serialization.JsonImplicits._`) and return `io.circe.Json`.
* **circe.** rescredis depends on `circe-core` and `circe-parser` 0.14.0, the lowest 0.14 release, with no version
  range. circe 0.14 is binary compatible within the line, so a newer 0.14.x on your classpath is used
  instead (the suite is also run with 0.14.17). `circe-generic` is not a dependency; add it if you derive codecs.
* Inside `package scredis`, `io.circe` means `scredis.io.circe`: write `_root_.io.circe` there.
* `RedisCluster`: `mGet` and `mSet` go to the node of the first key, like `MGET` and `MSET`: all keys must be in one
  hash slot (use a hash tag, `{user}:1`).
* `numIncBy` returns integers: if the new value is not an integer the future fails (the server has applied the increment).
  `JSON.NUMMULTBY` (deprecated by RedisJSON) is not included.

### Migrating from scredis-json

scredis-json 1.0.1 (`com.sopranoworks %% scredis-json`, whose code is now part of rescredis) copied `Redis`,
`RedisCluster` and `Client` to add the commands. Replace the class; `x.Json.<command>(...)` is unchanged (names,
parameters, defaults, result types):

| scredis-json | rescredis |
|---|---|
| `RedisJson` (`RedisJson(...)`, `RedisJson.withActorSystem(...)`) | `Redis` (`Redis(...)`, `Redis.withActorSystem(...)`): same parameters, defaults and ActorSystem handling |
| `RedisJsonCluster` | `RedisCluster`: same parameters and defaults |
| `JsonClient` | `Client`: same parameters and defaults |
| `scredis.serialization.JsonImplicits`, `JsonWriter` | the same names |

The differences a caller can see:

* The all-parameters constructors `new RedisJson(systemOrName = ..., host = ...)` and `new RedisJsonCluster(nodes = ...)`
  were public; those of `Redis` and `RedisCluster` are `private[scredis]`. Use `Redis(...)` / `Redis.withActorSystem(...)` and
  `RedisCluster(...)`, which take the same arguments.
* Dependencies: the artifact is `rescredis` (or `rescredis-pekko`), not `scredis-json` plus `com.github.scredis:scredis`;
  the two cannot be on one classpath. The licence is Apache-2.0 (scredis-json declared MIT).
* The bug fixes below change what some calls return.

Bug fixes in the JSON commands (all covered by tests that fail without them):

* `strAppend` built the JSON string with its own quote handling, which read a backslash as the start of an escape: a
  backslash in the value changed or broke the string. The value is now appended as it is.
* `arrIndex` / `arrIndexRaw` ignored the `Writer` of the value.
* `mGet` and `mSet` failed on `RedisCluster` ("This command is not supported for clusters").
* `numIncBy` returned an empty list for a legacy path (`.field`) and for a result that is not an integer.
* `types` failed for a key that does not exist; it returns an empty list.

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

The API documentation is the `-javadoc.jar` published with each artifact. javadoc.io serves published
javadoc jars at `https://javadoc.io/doc/<groupId>/<artifactId>/<version>`, so for the Akka artifact on Scala 2.13
it would be `https://javadoc.io/doc/com.sopranoworks/rescredis_2.13/1.0.0` (and `rescredis-pekko_2.13` for
Pekko); I have not checked that it is available.

The guide sources are in `src/main/paradox` (upstream's guide for scredis 2.4.3, whose API rescredis has) and
are converted with the sbt plugin [paradox](https://developer.lightbend.com/docs/paradox/current/index.html):
`sbt paradox` writes the html files to `target/paradox/site/main`. No generated documentation is committed.

All snippets should be added to `Examples.scala` in test scope and included from the documentation to
ensure snippets always compile with the latest version.

## License

The whole of rescredis is licensed under the Apache License, Version 2.0; see [LICENSE](LICENSE) and
[NOTICE](NOTICE).

* The code derived from scredis: Copyright (c) 2013 Livestream LLC.
* The changes (fixes, Redis 7 compatibility, Apache Pekko variant, Scala 3 support) and the files added
  by Sopranoworks: Copyright 2023-2026 Sopranoworks. Files written wholly by Sopranoworks carry
  `Copyright 2026 Sopranoworks` and `SPDX-License-Identifier: Apache-2.0`; files derived from scredis keep their
  form and carry a "Modified by Sopranoworks" notice where they were changed.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
