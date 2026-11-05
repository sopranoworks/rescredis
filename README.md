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

Upstream features added after 2.4.3 (`SMISMEMBER`, stream commands) are not included; rescredis adds its own
stream commands in 1.1.0.

Changes:

* 1.1.0 (in development, `1.1.0-SNAPSHOT`): stream commands `XADD`, `XLEN`, `XRANGE`, `XREVRANGE`, `XDEL`,
  `XTRIM`, `XREAD` and `XREAD ... BLOCK` (see "Streams"); consumer groups are not included yet. Fix (inherited
  from scredis): after a connection died, a reply could complete another request, or a request never completed,
  on `Client`, `Redis`, their blocking and subscriber clients, and `RedisCluster` (see "Connections").
  Reconnecting with a backoff, logged once per change. `WAIT`; on `RedisCluster`: `wait(key, ...)`,
  `SCRIPT LOAD` / `EXISTS` / `FLUSH` sent to every master, and a blocking client (`RedisCluster.blockingClient`).
  Connection health (state, subscription, idle `PING`) and availability objects to wait on; connection errors
  classified as `RedisNotSentException` and `RedisOutcomeUnknownException` (both `RedisIOException`s; see
  "Connections"). On `RedisCluster`: node health and slot reachability, the slots read again when a node's
  connection changes state; a request to a master known to be down fails at once as not sent, and only requests
  that were not sent are sent again. Fix: a permit of `max-concurrent-requests` is given back exactly once per
  request (some were given back without having been taken, so the limit grew). The blocking client is
  `blockingClient` / `withBlocking` on `Redis` and `RedisCluster`, with the same error classes and policy (a
  blocking call whose deadline passes fails with `RedisOutcomeUnknownException`); **`Redis.blocking` is
  deprecated** and keeps the errors it had in 1.0.0 (see "Connections", "Blocking client"). Retry helpers:
  `withRetry` on `Client`, `Redis`, `RedisCluster` and the blocking clients, and `RetryPolicy`.

## Features
* Supports all Redis commands up to v3.0.0
* Built on top of Akka (rescredis) or Apache Pekko (rescredis-pekko) non-blocking IO
* Automatic reconnection, with a backoff
* Connection health, availability events, connection errors classified (not sent / outcome unknown)
* Retry helpers (`withRetry`, `RetryPolicy`) that wait on the availability instead of polling
* Automatic pipelining
* Transactions
* Pub/Sub
  * Subscribe selectively with partial functions
  * Tracked Subscribe and Unsubscribe commands (they return a Future as any other commands)
  * Automatically resubscribes to previously subscribed channels/patterns upon reconnection
* Cluster support via `RedisCluster`, with blocking commands (`RedisCluster.blockingClient`)
* JSON commands (Redis Stack / RedisJSON) on `Client`, `Redis` and `RedisCluster`: `client.Json.set(...)`
* Stream commands (`XADD`, `XRANGE`, `XREAD`, ...; `XREAD ... BLOCK` on the blocking client)
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

One behaviour needs attention: `auth("")` sends `AUTH ""`, exactly as given. Redis 6.2 and later refuse it with an
error when the server has no password. scredis relied on `auth("")` to mean "no password" on older servers (the
empty password was stored, and sent again on reconnects); where your code does that, call `clearCredentials()`
instead: it makes the connection stop sending `AUTH` when it reconnects, and sends nothing to Redis.

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

### Connections

**Replies.** A connection pairs the n-th reply with the n-th request it sent. When a connection dies (the server
crashed or restarted, the connection was reset, a master failed over), every request sent on it that has no reply
yet fails with a `RedisOutcomeUnknownException`; requests waiting to be sent go out on the new connection. A reply completes
only the request it belongs to: a request gets its own reply or fails, never another request's. (scredis, and
rescredis up to 1.0.0, could complete a request with the reply of another one after a connection died, or leave
a request without an answer; see the 1.1.0 change above.) A request sent again by `RedisCluster` after `MOVED`,
`ASK`, `TRYAGAIN`, `CLUSTERDOWN` or because it was not sent is the same command, as a new request object.

**Reconnecting.** After the loss of a connection on which Redis had answered, the first attempt is made at once.
After an attempt that fails (no connection, or a connection closed before Redis answered on it, as through a
forwarder or load balancer whose server is down), the next one waits `min-backoff`, then twice as long each time,
up to `max-backoff`:

```
scredis.io.reconnect {
  min-backoff = 100 milliseconds   # default
  max-backoff = 2 seconds          # default
}
```

These two are read from the application's configuration for every client (as `max-cluster-hash-misses`).
Requests sent while an attempt is in progress wait for it and fail if it fails; requests sent while the client
waits for its next attempt fail at once with a `RedisNotSentException`, as do all requests with
`fail-command-on-connecting = true`. The connection logs the loss once (warning, with the
cause) and its return once (info, with the number of attempts and the time); single attempts are logged at debug
level, without stack traces. The receive timeout (`scredis.io.receive-timeout`) covers every request waiting for
its reply. A `RedisCluster` keeps a node's connection after errors; it reconnects with the backoff.

**WAIT.** `wait(numReplicas, timeout)` on `Client`, `Redis` and transactions sends `WAIT numreplicas timeout`
(timeout in milliseconds, 0 = no limit) and returns what Redis answers (the number of replicas that acknowledged
the writes). Redis applies it to the connection it is sent on: the writes sent before it on the same connection.
While it blocks, the commands sent after it on that connection wait behind it, and the receive timeout applies to
it. On a replica Redis answers an error. For `RedisCluster` see "Cluster".

**Health.** Every connection (`Client`, `Redis`, their blocking (`blockingClient`) and `subscriber` clients, each
node's connection of `RedisCluster`, the connections of `RedisCluster.blockingClient`) has a `health`: its `state`, one of
`ConnectionState.Connected` (connected and initialised: `AUTH`, `SELECT` and `CLIENT SETNAME`, as configured, have
been answered; without them, a `PING`), `Reconnecting(attempt, since, lastCause)` (the first connection is
attempt 1 with the cause "not connected yet") and `Closed` (after `QUIT`). `health.subscribe(listener)` calls the
listener with the current state, then with every change, in order, on a thread of rescredis (it must not block),
and returns a `HealthSubscription` to `cancel()`; a function rather than an actor event stream, so that it is the
same on both artifacts and needs no actor. While nothing is outstanding on a connection and nothing has been sent
or received for `idle-ping-interval`, the client sends a `PING` of its own on it (also on a subscribed connection),
so that a connection that died without being closed is found without waiting for the next request: when its reply
has not come within `idle-ping-timeout`, the connection is closed and made again. The `PING` and its reply never
reach a caller.

```
scredis.io.health {
  idle-ping-interval = 1 second    # default; 0 = no idle PING
  idle-ping-timeout = 2 seconds    # default
}
```

**Availability.** `client.availability` (`health.availability`) is an `Availability`: `whenAvailable: Future[Unit]`
is completed while the connection is connected and initialised, and otherwise completes when it is again;
`unavailableSince` and `cause` say since when and why it is not. There is one object per outage, shared by
everyone who asks and by the errors of that outage; the next outage has a new one, and rescredis keeps no
reference to it once it is completed. When the client is closed (`quit()`), `whenAvailable` fails with a
`RedisIOException`. On `RedisCluster`, `cluster.availability(key)` is that of the master of the key's slot and
`cluster.availability` that of the whole cluster (see "Cluster").

**Errors.** A request that fails because of its connection fails with one of two `RedisIOException`s, which say
whether Redis may have executed it; both carry the `availability` of the connection (on `RedisCluster`, of the
key's slot). What Redis answers with an error keeps its own types (`RedisErrorResponseException`,
`RedisClusterErrorResponseException`, ...).

| Class | When | Executed? |
|---|---|---|
| `RedisNotSentException` | no connection (an attempt failed, or the client waits for its next attempt); addressed to a node or a slot's master known to be down; `fail-command-on-connecting` while connecting; after `QUIT`; on `RedisCluster`, `CLUSTERDOWN` / `TRYAGAIN` after the retries; a blocking call made while another one blocks | certainly not: safe to send again |
| `RedisOutcomeUnknownException` | written, and the connection was lost before the reply, or the reply did not come within the receive timeout, or a blocking call's deadline (block time + 2 s) passed | unknown: send again only if executing twice is harmless |

rescredis sends again by itself only what was not sent (on `RedisCluster`, after `MOVED`, `ASK`, `TRYAGAIN`,
`CLUSTERDOWN` and a not sent request); a request whose outcome is unknown goes back to the caller, so a write is
never executed twice by the library; the blocking clients follow the same policy. Waiting for the connection and
sending again, by hand:

```scala
def setWithRetry(key: String, value: String): Future[Boolean] =
  client.set(key, value).recoverWith {
    case e: RedisNotSentException =>                // not executed: send it again once usable
      e.availability.whenAvailable.flatMap(_ => client.set(key, value))
    case e: RedisOutcomeUnknownException =>         // SET is idempotent: sending it again is harmless here
      e.availability.whenAvailable.flatMap(_ => client.set(key, value))
  }
```

**Retrying.** `withRetry(f, maxWaitingDuration = Duration.Inf, idempotent = false)` on `Client`, `Redis` and
`RedisCluster` (returning a `Future`) and on the blocking clients (returning a `Try`) runs `f(client)`; when it
fails with a retryable error it waits on **that error's** `availability.whenAvailable` (no polling, no sleeping)
and runs `f` again, and so on. Retryable: `RedisNotSentException`; also `RedisOutcomeUnknownException` with
`idempotent = true`, which is the caller's statement that executing `f` twice is harmless. Redis error replies are
never retried. `maxWaitingDuration` is counted on the wall clock from the first attempt; when it is used up, or
the client is closed meanwhile, the helper fails with the last error. There is no retry count unless the caller
gives one. The policy is a value, `RetryPolicy(maxWaitingDuration, retryOutcomeUnknown, maxRetries)`, with
`retry(() => future)` and `retryBlocking(() => tryValue)`, for helpers of one's own.

```scala
redis.withRetry(_.get[String]("key"), maxWaitingDuration = 30.seconds)       // Future[Option[String]]
cluster.withRetry(_.incr("counter"))                                          // not retried if the outcome is unknown
cluster.withRetry(_.set("key", "value"), idempotent = true)                   // SET may be sent twice: harmless
redis.withBlocking(_.withRetry(_.blPop[String](5, "jobs")))                   // Try[Option[(String, String)]]
RetryPolicy(maxWaitingDuration = 10.seconds).retry(() => client.ping())
```

**Blocking client.** `redis.blockingClient` is the `BlockingClient` of a `Redis`: one instance, created on first
use and kept for the life of the `Redis`, which `quit()` closes; keep it, or use it through
`redis.withBlocking(f)`, which is `f(redis.blockingClient)` (no client is made per call). One call is made at a
time. Its calls fail with the classes above: no connection, a call made while another one blocks (with an
availability that completes when that call ends) and a call after `quit()` as not sent; a call whose connection
was lost before its reply, or whose deadline (the block time + 2 s) passed, as outcome unknown (the
`TimeoutException` as its cause for the deadline). It does not send a call again.

`Redis.blocking` is **deprecated**: refactor to `blockingClient` / `withBlocking` and the error classes as soon as
possible. It is a client of its own (a second connection when both are used) and fails as it did in 1.0.0: a call
whose deadline passes with a bare `TimeoutException`, the connection errors as plain `RedisIOException`s (same
messages), and a call made while it waits for its next attempt to connect makes the attempt at once, as before.
Those errors hide what the caller needs to know: a call that was never sent and a call that Redis may have
executed (its connection was lost before the reply) look the same, so the caller can neither retry safely nor
know that it must not. A changed behaviour under the old name would have broken callers silently at run time (a
`catch` of `TimeoutException` compiles and stops matching); that is why the new behaviour has a new name.

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
redis.blockingClient.blPop(0, "queue")

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

## Cluster

`RedisCluster` sends a command to the node that holds the slot of its key, follows `MOVED` / `ASK` redirections
and retries `TRYAGAIN` / `CLUSTERDOWN`. A command with several keys (`MGET`, `MSET`, `MSETNX`, `DEL`,
`RENAME`, `SUNION`, `SINTERSTORE`, `ZUNIONSTORE`, `BITOP`, `PFCOUNT`, `PFMERGE`, `RPOPLPUSH`, `SMOVE`,
`SORT ... STORE`, `EVAL` with several keys, `XREAD` with several streams, `Json.mGet`, `Json.mSet`, ...) is routed by its first key, and Redis
Cluster executes it only if **all its keys hash to one slot**. Put a hash tag in the keys so that they do:
`{user:1}:name` and `{user:1}:email` are in the same slot, because only the part between the braces is hashed.

Keys in different slots fail the future with `RedisClusterErrorResponseException(CrossSlot, ...)`, whose message
is `CROSSSLOT Keys in request don't hash to the same slot`, and nothing is written. rescredis does not split the
command into several (that would lose the atomicity of `MSET` and `JSON.MSET`) and does not retry `CROSSSLOT`.
`JSON.MGET` is routed by its first key too, but the module declares only that key to Redis, so Redis raises no
`CROSSSLOT`: on RedisJSON 2.8.8 the keys that hash to other slots come back as `None`, without an error (the
first key's value is returned). rescredis returns what Redis answers; use one slot (a hash tag) for multi-key
JSON reads.

```scala
val cluster = RedisCluster(Server("localhost", 7000))
cluster.mSet(Map("{user:1}:name" -> "Ann", "{user:1}:email" -> "ann@example.org"))   // one slot: works
cluster.mSet(Map("user:1:name" -> "Ann", "user:2:name" -> "Bob"))                      // CROSSSLOT
```

**WAIT.** `WAIT` has no key, and Redis applies it to the connection it is sent on. `cluster.wait(key, numReplicas,
timeout)` sends `WAIT numreplicas timeout` on the connection to the master of `key`'s slot (the key is not sent):
the connection that writes to keys of that slot went through (`RedisCluster` has one connection per node). Nothing
else is done on the client side; when the slot cache is out of date WAIT may reach another node (a replica answers
an error). `cluster.wait(numReplicas, timeout)` without a key fails with `RedisInvalidArgumentException`, as other
keyless commands do.

```scala
cluster.set("{room:7}:state", bytes).flatMap(_ => cluster.wait("{room:7}:state", 1, 100.millis))  // Future[Long]
```

**Health and partial failures.** `cluster.health` has the `health` of the connection to every node it knows
(`nodes`), and slot reachability: a slot is reachable when the slot cache names a master for it and the connection
to that master is connected (`isReachable(slot)`, `unreachableSlots`). `subscribe` calls a listener with every
node's state changes. When a node's connection changes state, the client reads the slots again (`CLUSTER SLOTS`,
after `refresh-debounce`), and while some slot is unreachable it reads them again every
`unreachable-refresh-interval`, so that a replica promoted by a failover is found without waiting for a `MOVED`:

```
scredis.io.cluster {
  refresh-debounce = 100 milliseconds              # default
  unreachable-refresh-interval = 500 milliseconds  # default
}
```

A request to a master known to be down (its connection waits for the next reconnect attempt) is not sent to it:
it is sent once to a connected node instead, which answers `MOVED` without executing it; when that names the
master that is down (the failure is not handled by the cluster yet), or the node answers `CLUSTERDOWN` or
`TRYAGAIN`, the request fails at once with a `RedisNotSentException` carrying `cluster.availability(key)`, which
completes when the slot has a reachable master again (after the promotion, once the client has read the slots).
A request whose outcome is unknown is not sent again. With `cluster-require-full-coverage yes` the other masters
answer `CLUSTERDOWN` between the failure being detected and the promotion; such requests are retried for up to
`connect-timeout` and then fail as not sent; with `no` they are served.

```scala
cluster.set(key, value).recoverWith {
  case e: RedisNotSentException => e.availability.whenAvailable.flatMap(_ => cluster.set(key, value))
}
```

**Scripts.** `scriptLoad`, `scriptExists` and `scriptFlush` have no key; on `RedisCluster` they are sent to every
master (read from the cluster with `CLUSTER SLOTS` first), which is the routing a keyless script command needs on a
cluster (the command is not changed). `scriptLoad` returns the SHA1 once every master has answered it (it is the
same on every node); `scriptExists` is true for a script only if every master has it; an error from any master
fails the call with that error. The script cache is not replicated: a master added later, or a replica promoted by
a failover, does not have the script, and `EVALSHA` there is answered `NOSCRIPT` (a
`RedisErrorResponseException`); load it again, or use `EVAL`. `SCRIPT KILL` is still not supported on
`RedisCluster` (it would have to go to the node that runs the script).

**Blocking commands.** `cluster.blockingClient` (one instance, closed by `quit()`; `cluster.withBlocking(f)` uses
it) has `blPop`, `brPop`, `brPopLPush` and `xRead(blockMillis, ...)` with the signatures of `Redis.blockingClient`.
A call is routed by its (first, or source) key to the master of the key's slot, on a blocking connection of its
own to that node, so a blocked call does not delay the cluster's other commands; one call is made at a time. Keys
in different slots are sent as they are (Redis answers `CROSSSLOT`). It follows the policy of the cluster's other
commands: a `MOVED` reply updates the slot cache and the call goes to the named node at once; when the slot's
master is known to be down, the call is asked once of a connected node (a promoted replica is found by its
`MOVED`) and otherwise fails at once as not sent; on `CLUSTERDOWN`, `TRYAGAIN` or a call not sent the call waits
(the reconnect backoff), reads the slots again and is routed again, within its deadline (the block time + 2 s,
none for 0), after which it fails as not sent. A call that was written and whose connection was lost before the
reply, or whose deadline passed, fails as outcome unknown and is **not** sent again (an element popped on a master
that died before its reply arrived is lost with that reply); after a deadline, the node's blocking connection is
replaced. Both errors carry the availability of the key's slot, so `withRetry` makes the call again once the slot
has a reachable master. `cluster.blockingClient.health` has the health of its connection to each node.

```scala
cluster.blockingClient.blPop[String](5, "jobs")                // Try[Option[(String, String)]]
cluster.withBlocking(_.withRetry(_.blPop[String](5, "jobs")))  // waits for the slot's master after a failover
```

## Streams

The stream commands are part of `Client`, `Redis`, `RedisCluster` and transactions; they send the command as
Redis defines it and return what Redis answers (errors included):

| Method | Redis command |
|---|---|
| `xAdd(key, fields, id = "*", noMkStream = false, trim = None)` | `XADD key [NOMKSTREAM] [MAXLEN\|MINID [=\|~] threshold [LIMIT count]] *\|id field value ...` |
| `xLen(key)` | `XLEN key` |
| `xRange[R](key, start = "-", end = "+", count = None)` | `XRANGE key start end [COUNT count]` |
| `xRevRange[R](key, end = "+", start = "-", count = None)` | `XREVRANGE key end start [COUNT count]` |
| `xDel(key, ids*)` | `XDEL key id ...` |
| `xTrim(key, trim)` | `XTRIM key MAXLEN\|MINID [=\|~] threshold [LIMIT count]` |
| `xRead[R](streams, count = None)` | `XREAD [COUNT count] STREAMS key ... id ...` |
| `blockingClient.xRead[R](blockMillis, streams, count = None)` | `XREAD [COUNT count] BLOCK milliseconds STREAMS key ... id ...` |

Entry ids are `String`s, in any form Redis accepts (`*`, `<ms>-*`, `<ms>-<seq>`, `-`, `+`, `(<id>`, `$`, ...), and
come back as Redis writes them. An entry is a `StreamEntry[R](id, fields)` with its field-value pairs in Redis'
order; field values use the `Writer` / `Reader` of the other commands, so bytes round-trip. A trimming is
`StreamTrim.MaxLen(threshold, operator, limit)` or `StreamTrim.MinId(threshold, operator, limit)`, with the
operator `StreamTrimOperator.Exact` (`=`) or `Approximate` (`~`). Nil replies are `None`: `xAdd` with
`noMkStream = true` on a missing key, and `xRead` when no stream has entries (or the block time is over).

`XREAD` with `BLOCK` is a blocking command and is sent the way `BLPOP` is: on the connection of a
`BlockingClient` (`redis.blockingClient`), whose call returns a `Try` once Redis answers. The caller waits 2 s longer
than the block time; `BLOCK 0` waits without limit. On `RedisCluster` it is `cluster.blockingClient.xRead`.

```scala
val redis = Redis()
redis.xAdd("events", Seq("room" -> "42", "op" -> "join"), trim = Some(StreamTrim.MaxLen(10000, Some(StreamTrimOperator.Approximate))))
redis.xRead[String](Seq("events" -> "0"))                // Future[Option[List[(String, List[StreamEntry[String]])]]]
redis.blockingClient.xRead[String](5000, Seq("events" -> "$"))  // Try[...]: blocks up to 5 s for a new entry
```

On `RedisCluster`, a stream command goes to the node of its key; an `XREAD` of streams in several slots is sent as
it is and Redis answers `CROSSSLOT`. Consumer groups (`XGROUP`, `XREADGROUP`, `XACK`, `XPENDING`, `XCLAIM`,
`XAUTOCLAIM`, `XINFO`) are not implemented yet.

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

The tests need Redis servers on ports 6379, 6380 (configured by `src/test/resources/redis-6380.conf`),
a master with a replica on 6390 / 6391, a Redis cluster on ports 7000-7005 and a second one on 7100-7105 whose
masters the failover tests kill. They run in Docker containers only; see [testing.md](testing.md):

```
./docker-redis.sh start
./docker-redis.sh test "+test"      # both artifacts, every Scala version
./docker-redis.sh stop
```

## API notes

* Credentials are not rewritten. `auth(password, username = None)` (on `Redis`, `Client`, `BlockingClient` and
  `SubscriberClient`) sends `AUTH <password>` or `AUTH <username> <password>` exactly as given, and returns the
  Redis error if the server refuses it. Only when the server accepts it, the credentials become those of the
  connection: it sends them again every time it reconnects; when it is refused they stay as they were. The
  credentials the connection was constructed with (`authOpt`, or `scredis.redis.password` in the configuration)
  are sent when it connects and on every reconnect; with no `authOpt` no `AUTH` is sent. A refusal at connect or
  reconnect is logged by the connection with the Redis error.
* `clearCredentials()` (on the same four classes) removes the credentials of the connection: no `AUTH` is sent
  on later reconnects. It sends nothing to Redis, and the connection stays authenticated until it is closed.
* `Redis.auth(password, username)` authenticates the non-blocking, the blocking and the subscriber client with
  the same user name; `Redis.clearCredentials()` clears them on all of them.
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
