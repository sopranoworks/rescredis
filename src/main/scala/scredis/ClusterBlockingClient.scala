// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
package scredis

import java.time.Instant
import java.util.concurrent.TimeoutException
import java.util.concurrent.locks.ReentrantLock

import com.typesafe.scalalogging.LazyLogging
import scredis.commands.{BlockingListCommands, BlockingStreamCommands}
import scredis.exceptions.{RedisClusterErrorResponseException, RedisInvalidArgumentException, RedisNotSentException, RedisOutcomeUnknownException}
import scredis.io.{BlockingConnection, ClusterConnection}
import scredis.protocol.requests.ListRequests.BRPopLPush
import scredis.protocol.{ClusterCRC16, ClusterDown, Key, Moved, Request, TryAgain}

import scala.concurrent.{Await, Promise}
import scala.concurrent.duration._
import scala.util.{Failure, Try}

/**
 * The blocking commands on a Redis cluster: `BLPOP`, `BRPOP`, `BRPOPLPUSH` and `XREAD … BLOCK`, with the
 * signatures of [[scredis.BlockingClient]] ([[scredis.RedisCluster.blockingClient]], as `Redis.blockingClient`).
 *
 * A call is routed by its key (the first key of `BLPOP`, `BRPOP` and `XREAD`, the source of `BRPOPLPUSH`) to the
 * master of the key's slot, on a blocking connection of its own to that node, never on the connections that the
 * cluster's non-blocking commands share: a blocked call does not delay them. Several keys in different slots are
 * sent as they are, and Redis answers CROSSSLOT. As with `BlockingClient`, one call is made at a time; a call made
 * while another one blocks fails as not sent, with an availability that completes when that call ends.
 *
 * A call follows the policy of the cluster's non-blocking commands (connection management, not a changed command):
 * only what was not executed is sent again, as a new request with the command unchanged (block time included).
 *
 *  - The call has the deadline that `BlockingClient` gives it: the block time plus 2 s, none for a block time of 0.
 *  - `MOVED` updates the slot cache and the call is sent at once to the node it names.
 *  - When the slot's master is known to be down, the call is sent once to a connected node instead, which answers
 *    `MOVED` without executing it when it does not serve the slot (a replica promoted since the cache was read is
 *    found so); it fails at once as not sent, with the availability of the key's slot, when that node names the
 *    master that is down, answers `CLUSTERDOWN` / `TRYAGAIN`, or no node is connected.
 *  - `CLUSTERDOWN`, `TRYAGAIN` and a call not sent (no connection to the master): after a wait
 *    (`scredis.io.reconnect`: min-backoff, doubling up to max-backoff) the slots are read again (`CLUSTER SLOTS`) and
 *    the call is routed by its slot again, within its deadline; when the deadline has passed it fails as not sent.
 *  - A call that was written and whose connection was lost before the reply, or whose deadline passed, fails as
 *    outcome unknown ([[scredis.exceptions.RedisOutcomeUnknownException]], the `TimeoutException` as the cause for
 *    the deadline) with the availability of the key's slot, and is **not** sent again: Redis may have executed it
 *    (an element popped on a master that died before its reply arrived is lost with that reply). After a deadline,
 *    the connection of the node's blocking client, which may still be blocked in Redis, is closed and replaced for
 *    the next call, by the same rule as `Redis.blockingClient`: a late reply reaches nobody.
 *  - Every other reply, `ASK` and errors such as `CROSSSLOT` or `WRONGTYPE` included, is the call's result.
 *
 * `withRetry` waits on the availability of a failed call and makes it again (by default only when it was not sent).
 *
 * @since 1.1.0
 */
final class ClusterBlockingClient private[scredis] (
  cluster: ClusterConnection,
  connect: Server => BlockingClient
) extends BlockingConnection with BlockingListCommands with BlockingStreamCommands with LazyLogging {

  private val lock = new ReentrantLock()
  private var clients = Map.empty[Server, BlockingClient]
  @volatile private var closed = false
  // while a call is made: completes when it ends (the availability of a call refused meanwhile)
  @volatile private var busy: Availability = Availability.Available

  private val minBackoff = RedisConfigDefaults.IO.Reconnect.MinBackoff
  private val maxBackoff = RedisConfigDefaults.IO.Reconnect.MaxBackoff
  // the time in which the cluster's own connection to a node finds that the node died silently (the idle PING)
  private val lossNoticed = RedisConfigDefaults.IO.Health.IdlePingIntervalOpt.getOrElse(Duration.Zero) +
    RedisConfigDefaults.IO.Health.IdlePingTimeout

  private def keyOf(request: Request[_]): String = request match {
    case keyed: Key => keyed.key
    case r: BRPopLPush[_] => r.source
    case other => throw RedisInvalidArgumentException(s"Not a blocking command with a key: $other")
  }

  private def clientFor(server: Server): BlockingClient = synchronized {
    clients.getOrElse(server, {
      val client = connect(server)
      clients += server -> client
      client
    })
  }

  /** The cluster's connection to `server`, or this client's blocking connection to it, waits for its next attempt */
  private def isKnownDown(server: Server): Boolean =
    cluster.isNodeKnownDown(server) || synchronized(clients.get(server)).exists(_.health.isKnownDown)

  private def remaining(deadline: Option[Long]): Duration =
    deadline.map(d => math.max(0L, d - System.nanoTime()).nanos).getOrElse(Duration.Inf)

  private def expired(deadline: Option[Long]): Boolean = deadline.exists(System.nanoTime() >= _)

  private def pause(wait: FiniteDuration, deadline: Option[Long]): Unit = {
    val ms = remaining(deadline) match {
      case left: FiniteDuration => math.min(wait.toMillis, left.toMillis)
      case _ => wait.toMillis
    }
    if (ms > 0) Thread.sleep(ms)
  }

  /** Reads the slots from the cluster again, waiting at most until the deadline */
  private def refresh(deadline: Option[Long]): Unit = {
    val limit = remaining(deadline) match {
      case left: FiniteDuration => left.min(10.seconds)
      case _ => 10.seconds
    }
    Try(Await.ready(cluster.refreshSlots(), limit))
  }

  /**
   * The availability of `slot` after this client's blocking connection to its master `server` was lost. The
   * cluster's own connection to the node sees the loss on its own: until it does, the slot counts as reachable. It
   * waits (at most for the time in which the idle PING finds a dead connection) until that connection is no longer
   * connected, or this client's blocking connection to the node is back (the node is up).
   */
  private def availabilityAfterLoss(server: Server, slot: Int): Availability = {
    cluster.health.nodes.get(server).foreach { node =>
      if (node.state == ConnectionState.Connected) {
        val seen = Promise[Unit]()
        val subscriptions = node.subscribe(state => if (state != ConnectionState.Connected) seen.trySuccess(())) ::
          synchronized(clients.get(server)).toList.map(_.health.subscribe { state =>
            if (state == ConnectionState.Connected) seen.trySuccess(())
          })
        Try(Await.ready(seen.future, lossNoticed))
        subscriptions.foreach(_.cancel())
      }
    }
    cluster.health.slotAvailability(slot)
  }

  override protected[scredis] def sendBlocking[A](request: Request[A])(implicit timeout: Duration): Try[A] = {
    if (closed) throw new RedisNotSentException("Connection has been shutdown", cluster.health.availability)
    if (!lock.tryLock()) throw new RedisNotSentException("Trying to send request on a blocked connection", busy)
    busy = Availability.outage(Instant.now(), "a blocking call is in progress on the cluster's blocking client")
    try {
      val slot = ClusterCRC16.getSlot(keyOf(request))
      val deadline = timeout match {
        case finite: FiniteDuration => Some(System.nanoTime() + finite.toNanos)
        case _ => None
      }
      def notSent(message: String, cause: Throwable): Option[Try[A]] =
        Some(Failure(new RedisNotSentException(message, cluster.health.slotAvailability(slot), cause)))
      def masterDown(server: Server, cause: Throwable): Option[Try[A]] =
        notSent(s"Not sent: the master ${server.host}:${server.port} of slot $slot is down: $request", cause)

      var attempt = request
      var failures = 0
      var result: Option[Try[A]] = None
      while (result.isEmpty) {
        // the node to send to, and the master known to be down that it is asked instead of
        val target: Option[(Server, Option[Server])] = cluster.masterOf(slot) match {
          case Some(master) if isKnownDown(master) =>
            cluster.connectedNodeOtherThan(master) match {
              case Some(other) => Some((other, Some(master)))
              case None => result = masterDown(master, null); None
            }
          case Some(master) => Some((master, None))
          case None => cluster.anyNode match {
            case Some(any) => Some((any, None))
            case None => result = notSent("No cluster node connection available", null); None
          }
        }
        target.foreach { case (server, askedFor) =>
          Try(clientFor(server).sendBlocking(attempt)(remaining(deadline))).flatten match {
            case Failure(err @ RedisClusterErrorResponseException(Moved(movedSlot, host, port), _)) =>
              val moved = Server(host, port)
              cluster.slotMoved(movedSlot, moved)
              if (isKnownDown(moved)) result = masterDown(moved, err)
              else if (expired(deadline)) result = notSent(s"Blocking call $request not sent within its deadline", err)
              else attempt = request.copyForResend()
            case Failure(err @ RedisClusterErrorResponseException(ClusterDown | TryAgain, _)) if askedFor.isDefined =>
              result = masterDown(askedFor.get, err)
            case Failure(err: RedisNotSentException) if askedFor.isDefined =>
              result = masterDown(askedFor.get, err)
            case Failure(err) if isNotExecuted(err) =>
              // not executed: routed by the slot again after a wait, within the deadline
              if (expired(deadline)) result = notSent(s"Blocking call $request not sent within its deadline: $err", err)
              else {
                logger.debug(s"Blocking call $request on $server not sent ($err); routing it by slot $slot again")
                failures += 1
                pause(backoff(failures), deadline)
                refresh(deadline)
                attempt = request.copyForResend()
              }
            case Failure(err: RedisOutcomeUnknownException) =>
              // written, its reply lost: not sent again (Redis may have executed it)
              val (cause, availability) = err.getCause match {
                case deadlinePassed: TimeoutException =>
                  // the node's blocking client has replaced its connection (as Redis.blockingClient does)
                  (deadlinePassed, cluster.health.slotAvailability(slot))
                case _ => (err, availabilityAfterLoss(server, slot))
              }
              result = Some(Failure(new RedisOutcomeUnknownException(err.getMessage, availability, cause)))
            case other =>
              result = Some(other)
          }
        }
      }
      result.get
    } finally {
      busy.complete()
      lock.unlock()
    }
  }

  private def isNotExecuted(e: Throwable): Boolean = e match {
    case RedisClusterErrorResponseException(ClusterDown | TryAgain, _) => true
    case _: RedisNotSentException => true
    case _ => false
  }

  private def backoff(failures: Int): FiniteDuration =
    math.min(maxBackoff.toMillis, minBackoff.toMillis * (1L << math.min(failures - 1, 30))).millis

  /**
   * Runs `f(this)`, and again each time it fails with an error that is retried, once that error's availability has
   * completed (see [[scredis.RetryPolicy]]): by default only [[scredis.exceptions.RedisNotSentException]], also
   * [[scredis.exceptions.RedisOutcomeUnknownException]] with `idempotent`; Redis error replies never. The calling
   * thread waits between attempts. A call to a slot whose master is down fails at once as not sent, with the
   * availability of the key's slot: the next attempt is made once the slot has a reachable master again.
   *
   * {{{
   * cluster.blockingClient.withRetry(_.blPop[String](5, "queue"), maxWaitingDuration = 30.seconds)
   * }}}
   *
   * @param f one attempt
   * @param maxWaitingDuration the longest it waits, in all, from the first attempt; then it fails with the last error
   * @param idempotent retry an attempt whose outcome is unknown too: the caller states that `f` is idempotent
   *
   * @since 1.1.0
   */
  def withRetry[T](f: ClusterBlockingClient => Try[T], maxWaitingDuration: Duration = Duration.Inf, idempotent: Boolean = false): Try[T] =
    RetryPolicy(maxWaitingDuration, retryOutcomeUnknown = idempotent).retryBlocking(() => f(this))

  /**
   * The health of the blocking connection to each node that a call has been made to so far.
   *
   * @since 1.1.0
   */
  def health: Map[Server, ConnectionHealth] = synchronized(clients.map { case (server, client) => server -> client.health })

  /** Closes the blocking connections (QUIT, without waiting for calls that block) */
  private[scredis] def close(): Unit = synchronized {
    closed = true
    clients.values.foreach(_.closeAsync())
    clients = Map.empty
  }
}
