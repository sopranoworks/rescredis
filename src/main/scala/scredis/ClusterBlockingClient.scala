// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
package scredis

import java.util.concurrent.locks.ReentrantLock

import com.typesafe.scalalogging.LazyLogging
import scredis.commands.{BlockingListCommands, BlockingStreamCommands}
import scredis.exceptions.{RedisClusterErrorResponseException, RedisInvalidArgumentException, RedisIOException, RedisNotSentException, RedisOutcomeUnknownException}
import scredis.io.{BlockingConnection, ClusterConnection}
import scredis.protocol.requests.ListRequests.BRPopLPush
import scredis.protocol.{ClusterCRC16, ClusterDown, Key, Moved, Request, TryAgain}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Try}

/**
 * The blocking commands on a Redis cluster: `BLPOP`, `BRPOP`, `BRPOPLPUSH` and `XREAD … BLOCK`, with the
 * signatures of [[scredis.BlockingClient]] ([[scredis.RedisCluster.blocking]], as `Redis.blocking`).
 *
 * A call is routed by its key (the first key of `BLPOP`, `BRPOP` and `XREAD`, the source of `BRPOPLPUSH`) to the
 * master of the key's slot, on a blocking connection of its own to that node, never on the connections that the
 * cluster's non-blocking commands share: a blocked call does not delay them. Several keys in different slots are
 * sent as they are, and Redis answers CROSSSLOT. As with `BlockingClient`, one call is made at a time; a call made
 * while another one blocks fails with a [[scredis.exceptions.RedisIOException]].
 *
 * The rule for the slot's master changing while a call is made (connection management, not a changed command):
 *
 *  - The call has the deadline that `BlockingClient` gives it: the block time plus 2 s, none for a block time of 0.
 *  - A `MOVED` reply updates the slot cache and the call is sent again at once to the node it names.
 *  - `CLUSTERDOWN`, `TRYAGAIN`, or the connection failing (the node died, refuses or closes connections): after a
 *    wait (`scredis.io.reconnect`: min-backoff, doubling up to max-backoff) the slots are read from the cluster
 *    again (`CLUSTER SLOTS`) and the call is sent to the slot's master.
 *  - The command is sent again unchanged (block time included), as a new request; each attempt waits at most for
 *    what is left of the deadline. When the deadline has passed, the node's blocking connection, which may still be
 *    blocked in Redis, is closed and replaced by a new one for the next call, and the call fails with a
 *    [[scredis.exceptions.RedisOutcomeUnknownException]] when its last attempt was written (its reply did not come
 *    in time, or the connection was lost before it), or a [[scredis.exceptions.RedisNotSentException]] when it was
 *    not (no master for the slot, CLUSTERDOWN / TRYAGAIN, no connection); both carry the availability of the key.
 *  - Every other reply, `ASK` and errors such as `CROSSSLOT` or `WRONGTYPE` included, is the call's result.
 *
 * Unlike the cluster's non-blocking commands, a blocking call waits within its deadline for the slot's master and
 * is sent again after a lost connection (outcome unknown): a call whose connection failed after Redis had executed
 * it (the node died before its reply arrived) is sent again, and an element that the first `BLPOP` / `BRPOP` /
 * `BRPOPLPUSH` had popped on the dead master is not returned. A reply is only ever the reply of its own request.
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

  private val minBackoff = RedisConfigDefaults.IO.Reconnect.MinBackoff
  private val maxBackoff = RedisConfigDefaults.IO.Reconnect.MaxBackoff

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

  private def discard(server: Server): Unit = synchronized {
    clients.get(server).foreach { client =>
      clients -= server
      client.closeAsync()
    }
  }

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

  override protected[scredis] def sendBlocking[A](request: Request[A])(implicit timeout: Duration): Try[A] = {
    if (closed) throw new RedisNotSentException("Connection has been shutdown", cluster.health.availability)
    if (!lock.tryLock()) throw new RedisNotSentException("Trying to send request on a blocked connection", Availability.Available)
    try {
      val slot = ClusterCRC16.getSlot(keyOf(request))
      val deadline = timeout match {
        case finite: FiniteDuration => Some(System.nanoTime() + finite.toNanos)
        case _ => None
      }
      var attempt = request
      var failures = 0
      var result: Option[Try[A]] = None
      while (result.isEmpty) {
        cluster.masterOf(slot) match {
          case None =>
            refresh(deadline)
            if (cluster.masterOf(slot).isEmpty) {
              if (expired(deadline)) result = Some(Failure(new RedisNotSentException(
                s"No master for slot $slot", cluster.health.slotAvailability(slot)
              )))
              else { failures += 1; pause(backoff(failures), deadline) }
            }
          case Some(server) =>
            val reply = clientFor(server).sendBlocking(attempt)(remaining(deadline))
            reply match {
              case Failure(RedisClusterErrorResponseException(Moved(movedSlot, host, port), _)) if !expired(deadline) =>
                cluster.slotMoved(movedSlot, Server(host, port))
                attempt = request.copyForResend()
              case Failure(e) if retryable(e) && !expired(deadline) =>
                logger.debug(s"Blocking call $request on $server failed ($e); retrying at the master of slot $slot")
                discard(server)
                failures += 1
                pause(backoff(failures), deadline)
                refresh(deadline)
                attempt = request.copyForResend()
              case Failure(e) if retryable(e) || isMoved(e) =>
                // the deadline has passed
                discard(server)
                val message = s"Blocking call $request timed out after $e"
                result = Some(Failure(e match {
                  case _: RedisOutcomeUnknownException => new RedisOutcomeUnknownException(message, cluster.health.slotAvailability(slot), e)
                  case _ => new RedisNotSentException(message, cluster.health.slotAvailability(slot), e)
                }))
              case other =>
                result = Some(other)
            }
        }
      }
      result.get
    } finally {
      lock.unlock()
    }
  }

  private def isMoved(e: Throwable): Boolean = e match {
    case RedisClusterErrorResponseException(_: Moved, _) => true
    case _ => false
  }

  private def retryable(e: Throwable): Boolean = e match {
    case RedisClusterErrorResponseException(ClusterDown | TryAgain, _) => true
    case _: RedisIOException => true
    case _ => false
  }

  private def backoff(failures: Int): FiniteDuration =
    math.min(maxBackoff.toMillis, minBackoff.toMillis * (1L << math.min(failures - 1, 30))).millis

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
