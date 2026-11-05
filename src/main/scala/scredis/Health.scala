// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
package scredis

import java.time.Instant

import com.typesafe.scalalogging.LazyLogging
import scredis.exceptions.RedisIOException
import scredis.protocol.{ClusterCRC16, Protocol}

import scala.collection.immutable.BitSet
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal

/**
 * The state of a connection, as [[ConnectionHealth]] reports it.
 *
 * @since 1.1.0
 */
sealed trait ConnectionState

/** @since 1.1.0 */
object ConnectionState {

  /** Connected and initialised (AUTH, SELECT and CLIENT SETNAME, as configured, have been answered) */
  case object Connected extends ConnectionState

  /**
   * Not connected; the client is making attempt `attempt` or waits for it (the reconnect backoff,
   * `scredis.io.reconnect`). The first connection of a client is reported as attempt 1 with the cause
   * "not connected yet".
   *
   * @param attempt the number of the attempt being made or waited for, from 1 in each outage
   * @param since when the connection became unavailable (or the client was created)
   * @param lastCause why the connection was lost, or why the last attempt failed
   */
  final case class Reconnecting(attempt: Int, since: Instant, lastCause: String) extends ConnectionState

  /** Closed (QUIT): no request is sent on it any more */
  case object Closed extends ConnectionState
}

/**
 * A subscription to state changes ([[ConnectionHealth.subscribe]], [[ClusterHealth.subscribe]]).
 *
 * @since 1.1.0
 */
trait HealthSubscription {
  /** No more calls to the listener */
  def cancel(): Unit
}

/**
 * Whether a connection (or, on a cluster, a key's slot or the whole cluster) is usable, and an event to wait for when
 * it is not: `whenAvailable` completes when it is usable again. It is completed already while it is available.
 *
 * There is one object per outage, shared by everyone who asks during that outage (and carried by the
 * [[scredis.exceptions.RedisNotSentException]]s and [[scredis.exceptions.RedisOutcomeUnknownException]]s of that
 * outage); the next outage has a new one. The library keeps no reference to it once it is completed. When the
 * client is closed (`quit()`) while it is unavailable, `whenAvailable` fails with a
 * [[scredis.exceptions.RedisIOException]].
 *
 * @param unavailableSince when it became unavailable; `None` while it is available
 * @param cause why it became unavailable; `None` while it is available
 *
 * @since 1.1.0
 */
final class Availability private (
  val unavailableSince: Option[Instant],
  val cause: Option[String],
  promise: Promise[Unit]
) {
  /** Completes when it is usable again; completed already while it is available */
  def whenAvailable: Future[Unit] = promise.future

  /** True when `whenAvailable` has completed successfully */
  def isAvailable: Boolean = promise.future.value.exists(_.isSuccess)

  private[scredis] def complete(): Unit = promise.trySuccess(())
  private[scredis] def fail(e: Throwable): Unit = promise.tryFailure(e)

  override def toString: String = promise.future.value match {
    case Some(scala.util.Success(_)) if unavailableSince.isEmpty => "Availability(available)"
    case Some(scala.util.Success(_)) => s"Availability(available again; unavailable since ${unavailableSince.get}: ${cause.get})"
    case Some(scala.util.Failure(e)) => s"Availability(closed: ${e.getMessage})"
    case None => s"Availability(unavailable since ${unavailableSince.get}: ${cause.get})"
  }
}

/** @since 1.1.0 */
object Availability {
  /** The object of something that is available */
  private[scredis] val Available: Availability = new Availability(None, None, Promise.successful(()))

  /** A new outage, from `since`, because of `cause` */
  private[scredis] def outage(since: Instant, cause: String): Availability =
    new Availability(Some(since), Some(cause), Promise[Unit]())

  /** The object of something that has been closed: `whenAvailable` has failed */
  private[scredis] def closed(e: Throwable): Availability = new Availability(Some(Instant.now()), Some(e.getMessage), Promise.failed(e))
}

/**
 * Listeners of state changes: called in the order of the changes, on the thread that made the change (a thread of
 * scredis), and must not block. A listener that throws is logged and kept.
 */
private[scredis] final class Listeners[A] extends LazyLogging {
  private val lock = new Object
  @volatile private var listeners = Vector.empty[A => Unit]

  /** Adds `listener` and calls it with `current` (under the lock, so that no change is seen before them) */
  def add(listener: A => Unit, current: => Seq[A]): HealthSubscription = lock.synchronized {
    listeners :+= listener
    current.foreach(call(listener, _))
    new HealthSubscription {
      def cancel(): Unit = lock.synchronized { listeners = listeners.filterNot(_ eq listener) }
    }
  }

  /** Makes a change and calls the listeners with what it returns, in the order of the changes */
  def change(f: => Option[A]): Unit = lock.synchronized {
    f.foreach(value => listeners.foreach(call(_, value)))
  }

  private def call(listener: A => Unit, value: A): Unit =
    try listener(value) catch { case NonFatal(e) => logger.error(s"A health listener failed on $value", e) }
}

/**
 * The health of one connection: its state, which can be read and subscribed to, and its [[Availability]].
 *
 * `subscribe` takes a listener (a function) rather than publishing on an actor event stream: the same API on the
 * Akka and the Pekko artifact, no actor needed to receive the changes, and no change of a connection published to
 * every subscriber of a shared stream. A listener is called with the current state at once and then with every
 * change, in order, on a thread of scredis; it must not block.
 *
 * @param server the server the connection is made to
 *
 * @since 1.1.0
 */
final class ConnectionHealth private[scredis] (val server: Server) {
  import ConnectionState._

  private val stateLock = new Object
  private val listeners = new Listeners[ConnectionState]
  private var current: ConnectionState = Reconnecting(1, Instant.now(), "not connected yet")
  private var currentAvailability: Availability = {
    val r = current.asInstanceOf[Reconnecting]
    Availability.outage(r.since, r.lastCause)
  }
  private var waitingForNextAttempt = false

  /** The current state */
  def state: ConnectionState = stateLock.synchronized(current)

  /** The availability of the connection: completed while it is connected */
  def availability: Availability = stateLock.synchronized(currentAvailability)

  /** Calls `listener` with the current state, then with every change (see the class description) */
  def subscribe(listener: ConnectionState => Unit): HealthSubscription = listeners.add(listener, List(state))

  /** True while the client waits for its next attempt (no attempt in progress), or the connection is closed */
  private[scredis] def isKnownDown: Boolean = stateLock.synchronized(waitingForNextAttempt || current == Closed)

  /** The connection is up and initialised */
  private[scredis] def connected(): Unit = listeners.change {
    stateLock.synchronized {
      if (current == Connected || current == Closed) None
      else {
        val outage = currentAvailability
        current = Connected
        currentAvailability = Availability.Available
        waitingForNextAttempt = false
        outage.complete()
        Some(current)
      }
    }
  }

  /** Not connected: attempt `attempt` is being made (`waiting` false) or waited for (true), after `cause` */
  private[scredis] def reconnecting(attempt: Int, cause: String, waiting: Boolean): Unit = listeners.change {
    stateLock.synchronized {
      current match {
        case Closed => None
        case Connected =>
          val since = Instant.now()
          current = Reconnecting(attempt, since, cause)
          currentAvailability = Availability.outage(since, cause)
          waitingForNextAttempt = waiting
          Some(current)
        case r: Reconnecting =>
          waitingForNextAttempt = waiting
          val next = Reconnecting(attempt, r.since, cause)
          if (next == r) None else { current = next; Some(next) }
      }
    }
  }

  /** The connection is closed for good; waiters on its availability get a failed future */
  private[scredis] def closed(): Unit = listeners.change {
    stateLock.synchronized {
      if (current == Closed) None
      else {
        val e = RedisIOException(s"The connection to ${server.host}:${server.port} has been closed")
        currentAvailability.fail(e)
        current = Closed
        currentAvailability = Availability.closed(e)
        waitingForNextAttempt = false
        Some(current)
      }
    }
  }

  override def toString: String = s"ConnectionHealth(${server.host}:${server.port}, $state)"
}

/**
 * The health of a cluster client ([[scredis.RedisCluster.health]]): the health of its connection to every node it
 * knows, and slot reachability: a slot is reachable when the slot cache names a master for it and the connection to
 * that master is connected. A change of a node's connection state makes the client read the slots again
 * (`CLUSTER SLOTS`, debounced: `scredis.io.cluster.refresh-debounce`), and while a slot is unreachable it reads them
 * again every `scredis.io.cluster.unreachable-refresh-interval`, so that a promoted replica is found without waiting
 * for a `MOVED`.
 *
 * @since 1.1.0
 */
final class ClusterHealth private[scredis] (
  slots: () => Vector[Option[Server]],
  nodeHealth: () => Map[Server, ConnectionHealth],
  onNodeChange: () => Unit
)(implicit ec: ExecutionContext) extends LazyLogging {

  private val lock = new Object
  private val listeners = new Listeners[(Server, ConnectionState)]
  // the outages handed out: per master (None: slots without a master in the cache), the slots they were asked for
  private var slotOutages = Map.empty[Option[Server], (BitSet, Availability)]
  private var clusterOutage: Option[Availability] = None
  @volatile private var closedWith: Option[Throwable] = None

  /** The health of the connection to every node the client knows (masters and replicas) */
  def nodes: Map[Server, ConnectionHealth] = nodeHealth()

  /** True when the slot cache names a master for `slot` and the connection to it is connected */
  def isReachable(slot: Int): Boolean = slots()(slot).exists(connected)

  /** The slots that are not reachable now */
  def unreachableSlots: BitSet = {
    val s = slots()
    val nodes = nodeHealth()
    BitSet((0 until Protocol.CLUSTER_HASHSLOTS).filterNot(i => s(i).exists(m => nodes.get(m).exists(_.state == ConnectionState.Connected))): _*)
  }

  /**
   * The availability of `key`: completed while the key's slot is reachable, else completes when it is reachable
   * again (when the master's connection is back, or when a refresh of the slots names a master that is connected)
   */
  def availability(key: String): Availability = slotAvailability(ClusterCRC16.getSlot(key))

  /** The availability of the whole cluster: completed while every slot is reachable */
  def availability: Availability = lock.synchronized {
    closedWith match {
      case Some(e) => Availability.closed(e)
      case None =>
        val unreachable = unreachableSlots
        if (unreachable.isEmpty) Availability.Available
        else clusterOutage.filterNot(_.whenAvailable.isCompleted).getOrElse {
          val a = Availability.outage(java.time.Instant.now(), s"${unreachable.size} slot(s) have no reachable master")
          clusterOutage = Some(a)
          a
        }
    }
  }

  /** Calls `listener` with the current state of every node, then with every change of a node's state */
  def subscribe(listener: ((Server, ConnectionState)) => Unit): HealthSubscription =
    listeners.add(listener, nodeHealth().toList.map { case (server, health) => (server, health.state) })

  private[scredis] def slotAvailability(slot: Int): Availability = lock.synchronized {
    closedWith match {
      case Some(e) => Availability.closed(e)
      case None =>
        val master = slots()(slot)
        if (master.exists(connected)) Availability.Available
        else slotOutages.get(master) match {
          case Some((asked, outage)) if !outage.whenAvailable.isCompleted =>
            slotOutages += master -> ((asked + slot, outage))
            outage
          case _ =>
            val (since, cause) = master.flatMap(m => nodeHealth().get(m)).map(_.state) match {
              case Some(ConnectionState.Reconnecting(_, since, lastCause)) =>
                (since, s"the master ${master.get.host}:${master.get.port} of slot $slot is not connected ($lastCause)")
              case _ => (java.time.Instant.now(), master match {
                case Some(m) => s"the master ${m.host}:${m.port} of slot $slot is not connected"
                case None => s"no master is known for slot $slot"
              })
            }
            val outage = Availability.outage(since, cause)
            slotOutages += master -> ((BitSet(slot), outage))
            outage
        }
    }
  }

  private def connected(server: Server): Boolean = nodeHealth().get(server).exists(_.state == ConnectionState.Connected)

  /** A connection to a node has been made: its state changes are followed */
  private[scredis] def nodeAdded(health: ConnectionHealth): Unit = health.subscribe { state =>
    listeners.change(Some((health.server, state)))
    // not here: the listener runs on the connection's thread, under its listeners' lock
    Future { recompute(); onNodeChange() }
  }

  /** Completes the outages whose slots are all reachable again (after a node's state change or a slot refresh) */
  private[scredis] def recompute(): Unit = lock.synchronized {
    if (closedWith.isEmpty) {
      val s = slots()
      val nodes = nodeHealth()
      def reachable(slot: Int): Boolean = s(slot).exists(m => nodes.get(m).exists(_.state == ConnectionState.Connected))
      slotOutages.foreach { case (master, (asked, outage)) =>
        if (asked.forall(reachable)) {
          outage.complete()
          slotOutages -= master
        }
      }
      clusterOutage.foreach { outage =>
        if ((0 until Protocol.CLUSTER_HASHSLOTS).forall(reachable)) {
          outage.complete()
          clusterOutage = None
        }
      }
    }
  }

  /** True when some slot is not reachable */
  private[scredis] def anyUnreachable: Boolean = {
    val s = slots()
    val nodes = nodeHealth()
    (0 until Protocol.CLUSTER_HASHSLOTS).exists(i => !s(i).exists(m => nodes.get(m).exists(_.state == ConnectionState.Connected)))
  }

  /** The cluster client is closed: waiters get a failed future */
  private[scredis] def close(): Unit = lock.synchronized {
    val e = RedisIOException("The cluster client has been closed")
    closedWith = Some(e)
    slotOutages.values.foreach(_._2.fail(e))
    clusterOutage.foreach(_.fail(e))
    slotOutages = Map.empty
    clusterOutage = None
  }
}
