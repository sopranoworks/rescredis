// Modified by Sopranoworks: Scala 3 compatibility; configuration per build variant; a request sent again (after MOVED, ASK,
// TRYAGAIN, CLUSTERDOWN or a connection error) is a copy, never the same request object; a node's connection is kept after errors
// (it reconnects with the backoff) instead of being replaced by a new one; requests sent to every master (keyless script
// commands); the slot cache and the actor system for the cluster's blocking client; node health and slot reachability, the slots read
// again when a node's state changes and while a slot is unreachable; a request to a master known to be down fails at once as not sent;
// only requests that were not sent are sent again, an outcome unknown goes to the caller.
package scredis.io

import akka.actor.ActorSystem
import com.typesafe.scalalogging.LazyLogging
import scredis.exceptions._
import scredis.protocol._
import scredis.protocol.requests.ClusterRequests.{ClusterCountKeysInSlot, ClusterInfo, ClusterSlots}
import scredis.protocol.requests.ConnectionRequests.Quit
import scredis.util.{ConfigVariant, UniqueNameGenerator}
import scredis.{ClusterHealth, ClusterSlotRange, ConnectionState, RedisConfigDefaults, Server}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}

/**
 * The connection logic for a whole Redis cluster. Handles redirection and sharding logic as specified in
 * http://redis.io/topics/cluster-spec
 *
 * @param nodes List of servers to initialize the cluster connection with.
 * @param maxRetries Maximum number of retries or redirects for a request. The ClusterConnection may attempt to try
 *                   several nodes before giving up on sending a command.
 */
abstract class ClusterConnection(
    nodes: Seq[Server],
    maxRetries: Int = 4,
    receiveTimeoutOpt: Option[FiniteDuration] = RedisConfigDefaults.IO.ReceiveTimeoutOpt,
    connectTimeout: FiniteDuration = RedisConfigDefaults.IO.ConnectTimeout,
    maxWriteBatchSize: Int = RedisConfigDefaults.IO.MaxWriteBatchSize,
    tcpSendBufferSizeHint: Int = RedisConfigDefaults.IO.TCPSendBufferSizeHint,
    tcpReceiveBufferSizeHint: Int = RedisConfigDefaults.IO.TCPReceiveBufferSizeHint,
    akkaListenerDispatcherPath: String = RedisConfigDefaults.IO.Akka.ListenerDispatcherPath,
    akkaIODispatcherPath: String = RedisConfigDefaults.IO.Akka.IODispatcherPath,
    akkaDecoderDispatcherPath: String = RedisConfigDefaults.IO.Akka.DecoderDispatcherPath,
    tryAgainWait: FiniteDuration = RedisConfigDefaults.IO.Cluster.TryAgainWait,
    clusterDownWait: FiniteDuration = RedisConfigDefaults.IO.Cluster.ClusterDownWait,
    systemOpt:Option[ActorSystem] = None,
    failCommandOnConnecting: Boolean = RedisConfigDefaults.Global.FailCommandOnConnecting,
    authOpt: Option[AuthConfig] = RedisConfigDefaults.Config.Redis.AuthOpt
  ) extends NonBlockingConnection with LazyLogging {

  // Int parameter - count number of errors for given connection.
  // When defined threshold is reached connection to this server is removed and no longer used.
  type CONNECTIONS = Map[Server, (AkkaNonBlockingConnection, Int)]

  private val maxHashMisses = RedisConfigDefaults.Redis.MaxClusterHashMisses

  // Externally provided ActorSystem must be closed by caller.
  // Only internally created ActorSystem is stopped automatically on shutdown.
  private val systemState: ActorSystemState = {
    val systemName = RedisConfigDefaults.IO.Akka.ActorSystemName
    systemOpt.map(ActorSystemState(_, shouldTerminate = false))
      .getOrElse(ActorSystemState(ConfigVariant.createActorSystem(UniqueNameGenerator.getUniqueName(systemName)), shouldTerminate = true))
  }

  private def system: ActorSystem = systemState.system

  private val scheduler = system.scheduler

  /** hash slot - connection mapping */
  // TODO we can probably use a more space-efficient storage here, but this is simplest
  @volatile private[scredis] var hashSlots: Vector[Option[Server]] = Vector.fill(Protocol.CLUSTER_HASHSLOTS)(None)

  private val refreshDebounce = RedisConfigDefaults.IO.Cluster.RefreshDebounce
  private val unreachableRefreshInterval = RedisConfigDefaults.IO.Cluster.UnreachableRefreshInterval
  private val refreshLock = new Object
  private var refreshPending = false
  @volatile private var closing = false

  /**
   * The health of this cluster client: the state of its connection to every node, slot reachability, and the
   * availability of a key's slot or of the whole cluster (see [[scredis.ClusterHealth]]).
   *
   * @since 1.1.0
   */
  val health: ClusterHealth = new ClusterHealth(
    () => hashSlots,
    () => connections.map { case (server, (connection, _)) => server -> connection.health },
    () => scheduleRefresh(refreshDebounce)
  )

  /** Set of active cluster node connections. Initialized from `nodes` parameter. */
  // TODO it may be more efficient to save the connections in hashSlots directly
  @volatile private[scredis] var connections: CONNECTIONS = initialConnections

  // TODO keep master-slave associations to be able to ask slaves for data with appropriate config

  /** Miss counter for hashSlots accesses. */
  private var hashMisses = 0

  // bootstrapping: init with info from cluster
  // I guess is it okay to to a blocking await here? or move it to a factory method?
  Await.ready(updateCache(maxRetries), connectTimeout)

  /** Set up initial connections from configuration. */
  private def initialConnections: CONNECTIONS =
    nodes.map { server => (server, (makeConnection(server, system), 0))}.toMap

  /**
   * Update the ClusterClient's connections and hash slots cache.
   * Sends a CLUSTER SLOTS query to the cluster to get a current mapping of hash slots to servers, and updates
   * the internal cache based on the reply.
   */
  private def updateCache(retry: Int): Future[Unit] = {

    // only called when cluster is in ok state
    lazy val upd = send(ClusterSlots()).map { slotRanges =>
      val newConnections = slotRanges.foldLeft(connections) {
        case (cons, ClusterSlotRange(_, master, slaves)) =>
          val nodes = master :: slaves
          cons ++ nodes.flatMap { nodeInfo =>
            val server = nodeInfo.server
            if (cons contains server) Nil
            else List( (server, (makeConnection(server, system),0)) )
          }
      }

      val newSlots =
        slotRanges.foldLeft(hashSlots) {
          case (slots, ClusterSlotRange((begin, end), master, replicas)) =>
            val masterOpt = Option(master).map(_.server)
            (begin.toInt to end.toInt).foldLeft(slots) { (s, i) => s.updated(i, masterOpt) }
        }
      val changed = this.synchronized {
        val changed = newSlots != hashSlots || newConnections.size != connections.size
        connections = newConnections
        hashSlots = newSlots
        changed
      }
      health.recompute()

      // the slots are read again whenever a node's state changes and while a slot is unreachable: logged when they changed
      val message = s"initialized cluster slot cache and connections. Connected to Redis nodes: ${connections.keys}"
      if (changed) logger.info(message) else logger.debug(message)
    }

    if (retry <= 0) Future.failed(RedisClusterException(s"cluster_state not ok, aborting cache update after $maxRetries attempts"))
    // ensure availability of cluster first
    else send(ClusterInfo()).flatMap { info =>
      info.get("cluster_state") match {
        case Some("ok") => upd
        case _ => delayed(clusterDownWait) { updateCache(retry - 1) }
      }
    }
  }

  /** Creates a new connection to a server; its state changes are followed by [[health]]. */
  private def makeConnection(server: Server, system:ActorSystem): AkkaNonBlockingConnection = {
    logger.info("Starting new connection to server {}", server)
    val connection = new AkkaNonBlockingConnection(
      system = system,
      host = server.host,
      port = server.port,
      authOpt = authOpt,
      database = 0,
      nameOpt = None,
      decodersCount = 2,
      receiveTimeoutOpt,
      connectTimeout,
      maxWriteBatchSize,
      tcpSendBufferSizeHint,
      tcpReceiveBufferSizeHint,
      akkaListenerDispatcherPath,
      akkaIODispatcherPath,
      akkaDecoderDispatcherPath,
      failCommandOnConnecting
    ) {
      watchTermination()
    }
    health.nodeAdded(connection.health)
    connection
  }

  /**
   * Reads the slots again (CLUSTER SLOTS) after `delay`, unless a read is already scheduled; while a slot is
   * unreachable afterwards, again after the unreachable refresh interval.
   */
  private def scheduleRefresh(delay: FiniteDuration): Unit = if (!closing) refreshLock.synchronized {
    if (!refreshPending) {
      refreshPending = true
      scheduler.scheduleOnce(delay) {
        refreshLock.synchronized { refreshPending = false }
        if (!closing) updateCache(maxRetries).onComplete { _ =>
          health.recompute()
          if (health.anyUnreachable) scheduleRefresh(unreachableRefreshInterval)
        }
      }
    }
  }

  /** True when the client waits for the next attempt to connect to `server` (or the connection is closed) */
  private def isKnownDown(server: Server): Boolean = connections.get(server).exists(_._1.health.isKnownDown)

  /** A node to send a request to that any node can answer: a connected one, else one not known to be down */
  private def anyAvailableServer(exclude: Set[Server]): Option[Server] = {
    val candidates = connections.toList.filterNot { case (server, _) => exclude.contains(server) }
    candidates.find(_._2._1.health.state == ConnectionState.Connected)
      .orElse(candidates.find { case (server, _) => !isKnownDown(server) })
      .map(_._1)
  }

  /** The availability that a failure of a request concerns: its slot's, or the whole cluster's */
  private def availabilityOf(slotOpt: Option[Int]) = slotOpt match {
    case Some(slot) => health.slotAvailability(slot)
    case None => health.availability
  }

  /** The request fails at once as not sent: the master `server` of `slot` is known to be down */
  private def masterDown[A](request: Request[A], server: Server, slot: Int, error: Option[Throwable]): Future[A] =
    Future.failed(new RedisNotSentException(
      s"Not sent: the master ${server.host}:${server.port} of slot $slot is down: $request",
      health.slotAvailability(slot), error.orNull
    ))

  /**
   * Sends `request` to the master of `slot`. When that master is known to be down, the request is sent once to a
   * connected node instead, as a question: a node that does not serve the slot answers MOVED without executing it,
   * naming the master it knows (a replica promoted since the slot cache was read is found so), and the request
   * fails at once as not sent, with the availability of the slot, when it names the master that is down, answers
   * CLUSTERDOWN or TRYAGAIN, or no node is connected. When the cache has no master for the slot, it goes to any node
   * (which answers MOVED).
   */
  private def sendToSlot[A](request: Request[A], slot: Int, retry: Int, error: Option[RedisException]): Future[A] =
    hashSlots(slot) match {
      case Some(server) if isKnownDown(server) =>
        connections.keys.find(s => s != server && connections(s)._1.health.state == ConnectionState.Connected) match {
          case Some(other) => send(request, other, Some(slot), retry = retry, error = error, askedFor = Some(server))
          case None => masterDown(request, server, slot, error)
        }
      case Some(server) => send(request, server, Some(slot), retry = retry, error = error)
      case None => anyAvailableServer(Set.empty) match {
        case Some(server) => send(request, server, Some(slot), retry = retry, error = error)
        case None => Future.failed(new RedisNotSentException(
          "No cluster node connection available", health.slotAvailability(slot), error.orNull
        ))
      }
    }

  /** Delay a Future-returning operation. */
  private def delayed[A](delay: FiniteDuration)(f: => Future[A]): Future[A] = {
    val delayedF = Promise[A]()
    scheduler.scheduleOnce(delay) {
      delayedF.completeWith(f)
    }
    delayedF.future
  }

  /**
   * Send a Redis request object and handle cluster specific error cases. A retry sends a copy of the request
   * ([[scredis.protocol.Request.copyForResend]]): the request object that failed may still be held by the
   * connection it was sent on, and one object in two connections would pair a reply of one with the other.
   *
   * Only what was not executed is sent again: MOVED and ASK are followed, TRYAGAIN and CLUSTERDOWN retried, and a
   * request that was not sent ([[scredis.exceptions.RedisNotSentException]]) is routed again (by its slot, or to
   * another node). A request whose outcome is unknown ([[scredis.exceptions.RedisOutcomeUnknownException]]: it was
   * written, and the connection was lost before its reply) goes back to the caller.
   * @param request request object
   * @param server server to contact
   * @param slotOpt the slot the request is routed by (None: a request any node can answer)
   * @param retry remaining retries
   * @param askedFor the master known to be down that `server` is asked instead of (see [[sendToSlot]])
   * @param remainingTimeout how much longer to retry an action, rather than number of retries
   * @tparam A response type
   * @return
   */
  private def send[A](request: Request[A],
                      server: Server,
                      slotOpt: Option[Int],
                      triedServers: Set[Server] = Set.empty,
                      retry: Int = maxRetries,
                      remainingTimeout: Duration = connectTimeout,
                      error: Option[RedisException] = None,
                      askedFor: Option[Server] = None): Future[A] = {
    if (retry <= 0) Future.failed(new RedisNotSentException(
      s"Gave up on request after $maxRetries retries: $request", availabilityOf(slotOpt), error.orNull
    ))
    else {
      val connection = connections.getOrElse(server, createConnectionIfMissing(server))._1

      // handle cases that should be retried (MOVE, ASK, TRYAGAIN)
      connection.send(request).recoverWith {

        case err @ RedisClusterErrorResponseException(Moved(slot, host, port), _) =>
          // this will usually happen when cache is missed.
          val movedServer: Server = Server(host, port)
          updateHashMisses(slot, movedServer)

          if (isKnownDown(movedServer)) masterDown(request, movedServer, slot, Some(err))
          else send(request.copyForResend(), movedServer, slotOpt, triedServers+server, retry - 1, remainingTimeout, Option(err))

        case err @ RedisClusterErrorResponseException(ClusterDown | TryAgain, _) if askedFor.isDefined =>
          // asked instead of a master known to be down: no waiting for the cluster
          masterDown(request, askedFor.get, slotOpt.get, Some(err))

        case err: RedisNotSentException if askedFor.isDefined =>
          masterDown(request, askedFor.get, slotOpt.get, Some(err))

        case err @ RedisClusterErrorResponseException(Ask(hashSlot, host, port), _) =>
          val askServer = Server(host,port)
          send(request.copyForResend(), askServer, slotOpt, triedServers+server, retry - 1, remainingTimeout, Option(err))

        case err @ RedisClusterErrorResponseException(TryAgain, _) =>
          // TODO what is actually the intended semantics of TryAgain?
          delayed(tryAgainWait) { send(request.copyForResend(), server, slotOpt, triedServers+server, retry - 1, remainingTimeout - tryAgainWait, Option(err)) }

        case err @ RedisClusterErrorResponseException(ClusterDown, _) =>
          logger.debug(s"Received CLUSTERDOWN error from request $request. Retrying ...")
          val nextTimeout = remainingTimeout - clusterDownWait
          // not executed: the cluster is down (CLUSTERDOWN is answered without executing), until every slot is reachable
          if (nextTimeout <= 0.millis) Future.failed(new RedisNotSentException(
            s"Aborted request $request after trying for ${connectTimeout}", health.availability, err
          ))
          else delayed(clusterDownWait) { send(request.copyForResend(), server, slotOpt, triedServers, retry, nextTimeout, Option(err)) }
          // wait a bit because the cluster may not be fully initialized or fixing itself

        case err: RedisNotSentException =>
          updateServerErrors(server)
          slotOpt match {
            case Some(slot) =>
              // routed by the slot again: at once not sent if its master is known to be down now
              if (retry - 1 <= 0) send(request, server, slotOpt, retry = 0, error = Option(err))
              else sendToSlot(request.copyForResend(), slot, retry - 1, Option(err))
            case None =>
              // any node can answer it: one that isn't one we tried already
              val nextTriedServers = triedServers + server
              anyAvailableServer(nextTriedServers) match {
                case Some(nextServer) => send(request.copyForResend(), nextServer, None, nextTriedServers, retry, remainingTimeout, Option(err))
                case None => Future.failed(new RedisNotSentException("No valid connection available.", health.availability, err))
              }
          }

        case err: RedisOutcomeUnknownException =>
          // written, and the reply was lost: not sent again (a write would be executed twice)
          updateServerErrors(server)
          slotOpt match {
            case Some(slot) => Future.failed(new RedisOutcomeUnknownException(err.getMessage, health.slotAvailability(slot), err))
            case None => Future.failed(err)
          }
      }
    }
  }

  private def updateHashMisses(slot: Int, newServer: Server): Unit = this.synchronized {
    // I believe it is safe to synchronize only updates to hashSlots.
    // If another concurrent read is outdated it will be redirected anyway and potentially repeat the update.
    hashMisses += 1

    // do this update no matter what, updating cache may take a bit
    hashSlots = hashSlots.updated(slot, Option(newServer))

    if (hashMisses > maxHashMisses) {
      hashMisses = 0
      updateCache(maxRetries)
    }
  }

  /**
   * Called when `server` has an error: counts it. The connection is kept; its listener reconnects with the backoff
   * (`scredis.io.reconnect`). Closing it after a few errors, as before, made the next request to the server create
   * a new connection, whose first attempt is immediate: while a node was down (or answered by something that
   * closes connections), that was a loop of new connections.
   */
  private def updateServerErrors(server: Server): Unit = this.synchronized {
    for {
      (con, errors) <- connections.get(server)
    } {
      connections = connections.updated(server, (con, errors + 1))
    }
  }

  private def closeConnection(connToRemove: NonBlockingConnection): Future[Unit] = {
    logger.info("closing connection {}", connToRemove)
    val req = Quit()
    connToRemove.send(req)
    req.future
  }

  /** Create a new connection if required, and update connections.
    * Protects against race conditions leading to same connection being created multiple times
    * TODO: It is still possible to have race conditions when creating connections
    */
  private def createConnectionIfMissing(server: Server): (NonBlockingConnection, Int) =
    this.synchronized {
      connections.get(server) match {
        case Some(conn) => conn
        case None =>
          val con = (makeConnection(server, system), 0)
          connections = connections.updated(server, con)
          con
      }
    }

  override protected[scredis] def send[A](request: Request[A]): Future[A] = request match {
      case req @ ClusterCountKeysInSlot(slot) =>
        // special case handling for slot counting in clusters: redirect to the proper cluster node
        if (slot >= Protocol.CLUSTER_HASHSLOTS || slot < 0)
          Future.failed(RedisInvalidArgumentException(s"Invalid slot number: $slot"))
        else hashSlots(slot.toInt) match {
          case None => Future.failed(new RedisNotSentException(
            s"No cluster slot information available for $slot", health.slotAvailability(slot.toInt)
          ))
          case Some(_) => sendToSlot(req, slot.toInt, maxRetries, None)
        }

      case keyReq: Request[A] with Key =>
        // when we can't get a cached server, any node (it answers MOVED)
        sendToSlot(keyReq, ClusterCRC16.getSlot(keyReq.key), maxRetries, None)

      case clusterReq: Request[A] with Cluster =>
        // requests that are valid with any cluster node: one that is connected, else one not known to be down
        anyAvailableServer(Set.empty) match {
          case Some(server) => send(clusterReq, server, None)
          case None => Future.failed(new RedisNotSentException("No cluster node connection available", health.availability))
        }
      case _ =>
        // TODO what to do about non-key requests? they would be valid for any individual cluster node,
        // but arbitrary choice is probably not what's intended..
        Future.failed(RedisInvalidArgumentException("This command is not supported for clusters"))
    }

  /** The actor system of the connections (the caller's, or the one created for this cluster) */
  private[scredis] def actorSystem: ActorSystem = system

  /** The master of `slot`, as the slot cache has it */
  private[scredis] def masterOf(slot: Int): Option[Server] = hashSlots(slot)

  /** A MOVED reply said that `slot` is served by `server` */
  private[scredis] def slotMoved(slot: Int, server: Server): Unit = updateHashMisses(slot, server)

  /** Reads the slots and their masters from the cluster again (CLUSTER SLOTS) */
  private[scredis] def refreshSlots(): Future[Unit] = updateCache(maxRetries)

  /**
   * Sends a request made by `request` to every master, each on its own connection, without the retries of [[send]]
   * (the request is meant for each of these nodes, not for any one). The masters are read from the cluster first
   * (CLUSTER SLOTS), so that a failover since the last update is seen; if that fails, those of the slot cache.
   * The replies in the order of the masters, or the first failure.
   */
  private[scredis] def sendToMasters[A](request: () => Request[A]): Future[Seq[A]] =
    refreshSlots().recover { case _ => () }.flatMap { _ =>
      val masters = hashSlots.flatten.distinct
      if (masters.isEmpty) Future.failed(RedisIOException("No cluster slot information available"))
      else Future.sequence(masters.map { server =>
        connections.getOrElse(server, createConnectionIfMissing(server))._1.send(request())
      })
    }

  def quit(): Future[Unit] = {
    closing = true
    health.close()
    val toCloseConnections = this.synchronized {
      val cons = connections.values.map(_._1)
      connections = Map.empty
      cons
    }
    val result = Future.traverse(toCloseConnections) { connection =>
      val f = closeConnection(connection)
      connection.awaitTermination()
      f
    }.flatMap(_ => {
      if (systemState.shouldTerminate) {
        systemState.system.terminate()
        systemState.system.whenTerminated.map(_ => ())
      } else {
        Future.successful(())
      }
    })
    result.onComplete(r => logger.info("RedisCluster stopped with result {}", r))
    result
  }

  // TODO at init: fetch all hash slot-node associations: CLUSTER SLOTS
}

case class ActorSystemState(system: ActorSystem, shouldTerminate: Boolean)
