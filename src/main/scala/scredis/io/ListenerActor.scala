// Modified by Sopranoworks: fixes backported from upstream scredis (05592f8, 5debcf4); adapted to Redis 7.4 (auth); credentials kept only once accepted, clearCredentials;
// every request is completed by this actor only, by its reply or by the end of the connection it was sent on (messages and data of an
// earlier connection are ignored); reconnect with a backoff, state changes logged once; the receive timeout covers every outstanding request;
// the connection's health (state, availability), a PING to learn that Redis answers on a new connection without initialisation requests, an
// idle PING; requests that fail because of the connection fail as not sent or as outcome unknown; ConnectNow (the next attempt at once).
package scredis.io

import java.net.InetSocketAddress
import java.util

import akka.actor._
import akka.io.Tcp
import akka.routing._
import akka.util.ByteString
import scredis.{Availability, ConnectionHealth, RedisConfigDefaults, Subscription, Transaction}
import scredis.exceptions.{RedisIOException, RedisNotSentException, RedisOutcomeUnknownException}
import scredis.protocol.requests.ConnectionRequests.{Auth, Ping, Quit, Select}
import scredis.protocol.requests.ServerRequests
import scredis.protocol.{AuthConfig, Protocol, Request}
import scredis.util.UniqueNameGenerator

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import scredis.Transaction._

class ListenerActor(
  host: String,
  port: Int,
  var authOpt: Option[AuthConfig],
  var database: Int,
  var nameOpt: Option[String],
  var decodersCount: Int,
  receiveTimeoutOpt: Option[FiniteDuration],
  connectTimeout: FiniteDuration,
  maxWriteBatchSize: Int,
  tcpSendBufferSizeHint: Int,
  tcpReceiveBufferSizeHint: Int,
  akkaIODispatcherPath: String,
  akkaDecoderDispatcherPath: String,
  failCommandOnConnecting: Boolean,
  health: ConnectionHealth
) extends Actor with ActorLogging {
  
  import ListenerActor._
  import context.dispatcher
  
  override val supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case e: Exception => SupervisorStrategy.Stop
  }
  
  private val remote = InetSocketAddress.createUnresolved(host, port)
  
  private var remainingByteStringOpt: Option[ByteString] = None
  private var initializationRequestsCount = 0
  private var isConnecting = false
  private var isShuttingDown = false
  private var isShuttingDownBeforeConnected = false
  // why the current connection is being closed by this actor (a receive timeout, an unanswered PING)
  private var closingCause: Option[String] = None
  private var timeoutCancellableOpt: Option[Cancellable] = None
  
  protected val queuedRequests = new util.LinkedList[Request[_]]()
  // The requests sent on the current connection, in the order they were written: the n-th reply on the connection
  // belongs to the n-th request. Only this actor removes a request from it, when the reply has arrived or when the
  // connection has ended; a request is completed by whoever removes it, and by nobody else.
  protected val requests = new util.LinkedList[Request[_]]()
  protected var ioActor: ActorRef = _
  protected var decoders: Router = _

  // The current connection: its number (incremented for every IOActor), its TCP connection actor once connected
  // (data from any other one is from an earlier connection and is dropped), whether Redis has answered on it.
  protected var connectionNumber = 0
  private var tcpConnection: ActorRef = _
  private var answered = false

  // Reconnecting: the number of consecutive attempts that failed (no connection, or one closed before Redis
  // answered on it), the scheduled next attempt, and the state that is logged when it changes.
  private val minBackoff = RedisConfigDefaults.IO.Reconnect.MinBackoff
  private val maxBackoff = RedisConfigDefaults.IO.Reconnect.MaxBackoff
  private var failedAttempts = 0
  private var reconnectCancellableOpt: Option[Cancellable] = None
  private var lastCause = "connection closed"
  private var lastFailure = ""
  private var everConnected = false
  private var down = false
  private var downSince = 0L
  private var attemptsWhileDown = 0

  // Health: the attempt reported (from 1 in each outage; the first connection is attempt 1), the PING of the current
  // connection that is not answered yet (the one that tells that Redis answers, or an idle one), and when something
  // was last sent or received.
  private var healthAttempt = 1
  private var pingOpt: Option[Request[_]] = None
  private var pingNumber = 0
  private var lastActivity = System.nanoTime()
  private val idlePingIntervalOpt = RedisConfigDefaults.IO.Health.IdlePingIntervalOpt
  private val idlePingTimeout = RedisConfigDefaults.IO.Health.IdlePingTimeout

  /** A request that fails before it was sent: certainly not executed */
  protected def notSent(message: String): RedisNotSentException = new RedisNotSentException(message, health.availability)

  /** A request that was written and whose reply will not come: the outcome is unknown */
  protected def outcomeUnknown(message: String): RedisOutcomeUnknownException =
    new RedisOutcomeUnknownException(message, health.availability)

  /** A request that fails because the connection is being closed (QUIT): not sent */
  private def closing(message: String): RedisNotSentException =
    new RedisNotSentException(message, Availability.closed(RedisIOException(s"The connection to $remote has been closed")))

  protected val decodingSubscription: Option[Subscription] = None

  private def createIOActor(): ActorRef = {
    connectionNumber += 1
    tcpConnection = null
    answered = false
    remainingByteStringOpt = None
    context.actorOf(
    Props(
      classOf[IOActor],
      self,
      remote,
      connectTimeout,
      maxWriteBatchSize,
      tcpSendBufferSizeHint,
      tcpReceiveBufferSizeHint,
      connectionNumber == 1
    ).withDispatcher(akkaIODispatcherPath),
    UniqueNameGenerator.getUniqueName(s"${nameOpt.getOrElse(s"$host-$port")}-io-actor")
    )
  }

  private def isIOActor(ref: ActorRef): Boolean = ref == ioActor

  private def armReceiveTimeout(): Unit = receiveTimeoutOpt.foreach { receiveTimeout =>
    if (timeoutCancellableOpt.isEmpty) {
      timeoutCancellableOpt = Some {
        context.system.scheduler.scheduleOnce(receiveTimeout, self, ReceiveTimedOut(connectionNumber))
      }
    }
  }

  private def cancelReceiveTimeout(): Unit = {
    timeoutCancellableOpt.foreach(_.cancel())
    timeoutCancellableOpt = None
  }
  
  private def createDecodersRouter(): Router = {
    val routees = Vector.fill(decodersCount) {
      val ref = context.actorOf(
        Props(classOf[DecoderActor], decodingSubscription).withDispatcher(akkaDecoderDispatcherPath),
        UniqueNameGenerator.getNumberedName(s"${nameOpt.getOrElse(s"$host-$port")}-decoder-actor")
      )
      context.watch(ref)
      ActorRefRoutee(ref)
    }
    Router(SmallestMailboxRoutingLogic(), routees)
  }
  
  protected def doSend(request: Request[_]): Unit = {
    lastActivity = System.nanoTime()
    armReceiveTimeout()
    requests.addLast(request)
    ioActor ! request
  }
  
  protected def send(requests: Request[_]*): Unit = {
    var isShuttingDown = false
    requests.foreach { request =>
      if (isShuttingDown) {
        request.failure(closing("Shutting down"))
      } else {
        request match {
          case select @ Select(db) =>
            database = db
            doSend(select)
          case setName @ ServerRequests.ClientSetName(name) => if (name.isEmpty) {
            nameOpt = None
            doSend(setName)
          } else {
            nameOpt = Some(name)
            doSend(setName)
          }
          case request @ (Quit() | ServerRequests.Shutdown(_)) =>
            isShuttingDown = true
            this.isShuttingDown = true
            health.closed()
            doSend(request)
            become(shuttingDown)
            // FIXME shutdown request does not complete on success
          case _ => doSend(request)
        }
      }
    }
  }
  
  protected def sendAllQueuedRequests(): Unit ={
    while (!queuedRequests.isEmpty) {
      send(queuedRequests.pop())
    }
  }
  
  protected def failAllQueuedRequests(throwable: Throwable): Unit = {
    while (!queuedRequests.isEmpty) {
      queuedRequests.pop().failure(throwable)
    }
  }
  
  protected def failAllSentRequests(throwable: Throwable): Unit = {
    while (!requests.isEmpty) {
      requests.pop().failure(throwable)
    }
  }
  
  protected def handleData(data: ByteString, responsesCount: Int): Unit = {
    val requestsCount = this.requests.size
    val (count, skip) = if (responsesCount > requestsCount) {
      (requestsCount, responsesCount - requestsCount)
    } else {
      (responsesCount, 0)
    }
    val requests = ListBuffer[Request[_]]()
    for (i <- 1 to count) {
      requests += this.requests.pop()
    }
    decoders.route(DecoderActor.Partition(data, requests.toList.iterator, skip), self)
  }
  
  protected def receiveData(data: ByteString): Int = {
    log.debug(s"Received data: ${data.decodeString("UTF-8").replace("\r\n", "\\r\\n")}")
    lastActivity = System.nanoTime()
    
    if (!answered) {
      answered = true
      connectionUp()
    }
    cancelReceiveTimeout()
    
    val completedData = remainingByteStringOpt match {
      case Some(remains) => remains ++ data
      case None => data
    }
    
    val buffer = completedData.asByteBuffer
    val responsesCount = Protocol.count(buffer)
    val position = buffer.position()
    
    if (buffer.remaining > 0) {
      remainingByteStringOpt = Some(ByteString(buffer))
    } else {
      remainingByteStringOpt = None
    }
    
    if (responsesCount > 0) {
      val trimmedData = if (remainingByteStringOpt.isDefined) {
        completedData.take(position)
      } else {
        completedData
      }
      handleData(trimmedData, responsesCount)
    }
    // requests still waiting for their replies are covered by the receive timeout too
    if (!requests.isEmpty) armReceiveTimeout()
    responsesCount
  }

  /** True for data from the current connection; data from an earlier connection is dropped */
  private def isCurrent(sender: ActorRef): Boolean = sender == tcpConnection && tcpConnection != null

  private def describe(closed: Tcp.ConnectionClosed): String = closed match {
    case Tcp.ErrorClosed(cause) => cause
    case Tcp.PeerClosed => "closed by the server"
    case Tcp.Aborted => "reset"
    case other => other.toString
  }

  /** Called when the connection has ended, after the requests sent on it have been failed */
  protected def onConnectionLost(): Unit = ()

  /**
   * The connection of the current IOActor has ended (the actor has stopped): every request sent on it is failed,
   * and a new connection is made, at once if Redis had answered on this one, else after the backoff.
   */
  protected def connectionEnded(sentMessage: String): Unit = {
    cancelReceiveTimeout()
    remainingByteStringOpt = None
    tcpConnection = null
    pingOpt = None
    if (answered) failedAttempts = 0 else failedAttempts += 1
    // the health first: the requests failed below carry the availability of this outage
    health.reconnecting(healthAttempt + 1, lastCause, waiting = failedAttempts > 0)
    failAllSentRequests(outcomeUnknown(sentMessage))
    onConnectionLost()
    if (!down) {
      down = true
      downSince = System.nanoTime()
      attemptsWhileDown = 0
      if (everConnected) {
        log.warning(s"Connection to $remote lost ($lastCause); reconnecting (backoff $minBackoff to $maxBackoff)")
      } else {
        log.warning(s"Could not connect to $remote ($lastCause); retrying (backoff $minBackoff to $maxBackoff)")
      }
    } else {
      log.debug(s"Attempt $attemptsWhileDown to connect to $remote failed ($lastCause)")
    }
    lastFailure = lastCause
    lastCause = "connection closed"
    if (failedAttempts == 0) {
      reconnect()
    } else {
      val delay = backoff(failedAttempts)
      val number = connectionNumber
      reconnectCancellableOpt = Some(context.system.scheduler.scheduleOnce(delay, self, ReconnectNow(number)))
      become(connecting)
    }
  }

  /** The wait before the next attempt after `failures` consecutive failed attempts: min, 2 min, 4 min, ... up to max */
  private def backoff(failures: Int): FiniteDuration = {
    val factor = 1L << math.min(failures - 1, 30)
    val millis = math.min(maxBackoff.toMillis, minBackoff.toMillis * factor)
    millis.millis
  }

  /** Redis has answered on the connection: it is up (logged once if it had been lost) */
  private def connectionUp(): Unit = {
    everConnected = true
    if (down) {
      down = false
      log.info(s"Connection to $remote is back after $attemptsWhileDown attempt(s), ${(System.nanoTime() - downSince) / 1000000} ms")
    }
  }
  
  protected def unhandled: Receive = {
    case x => log.error(s"Received unexpected message: $x")
  }
  
  protected def always: Receive = {
    case ProbeAnswered(number) =>
      if (number == connectionNumber) connectedAndInitialized()
    case CredentialsAccepted(credentials) =>
      authOpt = Some(credentials)
    case ClearCredentials =>
      authOpt = None
    case Abort if isIOActor(sender()) =>
      ioActor ! IOActor.AbortAck
      become(reconnecting)
    case Shutdown if isIOActor(sender()) =>
      ioActor ! IOActor.ShutdownAck
      become(reconnecting)
    case ConnectFailed(cause) =>
      if (isIOActor(sender())) lastCause = cause
    case closed: Tcp.ConnectionClosed =>
      // not when this actor closed it (receive timeout, unanswered PING): that is the cause
      if (isCurrent(sender()) && closingCause.isEmpty) lastCause = describe(closed)
    // from an earlier connection (its data, its IOActor, its timers), or a decoder that stopped
    case Tcp.Received(_) =>
    case Abort | Shutdown =>
    case Terminated(_) =>
    case ReceiveTimedOut(_) =>
    case ReconnectNow(_) =>
    case ConnectNow =>
    case IdleTick(_) =>
    case PingTimedOut(_, _) =>
  }
  
  protected def fail: Receive = {
    case request: Request[_] => request.failure(closing("Shutting down"))
    case transaction: Transaction => transaction.execRequest.failure(
      closing("Shutting down")
    )
  }
  
  protected def queue: Receive = {
    case request: Request[_] => queuedRequests.addLast(request)
    case t @ Transaction(requests) =>
      queuedRequests.addLast(MultiRequest)
      requests.foreach { request =>
        queuedRequests.addLast(request)
      }
      queuedRequests.addLast(t.execRequest)
  }
  
  protected def sendReceive: Receive = {
    case request: Request[_] =>
      send(request)
    case t @ Transaction(requests) =>
      send(MultiRequest)
      send(requests: _*)
      send(t.execRequest)
  }
  
  protected def become(state: Receive): Unit = context.become(state orElse always orElse unhandled)
  
  protected def reconnect(): Unit = {
    reconnectCancellableOpt.foreach(_.cancel())
    reconnectCancellableOpt = None
    if (down) attemptsWhileDown += 1
    healthAttempt += 1
    health.reconnecting(healthAttempt, lastFailure, waiting = false)
    ioActor = createIOActor()
    context.watch(ioActor)
    isConnecting = true
    closingCause = None
    become(connecting)
  }
  
  protected def handleReceiveTimeout(): Unit = {
    log.error("Receive timeout")
    closeConnection("receive timeout")
  }

  /** Redis has answered the initialisation of the current connection (or the PING made instead) */
  private def connectedAndInitialized(): Unit = {
    healthAttempt = 0
    health.connected()
  }

  /** Closes the current connection because of `cause`; what was sent on it fails (outcome unknown) */
  private def closeConnection(cause: String): Unit = {
    closingCause = Some(cause)
    lastCause = cause
    remainingByteStringOpt = None
    cancelReceiveTimeout()
    ioActor ! IOActor.Shutdown
    become(reconnecting)
  }

  /** True when nothing is outstanding on the connection (a subscriber adds its partly answered request) */
  protected def isIdle: Boolean = requests.isEmpty && queuedRequests.isEmpty

  /** Sends a PING of this actor's own: its reply goes to nobody else; unanswered within the timeout, the connection is closed */
  private def sendPing(): Request[_] = {
    val ping = Ping()
    pingNumber += 1
    pingOpt = Some(ping)
    send(ping)
    context.system.scheduler.scheduleOnce(idlePingTimeout, self, PingTimedOut(connectionNumber, pingNumber))
    ping
  }

  /** Redis answers on the new connection (no initialisation request to tell it): a PING, then the connection is up */
  private def probe(): Unit = {
    val number = connectionNumber
    sendPing().future.onComplete {
      case Failure(_: RedisIOException) =>
      case _ => self ! ProbeAnswered(number)
    }
  }

  private def scheduleIdleTick(): Unit = idlePingIntervalOpt.foreach { interval =>
    context.system.scheduler.scheduleOnce(interval, self, IdleTick(connectionNumber))
  }

  /** In the initialized state: the idle check and the timeout of a PING */
  private def healthChecks: Receive = {
    case IdleTick(number) if number == connectionNumber =>
      idlePingIntervalOpt.foreach { interval =>
        val unansweredPing = pingOpt.exists(!_.future.isCompleted)
        if (!unansweredPing && isIdle && System.nanoTime() - lastActivity >= interval.toNanos) sendPing()
      }
      scheduleIdleTick()
    case PingTimedOut(number, ping) if number == connectionNumber && ping == pingNumber &&
      pingOpt.exists(!_.future.isCompleted) =>
      log.warning(s"No reply to PING from $remote within $idlePingTimeout; closing the connection")
      closeConnection(s"no reply to PING within $idlePingTimeout")
  }
  
  protected def onConnect(): Unit = ()
  protected def onInitialized(): Unit = ()
  
  protected def shutdown(): Unit = {
    reconnectCancellableOpt.foreach(_.cancel())
    reconnectCancellableOpt = None
    decoders.route(Broadcast(PoisonPill), self)
    become(awaitingDecodersShutdown)
  }
  
  override def preStart(): Unit = {
    ioActor = createIOActor()
    decoders = createDecodersRouter()
    context.watch(ioActor)
    isConnecting = true
    become(connecting)
  }
  
  def receive: Receive = unhandled
  
  // Connecting (an attempt is in progress: isConnecting), or waiting for the next attempt after a failed one (the
  // backoff, ReconnectNow is scheduled). Requests wait for an attempt in progress; while there is none they fail at
  // once (a RedisCluster then sends them to another node), as do all with failCommandOnConnecting.
  def connecting: Receive = {
    case request: Quit =>
      request.success(())
      health.closed()
      failAllQueuedRequests(closing("Connection has been shutdown by QUIT command"))
      isShuttingDownBeforeConnected = true
      shutdown()
    case request: Request[_] =>
      if(failCommandOnConnecting) {
        request.failure(notSent("Trying to connect..."))
      } else if (!isConnecting) {
        request.failure(notSent(s"No connection to $remote ($lastFailure); reconnecting"))
      } else {
        queuedRequests.addLast(request)
      }
    case t @ Transaction(requests) =>
      if (!isConnecting && !failCommandOnConnecting) {
        t.execRequest.failure(notSent(s"No connection to $remote ($lastFailure); reconnecting"))
      } else {
        queuedRequests.addLast(MultiRequest)
        requests.foreach { request =>
          queuedRequests.addLast(request)
        }
        queuedRequests.addLast(t.execRequest)
      }
    case ReconnectNow(number) if number == connectionNumber && !isConnecting =>
      reconnect()
    case ConnectNow if !isConnecting && !isShuttingDownBeforeConnected =>
      reconnect()
    case Connected(connection) if isIOActor(sender()) =>
      isConnecting = false
      tcpConnection = connection

      if (isShuttingDownBeforeConnected) {
        shutdown()
      } else {
        onConnect()

        val authRequestOpt = authOpt.map { auth =>
          Auth(auth.password, auth.username)
        }
        val selectRequestOpt = if (database > 0) {
          Some(Select(database))
        } else {
          None
        }
        val setNameRequestOpt = nameOpt.map { name =>
          ServerRequests.ClientSetName(name)
        }

        val authFuture = authRequestOpt match {
          case Some(request) => request.future
          case None => Future.successful(())
        }
        val selectFuture = selectRequestOpt match {
          case Some(request) => request.future
          case None => Future.successful(())
        }
        val setNameFuture = setNameRequestOpt match {
          case Some(request) => request.future
          case None => Future.successful(())
        }

        val requests = List[Option[Request[Unit]]](
          authRequestOpt, selectRequestOpt, setNameRequestOpt
        ).flatten

        initializationRequestsCount = requests.size

        if (initializationRequestsCount > 0) {
          send(requests: _*)
          become(initializing)
        } else {
          probe()
          onInitialized()
          sendAllQueuedRequests()
          if (isShuttingDown) {
            become(shuttingDown)
          } else {
            scheduleIdleTick()
            become(initialized)
          }
        }

        // without a stack trace; a failure because the connection was lost is logged as the connection's state
        authFuture.failed.foreach {
          case _: RedisIOException =>
          case e => log.error(s"Could not authenticate to $remote: $e")
        }
        selectFuture.failed.foreach {
          case _: RedisIOException =>
          case e => log.error(s"Could not select database '$database' in $remote: $e")
        }
        setNameFuture.failed.foreach {
          case _: RedisIOException =>
          case e => log.error(s"Could not set client name in $remote: $e")
        }
      }
    case Terminated(ref) if isIOActor(ref) =>
      isConnecting = false
      // the health first: the queued requests carry the availability of this outage
      health.reconnecting(healthAttempt, lastCause, waiting = false)
      failAllQueuedRequests(notSent(s"Could not connect to $remote ($lastCause)"))
      connectionEnded(s"Could not connect to $remote")
  }
  
  def initializing: Receive = queue orElse {
    case Tcp.Received(data) if isCurrent(sender()) =>
      val responsesCount = receiveData(data)
      initializationRequestsCount -= responsesCount
      if (initializationRequestsCount == 0) {
        connectedAndInitialized()
        onInitialized()
        sendAllQueuedRequests()
        if (isShuttingDown) {
          become(shuttingDown)
        } else {
          scheduleIdleTick()
          become(initialized)
        }
      }
    case ReceiveTimedOut(number) if number == connectionNumber => handleReceiveTimeout()
    case Terminated(ref) if isIOActor(ref) =>
      health.reconnecting(healthAttempt, lastCause, waiting = false)
      failAllQueuedRequests(notSent(s"Could not initialize connection to $remote"))
      connectionEnded(s"Could not initialize connection to $remote")
  }
  
  def initialized: Receive = sendReceive orElse healthChecks orElse {
    case Tcp.Received(data) if isCurrent(sender()) => receiveData(data)
    case ReceiveTimedOut(number) if number == connectionNumber => handleReceiveTimeout()
    case Terminated(ref) if isIOActor(ref) =>
      connectionEnded("Connection has been shutdown abruptly")
  }
  
  def reconnecting: Receive = queue orElse {
    case Terminated(ref) if isIOActor(ref) =>
      closingCause match {
        case Some("receive timeout") => connectionEnded("Receive timeout")
        case Some(cause) => connectionEnded(s"Connection closed: $cause")
        case None => connectionEnded("Connection has been shutdown abruptly")
      }
  }
  
  def shuttingDown: Receive = fail orElse {
    case Tcp.Received(data) if isCurrent(sender()) => receiveData(data)
    case ReceiveTimedOut(number) if number == connectionNumber => handleReceiveTimeout()
    case Shutdown if isIOActor(sender()) => ioActor ! IOActor.ShutdownAck
    case Terminated(ref) if isIOActor(ref) =>
      // what was sent and not answered before the connection closed will not be answered
      failAllSentRequests(outcomeUnknown("Connection has been shutdown"))
      shutdown()
  }
  
  override def postStop(): Unit = health.closed()

  def awaitingDecodersShutdown: Receive = fail orElse {
    case Terminated(ref) if !isIOActor(ref) =>
      decodersCount -= 1
      if (decodersCount == 0) {
        log.info("Connection has been shutdown gracefully")
        context.stop(self)
      }
  }
  
}

object ListenerActor {
  /** The IOActor is connected; `connection` is the TCP connection actor, which sends the data to the listener */
  case class Connected(connection: ActorRef)
  /** The IOActor could not connect (it stops) */
  case class ConnectFailed(cause: String)
  case object Abort
  case object Shutdown
  private case class ReceiveTimedOut(connectionNumber: Int)
  private case class ReconnectNow(connectionNumber: Int)
  private case class IdleTick(connectionNumber: Int)
  private case class PingTimedOut(connectionNumber: Int, ping: Int)
  private case class ProbeAnswered(connectionNumber: Int)
  /** The server accepted an `AUTH` with these credentials. */
  case class CredentialsAccepted(credentials: AuthConfig)
  /** Do not send `AUTH` on later reconnects. */
  case object ClearCredentials
  /**
   * While waiting for the next attempt to connect (the backoff): make it now; the requests that follow wait for it.
   * Ignored otherwise. The deprecated `Redis.blocking` sends it before every call, as a call made a connection
   * before 1.1.0.
   */
  private[scredis] case object ConnectNow
}
