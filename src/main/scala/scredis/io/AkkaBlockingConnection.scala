// Modified by Sopranoworks: credentials kept only once accepted; closeAsync; health; a call refused while another one blocks fails as
// not sent (with an availability that completes when that call ends), a call whose deadline passes as outcome unknown, a call
// made after QUIT as not sent.
package scredis.io

import java.time.Instant
import java.util.concurrent.TimeoutException
import java.util.concurrent.locks.ReentrantLock

import akka.actor._
import scredis.Availability
import scredis.exceptions._
import scredis.protocol._
import scredis.protocol.requests.ConnectionRequests.Quit
import scredis.util.UniqueNameGenerator

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Try}

/**
 * This trait represents a blocking connection to a `Redis` server.
 */
abstract class AkkaBlockingConnection(
  system: ActorSystem,
  host: String,
  port: Int,
  authOpt: Option[AuthConfig],
  database: Int,
  nameOpt: Option[String],
  decodersCount: Int,
  connectTimeout: FiniteDuration,
  maxWriteBatchSize: Int,
  tcpSendBufferSizeHint: Int,
  tcpReceiveBufferSizeHint: Int,
  akkaListenerDispatcherPath: String,
  akkaIODispatcherPath: String,
  akkaDecoderDispatcherPath: String,
  failCommandOnConnecting:Boolean
) extends AbstractAkkaConnection(
  system = system,
  host = host,
  port = port,
  authOpt = authOpt,
  database = database,
  nameOpt = nameOpt,
  decodersCount = decodersCount,
  receiveTimeoutOpt = None,
  connectTimeout = connectTimeout,
  maxWriteBatchSize = maxWriteBatchSize,
  tcpSendBufferSizeHint = tcpSendBufferSizeHint,
  tcpReceiveBufferSizeHint = tcpReceiveBufferSizeHint,
  akkaListenerDispatcherPath = akkaListenerDispatcherPath,
  akkaIODispatcherPath = akkaIODispatcherPath,
  akkaDecoderDispatcherPath = akkaDecoderDispatcherPath
) with BlockingConnection {
  
  private val lock = new ReentrantLock()
  
  protected val listenerActor = system.actorOf(
    Props(
      classOf[ListenerActor],
      host,
      port,
      authOpt,
      database,
      nameOpt,
      decodersCount,
      receiveTimeoutOpt,
      connectTimeout,
      maxWriteBatchSize,
      tcpSendBufferSizeHint,
      tcpReceiveBufferSizeHint,
      akkaIODispatcherPath,
      akkaDecoderDispatcherPath,
      failCommandOnConnecting,
      health
    ).withDispatcher(akkaListenerDispatcherPath),
    UniqueNameGenerator.getUniqueName(s"${nameOpt.getOrElse(s"$host-$port")}-listener-actor")
  )
  
  // while a call is made: completes when it ends (the availability of a call refused meanwhile)
  @volatile private var busy: Availability = Availability.Available

  private def withLock[A](f: => A): A = {
    if (lock.tryLock) {
      busy = Availability.outage(Instant.now(), "a blocking call is in progress on the connection")
      try {
        f
      } finally {
        busy.complete()
        lock.unlock()
      }
    } else {
      throw new RedisNotSentException("Trying to send request on a blocked connection", busy)
    }
  }

  /** False: a call made after the connection was closed goes to the stopped connection (as before 1.1.0) */
  protected def refusesAfterClose: Boolean = true

  override protected[scredis] def sendBlocking[A](request: Request[A])(
    implicit timeout: Duration
  ): Try[A] = withLock {
    if (isShuttingDown && refusesAfterClose) Failure(closedNotSent("Connection has been shutdown"))
    else write(request)
  }

  private def write[A](request: Request[A])(implicit timeout: Duration): Try[A] = {
    logger.debug(s"Sending blocking request: $request")
    updateState(request)
    val future = keepCredentialsOnceAccepted(request, Protocol.send(request, listenerActor))
    // the deadline of a blocking call passing: it was written, and its reply has not come (the connection may still
    // be blocked in Redis)
    Try(Await.result(future, timeout)).recoverWith {
      case e: TimeoutException => Failure(new RedisOutcomeUnknownException(
        s"No reply to $request within $timeout", health.availability, e
      ))
    }
  }

  /**
   * Sends QUIT without waiting for it: after a call has timed out on the client side the connection may still be
   * blocked in Redis, and QUIT is answered only after the blocked command.
   */
  private[scredis] def closeAsync(): Future[Unit] = {
    val quit = Quit()
    updateState(quit)
    Protocol.send(quit, listenerActor)
  }
  
}