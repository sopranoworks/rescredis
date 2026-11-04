// Modified by Sopranoworks: reports the buffer sizes it connects with (at info level for a listener's first connection); never completes a request itself (the listener
// fails what it sent on a connection when the connection's IOActor has stopped), a failed write closes the connection
// instead of writing the batch again, connect failures are reported to the listener with their cause.
package scredis.io

import java.net.InetSocketAddress
import java.nio.Buffer

import akka.actor._
import akka.io.{IO, Tcp}
import akka.util.ByteString
import scredis.protocol.{Protocol, Request}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.language.postfixOps

class IOActor(
  listenerActor: ActorRef,
  remote: InetSocketAddress,
  connectTimeout: FiniteDuration,
  maxWriteBatchSize: Int,
  tcpSendBufferSizeHint: Int,
  tcpReceiveBufferSizeHint: Int,
  firstConnection: Boolean = true
) extends Actor with ActorLogging {
  
  import IOActor._
  import Tcp._
  import context.{dispatcher, system}
  
  private val scheduler = context.system.scheduler
  
  private val bufferPool = new scredis.util.BufferPool(
    maxCapacity = 1,
    maxBufferSize = maxWriteBatchSize + (0.25 * maxWriteBatchSize).toInt
  )
  
  private var canWrite = false
  private var timeoutCancellableOpt: Option[Cancellable] = None
  
  protected val requests = new java.util.LinkedList[Request[_]]()
  protected var connection: ActorRef = _
  
  protected def connect(): Unit = {
    val sendBuffer = SO.SendBufferSize(tcpSendBufferSizeHint)
    val receiveBuffer = SO.ReceiveBufferSize(tcpReceiveBufferSizeHint)
    // the first connection of a listener at info level; reconnect attempts are logged by the listener when its
    // connection's state changes
    val message = s"Connecting to $remote (tcp send buffer ${sendBuffer.size} bytes, " +
      s"receive buffer ${receiveBuffer.size} bytes)"
    if (firstConnection) log.info(message) else log.debug(message)
    IO(Tcp) ! Connect(
      remoteAddress = remote,
      options = List[akka.io.Inet.SocketOption](
        SO.KeepAlive(true),
        SO.TcpNoDelay(true),
        SO.ReuseAddress(true),
        sendBuffer,
        receiveBuffer
      ),
      timeout = Some(connectTimeout)
    )
    timeoutCancellableOpt = Some {
      scheduler.scheduleOnce(2 seconds, self, ConnectTimeout)
    }
  }
  
  protected def abort(): Unit = {
    listenerActor ! ListenerActor.Abort
    become(awaitingAbort)
  }
  
  protected def encode(request: Request[_]): Int = {
    request.encode()
    request.encoded match {
      case Left(bytes) => bytes.length
      case Right(buffer) => buffer.remaining
    }
  }
  
  protected def write(requests: Seq[Request[_]], lengthOpt: Option[Int] = None): Unit = {
    val length = lengthOpt.getOrElse {
      requests.foldLeft(0)((length, request) => length + encode(request))
    }
    val buffer = bufferPool.acquire(length)
    requests.foreach { request =>
      request.encoded match {
        case Left(bytes) => buffer.put(bytes)
        case Right(buff) =>
          buffer.put(buff)
          Protocol.releaseBuffer(buff)
      }
    }
    (buffer: Buffer).flip()
    val data = ByteString(buffer)
    log.debug(s"Writing data: ${data.decodeString("UTF-8")}")
    connection ! Write(data, WriteAck)
    bufferPool.release(buffer)
    canWrite = false
  }
  
  protected def write(): Unit = {
    if (requests.isEmpty) {
      canWrite = true
      return
    }
    
    var length = 0
    val batch = ListBuffer[Request[_]]()
    while (!requests.isEmpty && length < maxWriteBatchSize) {
      val request = requests.pop()
      length += encode(request)
      batch += request
    }
    write(batch.toList, Some(length))
  }
  
  protected def always: Receive = {
    case Shutdown => abort()
    case Terminated(_) =>
      listenerActor ! ListenerActor.Shutdown
      become(awaitingShutdown)
    case _: ConnectionClosed => // the listener (the connection's handler) records why it closed
  }
  
  protected def fail: Receive = {
    case x => log.error(s"Received unhandled message: $x")
  }
  
  protected def become(state: Receive): Unit = context.become(state orElse always orElse fail)
  
  override def preStart(): Unit = {
    connect()
    become(connecting)
  }
  
  def receive: Receive = fail
  
  def connecting: Receive = {
    case Connected(remoteConnected, localAddress) =>
      log.debug(s"Connected to $remoteConnected from $localAddress")
      connection = sender()
      connection ! Register(listenerActor)
      context.watch(connection)
      listenerActor ! ListenerActor.Connected(connection)
      timeoutCancellableOpt.foreach(_.cancel())
      canWrite = true
      write()
      become(connected)
    case failed @ CommandFailed(_: Connect) =>
      // the listener logs it (once per change of the connection's state)
      listenerActor ! ListenerActor.ConnectFailed(failed.cause.map(_.toString).getOrElse("connect failed"))
      timeoutCancellableOpt.foreach(_.cancel())
      context.stop(self)
    case ConnectTimeout =>
      listenerActor ! ListenerActor.ConnectFailed(s"connect timeout")
      context.stop(self)
  }
  
  def connected: Receive = {
    case request: Request[_] =>
      requests.addLast(request)
      if (canWrite) {
        write()
      }
    case WriteAck =>
      write()
    case CommandFailed(_: Write) =>
      // The requests of the failed write cannot be written again: their encoded buffers have been released, and
      // which of them reached the server is unknown. The connection is closed; the listener fails what it sent on
      // it and reconnects.
      log.error(s"Write failed, closing the connection")
      connection ! Abort
      become(closingAfterFailedWrite)
  }
  
  def closingAfterFailedWrite: Receive = {
    case _: Request[_] =>
    case WriteAck =>
    case CommandFailed(_: Write) =>
  }
  
  // A request that reaches this actor after its connection has gone is not written and not completed here: the
  // listener holds every request it sent on this connection, in order, and fails them all when this actor has
  // stopped. Completing it here as well made the listener remove a request it could not identify (the oldest).
  def awaitingShutdown: Receive = {
    case _: Request[_] =>
    case WriteAck =>
    case CommandFailed(_: Write) =>
    case ShutdownAck => context.stop(self)
  }
  
  def awaitingAbort: Receive = {
    case _: Request[_] =>
    case WriteAck =>
    case CommandFailed(_: Write) =>
    case AbortAck =>
      connection ! Abort
      timeoutCancellableOpt = Some {
        scheduler.scheduleOnce(3 seconds, self, AbortTimeout)
      }
      become(aborting)
  }
  
  def aborting: Receive = {
    case WriteAck =>
    case CommandFailed(_: Write) =>
    case Aborted =>
    case Terminated(_) =>
      timeoutCancellableOpt.foreach(_.cancel())
      context.stop(self)
    case AbortTimeout =>
      log.error(s"A timeout occurred while resetting the connection")
      context.stop(connection)
  }
  
}

object IOActor {
  object WriteAck extends Tcp.Event
  case object ConnectTimeout
  case object AbortTimeout
  case object AbortAck
  case object ShutdownAck
  case object Shutdown
}
