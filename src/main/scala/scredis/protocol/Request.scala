// Modified by Sopranoworks: copyForResend (a request object is sent on one connection only).
package scredis.protocol

import java.nio.ByteBuffer

import scredis.exceptions._
import scredis.serialization.UTF8StringReader

import scala.concurrent.{Future, Promise}

/** A trait for requests which operate on at least one key.
  * Needed to handle cluster sharding. */
trait Key {
  val key: String
}

/** Marker trait for requests that make sense on any member of a cluster. */
trait Cluster

abstract class Request[A](command: Command, args: Any*) {
  private var promise = Promise[A]()
  private var _buffer: ByteBuffer = null
  private var _bytes: Array[Byte] = null
  def future: Future[A] = promise.future
  val repliesCount = 1

  /**
   * A new request with the same command and arguments that decodes its reply in the same way, for sending this
   * request again on another connection. A request object is sent on one connection only: a connection's listener
   * pairs replies with the requests it holds, and the request's encoded buffer is released once it is written.
   */
  private[scredis] def copyForResend(): Request[A] = {
    val original = this
    new Request[A](command, args: _*) {
      override val repliesCount: Int = original.repliesCount
      override def decode: Decoder[A] = original.decode
      override def isReadOnly: Boolean = original.isReadOnly
      override def toString: String = original.toString
    }
  }

  /** Reset the state of this Request to reuse it. */
  private[scredis] def reset(): Unit = {
    promise = Promise[A]()
    _buffer = null
    _bytes = null
  }
  
  private[scredis] def encode(): Unit = if (_buffer == null && _bytes == null) {
    command match {
      case x: ZeroArgCommand  => _bytes = x.encoded
      case _                  => _buffer = command.encode(args.toList)
    }
  }
  
  private[scredis] def encoded: Either[Array[Byte], ByteBuffer] = if (_bytes != null) {
    Left(_bytes)
  } else {
    Right(_buffer)
  }
  
  private[scredis] def complete(response: Response): Unit = {
    response match {
      case SimpleStringResponse("QUEUED") =>
      case ClusterErrorResponse(error,message) => failure(RedisClusterErrorResponseException(error,message))
      case ErrorResponse(message) => failure(RedisErrorResponseException(message))
      case response => try {
        success(decode(response))
      } catch {
        case e @ RedisTransactionAbortedException => failure(e)
        case e: RedisReaderException => failure(e)
        case e: Throwable => failure(
            RedisProtocolException(s"Unexpected response for request '$this': $response", e)
           )
      }
    }
  }
  
  private[scredis] def success(value: Any): Unit = {
    try {
      promise.success(value.asInstanceOf[A])
    } catch {
      case e: IllegalStateException =>
    } finally {
      Protocol.release()
    }
  }
  
  private[scredis] def failure(throwable: Throwable): Unit = {
    try {
      promise.failure(throwable)
    } catch {
      case e: IllegalStateException =>
    } finally {
      Protocol.release()
    }
  }
  
  def decode: Decoder[A]

  def argsCount: Int = args.size
  def isReadOnly: Boolean = command.isReadOnly
  
  override def toString: String = (command +: args).map {
    case bytes: Array[Byte] => UTF8StringReader.read(bytes)
    case x                  => x.toString
  }.mkString(" ")
  
}
