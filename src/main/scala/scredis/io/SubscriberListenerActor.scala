// Modified by Sopranoworks: every reply completes the pending request (status replies, errors, unmatched replies); QUIT fails what is outstanding.
package scredis.io

import akka.util.ByteString
import scredis.exceptions._
import scredis.protocol.{AuthConfig, Request, Response}
import scredis.protocol.requests.ConnectionRequests.Quit
import scredis.protocol.requests.PubSubRequests
import scredis.{PubSubMessage, Subscription}

import scala.collection.mutable.{HashSet => MHashSet}
import scala.concurrent.duration._
import scala.language.existentials

class SubscriberListenerActor(
  subscription: Subscription,
  host: String,
  port: Int,
  authOpt: Option[AuthConfig],
  nameOpt: Option[String],
  decodersCount: Int,
  receiveTimeoutOpt: Option[FiniteDuration],
  connectTimeout: FiniteDuration,
  maxWriteBatchSize: Int,
  tcpSendBufferSizeHint: Int,
  tcpReceiveBufferSizeHint: Int,
  akkaIODispatcherPath: String,
  akkaDecoderDispatcherPath: String
) extends ListenerActor(
  host = host,
  port = port,
  authOpt = authOpt,
  database = 0,
  nameOpt = nameOpt,
  decodersCount = decodersCount,
  receiveTimeoutOpt = receiveTimeoutOpt,
  connectTimeout = connectTimeout,
  maxWriteBatchSize = maxWriteBatchSize,
  tcpSendBufferSizeHint = tcpSendBufferSizeHint,
  tcpReceiveBufferSizeHint = tcpReceiveBufferSizeHint,
  akkaIODispatcherPath = akkaIODispatcherPath,
  akkaDecoderDispatcherPath = akkaDecoderDispatcherPath,
  failCommandOnConnecting = false
) {
  
  import SubscriberListenerActor._
  
  private val subscribedChannels = MHashSet[String]()
  private val subscribedPatterns = MHashSet[String]()
  
  private val savedSubscribedChannels = MHashSet[String]()
  private val savedSubscribedPatterns = MHashSet[String]()
  
  private var isInitialized = false
  private var isClosing = false
  private var shouldSendRequests = false
  private var requestOpt: Option[Request[_]] = None
  private var requestResponsesCount = 0
  private var subscribedCount = 0
  private var subscribedChannelsCount = 0
  private var subscribedPatternsCount = 0

  override protected val decodingSubscription: Option[Subscription] = Some(subscription)
  
  override protected def onConnect(): Unit = {
    isInitialized = false
    shouldSendRequests = false
    requestOpt = None
    requestResponsesCount = 0
    subscribedCount = 0
    subscribedChannelsCount = 0
    subscribedPatternsCount = 0
  }
  
  override protected def onInitialized(): Unit = {
    isInitialized = true
    subscribedChannels.foreach { channel =>
      log.info(s"Automatically re-subscribing to channel: $channel")
      val request = PubSubRequests.Subscribe(channel)
      if (shouldSendRequests) {
        send(request)
      } else {
        queuedRequests.push(request)
      }
    }
    subscribedPatterns.foreach { pattern =>
      log.info(s"Automatically re-subscribing to pattern: $pattern")
      val request = PubSubRequests.PSubscribe(pattern)
      if (shouldSendRequests) {
        send(request)
      } else {
        queuedRequests.push(request)
      }
    }
    subscribedChannels.clear()
    subscribedPatterns.clear()
  }
  
  override protected def handleData(
    data: ByteString, responsesCount: Int
  ): Unit = decoders.route(DecoderActor.SubscribePartition(data), self)

  /** The request the next reply belongs to: the one that is partially answered, else the oldest one sent. */
  private def pendingRequest(): Option[Request[_]] = requestOpt match {
    case some @ Some(_) => some
    case None => if (requests.isEmpty) None else Some(requests.pop())
  }

  private def clearPendingRequest(): Unit = {
    requestOpt = None
    requestResponsesCount = 0
  }

  /** Completes the pending request, if there is one, with an error. */
  private def failPendingRequest(throwable: Throwable): Unit = {
    pendingRequest().foreach(_.failure(throwable))
    clearPendingRequest()
  }

  override protected def always: Receive = super.always orElse {
    case Complete(message) => {
      requestResponsesCount += 1
      val countOpt = message match {
        case PubSubMessage.Subscribe(channel, count) => {
          log.info(s"Subscribed to channel: $channel")
          subscribedChannels += channel
          subscribedCount += 1
          subscribedChannelsCount += 1
          Some(count)
        }
        case PubSubMessage.PSubscribe(pattern, count) => {
          log.info(s"Subscribed to pattern: $pattern")
          subscribedPatterns += pattern
          subscribedCount += 1
          subscribedPatternsCount += 1
          Some(count)
        }
        case PubSubMessage.Unsubscribe(channelOpt, count) => {
          channelOpt.foreach { channel =>
            log.info(s"Unsubscribed from channel: $channel")
            subscribedChannels -= channel
          }
          val difference = subscribedCount - count
          subscribedCount -= difference
          subscribedChannelsCount -= difference
          Some(count)
        }
        case PubSubMessage.PUnsubscribe(patternOpt, count) => {
          patternOpt.foreach { pattern =>
            log.info(s"Unsubscribed from pattern: $pattern")
            subscribedPatterns -= pattern
          }
          val difference = subscribedCount - count
          subscribedCount -= difference
          subscribedPatternsCount -= difference
          Some(count)
        }
        case _ => None
      }

      countOpt match {
        case None =>
          // a reply that matches no request: complete the pending one with an error instead of leaving it open
          failPendingRequest(RedisProtocolException(s"Unexpected pub sub message received: $message"))
        case Some(count) => pendingRequest() match {
          case None =>
            log.warning(s"Received '$message' without a pending request")
            clearPendingRequest()
          case Some(request) =>
            val argsCount = request.argsCount
            // Unsubscribe() or PUnsubscribe()
            if (argsCount == 0) {
              val otherCountOpt = message match {
                case PubSubMessage.Unsubscribe(_, _) => Some(subscribedPatternsCount)
                case PubSubMessage.PUnsubscribe(_, _) => Some(subscribedChannelsCount)
                case _ => None
              }
              otherCountOpt match {
                case None =>
                  request.failure(RedisProtocolException(
                    s"Unexpected pub sub message received: '$message' in response to '$request'"
                  ))
                  clearPendingRequest()
                case Some(otherCount) =>
                  if (count == otherCount) {
                    request.success(count)
                    clearPendingRequest()
                  } else {
                    requestOpt = Some(request)
                  }
              }
            } else {
              if (argsCount == requestResponsesCount) {
                request.success(count)
                clearPendingRequest()
              } else {
                requestOpt = Some(request)
              }
            }
        }
      }
    }
    case Confirm(value) =>
      // a status reply (e.g. +OK to AUTH, CLIENT SETNAME or QUIT): the request decodes it itself
      pendingRequest() match {
        case Some(request) =>
          log.debug(s"Completing ${request} with ${value}.")
          request.complete(value)
          clearPendingRequest()
        case None => log.warning(s"Received '$value' without a pending request")
      }
    case Fail(message) =>
      failPendingRequest(RedisErrorResponseException(message))
    case SaveSubscriptions =>
      savedSubscribedChannels ++= subscribedChannels
      savedSubscribedPatterns ++= subscribedPatterns
    case SendAsRegularClient(request) =>
      onConnect()
      send(request)
    case RecoverPreviousSubscriberState =>
      if (isClosing) {
        savedSubscribedChannels.clear()
        savedSubscribedPatterns.clear()
      } else {
        shouldSendRequests = true
        subscribedChannels ++= savedSubscribedChannels
        subscribedPatterns ++= savedSubscribedPatterns
        savedSubscribedChannels.clear()
        savedSubscribedPatterns.clear()
        onInitialized()
        shouldSendRequests = false
      }
    case Shutdown(quit) =>
      isClosing = true
      // requests still outstanding when the connection is closed will never be answered
      val closed = RedisIOException("Connection has been closed")
      requestOpt.foreach(_.failure(closed))
      failAllSentRequests(closed)
      failAllQueuedRequests(closed)
      onConnect()
      send(quit)
  }
  
}

object SubscriberListenerActor {
  case class Complete(message: PubSubMessage)
  case class Confirm(message: Response)
  case class Fail(message: String)
  case object SaveSubscriptions
  case class SendAsRegularClient(request: Request[_])
  case object RecoverPreviousSubscriberState
  case class Shutdown(quit: Quit)
}
