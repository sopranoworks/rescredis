// Modified by Sopranoworks: tests the Redis methods that forward to the subscriber client.
package scredis

import java.net.ServerSocket
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import org.scalatest._
import org.scalatest.concurrent._
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec
import scredis.exceptions.RedisIOException
import scredis.protocol.AuthConfig
import scredis.tags._
import scredis.util.TestUtils._

import scala.concurrent.{ExecutionContext, Future}

/**
 * `Redis.auth`, `Redis.clientSetName` and `Redis.quit` forward to the subscriber client. They are tried with the
 * subscriber in every state: not initialised, connected but not subscribed, subscribed to a channel, subscribed
 * to a channel and a pattern.
 *
 * Every wait has a time limit, so that a request that is never completed fails the test instead of blocking
 * the suite.
 */
class RedisSubscriberForwardingSpec extends AnyWordSpec
  with Matchers
  with ScalaFutures {

  private implicit val ec: ExecutionContext = ExecutionContext.global
  private def within(seconds: Int) = timeout(Span(seconds, Seconds))

  private val redisPassword = "foobar"

  /** A subscriber state, and what it takes to bring a `Redis` into it. */
  private sealed abstract class State(val description: String) {
    def prepare(redis: Redis): Unit
  }
  private case object NotInitialised extends State("not initialised") {
    def prepare(redis: Redis): Unit = ()
  }
  private case object NotSubscribed extends State("connected, not subscribed") {
    def prepare(redis: Redis): Unit = redis.subscriber should not be null
  }
  private case object ChannelSubscribed extends State("subscribed to a channel") {
    def prepare(redis: Redis): Unit = {
      redis.subscriber.subscribe("fwd-channel").futureValue(within(5)) should be (1)
    }
  }
  private case object ChannelAndPatternSubscribed extends State("subscribed to a channel and a pattern") {
    def prepare(redis: Redis): Unit = {
      redis.subscriber.subscribe("fwd-channel").futureValue(within(5)) should be (1)
      redis.subscriber.pSubscribe("fwd-pattern-*").futureValue(within(5)) should be (2)
    }
  }
  private val states = List(NotInitialised, NotSubscribed, ChannelSubscribed, ChannelAndPatternSubscribed)
  private val subscribedStates = List(ChannelSubscribed, ChannelAndPatternSubscribed)

  private class Subscribing {
    val received = new LinkedBlockingQueue[PubSubMessage]()
    val subscription: Subscription = message => received.add(message)

    /** Waits for a message published to `channel`. */
    def awaitMessage(channel: String): Boolean = {
      val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
      var found = false
      while (!found && System.nanoTime() < deadline) {
        val message = received.poll(100, TimeUnit.MILLISECONDS)
        found = message match {
          case PubSubMessage.Message(c, _) => c == channel
          case PubSubMessage.PMessage(_, c, _) => c == channel
          case _ => false
        }
      }
      found
    }
  }

  /** Runs `Redis.quit()` on another thread: it takes locks and may block when a request is stuck. */
  private def quitWithin(redis: Redis, seconds: Int): Unit =
    Future(redis.quit()).flatten.futureValue(within(seconds)) should be (())

  private def clientList(admin: Client): List[Map[String, String]] =
    admin.clientList().futureValue(within(5))

  "Redis.clientSetName" when {
    for (state <- states) {
      s"the subscriber is ${state.description}" should {
        "complete, set the name on the clients, keep the subscriptions, and quit() must not block" taggedAs (V260) in {
          val subscribing = new Subscribing
          val redis = new Redis(RedisConfig(), subscribing.subscription)
          val admin = Client()
          try {
            redis.blocking should not be null
            state.prepare(redis)
            redis.clientSetName("rescredis-" + state.hashCode).futureValue(within(5)) should be (())
            val expected = "rescredis-" + state.hashCode
            // non-blocking, blocking and (when it exists) the subscriber connection carry the name
            val named = clientList(admin).flatMap(_.get("name")).count(_ == expected)
            named should be (if (state == NotInitialised) 2 else 3)

            if (subscribedStates.contains(state)) {
              // the subscriptions are restored
              admin.publish("fwd-channel", "hello").futureValue(within(5)) should be >= 1L
              subscribing.awaitMessage("fwd-channel") should be (true)
            }
            if (state == ChannelAndPatternSubscribed) {
              admin.publish("fwd-pattern-1", "hello").futureValue(within(5)) should be >= 1L
              subscribing.awaitMessage("fwd-pattern-1") should be (true)
            }
          } finally {
            try quitWithin(redis, 10)
            finally admin.quit().futureValue(within(5))
          }
        }
      }
    }

    "the name is cleared with the empty string while subscribed" should {
      "complete and clear the name on the subscriber connection" taggedAs (V260) in {
        val redis = Redis()
        val admin = Client()
        try {
          redis.subscriber.subscribe("fwd-channel").futureValue(within(5)) should be (1)
          redis.clientSetName("rescredis-clear").futureValue(within(5)) should be (())
          clientList(admin).flatMap(_.get("name")).count(_ == "rescredis-clear") should be (2)
          redis.clientSetName("").futureValue(within(5)) should be (())
          clientList(admin).flatMap(_.get("name")).count(_ == "rescredis-clear") should be (0)
        } finally {
          try quitWithin(redis, 10)
          finally admin.quit().futureValue(within(5))
        }
      }
    }
  }

  "A subscriber with a configured client name" should {
    "complete its subscriptions" taggedAs (V260) in {
      val redis = Redis(nameOpt = Some("rescredis-configured"))
      val admin = Client()
      try {
        redis.subscriber.subscribe("fwd-channel").futureValue(within(5)) should be (1)
        redis.subscriber.pSubscribe("fwd-pattern-*").futureValue(within(5)) should be (2)
        clientList(admin).flatMap(_.get("name")).count(_ == "rescredis-configured") should be >= 1
      } finally {
        try quitWithin(redis, 10)
        finally admin.quit().futureValue(within(5))
      }
    }
  }

  "Redis.auth" when {
    for (state <- states) {
      s"the subscriber is ${state.description}" should {
        "complete and authenticate every connection as the ACL user" taggedAs (V260) in {
          val subscribing = new Subscribing
          val redis = new Redis(
            port = 6380, authOpt = Some(AuthConfig(None, redisPassword)), subscription = subscribing.subscription
          )
          val admin = Client(port = 6380, authOpt = Some(AuthConfig(None, redisPassword)))
          try {
            redis.blocking should not be null
            state.prepare(redis)
            redis.auth("secretpass", Some("tester")).futureValue(within(5)) should be (())
            redis.ping().futureValue(within(5)) should be ("PONG")
            val users = clientList(admin).flatMap(_.get("user"))
            users.count(_ == "tester") should be (if (state == NotInitialised) 2 else 3)

            if (subscribedStates.contains(state)) {
              admin.publish("fwd-channel", "hello").futureValue(within(5)) should be >= 1L
              subscribing.awaitMessage("fwd-channel") should be (true)
            }
          } finally {
            try quitWithin(redis, 10)
            finally admin.quit().futureValue(within(5))
          }
        }
      }
    }
  }

  "Redis.quit" when {
    for (state <- states) {
      s"the subscriber is ${state.description}" should {
        "complete" taggedAs (V100) in {
          val redis = Redis()
          state.prepare(redis)
          quitWithin(redis, 10)
        }
      }
    }
  }

  "SubscriberClient.quit" when {
    "a request is outstanding because the server does not answer" should {
      "not block, and fail that request" taggedAs (V100) in {
        val silentServer = new ServerSocket(0)
        val connections = new LinkedBlockingQueue[java.net.Socket]()
        val acceptor = new Thread(new Runnable {
          def run(): Unit = try {
            while (true) connections.add(silentServer.accept())
          } catch { case _: java.io.IOException => }
        })
        acceptor.setDaemon(true)
        acceptor.start()
        try {
          val subscriber = SubscriberClient(_ => (), port = silentServer.getLocalPort)
          val subscribe = subscriber.subscribe("fwd-channel")
          connections.poll(5, TimeUnit.SECONDS) should not be null
          subscribe.isCompleted should be (false)

          // quit() takes the lock that the subscribe holds: it must neither wait for it nor leave it open
          Future(subscriber.quit()).futureValue(within(5))
          subscribe.failed.futureValue(within(5)) shouldBe a [RedisIOException]
        } finally {
          silentServer.close()
          connections.forEach(_.close())
        }
      }
    }
  }
}
