// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
package scredis

import java.util.concurrent.TimeoutException

import akka.actor.ActorSystem
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec
import scredis.ConnectionState.Reconnecting
import scredis.exceptions._
import scredis.protocol.{Decoder, IntegerResponse}
import scredis.util.{RawRedis, Spin, StallingProxy, TcpProxy}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

/**
 * The classes of the errors of a request that fails because of its connection, on the standalone clients:
 * [[scredis.exceptions.RedisNotSentException]] (certainly not executed) and
 * [[scredis.exceptions.RedisOutcomeUnknownException]] (written, its reply lost), both with the availability of the
 * connection; Redis' error replies keep their types. A counter in a script shows how often a write was executed.
 */
class ErrorClassesSpec extends AnyWordSpec with Matchers with ScalaFutures with Eventually with BeforeAndAfterAll {

  private implicit val system: ActorSystem = ActorSystem("error-classes-spec")
  private implicit val ec: ExecutionContext = ExecutionContext.global
  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(20, Seconds), interval = Span(20, Millis))

  private implicit val long: Decoder[Long] = { case IntegerResponse(value) => value }

  private val Incr = "return redis.call('INCR', KEYS[1])"

  private val admin = Client(port = 6379)

  override def afterAll(): Unit = {
    admin.del("error-classes-spec:counter-1", "error-classes-spec:counter-2", "error-classes-spec:counter-3",
      "error-classes-spec:string").futureValue
    admin.quit().futureValue
    system.terminate()
  }

  private def counter(key: String): Long =
    RawRedis.withConnection("127.0.0.1", 6379)(_.command("GET", key)) match {
      case null => 0L
      case s: String => s.toLong
      case other => fail(s"unexpected $other")
    }

  private def timed[A](f: => A): (A, Long) = {
    val start = System.nanoTime()
    val a = f
    (a, (System.nanoTime() - start) / 1000000L)
  }

  "A request" when {
    "there is no connection (the client waits for its next attempt)" should {
      "fail at once as not sent, with the outage's availability; waiting on it, then retrying, works" in {
        val proxy = new TcpProxy("127.0.0.1", 6379)
        val client = Client(port = proxy.port)
        try {
          client.ping().futureValue
          proxy.refuse()
          proxy.cutAll()
          eventually { client.health.state match {
            case Reconnecting(attempt, _, _) => attempt should be >= 2
            case other => fail(other.toString)
          } }
          val (failure, ms) = timed(client.get[String]("error-classes-spec:missing").failed.futureValue)
          info(s"failed after $ms ms: $failure")
          failure shouldBe a [RedisNotSentException]
          failure shouldBe a [RedisIOException]
          ms should be < 200L
          val availability = failure.asInstanceOf[RedisNotSentException].availability
          availability shouldBe theSameInstanceAs (client.availability)
          availability.whenAvailable.isCompleted shouldBe false

          // the retry of the README: wait for the connection, then send again (safe: it was not sent)
          def getWithRetry(): Future[Option[String]] = client.get[String]("error-classes-spec:missing").recoverWith {
            case e: RedisNotSentException => e.availability.whenAvailable.flatMap(_ => client.get[String]("error-classes-spec:missing"))
          }
          val retried = getWithRetry()
          Thread.sleep(300)
          proxy.listen()
          retried.futureValue shouldBe None
          client.quit().futureValue
        } finally proxy.stop()
      }
    }
    "fail-command-on-connecting is set and the client is connecting" should {
      "fail as not sent" in {
        val proxy = new TcpProxy("127.0.0.1", 6379)
        proxy.refuse()
        val client = Client(port = proxy.port, failCommandOnConnecting = true)
        try {
          val failure = client.ping().failed.futureValue
          failure shouldBe a [RedisNotSentException]
          failure.getMessage should include ("Trying to connect")
        } finally {
          client.quit()
          proxy.stop()
        }
      }
    }
    "the client has been closed (QUIT)" should {
      "fail as not sent, with an availability that has failed" in {
        val client = Client(port = 6379)
        client.ping().futureValue
        client.quit().futureValue
        val failure = client.ping().failed.futureValue
        failure shouldBe a [RedisNotSentException]
        failure.asInstanceOf[RedisNotSentException].availability.whenAvailable.failed.futureValue shouldBe a [RedisIOException]
      }
    }
    "it was written and the connection was lost before its reply" should {
      "fail as outcome unknown and not be sent again: the write is executed once" in {
        val proxy = new TcpProxy("127.0.0.1", 6379)
        val client = Client(port = proxy.port)
        val key = "error-classes-spec:counter-1"
        try {
          admin.del(key).futureValue
          client.ping().futureValue
          proxy.cutInNextReply(0)
          val failure = client.eval[Long, String, String](Incr, Seq(key)).failed.futureValue
          info(s"$failure")
          failure shouldBe a [RedisOutcomeUnknownException]
          failure shouldBe a [RedisIOException]
          val availability = failure.asInstanceOf[RedisOutcomeUnknownException].availability
          availability.whenAvailable.futureValue shouldBe (())
          Thread.sleep(500)
          counter(key) shouldBe 1L
          client.eval[Long, String, String](Incr, Seq(key)).futureValue shouldBe 2L
          client.quit().futureValue
        } finally proxy.stop()
      }
    }
    "its reply does not arrive within the receive timeout" should {
      "fail as outcome unknown and not be sent again: the write is executed once" in {
        // the request reaches Redis, its reply is dropped
        val proxy = new StallingProxy("127.0.0.1", 6379)
        val client = Client(port = proxy.port, receiveTimeoutOpt = Some(500.millis))
        val key = "error-classes-spec:counter-2"
        try {
          admin.del(key).futureValue
          client.ping().futureValue
          proxy.stallReplies(true)
          val failure = client.eval[Long, String, String](Incr, Seq(key)).failed.futureValue
          info(s"$failure")
          failure shouldBe a [RedisOutcomeUnknownException]
          failure.getMessage should include ("Receive timeout")
          proxy.stallReplies(false)
          Thread.sleep(500)
          counter(key) shouldBe 1L
          client.availability.whenAvailable.futureValue shouldBe (())
          client.ping().futureValue shouldBe "PONG"
          counter(key) shouldBe 1L
          client.quit().futureValue
        } finally proxy.stop()
      }
    }
    "Redis answers an error" should {
      "fail with Redis' error, as before" in {
        admin.set("error-classes-spec:string", "x").futureValue
        val failure = admin.lPush("error-classes-spec:string", "y").failed.futureValue
        failure shouldBe a [RedisErrorResponseException]
        failure.getMessage should include ("WRONGTYPE")
      }
    }
  }

  "A blocking call" when {
    "its deadline (the block time + 2 s) passes without a reply" should {
      "fail as outcome unknown" in {
        val proxy = new StallingProxy("127.0.0.1", 6379)
        val blocking = BlockingClient(port = proxy.port)
        try {
          blocking.blPop[String](1, "error-classes-spec:list").get shouldBe None
          proxy.stall(true)
          val (result, ms) = timed(blocking.blPop[String](1, "error-classes-spec:list"))
          info(s"$result after $ms ms")
          val failure = result.failed.get
          failure shouldBe a [RedisOutcomeUnknownException]
          failure.getCause shouldBe a [TimeoutException]
          ms should be >= 2900L
        } finally proxy.stop()
      }
    }
    "its connection is lost while it blocks" should {
      "fail as outcome unknown; a call made meanwhile fails as not sent" in {
        val proxy = new TcpProxy("127.0.0.1", 6379)
        val blocking = BlockingClient(port = proxy.port)
        try {
          blocking.blPop[String](1, "error-classes-spec:list").get shouldBe None
          val blocked = Future(blocking.blPop[String](10, "error-classes-spec:list"))
          Thread.sleep(300)
          val second = scala.util.Try(blocking.blPop[String](1, "error-classes-spec:list"))
          second.failed.get shouldBe a [RedisNotSentException]
          proxy.cutAll()
          val failure = blocked.futureValue.failed.get
          info(s"$failure")
          failure shouldBe a [RedisOutcomeUnknownException]
          blocking.availability.whenAvailable.futureValue shouldBe (())
          blocking.blPop[String](1, "error-classes-spec:list").get shouldBe None
          blocking.quit()(5.seconds)
        } finally proxy.stop()
      }
    }
  }

  "A subscriber's request" when {
    "there is no connection" should {
      "fail as not sent" in {
        val proxy = new TcpProxy("127.0.0.1", 6379)
        val subscriber = SubscriberClient((_: PubSubMessage) => (), port = proxy.port)
        try {
          subscriber.subscribe("error-classes-spec:a").futureValue shouldBe 1
          proxy.refuse()
          proxy.cutAll()
          eventually { subscriber.health.state match {
            case Reconnecting(attempt, _, _) => attempt should be >= 2
            case other => fail(other.toString)
          } }
          subscriber.subscribe("error-classes-spec:b").failed.futureValue shouldBe a [RedisNotSentException]
          proxy.listen()
          subscriber.availability.whenAvailable.futureValue shouldBe (())
          eventually { subscriber.subscribe("error-classes-spec:c").futureValue should be >= 2 }
          subscriber.quit().futureValue
        } finally proxy.stop()
      }
    }
    "it was written and the connection was lost before its reply" should {
      "fail as outcome unknown" in {
        val proxy = new StallingProxy("127.0.0.1", 6379)
        val subscriber = SubscriberClient((_: PubSubMessage) => (), port = proxy.port)
        try {
          subscriber.subscribe("error-classes-spec:a").futureValue shouldBe 1
          proxy.stall(true)
          val outstanding = subscriber.subscribe("error-classes-spec:b")
          Thread.sleep(300)
          proxy.stall(false)
          proxy.cutAll()
          outstanding.failed.futureValue shouldBe a [RedisOutcomeUnknownException]
          subscriber.availability.whenAvailable.futureValue shouldBe (())
          subscriber.quit().futureValue
        } finally proxy.stop()
      }
    }
  }
}
