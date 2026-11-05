// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
package scredis

import java.util.concurrent.TimeoutException

import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec
import scredis.ConnectionState.Closed
import scredis.exceptions._
import scredis.util.{RawRedis, StallingProxy, TcpProxy}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

/**
 * `Redis.blockingClient` and `Redis.withBlocking` (one held instance, the error classes of 1.1.0), and the deprecated
 * `Redis.blocking`, which fails as it did in 1.0.0. Faults through a proxy ([[scredis.util.TcpProxy]],
 * [[scredis.util.StallingProxy]]) in front of 6379.
 */
class BlockingAccessorSpec extends AnyWordSpec with Matchers with ScalaFutures with Eventually {

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(20, Seconds), interval = Span(20, Millis))

  private val L = "blocking-accessor-spec:list"

  private def timed[A](f: => A): (A, Long) = {
    val start = System.nanoTime()
    val a = f
    (a, (System.nanoTime() - start) / 1000000L)
  }

  private def named(name: String): Int =
    RawRedis.withConnection("127.0.0.1", 6379)(_.string("CLIENT", "LIST")).split("\n").count(_.contains(s" name=$name "))

  /** The class of a failure, and whether it was thrown or returned */
  private def failureOf(f: => Try[_]): Throwable = Try(f).flatten match {
    case Failure(e) => e
    case Success(v) => fail(s"expected a failure, got $v")
  }

  "Redis.blockingClient" should {
    "be one instance, created once, used by withBlocking, and closed by quit()" in {
      val name = "blocking-accessor-spec-one"
      val redis = Redis(port = 6379, nameOpt = Some(name))
      redis.ping().futureValue shouldBe "PONG"
      val client = redis.blockingClient
      redis.blockingClient shouldBe theSameInstanceAs (client)
      redis.withBlocking(b => b) shouldBe theSameInstanceAs (client)
      (1 to 5).foreach(_ => redis.withBlocking(_.blPop[String](1, L)) shouldBe Success(None))
      client.blPop[String](1, L) shouldBe Success(None)
      // the non-blocking connection and one blocking connection
      eventually { named(name) shouldBe 2 }
      redis.quit().futureValue
      eventually { client.health.state shouldBe Closed }
      val (failure, ms) = timed(failureOf(redis.withBlocking(_.blPop[String](1, L))))
      failure shouldBe a [RedisNotSentException]
      ms should be < 500L
      eventually { named(name) shouldBe 0 }
    }

    "fail with the error classes: a passed deadline as outcome unknown (cause TimeoutException), a lost connection as outcome unknown, a concurrent call as not sent" in {
      val stalling = new StallingProxy("127.0.0.1", 6379)
      val proxy = new TcpProxy("127.0.0.1", 6379)
      val r1 = Redis(port = stalling.port)
      val r2 = Redis(port = proxy.port)
      try {
        r1.blockingClient.blPop[String](1, L) shouldBe Success(None)
        stalling.stall(true)
        val (deadline, ms) = timed(failureOf(r1.blockingClient.blPop[String](1, L)))
        deadline shouldBe a [RedisOutcomeUnknownException]
        deadline.getCause shouldBe a [TimeoutException]
        ms should be >= 2900L
        stalling.stall(false)

        r2.blockingClient.blPop[String](1, L) shouldBe Success(None)
        val blocked = Future(r2.blockingClient.blPop[String](5, L))
        Thread.sleep(300)
        val concurrent = failureOf(r2.blockingClient.blPop[String](1, L))
        concurrent shouldBe a [RedisNotSentException]
        proxy.cutAll()
        val lost = blocked.futureValue.failed.get
        lost shouldBe a [RedisOutcomeUnknownException]
        lost.asInstanceOf[RedisOutcomeUnknownException].availability.whenAvailable.futureValue shouldBe (())
        r2.blockingClient.blPop[String](1, L) shouldBe Success(None)
      } finally {
        Try(r1.quit().futureValue)
        Try(r2.quit().futureValue)
        stalling.stop()
        proxy.stop()
      }
    }
  }

  "The deprecated Redis.blocking" should {
    "fail as in 1.0.0 through a server restart: RedisIOException while it is down, working at once when it is back" in {
      val proxy = new TcpProxy("127.0.0.1", 6379)
      val redis = Redis(port = proxy.port)
      try {
        redis.blocking.blPop[String](1, L) shouldBe Success(None)
        proxy.refuse()
        proxy.cutAll()
        Thread.sleep(200)
        (1 to 4).foreach { _ =>
          val (failure, ms) = timed(failureOf(redis.blocking.blPop[String](1, L)))
          failure.getClass shouldBe classOf[RedisIOException]
          failure.getMessage should startWith ("Could not connect to")
          ms should be < 1000L
          Thread.sleep(300)
        }
        proxy.listen()
        redis.blocking.blPop[String](1, L) shouldBe Success(None)
        // a longer outage (several attempts with the backoff), then a call 50 ms after the server is back
        proxy.refuse()
        proxy.cutAll()
        Thread.sleep(3000)
        proxy.listen()
        Thread.sleep(50)
        redis.blocking.blPop[String](1, L) shouldBe Success(None)
      } finally {
        Try(redis.quit().futureValue)
        proxy.stop()
      }
    }

    "fail as in 1.0.0: a lost connection and a concurrent call with RedisIOException, a passed deadline and a call after quit() with the bare TimeoutException" in {
      val proxy = new TcpProxy("127.0.0.1", 6379)
      val stalling = new StallingProxy("127.0.0.1", 6379)
      val r1 = Redis(port = proxy.port)
      val r2 = Redis(port = stalling.port)
      try {
        r1.blocking.blPop[String](1, L) shouldBe Success(None)
        val blocked = Future(r1.blocking.blPop[String](5, L))
        Thread.sleep(300)
        val concurrent = intercept[RedisIOException](r1.blocking.blPop[String](1, L))
        concurrent.getClass shouldBe classOf[RedisIOException]
        concurrent.getMessage shouldBe "Trying to send request on a blocked connection"
        proxy.cutAll()
        val lost = blocked.futureValue.failed.get
        lost.getClass shouldBe classOf[RedisIOException]
        lost.getMessage shouldBe "Connection has been shutdown abruptly"
        r1.blocking.blPop[String](1, L) shouldBe Success(None)

        r2.blocking.blPop[String](1, L) shouldBe Success(None)
        stalling.stall(true)
        val (deadline, ms) = timed(failureOf(r2.blocking.blPop[String](1, L)))
        deadline.getClass shouldBe classOf[TimeoutException]
        ms should be >= 2900L
        stalling.stall(false)
      } finally {
        Try(r1.quit().futureValue)
        Try(r2.quit().futureValue)
        proxy.stop()
        stalling.stop()
      }
      val r3 = Redis(port = 6379)
      r3.blocking.blPop[String](1, L) shouldBe Success(None)
      r3.quit().futureValue
      val (afterQuit, ms) = timed(failureOf(r3.blocking.blPop[String](1, L)))
      afterQuit.getClass shouldBe classOf[TimeoutException]
      ms should be >= 2900L
    }

    "be a client of its own, beside blockingClient, and close with quit()" in {
      val name = "blocking-accessor-spec-both"
      val redis = Redis(port = 6379, nameOpt = Some(name))
      redis.blocking should not be theSameInstanceAs (redis.blockingClient)
      redis.blocking.blPop[String](1, L) shouldBe Success(None)
      redis.blockingClient.blPop[String](1, L) shouldBe Success(None)
      eventually { named(name) shouldBe 3 }
      redis.quit().futureValue
      eventually { redis.blocking.health.state shouldBe Closed }
      eventually { redis.blockingClient.health.state shouldBe Closed }
      eventually { named(name) shouldBe 0 }
    }
  }
}
