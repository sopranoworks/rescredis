// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
package scredis

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec
import scredis.exceptions._
import scredis.protocol.{ClusterCRC16, Decoder, IntegerResponse}
import scredis.util.{FailoverCluster, RawRedis, TcpProxy}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Success, Try}

/**
 * The retry helpers (`withRetry`, [[scredis.RetryPolicy]]): an attempt that fails as not sent is made again once
 * the error's availability has completed (one retry per outage, no polling); the helper gives up when
 * `maxWaitingDuration` is used up, with the last error; an outcome unknown is retried only when the caller says the
 * attempt is idempotent; a Redis error reply never. Standalone through a proxy in front of 6379, and on the failover
 * cluster 7100 - 7105. Attempts are counted by the attempt function; executions by a counter in a script.
 */
class RetryHelpersSpec extends AnyWordSpec with Matchers with ScalaFutures with Eventually with BeforeAndAfterAll {

  private implicit val system: ActorSystem = ActorSystem("retry-helpers-spec")
  private implicit val ec: ExecutionContext = ExecutionContext.global

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(30, Seconds), interval = Span(20, Millis))

  private implicit val long: Decoder[Long] = { case IntegerResponse(value) => value }

  private val Incr = "return redis.call('INCR', KEYS[1])"
  private val admin = Client(port = 6379)

  override def afterAll(): Unit = {
    admin.del("retry-helpers-spec:counter-1", "retry-helpers-spec:counter-2", "retry-helpers-spec:string",
      "retry-helpers-spec:key").futureValue
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

  /** The proxy refuses connections and the client knows it: it waits for its next attempt */
  private def down(proxy: TcpProxy, health: ConnectionHealth): Unit = {
    proxy.refuse()
    proxy.cutAll()
    eventually { health.isKnownDown shouldBe true }
  }

  "withRetry on a Redis" should {
    "wait on the availability while the server is down and retry once it is back: one retry per outage" in {
      val proxy = new TcpProxy("127.0.0.1", 6379)
      val redis = Redis(port = proxy.port)
      try {
        redis.ping().futureValue shouldBe "PONG"
        down(proxy, redis.health)
        val attempts = new AtomicInteger(0)
        val started = System.nanoTime()
        val result = redis.withRetry { r => attempts.incrementAndGet(); r.get[String]("retry-helpers-spec:key") }
        Thread.sleep(1500)
        result.isCompleted shouldBe false
        attempts.get shouldBe 1
        proxy.listen()
        result.futureValue shouldBe None
        val ms = (System.nanoTime() - started) / 1000000L
        info(s"answered after $ms ms in ${attempts.get} attempts")
        attempts.get shouldBe 2
        ms should be >= 1500L
      } finally {
        Try(redis.quit().futureValue)
        proxy.stop()
      }
    }

    "give up when maxWaitingDuration is used up, with the last error; and when the client is closed meanwhile" in {
      val proxy = new TcpProxy("127.0.0.1", 6379)
      val client = Client(port = proxy.port)
      try {
        client.ping().futureValue shouldBe "PONG"
        down(proxy, client.health)
        val attempts = new AtomicInteger(0)
        val (failure, ms) = timed(client.withRetry({ c => attempts.incrementAndGet(); c.ping() }, maxWaitingDuration = 800.millis).failed.futureValue)
        info(s"gave up after $ms ms, ${attempts.get} attempt(s): $failure")
        failure shouldBe a [RedisNotSentException]
        attempts.get shouldBe 1
        ms should be >= 750L
        ms should be < 2000L
        // a policy with a number of retries: none here
        val (noRetry, ms2) = timed(RetryPolicy(maxRetries = Some(0)).retry(() => client.ping()).failed.futureValue)
        noRetry shouldBe a [RedisNotSentException]
        ms2 should be < 500L
        // closed while waiting: the availability fails, and so does the helper, with the last error
        val waiting = client.withRetry { c => attempts.incrementAndGet(); c.ping() }
        Thread.sleep(300)
        client.quit()
        waiting.failed.futureValue shouldBe a [RedisNotSentException]
      } finally {
        Try(client.quit().futureValue)
        proxy.stop()
      }
    }

    "not retry an outcome unknown by default, and retry it when the caller says the attempt is idempotent" in {
      val proxy = new TcpProxy("127.0.0.1", 6379)
      val client = Client(port = proxy.port)
      val key1 = "retry-helpers-spec:counter-1"
      val key2 = "retry-helpers-spec:counter-2"
      try {
        admin.del(key1, key2).futureValue
        client.ping().futureValue shouldBe "PONG"
        val attempts = new AtomicInteger(0)
        proxy.cutInNextReply(0)
        val failure = client.withRetry { c => attempts.incrementAndGet(); c.eval[Long, String, String](Incr, Seq(key1)) }.failed.futureValue
        failure shouldBe a [RedisOutcomeUnknownException]
        attempts.get shouldBe 1
        Thread.sleep(300)
        counter(key1) shouldBe 1L

        attempts.set(0)
        client.availability.whenAvailable.futureValue
        proxy.cutInNextReply(0)
        client.withRetry({ c => attempts.incrementAndGet(); c.eval[Long, String, String](Incr, Seq(key2)) }, idempotent = true).futureValue shouldBe 2L
        attempts.get shouldBe 2
        counter(key2) shouldBe 2L
      } finally {
        Try(client.quit().futureValue)
        proxy.stop()
      }
    }

    "never retry a Redis error reply" in {
      admin.set("retry-helpers-spec:string", "x").futureValue
      val attempts = new AtomicInteger(0)
      val failure = admin.withRetry({ c => attempts.incrementAndGet(); c.lPush("retry-helpers-spec:string", "y") }, idempotent = true).failed.futureValue
      failure shouldBe a [RedisErrorResponseException]
      attempts.get shouldBe 1
    }
  }

  "withRetry on a blocking client" should {
    "wait on the availability while the server is down and retry once it is back" in {
      val proxy = new TcpProxy("127.0.0.1", 6379)
      val redis = Redis(port = proxy.port)
      try {
        redis.blockingClient.blPop[String](1, "retry-helpers-spec:list") shouldBe Success(None)
        down(proxy, redis.blockingClient.health)
        val attempts = new AtomicInteger(0)
        val result = Future(redis.withBlocking(_.withRetry { b => attempts.incrementAndGet(); b.blPop[String](1, "retry-helpers-spec:list") }))
        Thread.sleep(1500)
        result.isCompleted shouldBe false
        proxy.listen()
        result.futureValue shouldBe Success(None)
        attempts.get shouldBe 2
      } finally {
        Try(redis.quit().futureValue)
        proxy.stop()
      }
    }

    "wait for a call that blocks to end when it refuses a second one" in {
      val redis = Redis(port = 6379)
      try {
        val blocked = Future(redis.blockingClient.blPop[String](2, "retry-helpers-spec:list"))
        Thread.sleep(300)
        val attempts = new AtomicInteger(0)
        val (second, ms) = timed(redis.blockingClient.withRetry { b => attempts.incrementAndGet(); b.blPop[String](1, "retry-helpers-spec:list-2") })
        info(s"the second call returned $second after $ms ms in ${attempts.get} attempts")
        second shouldBe Success(None)
        attempts.get shouldBe 2
        ms should be >= 2400L
        blocked.futureValue shouldBe Success(None)
      } finally redis.quit().futureValue
    }
  }

  "withRetry on a RedisCluster during a failover" should {
    "fail the slot's requests as not sent until the promotion, wait on the slot's availability, and succeed once" in {
      FailoverCluster.awaitWhole()
      val c = RedisCluster(Server("127.0.0.1", 7100))
      val key = "retry-helpers-spec:failover"
      val slot = ClusterCRC16.getSlot(key)
      try {
        c.del(key).futureValue
        val victim = FailoverCluster.masterOf(slot)
        c.set(key, "before").futureValue shouldBe true
        FailoverCluster.kill(victim.port)
        eventually { c.health.isReachable(slot) shouldBe false }
        val attempts = new AtomicInteger(0)
        val (result, ms) = timed(Await.result(c.withRetry { cl => attempts.incrementAndGet(); cl.set(key, "after") }, 40.seconds))
        val promoted = FailoverCluster.awaitFailover(victim.port, slot)
        info(s"killed master ${victim.port}, ${promoted.port} promoted; withRetry returned $result after $ms ms in ${attempts.get} attempts")
        result shouldBe true
        attempts.get shouldBe 2
        c.get[String](key).futureValue shouldBe Some("after")
      } finally {
        Try(Await.result(c.del(key), 10.seconds))
        Try(Await.result(c.quit(), 30.seconds))
        FailoverCluster.awaitWhole()
      }
    }
  }
}
