// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
package scredis

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec
import scredis.exceptions.{RedisClusterErrorResponseException, RedisIOException, RedisOutcomeUnknownException}
import scredis.protocol.{ClusterCRC16, CrossSlot}
import scredis.util.{FailoverCluster, RawRedis}
import scredis.util.TestUtils._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
 * The blocking commands on RedisCluster (`RedisCluster.blockingClient`): BLPOP, BRPOP, BRPOPLPUSH and XREAD BLOCK routed
 * by key on a blocking connection of their own (the cluster 7000 - 7005), and during a failover (the failover
 * cluster 7100 - 7105), see testing.md.
 */
class ClusterBlockingSpec extends AnyWordSpec with Matchers with ScalaFutures with BeforeAndAfterAll {

  private implicit val ec: ExecutionContext = ExecutionContext.global
  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(30, Seconds), interval = Span(20, Millis))

  private val cluster = RedisCluster(Server("127.0.0.1", 7000))
  private val L = "cluster-blocking-spec:list"
  private val L2 = "cluster-blocking-spec:list-2"
  private val Source = "{cluster-blocking-spec}:source"
  private val Destination = "{cluster-blocking-spec}:destination"
  private val Stream = "cluster-blocking-spec:stream"
  private val keys = Seq(L, L2, Source, Destination, Stream)

  private def later[A](ms: Long)(f: => Future[A]): Future[A] = Future { Thread.sleep(ms) }.flatMap(_ => f)

  private def timed[A](f: => A): (A, Long) = {
    val start = System.nanoTime()
    val a = f
    (a, (System.nanoTime() - start) / 1000000L)
  }

  override def afterAll(): Unit = {
    keys.foreach(k => cluster.del(k).!)
    cluster.quit().futureValue
  }

  "RedisCluster.blockingClient" when {
    "a list gets an element while BLPOP / BRPOP block" should {
      "return it" in {
        L2.length should be > 0
        ClusterCRC16.getSlot(L) should not be ClusterCRC16.getSlot(L2)
        later(300)(cluster.lPush(L, "left"))
        val (popped, ms) = timed(cluster.blockingClient.blPop[String](5, L))
        popped shouldBe Success(Some((L, "left")))
        ms should be >= 250L
        later(300)(cluster.rPush(L2, "a", "b"))
        cluster.blockingClient.brPop[String](5, L2) shouldBe Success(Some((L2, "b")))
      }
    }
    "nothing arrives" should {
      "time out as Redis does" in {
        val (popped, ms) = timed(cluster.blockingClient.blPop[String](1, L))
        popped shouldBe Success(None)
        ms should be >= 900L
      }
    }
    "BRPOPLPUSH and XREAD BLOCK wait" should {
      "return what arrives" in {
        later(300)(cluster.lPush(Source, "moved"))
        cluster.blockingClient.brPopLPush[String](Source, Destination, 5) shouldBe Success(Some("moved"))
        cluster.lRange[String](Destination).futureValue shouldBe List("moved")
        later(300)(cluster.xAdd(Stream, Seq("f" -> "v")))
        val read = cluster.blockingClient.xRead[String](5000, Seq(Stream -> "$")).get
        read.map(_.map { case (k, entries) => k -> entries.map(_.fields) }) shouldBe Some(List(Stream -> List(List("f" -> "v"))))
      }
    }
    "the keys are in different slots" should {
      "return Redis' CROSSSLOT" in {
        cluster.blockingClient.blPop[String](1, L, L2) match {
          case Failure(RedisClusterErrorResponseException(CrossSlot, _)) =>
          case other => fail(s"expected CROSSSLOT, got $other")
        }
      }
    }
    "a call blocks" should {
      "not delay the cluster's other commands, also on the same node" in {
        val blocked = Future(cluster.blockingClient.blPop[String](3, L))
        Thread.sleep(200)
        val (_, ms) = timed {
          (1 to 200).foreach { i =>
            // L's slot is on the node the blocking call waits on; the others on any node
            cluster.set(s"{$L}:other-$i", s"v$i").futureValue shouldBe true
            cluster.get[String](s"cluster-blocking-spec:other-$i").futureValue shouldBe None
          }
        }
        info(s"400 other commands took $ms ms while BLPOP blocked")
        ms should be < 2000L
        blocked.isCompleted shouldBe false
        cluster.lPush(L, "release").futureValue
        Await.result(blocked, 10.seconds) shouldBe Success(Some((L, "release")))
        (1 to 200).foreach(i => cluster.del(s"{$L}:other-$i").!)
      }
      "refuse a second blocking call meanwhile, as Redis.blocking does" in {
        val blocked = Future(cluster.blockingClient.blPop[String](2, L))
        Thread.sleep(200)
        val second = Try(cluster.blockingClient.blPop[String](1, L2))
        second.failed.get shouldBe a [RedisIOException]
        Await.result(blocked, 10.seconds) shouldBe Success(None)
      }
    }
  }

  "RedisCluster.blockingClient during a failover" should {
    "fail the blocked call as outcome unknown, with an availability that completes after the promotion; a call through withRetry returns the value pushed after the failover; and work afterwards" in {
      FailoverCluster.awaitWhole()
      val c = RedisCluster(Server("127.0.0.1", 7100))
      val key = "cluster-blocking-spec:failover"
      val slot = ClusterCRC16.getSlot(key)
      try {
        c.del(key).futureValue
        val victim = FailoverCluster.masterOf(slot)
        // the slot cache and the blocking connection point at the master that is killed
        c.blockingClient.blPop[String](1, key) shouldBe Success(None)
        val blocked = Future(timed(c.blockingClient.blPop[String](30, key)))
        Thread.sleep(500)
        FailoverCluster.kill(victim.port)
        val (result, ms) = Await.result(blocked, 40.seconds)
        info(s"killed master ${victim.port}; the blocked call returned $result after $ms ms")
        val failure = result.failed.get
        failure shouldBe a [RedisOutcomeUnknownException]
        val availability = failure.asInstanceOf[RedisOutcomeUnknownException].availability
        availability.isAvailable shouldBe false
        val availableAt = availability.whenAvailable.map(_ => System.nanoTime())
        // made while the slot's master is down: not sent, and sent again once the slot has a master
        val attempts = new AtomicInteger(0)
        val retried = Future(timed(c.blockingClient.withRetry { b => attempts.incrementAndGet(); b.blPop[String](30, key) }))
        // the promotion happened after the start of the last look at the survivors that did not show it
        var notPromotedAt = System.nanoTime()
        var promoted = false
        while (!promoted) {
          val lookedAt = System.nanoTime()
          val views = FailoverCluster.Ports.filter(_ != victim.port).toList.flatMap(p => Try(RawRedis.clusterNodes(p)).toOption)
          promoted = views.exists(_.exists(n => n.isMaster && !n.isFailed && n.port != victim.port && n.owns(slot)))
          if (!promoted) { notPromotedAt = lookedAt; Thread.sleep(20) }
        }
        val promotedNode = FailoverCluster.awaitFailover(victim.port, slot)
        val at = Await.result(availableAt, 20.seconds)
        info(s"${promotedNode.port} promoted; the availability completed ${(at - notPromotedAt) / 1000000L} ms after the last look that did not show the promotion")
        at should be >= notPromotedAt
        c.lPush(key, "after-failover").futureValue
        val (retriedResult, retriedMs) = Await.result(retried, 40.seconds)
        info(s"the call through withRetry returned $retriedResult after $retriedMs ms, in ${attempts.get} attempts")
        retriedResult shouldBe Success(Some((key, "after-failover")))
        attempts.get shouldBe 2
        // the killed node is back as a replica: with the slot cache pointing at it again (as a cache that is out of
        // date does), it answers MOVED, and the call goes to the node MOVED names
        FailoverCluster.awaitWhole()
        c.slotMoved(slot, Server("127.0.0.1", victim.port))
        later(300)(c.lPush(key, "moved"))
        c.blockingClient.blPop[String](5, key) shouldBe Success(Some((key, "moved")))
        c.masterOf(slot).map(_.port) shouldBe Some(promotedNode.port)
        c.blockingClient.blPop[String](1, key) shouldBe Success(None)
      } finally {
        Try(Await.result(c.del(key), 10.seconds))
        Try(Await.result(c.quit(), 30.seconds))
        FailoverCluster.awaitWhole()
      }
    }
  }
}
