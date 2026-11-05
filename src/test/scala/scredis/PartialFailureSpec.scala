// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
package scredis

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec
import scredis.exceptions.{RedisNotSentException, RedisOutcomeUnknownException}
import scredis.protocol.{ClusterCRC16, Decoder, IntegerResponse}
import scredis.util.{FailoverCluster, RawRedis, Spin}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Promise}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

import PartialFailureSpec._

object PartialFailureSpec {
  final case class Outcome(side: String, key: String, sent: Long, done: Long, result: Try[Long]) {
    def ms: Long = (done - sent) / 1000000L
    def kind: String = result match {
      case Success(_) => "ok"
      case Failure(_: RedisNotSentException) => "not-sent"
      case Failure(_: RedisOutcomeUnknownException) => "outcome-unknown"
      case Failure(e) => s"other: $e"
    }
  }
}

/**
 * A partial cluster failure, on the failover cluster 7100 - 7105 (3 masters with a replica each,
 * cluster-node-timeout 2000, see testing.md): one master is killed under load, with
 * cluster-require-full-coverage yes and no. Every write is a script that increments a counter of its own, so the
 * number of times it was executed can be read afterwards. The phases are taken from the surviving nodes' CLUSTER
 * NODES (the first of them to show a change): before the failure is detected (the killed master not flagged
 * `fail`), after it is flagged `fail` and before a replica is promoted, and after the promotion.
 */
class PartialFailureSpec extends AnyWordSpec with Matchers with ScalaFutures with Eventually {

  private implicit val ec: ExecutionContext = ExecutionContext.global
  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(30, Seconds), interval = Span(20, Millis))

  private implicit val long: Decoder[Long] = { case IntegerResponse(value) => value }

  private val Incr = "return redis.call('INCR', KEYS[1])"

  private val connectTimeout = RedisConfigDefaults.IO.ConnectTimeout

  private def now(): Long = System.nanoTime()
  private def msBetween(a: Long, b: Long): Long = (b - a) / 1000000L

  private def setCoverage(value: String): Unit = FailoverCluster.Ports.foreach { port =>
    Try(RawRedis.withConnection("127.0.0.1", port)(_.command("CONFIG", "SET", "cluster-require-full-coverage", value)))
  }

  /** A key of the slot of a master: `tag` is a hash tag in that slot */
  private def tagOf(master: RawRedis.Node): String =
    Iterator.from(0).map(i => s"partial-failure-$i").find(k => master.owns(ClusterCRC16.getSlot(k))).get

  private def counters(port: Int, keys: Seq[String]): Map[String, Long] = RawRedis.withConnection("127.0.0.1", port) { r =>
    keys.map { k =>
      k -> (r.command("GET", k) match {
        case null => 0L
        case s: String => s.toLong
        case other => fail(s"GET $k: $other")
      })
    }.toMap
  }

  /** CLUSTER NODES of every surviving node that answers: a change is seen as soon as one of them has it */
  private def survivorViews(killed: Int): List[List[RawRedis.Node]] =
    FailoverCluster.Ports.filter(_ != killed).toList.flatMap(p => Try(RawRedis.clusterNodes(p)).toOption)

  private def phases(coverage: String): Unit = {
    FailoverCluster.awaitWhole()
    setCoverage(coverage)
    val cluster = RedisCluster(Server("127.0.0.1", 7100))
    // a client that sends nothing during the failure: whatever it learns comes from its own refreshes, not from MOVED
    val idle = RedisCluster(Server("127.0.0.1", 7100))
    try {
      cluster.availability.whenAvailable.futureValue shouldBe (())
      idle.availability.whenAvailable.futureValue shouldBe (())
      val masters = FailoverCluster.masters()
      val victim = masters.head
      val other = masters(1)
      val deadTag = tagOf(victim)
      val otherTag = tagOf(other)
      val deadSlot = ClusterCRC16.getSlot(deadTag)
      val runId = s"${coverage}-${System.currentTimeMillis()}"

      val victimStates = new ConcurrentLinkedQueue[(Long, ConnectionState)]()
      cluster.health.subscribe { case (server, state) =>
        if (server.port == victim.port) victimStates.add((now(), state))
        ()
      }

      // load: a write to the dead master's slot and one to another master's slot every 10 ms
      val outcomes = new ConcurrentLinkedQueue[Outcome]()
      val sentCount = new AtomicInteger(0)
      @volatile var running = true
      val load = new Thread(new Runnable {
        def run(): Unit = {
          var i = 0
          while (running) {
            i += 1
            for ((side, tag) <- List("dead" -> deadTag, "other" -> otherTag)) {
              val key = s"{$tag}:$runId:$i"
              val sent = now()
              sentCount.incrementAndGet()
              cluster.eval[Long, String, String](Incr, Seq(key)).onComplete(r => outcomes.add(Outcome(side, key, sent, now(), r)))
            }
            Thread.sleep(10)
          }
        }
      }, "partial-failure-load")
      load.setDaemon(true)
      load.start()
      Thread.sleep(800)

      val kill = now()
      FailoverCluster.kill(victim.port)
      var failed = 0L
      var promoted = 0L
      var promotedPort = 0
      var unreachable = 0L
      var availabilityAsked: Option[Availability] = None
      val available = Promise[Long]()
      var idleLearnt = 0L
      val deadline = kill + 30L * 1000000000L
      while ((promoted == 0L || idleLearnt == 0L || !available.isCompleted) && now() < deadline) {
        if (unreachable == 0L && !cluster.health.isReachable(deadSlot)) {
          unreachable = now()
          val a = cluster.availability(deadTag)
          availabilityAsked = Some(a)
          a.whenAvailable.onComplete(_ => available.trySuccess(now()))
        }
        survivorViews(victim.port).foreach { view =>
          if (failed == 0L && view.exists(n => n.port == victim.port && n.flags.contains("fail"))) failed = now()
          if (promoted == 0L) view.find(n => n.isMaster && !n.isFailed && n.port != victim.port && n.owns(deadSlot)).foreach { n =>
            promoted = now()
            promotedPort = n.port
          }
        }
        if (promoted != 0L && idleLearnt == 0L && idle.masterOf(deadSlot).exists(_.port != victim.port)) idleLearnt = now()
        Thread.sleep(20)
      }
      Thread.sleep(2500)
      running = false
      load.join()
      eventually(timeout(Span(20, Seconds))) { outcomes.size shouldBe sentCount.get }

      info(s"coverage $coverage: killed master ${victim.port} (slot $deadSlot); the client saw the slot unreachable after " +
        s"${msBetween(kill, unreachable)} ms; flagged fail after ${msBetween(kill, failed)} ms; ${promotedPort} promoted after " +
        s"${msBetween(kill, promoted)} ms; availability(key) completed after ${available.future.value.map(t => msBetween(kill, t.get))} ms; " +
        s"the idle client had the new master after ${msBetween(kill, idleLearnt)} ms")
      failed should be > 0L
      promoted should be > failed

      // the phases, by the time a request was sent
      def phase(o: Outcome): String =
        if (o.sent < kill) "0 before the kill"
        else if (o.sent < failed) "1 before the failure is detected"
        else if (o.sent < promoted) "2 flagged fail, before promotion"
        else if (o.sent < promoted + 1000000000L) "3a promoted, first second"
        else "3b promoted, after the first second"
      val all = outcomes.asScala.toList
      all.groupBy(o => (o.side, phase(o))).toList.sortBy(_._1).foreach { case ((side, p), os) =>
        val kinds = os.groupBy(_.kind).map { case (k, v) => s"$k ${v.size}" }.mkString(", ")
        info(f"$side%-5s phase $p: ${os.size} requests: $kinds; time max ${os.map(_.ms).max} ms, median ${os.map(_.ms).sorted.apply(os.size / 2)} ms")
      }

      // the dead master's slot: once the client knows the master is down, every request fails at once as not sent,
      // until the promotion; after it, they succeed
      // (a request sent within the last poll before the promotion was seen may have been answered by the new master)
      val deadWhileDown = all.filter(o => o.side == "dead" && o.sent >= unreachable + 100000000L && o.sent < promoted - 50000000L)
      deadWhileDown should not be empty
      deadWhileDown.map(_.kind).distinct shouldBe List("not-sent")
      deadWhileDown.map(_.ms).max should be < 200L
      all.filter(o => o.side == "dead" && o.sent >= promoted + 1000000000L).map(_.kind).distinct shouldBe List("ok")

      // the other masters' slots
      val otherOutcomes = all.filter(_.side == "other")
      otherOutcomes.map(_.kind).filter(_ == "outcome-unknown") shouldBe empty
      if (coverage == "no") otherOutcomes.map(_.kind).distinct shouldBe List("ok")
      else {
        otherOutcomes.filter(o => o.sent < failed - 300000000L).map(_.kind).distinct shouldBe List("ok")
        otherOutcomes.map(_.kind).distinct.toSet.subsetOf(Set("ok", "not-sent")) shouldBe true
        otherOutcomes.map(_.ms).max should be < connectTimeout.toMillis + 1000L
      }
      otherOutcomes.filter(o => o.sent >= promoted + 1000000000L).map(_.kind).distinct shouldBe List("ok")

      // availability(key): asked while the slot was unreachable, completed after the promotion
      availabilityAsked shouldBe defined
      val availableAt = available.future.futureValue
      availableAt should be > failed
      msBetween(promoted, availableAt) should be > -300L
      msBetween(promoted, availableAt) should be < 3000L
      // the idle client found the new master by its own refreshes
      msBetween(promoted, idleLearnt) should be < 2000L
      // the victim's connection was reported reconnecting
      victimStates.asScala.exists { case (t, s) => t > kill && s.isInstanceOf[ConnectionState.Reconnecting] } shouldBe true

      // how often every write was executed: never more than once; never when it was not sent
      FailoverCluster.awaitWhole()
      val bySide = all.groupBy(_.side)
      val executed = bySide.flatMap { case (side, os) =>
        val tag = if (side == "dead") deadTag else otherTag
        counters(FailoverCluster.masterOf(ClusterCRC16.getSlot(tag)).port, os.map(_.key))
      }
      all.foreach { o =>
        val n = executed(o.key)
        withClue(s"${o.side} ${o.key} ${o.kind}: executed $n times: ") {
          n should be <= 1L
          if (o.kind == "not-sent") n shouldBe 0L
        }
      }
      val lostAcknowledged = all.count(o => o.kind == "ok" && executed(o.key) == 0L)
      val unknownExecuted = all.filter(_.kind == "outcome-unknown").map(o => executed(o.key))
      info(s"outcome unknown: ${unknownExecuted.size} (executed: ${unknownExecuted.count(_ == 1L)}); acknowledged writes " +
        s"lost by the failover: $lostAcknowledged")
      eventually { victimStates.asScala.last._2 shouldBe ConnectionState.Connected }

      // the same client works afterwards, on every master
      (1 to 100).foreach { i =>
        for (tag <- List(deadTag, otherTag, tagOf(masters(2)))) {
          cluster.eval[Long, String, String](Incr, Seq(s"{$tag}:$runId:after-$i")).futureValue shouldBe 1L
        }
      }
      all.foreach(o => cluster.del(o.key).futureValue)
      (1 to 100).foreach(i => List(deadTag, otherTag, tagOf(masters(2))).foreach(t => cluster.del(s"{$t}:$runId:after-$i").futureValue))
    } finally {
      Try(cluster.quit().futureValue)
      Try(idle.quit().futureValue)
      setCoverage("yes")
      FailoverCluster.awaitWhole()
    }
  }

  "One master of three killed under load" when {
    "cluster-require-full-coverage is yes" should {
      "fail requests to its slots at once as not sent until the promotion, execute no write twice, and work after" in {
        phases("yes")
      }
    }
    "cluster-require-full-coverage is no" should {
      "fail requests to its slots at once as not sent until the promotion, serve the other slots, and work after" in {
        phases("no")
      }
    }
  }

  "A write on the cluster whose reply does not arrive within the receive timeout" should {
    "fail as outcome unknown and not be sent again: it is executed once" in {
      FailoverCluster.awaitWhole()
      val cluster = RedisCluster(nodes = Seq(Server("127.0.0.1", 7100)), receiveTimeoutOpt = Some(500.millis))
      val key = s"partial-failure-slow-${System.currentTimeMillis()}"
      try {
        cluster.availability.whenAvailable.futureValue shouldBe (())
        val port = FailoverCluster.masterOf(ClusterCRC16.getSlot(key)).port
        // the INCR is executed at once, its reply comes after about 1.2 s
        val iterations = Spin.iterationsFor(port, 1200)
        val failure = cluster.eval[Long, String, String](Spin.IncrThenSpin, Seq(key), Seq(iterations.toString)).failed.futureValue
        info(s"$failure")
        failure shouldBe a [RedisOutcomeUnknownException]
        failure.asInstanceOf[RedisOutcomeUnknownException].availability.whenAvailable.futureValue shouldBe (())
        Thread.sleep(1500)
        counters(port, Seq(key))(key) shouldBe 1L
        cluster.del(key).futureValue
      } finally Try(cluster.quit().futureValue)
    }
  }
}
