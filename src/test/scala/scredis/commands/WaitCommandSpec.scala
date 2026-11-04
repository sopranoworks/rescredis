// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
package scredis.commands

import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec
import scredis._
import scredis.exceptions.{RedisErrorResponseException, RedisInvalidArgumentException}
import scredis.protocol.ClusterCRC16
import scredis.tags._
import scredis.util.RawRedis
import scredis.util.TestUtils._

import scala.concurrent.duration._

/**
 * WAIT: standalone on the master 6390 with its replica 6391, on 6379 (no replica) and on the replica itself; on
 * the cluster 7000 - 7005 (3 masters with a replica each) through `RedisCluster.wait(key, ...)`, see testing.md.
 */
class WaitCommandSpec extends AnyWordSpec with Matchers with ScalaFutures with BeforeAndAfterAll {

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(20, Seconds), interval = Span(20, Millis))

  private val master = Client(host = "127.0.0.1", port = 6390)
  private val replica = Client(host = "127.0.0.1", port = 6391)
  private val alone = Client(host = "127.0.0.1", port = 6379)
  private val cluster = RedisCluster(Server("127.0.0.1", 7000))

  private val Key = "wait-spec"

  private def elapsedMillis[A](f: => A): (A, Long) = {
    val start = System.nanoTime()
    val a = f
    (a, (System.nanoTime() - start) / 1000000L)
  }

  /** A key whose slot is served by each master of the cluster */
  private lazy val keyPerMaster: List[(Int, String)] = {
    val masters = RawRedis.clusterNodes(7000).filter(n => n.isMaster && n.slots.nonEmpty)
    masters.map { m =>
      m.port -> Iterator.from(0).map(i => s"wait-spec-$i").find(k => m.owns(ClusterCRC16.getSlot(k))).get
    }
  }

  override def afterAll(): Unit = {
    master.del(Key).!
    alone.del(Key).!
    keyPerMaster.foreach { case (_, k) => cluster.del(k).! }
    master.quit().!
    replica.quit().!
    alone.quit().!
    cluster.quit().!
  }

  "WAIT" when {
    "the master has a replica" should {
      "answer 1 once the replica has the preceding write" taggedAs V300 in {
        master.set(Key, "written").futureValue shouldBe true
        master.wait(1, 5.seconds).futureValue shouldBe 1
        replica.get(Key).futureValue shouldBe Some("written")
      }
      "answer the replicas that acknowledged, after the timeout, when fewer than asked do" taggedAs V300 in {
        master.set(Key, "again").futureValue shouldBe true
        val (replicas, ms) = elapsedMillis(master.wait(2, 300.millis).futureValue)
        replicas shouldBe 1
        ms should be >= 280L
      }
    }
    "the server has no replica" should {
      "answer 0 after the timeout" taggedAs V300 in {
        alone.set(Key, "alone").futureValue shouldBe true
        val (replicas, ms) = elapsedMillis(alone.wait(1, 200.millis).futureValue)
        replicas shouldBe 0
        ms should be >= 180L
      }
    }
    "sent to a replica" should {
      "return Redis' error" taggedAs V300 in {
        val e = replica.wait(0, 100.millis).failed.futureValue
        e shouldBe a [RedisErrorResponseException]
        e.getMessage should include ("replica")
      }
    }
  }

  "RedisCluster.wait(key, numReplicas, timeout)" should {
    "wait on the master of the key's slot: 1 after a write to the key, on every master" taggedAs V300 in {
      keyPerMaster should have size 3
      keyPerMaster.foreach { case (port, k) =>
        withClue(s"$k (master $port): ") {
          cluster.set(k, s"on-$port").futureValue shouldBe true
          cluster.wait(k, 1, 5.seconds).futureValue shouldBe 1
        }
      }
    }
    "answer what Redis answers when fewer replicas than asked acknowledge" taggedAs V300 in {
      val (_, k) = keyPerMaster.head
      cluster.set(k, "more").futureValue shouldBe true
      val (replicas, ms) = elapsedMillis(cluster.wait(k, 2, 300.millis).futureValue)
      replicas shouldBe 1
      ms should be >= 280L
    }
  }

  "RedisCluster.wait(numReplicas, timeout) without a key" should {
    "not be supported (WAIT has no key to route it by)" taggedAs V300 in {
      cluster.wait(1, 100.millis).failed.futureValue shouldBe a [RedisInvalidArgumentException]
    }
  }
}
