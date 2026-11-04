// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
package scredis.commands

import java.security.MessageDigest

import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec
import scredis._
import scredis.exceptions.RedisErrorResponseException
import scredis.protocol.{BulkStringResponse, ClusterCRC16, Decoder}
import scredis.tags._
import scredis.util.{FailoverCluster, RawRedis}

/**
 * SCRIPT LOAD, SCRIPT EXISTS and SCRIPT FLUSH on RedisCluster, sent to every master; on the failover cluster
 * 7100 - 7105 (see testing.md), which also shows what a promoted replica answers to EVALSHA.
 */
class ClusterScriptingSpec extends AnyWordSpec with Matchers with ScalaFutures with BeforeAndAfterAll {

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(30, Seconds), interval = Span(20, Millis))

  private implicit val bulkString: Decoder[String] = {
    case b: BulkStringResponse => b.flattened[String]
  }

  private val Script = "return 'script-spec:' .. KEYS[1]"
  private val Sha1 = MessageDigest.getInstance("SHA-1").digest(Script.getBytes("UTF-8")).map("%02x".format(_)).mkString

  private lazy val cluster = {
    FailoverCluster.awaitWhole()
    RedisCluster(Server("127.0.0.1", 7100))
  }

  private def masterPorts: List[Int] = FailoverCluster.masters().map(_.port)

  private def existsOn(port: Int, sha1: String): Boolean =
    RawRedis.withConnection("127.0.0.1", port)(_.command("SCRIPT", "EXISTS", sha1)) == List(1L)

  /** A key whose slot is served by each master */
  private def keyPerMaster(): List[(Int, String)] = FailoverCluster.masters().map { m =>
    m.port -> Iterator.from(0).map(i => s"script-spec-$i").find(k => m.owns(ClusterCRC16.getSlot(k))).get
  }

  private def flushEverywhere(): Unit = masterPorts.foreach(p => RawRedis.withConnection("127.0.0.1", p)(_.command("SCRIPT", "FLUSH")))

  override def afterAll(): Unit = {
    flushEverywhere()
    cluster.quit().futureValue
  }

  "SCRIPT LOAD on RedisCluster" should {
    "load the script on every master and return its SHA1" taggedAs V260 in {
      flushEverywhere()
      cluster.scriptLoad(Script).futureValue shouldBe Sha1
      masterPorts should have size 3
      masterPorts.foreach(p => withClue(s"master $p: ")(existsOn(p, Sha1) shouldBe true))
      keyPerMaster().foreach { case (p, k) =>
        withClue(s"EVALSHA on $k (master $p): ") {
          cluster.evalSHA[String, String, String](Sha1, Seq(k), Nil).futureValue shouldBe s"script-spec:$k"
        }
      }
    }
    "fail with the error a master answers" taggedAs V260 in {
      val e = cluster.scriptLoad("this is not lua").failed.futureValue
      e shouldBe a [RedisErrorResponseException]
      e.getMessage should include ("Error compiling script")
    }
  }

  "SCRIPT EXISTS on RedisCluster" should {
    "say true only for a script that every master has" taggedAs V260 in {
      flushEverywhere()
      val unknown = "0" * 40
      cluster.scriptExists(Sha1, unknown).futureValue shouldBe Map(Sha1 -> false, unknown -> false)
      // on one master only
      RawRedis.withConnection("127.0.0.1", masterPorts.head)(_.command("SCRIPT", "LOAD", Script)) shouldBe Sha1
      cluster.scriptExists(Sha1).futureValue shouldBe Map(Sha1 -> false)
      cluster.scriptLoad(Script).futureValue shouldBe Sha1
      cluster.scriptExists(Sha1, unknown).futureValue shouldBe Map(Sha1 -> true, unknown -> false)
    }
  }

  "SCRIPT FLUSH on RedisCluster" should {
    "remove the scripts from every master" taggedAs V260 in {
      cluster.scriptLoad(Script).futureValue shouldBe Sha1
      cluster.scriptFlush().futureValue shouldBe (())
      masterPorts.foreach(p => withClue(s"master $p: ")(existsOn(p, Sha1) shouldBe false))
    }
  }

  "A replica promoted by a failover" should {
    "answer NOSCRIPT to EVALSHA of a script loaded before, until it is loaded again" taggedAs V260 in {
      FailoverCluster.awaitWhole()
      cluster.scriptLoad(Script).futureValue shouldBe Sha1
      val (port, k) = keyPerMaster().head
      cluster.evalSHA[String, String, String](Sha1, Seq(k), Nil).futureValue shouldBe s"script-spec:$k"
      FailoverCluster.kill(port)
      val promoted = FailoverCluster.awaitFailover(port, ClusterCRC16.getSlot(k))
      info(s"killed master $port, ${promoted.port} promoted")
      try {
        val e = eventuallyFailure(cluster.evalSHA[String, String, String](Sha1, Seq(k), Nil))
        e shouldBe a [RedisErrorResponseException]
        e.getMessage should startWith ("NOSCRIPT")
        cluster.scriptLoad(Script).futureValue shouldBe Sha1
        cluster.evalSHA[String, String, String](Sha1, Seq(k), Nil).futureValue shouldBe s"script-spec:$k"
      } finally {
        FailoverCluster.awaitWhole()
      }
    }
  }

  /** The first failure that is not the cluster still settling (connection errors, CLUSTERDOWN), within 20 s */
  private def eventuallyFailure(f: => scala.concurrent.Future[String]): Throwable = {
    val deadline = System.nanoTime() + 20L * 1000000000L
    var result: Option[Throwable] = None
    while (result.isEmpty) {
      val r = scala.util.Try(scala.concurrent.Await.result(f, scala.concurrent.duration.Duration(10, "s")))
      r match {
        case scala.util.Failure(e: RedisErrorResponseException) => result = Some(e)
        case other =>
          if (System.nanoTime() > deadline) fail(s"no error reply from the promoted master: $other")
          Thread.sleep(200)
      }
    }
    result.get
  }
}
