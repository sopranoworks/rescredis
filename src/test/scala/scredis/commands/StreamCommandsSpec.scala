// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
package scredis.commands

import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec
import scredis._
import scredis.exceptions.{RedisClusterErrorResponseException, RedisErrorResponseException}
import scredis.protocol.{ClusterCRC16, CrossSlot}
import scredis.protocol.requests.StreamRequests._
import scredis.serialization.Implicits.bytesReader
import scredis.tags._
import scredis.util.TestUtils._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

/**
 * The stream commands against Redis: the standalone server (6379, Client, BlockingClient, Redis and a transaction)
 * and the cluster (7000 - 7005, RedisCluster), see testing.md.
 */
class StreamCommandsSpec extends AnyWordSpec with Matchers with ScalaFutures with BeforeAndAfterAll {

  private implicit val ec: ExecutionContext = ExecutionContext.global
  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(20, Seconds), interval = Span(20, Millis))

  private val client = Client()
  private val blockingClient = BlockingClient()
  private lazy val cluster = RedisCluster(Server("localhost", 7000))

  private val S = "stream-spec"
  private val S2 = "stream-spec-2"
  private val Missing = "stream-spec-missing"
  private val Hash = "stream-spec-hash"
  private val keys = Seq(S, S2, Missing, Hash)

  private def ids(entries: List[StreamEntry[String]]): List[String] = entries.map(_.id)

  /** The number of entries of a macro node (stream-node-max-entries) */
  private def streamNodeMaxEntries(): Int = client.configGet("stream-node-max-entries").futureValue.apply("stream-node-max-entries").toInt

  private def reset(): Unit = client.del(keys: _*).!

  /** Adds `n` entries with the ids 1-1 ... n-1 to `key` */
  private def addNumbered(key: String, n: Int): Unit =
    (1 to n).foreach(i => client.xAdd(key, Seq("n" -> i.toString), id = s"$i-1").!)

  override def afterAll(): Unit = {
    reset()
    client.quit().!
    blockingClient.quit()(5.seconds).get
    cluster.quit().futureValue
  }

  XAdd.toString when {
    "the id is *" should {
      "add an entry with an id that Redis generates, and create the stream" taggedAs V500 in {
        reset()
        val id = client.xAdd(S, Seq("a" -> "1", "b" -> "2")).futureValue
        id should be (defined)
        id.get should fullyMatch regex """\d+-\d+"""
        client.xRange[String](S).futureValue should be (List(StreamEntry(id.get, List("a" -> "1", "b" -> "2"))))
      }
    }
    "the id is given, or <ms>-*" should {
      "add the entry with that id, or the next sequence of that millisecond" taggedAs V700 in {
        reset()
        client.xAdd(S, Seq("a" -> "1"), id = "100-5").futureValue should be (Some("100-5"))
        client.xAdd(S, Seq("a" -> "2"), id = "100-*").futureValue should be (Some("100-6"))
        client.xAdd(S, Seq("a" -> "3"), id = "200-*").futureValue should be (Some("200-0"))
      }
    }
    "the id is not greater than the stream's last id" should {
      "fail with the error of Redis" taggedAs V500 in {
        reset()
        client.xAdd(S, Seq("a" -> "1"), id = "100-5").futureValue
        val e = client.xAdd(S, Seq("a" -> "2"), id = "100-5").failed.futureValue
        e shouldBe a [RedisErrorResponseException]
        e.getMessage should include ("equal or smaller")
      }
    }
    "a field repeats" should {
      "keep both pairs, in their order" taggedAs V500 in {
        reset()
        client.xAdd(S, Seq("f" -> "1", "g" -> "2", "f" -> "3"), id = "1-1").futureValue
        client.xRange[String](S).futureValue.head.fields should be (List("f" -> "1", "g" -> "2", "f" -> "3"))
      }
    }
    "NOMKSTREAM is given" should {
      "return None and not create a missing key" taggedAs V620 in {
        reset()
        client.xAdd(Missing, Seq("a" -> "1"), noMkStream = true).futureValue should be (empty)
        client.exists(Missing).futureValue should be (false)
      }
      "add to an existing stream" taggedAs V620 in {
        reset()
        client.xAdd(S, Seq("a" -> "1"), id = "1-1").futureValue
        client.xAdd(S, Seq("a" -> "2"), id = "2-1", noMkStream = true).futureValue should be (Some("2-1"))
        client.xLen(S).futureValue should be (2L)
      }
    }
    "MAXLEN = is given" should {
      "trim the stream exactly, with and without the operator" taggedAs V500 in {
        reset()
        addNumbered(S, 20)
        client.xAdd(S, Seq("n" -> "21"), id = "21-1", trim = Some(StreamTrim.MaxLen(5))).futureValue
        ids(client.xRange[String](S).futureValue) should be ((17 to 21).map(i => s"$i-1").toList)
        client.xAdd(S, Seq("n" -> "22"), id = "22-1",
          trim = Some(StreamTrim.MaxLen(3, Some(StreamTrimOperator.Exact)))).futureValue
        ids(client.xRange[String](S).futureValue) should be (List("20-1", "21-1", "22-1"))
      }
    }
    "MAXLEN ~ is given" should {
      "remove only whole macro nodes, keeping at least the threshold" taggedAs V500 in {
        reset()
        val nodeSize = streamNodeMaxEntries()
        addNumbered(S, 10 * nodeSize)
        client.xAdd(S, Seq("n" -> "x"), id = s"${10 * nodeSize + 1}-1",
          trim = Some(StreamTrim.MaxLen(10, Some(StreamTrimOperator.Approximate)))).futureValue
        // 10 full nodes and one with the new entry: nine are removed; a tenth would leave fewer than 10
        client.xLen(S).futureValue should be (nodeSize + 1L)
        client.xRange[String](S, count = Some(1)).futureValue.head.id should be (s"${9 * nodeSize + 1}-1")
      }
    }
    "MINID and LIMIT are given" should {
      "remove the entries below the id, at most LIMIT of them" taggedAs V620 in {
        reset()
        addNumbered(S, 10)
        client.xAdd(S, Seq("n" -> "11"), id = "11-1", trim = Some(StreamTrim.MinId("4"))).futureValue
        ids(client.xRange[String](S).futureValue).head should be ("4-1")
        val nodeSize = streamNodeMaxEntries()
        client.del(S).futureValue
        addNumbered(S, 3 * nodeSize)
        client.xAdd(S, Seq("n" -> "x"), id = s"${3 * nodeSize + 1}-1", trim = Some(StreamTrim.MinId(
          s"${3 * nodeSize}", Some(StreamTrimOperator.Approximate), limit = Some(nodeSize.toLong)))).futureValue
        // without LIMIT two nodes would go; LIMIT lets one go
        client.xLen(S).futureValue should be (2L * nodeSize + 1)
      }
    }
    "LIMIT is given without ~" should {
      "fail with the error of Redis" taggedAs V620 in {
        reset()
        val e = client.xAdd(S, Seq("a" -> "1"), trim = Some(StreamTrim.MaxLen(1, limit = Some(10)))).failed.futureValue
        e shouldBe a [RedisErrorResponseException]
        e.getMessage should include ("LIMIT")
        client.exists(S).futureValue should be (false)
      }
    }
    "the key is not a stream" should {
      "fail with WRONGTYPE" taggedAs V500 in {
        reset()
        client.hSet(Hash, "f", "v").futureValue
        client.xAdd(Hash, Seq("a" -> "1")).failed.futureValue.getMessage should startWith ("WRONGTYPE")
      }
    }
    "the values are bytes" should {
      "return them unchanged" taggedAs V500 in {
        reset()
        val bytes = (0 to 255).map(_.toByte).toArray
        client.xAdd(S, Seq("b" -> bytes, "empty" -> Array.emptyByteArray), id = "1-1").futureValue
        val fields = client.xRange[Array[Byte]](S).futureValue.head.fields
        fields.map(_._1) should be (List("b", "empty"))
        fields(0)._2.toSeq should be (bytes.toSeq)
        fields(1)._2.toSeq should be (empty)
        client.xRead[Array[Byte]](Seq(S -> "0")).futureValue.get.head._2.head.fields(0)._2.toSeq should be (bytes.toSeq)
      }
    }
  }

  XLen.toString when {
    "the key does not exist" should {
      "return 0" taggedAs V500 in {
        reset()
        client.xLen(Missing).futureValue should be (0L)
      }
    }
    "the stream has entries" should {
      "return their number" taggedAs V500 in {
        reset()
        addNumbered(S, 7)
        client.xLen(S).futureValue should be (7L)
      }
    }
  }

  XRange.toString when {
    "it is called with bounds and COUNT" should {
      "return the entries in the range, in order, at most COUNT" taggedAs V500 in {
        reset()
        addNumbered(S, 10)
        ids(client.xRange[String](S).futureValue) should be ((1 to 10).map(i => s"$i-1").toList)
        ids(client.xRange[String](S, "3", "5").futureValue) should be (List("3-1", "4-1", "5-1"))
        ids(client.xRange[String](S, "3-1", "+", count = Some(2)).futureValue) should be (List("3-1", "4-1"))
        client.xRange[String](S, "2-1", "2-1").futureValue should be (List(StreamEntry("2-1", List("n" -> "2"))))
        client.xRange[String](Missing).futureValue should be (empty)
      }
      "take exclusive bounds" taggedAs V620 in {
        reset()
        addNumbered(S, 5)
        ids(client.xRange[String](S, "(2-1", "(5-1").futureValue) should be (List("3-1", "4-1"))
      }
    }
  }

  XRevRange.toString when {
    "it is called with bounds and COUNT" should {
      "return the entries in the range, highest first, at most COUNT" taggedAs V500 in {
        reset()
        addNumbered(S, 10)
        ids(client.xRevRange[String](S).futureValue) should be ((10 to 1 by -1).map(i => s"$i-1").toList)
        ids(client.xRevRange[String](S, "5", "3").futureValue) should be (List("5-1", "4-1", "3-1"))
        ids(client.xRevRange[String](S, count = Some(2)).futureValue) should be (List("10-1", "9-1"))
        ids(client.xRevRange[String](S, "(5-1", "(2-1").futureValue) should be (List("4-1", "3-1"))
      }
    }
  }

  XDel.toString when {
    "ids are given" should {
      "remove those that exist, and return their number" taggedAs V500 in {
        reset()
        addNumbered(S, 5)
        client.xDel(S, "2-1", "4-1", "9-9").futureValue should be (2L)
        ids(client.xRange[String](S).futureValue) should be (List("1-1", "3-1", "5-1"))
        client.xDel(Missing, "1-1").futureValue should be (0L)
      }
    }
  }

  XTrim.toString when {
    "MAXLEN is given" should {
      "trim exactly with = and without an operator" taggedAs V500 in {
        reset()
        addNumbered(S, 10)
        client.xTrim(S, StreamTrim.MaxLen(7)).futureValue should be (3L)
        client.xTrim(S, StreamTrim.MaxLen(4, Some(StreamTrimOperator.Exact))).futureValue should be (3L)
        ids(client.xRange[String](S).futureValue) should be (List("7-1", "8-1", "9-1", "10-1"))
      }
      "with ~, remove only whole macro nodes; with LIMIT, at most LIMIT entries" taggedAs V620 in {
        reset()
        val nodeSize = streamNodeMaxEntries()
        addNumbered(S, 10 * nodeSize)
        // = would remove 10 * nodeSize - 10; ~ removes whole nodes only, and LIMIT caps that at three nodes
        client.xTrim(S, StreamTrim.MaxLen(10, Some(StreamTrimOperator.Approximate), Some(3L * nodeSize)))
          .futureValue should be (3L * nodeSize)
        val removed = client.xTrim(S, StreamTrim.MaxLen(10, Some(StreamTrimOperator.Approximate))).futureValue
        removed should be (6L * nodeSize)
        removed % nodeSize should be (0L)
        client.xLen(S).futureValue should be (nodeSize.toLong)
        // the same stream trimmed exactly
        client.xTrim(S, StreamTrim.MaxLen(10, Some(StreamTrimOperator.Exact))).futureValue should be (nodeSize - 10L)
      }
    }
    "MINID is given" should {
      "remove the entries with lower ids" taggedAs V620 in {
        reset()
        addNumbered(S, 10)
        client.xTrim(S, StreamTrim.MinId("6-1")).futureValue should be (5L)
        client.xTrim(S, StreamTrim.MinId("8", Some(StreamTrimOperator.Exact))).futureValue should be (2L)
        ids(client.xRange[String](S).futureValue) should be (List("8-1", "9-1", "10-1"))
      }
    }
    "LIMIT is given with =" should {
      "fail with the error of Redis" taggedAs V620 in {
        reset()
        addNumbered(S, 3)
        val e = client.xTrim(S, StreamTrim.MaxLen(1, Some(StreamTrimOperator.Exact), Some(10))).failed.futureValue
        e shouldBe a [RedisErrorResponseException]
        e.getMessage should include ("LIMIT")
      }
    }
    "the key does not exist" should {
      "return 0" taggedAs V500 in {
        reset()
        client.xTrim(Missing, StreamTrim.MaxLen(0)).futureValue should be (0L)
      }
    }
  }

  XRead.toString when {
    "it is called without BLOCK" should {
      "read from 0, from a given id, with COUNT, from several streams" taggedAs V500 in {
        reset()
        addNumbered(S, 5)
        client.xAdd(S2, Seq("m" -> "x"), id = "3-0").futureValue
        val all = client.xRead[String](Seq(S -> "0")).futureValue
        all.map(_.map { case (k, e) => (k, ids(e)) }) should be (Some(List(S -> (1 to 5).map(i => s"$i-1").toList)))
        all.get.head._2.head should be (StreamEntry("1-1", List("n" -> "1")))
        ids(client.xRead[String](Seq(S -> "3-1")).futureValue.get.head._2) should be (List("4-1", "5-1"))
        ids(client.xRead[String](Seq(S -> "0"), count = Some(2)).futureValue.get.head._2) should be (List("1-1", "2-1"))
        client.xRead[String](Seq(S -> "4-1", S2 -> "0")).futureValue.map(_.map { case (k, e) => (k, ids(e)) }) should be (
          Some(List(S -> List("5-1"), S2 -> List("3-0"))))
        // a stream without entries after its id is left out
        client.xRead[String](Seq(S -> "5-1", S2 -> "0")).futureValue.map(_.map(_._1)) should be (Some(List(S2)))
      }
      "return None from $, after the last id, and for a missing key" taggedAs V500 in {
        reset()
        addNumbered(S, 2)
        client.xRead[String](Seq(S -> "$")).futureValue should be (None)
        client.xRead[String](Seq(S -> "2-1")).futureValue should be (None)
        client.xRead[String](Seq(Missing -> "0")).futureValue should be (None)
      }
      "read the last entry with +" taggedAs V740 in {
        reset()
        addNumbered(S, 3)
        client.xRead[String](Seq(S -> "+")).futureValue.map(_.head._2) should be (Some(List(StreamEntry("3-1", List("n" -> "3")))))
      }
      "fail with WRONGTYPE for a key that is not a stream" taggedAs V500 in {
        reset()
        client.hSet(Hash, "f", "v").futureValue
        client.xRead[String](Seq(Hash -> "0")).failed.futureValue.getMessage should startWith ("WRONGTYPE")
      }
    }
    "it is called with BLOCK" should {
      "return None when the time is over" taggedAs V500 in {
        reset()
        addNumbered(S, 1)
        val start = System.nanoTime()
        blockingClient.xRead[String](500, Seq(S -> "$")) should be (Success(None))
        (System.nanoTime() - start).nanos should be >= 450.millis
      }
      "return the entry added while it blocks" taggedAs V500 in {
        reset()
        addNumbered(S, 1)
        val read = Future(blockingClient.xRead[String](10000, Seq(Missing -> "$", S -> "$")).get)
        Thread.sleep(500)
        read.isCompleted should be (false)
        client.xAdd(S, Seq("n" -> "2"), id = "2-1").futureValue
        read.futureValue should be (Some(List(S -> List(StreamEntry("2-1", List("n" -> "2"))))))
      }
      "block without limit with BLOCK 0, and return entries at once when there are some" taggedAs V500 in {
        reset()
        addNumbered(S, 2)
        blockingClient.xRead[String](0, Seq(S -> "1-1")).get.map(_.head._2.map(_.id)) should be (Some(List("2-1")))
        val read = Future(blockingClient.xRead[String](0, Seq(S -> "$"), count = Some(1)).get)
        Thread.sleep(500)
        read.isCompleted should be (false)
        client.xAdd(S, Seq("n" -> "3"), id = "3-1").futureValue
        read.futureValue.map(_.head._2.map(_.id)) should be (Some(List("3-1")))
      }
    }
  }

  "the stream commands" should {
    "be in Redis, its blocking client, and a transaction" taggedAs V500 in {
      reset()
      val redis = Redis()
      try {
        redis.xAdd(S, Seq("a" -> "1"), id = "1-1").futureValue should be (Some("1-1"))
        redis.blocking.xRead[String](1000, Seq(S -> "0")).get.map(_.head._2.map(_.id)) should be (Some(List("1-1")))
        val results = redis.inTransaction { t =>
          t.xAdd(S, Seq("a" -> "2"), id = "2-1")
          t.xLen(S)
          t.xRange[String](S, "2-1")
        }.futureValue
        results.map(_.get) should be (Vector(Some("2-1"), 2L, List(StreamEntry("2-1", List("a" -> "2")))))
      } finally redis.quit().futureValue
    }
  }

  "RedisCluster" should {
    // keys in different slots, and two keys in one slot
    val ca = "stream-cluster-a"
    val cb = "stream-cluster-b"
    val t1 = "{stream-cluster}1"
    val t2 = "{stream-cluster}2"

    "route the stream commands by their key" taggedAs V500 in {
      ClusterCRC16.getSlot(ca) should not be ClusterCRC16.getSlot(cb)
      cluster.del(ca).futureValue
      cluster.del(cb).futureValue
      cluster.xAdd(ca, Seq("a" -> "1"), id = "1-1").futureValue should be (Some("1-1"))
      cluster.xAdd(cb, Seq("b" -> "1"), id = "1-1").futureValue should be (Some("1-1"))
      cluster.xAdd(cb, Seq("b" -> "2"), id = "2-1", trim = Some(StreamTrim.MaxLen(1))).futureValue should be (Some("2-1"))
      cluster.xLen(ca).futureValue should be (1L)
      ids(cluster.xRange[String](cb).futureValue) should be (List("2-1"))
      ids(cluster.xRevRange[String](ca).futureValue) should be (List("1-1"))
      cluster.xRead[String](Seq(cb -> "0")).futureValue.map(_.head._2.map(_.id)) should be (Some(List("2-1")))
      cluster.xDel(ca, "1-1").futureValue should be (1L)
      cluster.xTrim(cb, StreamTrim.MaxLen(0)).futureValue should be (1L)
      cluster.del(ca).futureValue
      cluster.del(cb).futureValue
    }
    "read several streams of one slot with XREAD" taggedAs V500 in {
      cluster.del(t1, t2).futureValue
      cluster.xAdd(t1, Seq("a" -> "1"), id = "1-1").futureValue
      cluster.xAdd(t2, Seq("b" -> "1"), id = "1-2").futureValue
      cluster.xRead[String](Seq(t1 -> "0", t2 -> "0")).futureValue.map(_.map { case (k, e) => (k, ids(e)) }) should be (
        Some(List(t1 -> List("1-1"), t2 -> List("1-2"))))
      cluster.del(t1, t2).futureValue
    }
    "pass the CROSSSLOT of Redis for an XREAD of streams in several slots" taggedAs V500 in {
      val failure = cluster.xRead[String](Seq(ca -> "0", cb -> "0")).failed.futureValue
      failure shouldBe a [RedisClusterErrorResponseException]
      failure.asInstanceOf[RedisClusterErrorResponseException].error should be (CrossSlot)
    }
  }
}
