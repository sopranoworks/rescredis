// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
package scredis

import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec
import scredis.exceptions.RedisClusterErrorResponseException
import scredis.protocol._

import scala.concurrent.{ExecutionContext, Future}

/**
 * Commands with several keys on `RedisCluster`: Redis Cluster executes them only when all keys hash to
 * one slot (use a hash tag, `{tag}`) and answers `CROSSSLOT` otherwise. The cluster client does not split
 * them. `JSON.MGET` is the exception on the server (it declares only its first key, so Redis answers
 * with a nil for every key of another node); there the cluster client answers `CROSSSLOT` itself.
 * Needs the cluster on ports 7000 - 7005 with the JSON module, see testing.md.
 */
class RedisClusterMultiKeySpec extends AnyWordSpec with Matchers with ScalaFutures with BeforeAndAfterAll {

  private implicit val ec: ExecutionContext = ExecutionContext.global
  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(10, Seconds), interval = Span(20, Millis))

  private lazy val cluster = RedisCluster(Server("localhost", 7000))

  // two keys in different slots, and two keys in one slot
  private val ka = "multikey-a"
  private val kb = "multikey-b"
  private val t1 = "{multikey}1"
  private val t2 = "{multikey}2"

  private implicit val integerDecoder: Decoder[Long] = {
    case IntegerResponse(x) => x
  }

  override def afterAll(): Unit = cluster.quit().futureValue

  private def shouldBeCrossSlot(future: => Future[Any]): Unit = {
    val failure = future.failed.futureValue
    failure shouldBe a [RedisClusterErrorResponseException]
    val error = failure.asInstanceOf[RedisClusterErrorResponseException]
    error.error should be (CrossSlot)
    error.message should startWith ("CROSSSLOT")
  }

  "the test keys" should {
    "be in different slots, and in one slot" in {
      ClusterCRC16.getSlot(ka) should not be ClusterCRC16.getSlot(kb)
      ClusterCRC16.getSlot(t1) should be (ClusterCRC16.getSlot(t2))
    }
  }

  "multi-key commands with keys in one slot" should {
    "work: MGET, MGETASMAP, MSET, MSETNX" in {
      cluster.del(t1, t2).futureValue
      cluster.mSet(Map(t1 -> "1", t2 -> "2")).futureValue should be (())
      cluster.mGet[String](t1, t2).futureValue should be (List(Some("1"), Some("2")))
      cluster.mGetAsMap[String](t1, t2).futureValue should be (Map(t1 -> "1", t2 -> "2"))
      cluster.mSetNX(Map(t1 -> "x", t2 -> "y")).futureValue should be (false)
      cluster.del(t1, t2).futureValue should be (2L)
      cluster.mSetNX(Map(t1 -> "x", t2 -> "y")).futureValue should be (true)
      cluster.mGet[String](t1, t2).futureValue should be (List(Some("x"), Some("y")))
      cluster.del(t1, t2).futureValue
    }

    "work: Json.mSet and Json.mGet" in {
      cluster.del(t1, t2).futureValue
      cluster.Json.mSet((t1, "$", 1), (t2, "$", 2)).futureValue should be (true)
      cluster.Json.mGet[Int](Seq(t1, t2)).futureValue should be (List(Some(List(Some(1))), Some(List(Some(2)))))
      cluster.del(t1, t2).futureValue
    }

    "work: SMOVE, RPOPLPUSH" in {
      cluster.del(t1, t2).futureValue
      cluster.sAdd(t1, "m").futureValue
      cluster.sMove(t1, t2, "m").futureValue should be (true)
      cluster.sIsMember(t2, "m").futureValue should be (true)
      cluster.rPush(t1, "x").futureValue
      cluster.rPopLPush[String](t1, "{multikey}4").futureValue should be (Some("x"))
      cluster.lRange[String]("{multikey}4").futureValue should be (List("x"))
      cluster.del(t1, t2, "{multikey}4").futureValue
    }

    "work: DEL, SUNION, RENAME" in {
      cluster.del(t1, t2).futureValue
      cluster.sAdd(t1, "x").futureValue
      cluster.sAdd(t2, "y").futureValue
      cluster.sUnion[String](t1, t2).futureValue should be (Set("x", "y"))
      cluster.rename(t1, "{multikey}3").futureValue should be (())
      cluster.del(t1, t2, "{multikey}3").futureValue should be (2L)
    }
  }

  "multi-key commands with keys in different slots" should {
    "fail with CROSSSLOT and write nothing: MSET, MSETNX" in {
      cluster.del(ka).futureValue
      cluster.del(kb).futureValue
      shouldBeCrossSlot(cluster.mSet(Map(ka -> "1", kb -> "2")))
      shouldBeCrossSlot(cluster.mSetNX(Map(ka -> "1", kb -> "2")))
      cluster.get[String](ka).futureValue should be (None)
      cluster.get[String](kb).futureValue should be (None)
    }

    "fail with CROSSSLOT and write nothing: Json.mSet" in {
      cluster.del(ka).futureValue
      cluster.del(kb).futureValue
      shouldBeCrossSlot(cluster.Json.mSet((ka, "$", 1), (kb, "$", 2)))
      cluster.Json.get[Int](ka).futureValue should be (None)
      cluster.Json.get[Int](kb).futureValue should be (None)
    }

    "fail with CROSSSLOT and read nothing: MGET, MGETASMAP" in {
      cluster.set(ka, "1").futureValue
      cluster.set(kb, "2").futureValue
      shouldBeCrossSlot(cluster.mGet[String](ka, kb))
      shouldBeCrossSlot(cluster.mGetAsMap[String](ka, kb))
      cluster.del(ka).futureValue
      cluster.del(kb).futureValue
    }

    "fail with CROSSSLOT, not with a nil for the key of another node: Json.mGet" in {
      cluster.Json.set(ka, "$", 1).futureValue
      cluster.Json.set(kb, "$", 2).futureValue
      shouldBeCrossSlot(cluster.Json.mGetRaw(Seq(ka, kb)))
      shouldBeCrossSlot(cluster.Json.mGet[Int](Seq(kb, ka)))
      // one key, or keys of one slot, are not affected
      cluster.Json.mGet[Int](Seq(ka, ka)).futureValue should be (List(Some(List(Some(1))), Some(List(Some(1)))))
      cluster.del(ka).futureValue
      cluster.del(kb).futureValue
    }

    // every other command of RedisCluster with several keys: Redis answers CROSSSLOT
    List[(String, () => Future[Any])](
      "DEL" -> (() => cluster.del(ka, kb)),
      "RENAME" -> (() => cluster.rename(ka, kb)),
      "RENAMENX" -> (() => cluster.renameNX(ka, kb)),
      "SORT ... STORE" -> (() => cluster.sortAndStore(ka, kb)),
      "BITOP" -> (() => cluster.bitOp(scredis.BitOp.And, ka, kb)),
      "SDIFF" -> (() => cluster.sDiff[String](ka, kb)),
      "SDIFFSTORE" -> (() => cluster.sDiffStore(ka, kb)),
      "SINTER" -> (() => cluster.sInter[String](ka, kb)),
      "SINTERSTORE" -> (() => cluster.sInterStore(ka, kb)),
      "SUNION" -> (() => cluster.sUnion[String](ka, kb)),
      "SUNIONSTORE" -> (() => cluster.sUnionStore(ka, kb)),
      "SMOVE" -> (() => cluster.sMove(ka, kb, "member")),
      "ZUNIONSTORE" -> (() => cluster.zUnionStore(ka, Seq(kb))),
      "ZUNIONSTORE (weighted)" -> (() => cluster.zUnionStoreWeighted(ka, Map(kb -> 1.0))),
      "ZINTERSTORE" -> (() => cluster.zInterStore(ka, Seq(kb))),
      "ZINTERSTORE (weighted)" -> (() => cluster.zInterStoreWeighted(ka, Map(kb -> 1.0))),
      "RPOPLPUSH" -> (() => cluster.rPopLPush[String](ka, kb)),
      "PFCOUNT" -> (() => cluster.pfCount(ka, kb)),
      "PFMERGE" -> (() => cluster.pfMerge(ka, kb)),
      "EVAL" -> (() => cluster.eval[Long, String, String]("return 1", keys = Seq(ka, kb)))
    ).foreach { case (name, command) =>
      s"fail with CROSSSLOT: $name" in shouldBeCrossSlot(command())
    }
  }
}
