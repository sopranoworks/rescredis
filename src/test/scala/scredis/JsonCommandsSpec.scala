// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
package scredis

// inside package scredis, `io` alone is scredis.io
import _root_.io.circe.{Decoder, Encoder, Json => CJson}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec
import scredis.commands.JsonCommands
import scredis.serialization.JsonImplicits._
import scredis.serialization.Writer
import scredis.tags._
import scredis.util.TestUtils._

import scala.concurrent.ExecutionContext

object JsonCommandsSpec {
  case class JsonRecord(
    name: String, score: Int, flag: Boolean, optValue: Option[Int], arr: List[Int], emptyArr: List[Int]
  )

  object JsonRecord {
    implicit val encoder: Encoder[JsonRecord] =
      Encoder.forProduct6("name", "score", "flag", "optValue", "arr", "emptyArr")(
        r => (r.name, r.score, r.flag, r.optValue, r.arr, r.emptyArr)
      )
    implicit val decoder: Decoder[JsonRecord] =
      Decoder.forProduct6("name", "score", "flag", "optValue", "arr", "emptyArr")(
        (name: String, score: Int, flag: Boolean, optValue: Option[Int], arr: List[Int], emptyArr: List[Int]) =>
          JsonRecord(name, score, flag, optValue, arr, emptyArr)
      )
  }

  case class Level(level: Int)

  /** Its toString is not what its Writer writes. */
  case class Marker(n: Int)

  implicit val markerWriter: Writer[Marker] = new Writer[Marker] {
    override protected def writeImpl(value: Marker): Array[Byte] = value.n.toString.getBytes("UTF-8")
  }

  object Level {
    implicit val encoder: Encoder[Level] = Encoder.forProduct1("level")((l: Level) => l.level)
  }
}

/**
 * Every `Json.*` method, through `Client`, `Redis` and `RedisCluster`. Needs the Redis Stack JSON module on
 * 6379 and on every cluster node (7000 - 7005); see testing.md.
 */
class JsonCommandsSpec extends AnyWordSpec
  with Matchers
  with ScalaFutures
  with BeforeAndAfterAll {

  import JsonCommandsSpec._

  private implicit val ec: ExecutionContext = ExecutionContext.global
  private implicit val patience: PatienceConfig =
    PatienceConfig(timeout = Span(10, Seconds), interval = Span(20, Millis))

  private val client = Client()
  private val redis = Redis()
  private lazy val cluster = RedisCluster(Server("localhost", 7000))

  override def afterAll(): Unit = {
    client.quit().futureValue
    redis.quit().futureValue
    cluster.quit().futureValue
  }

  private val record = JsonRecord("hello json", 100, true, Some(-1), List(1, 2, 3), Nil)

  List[(String, () => JsonCommands)](
    "Client" -> (() => client), "Redis" -> (() => redis), "RedisCluster" -> (() => cluster)
  ).foreach { case (label, commands) =>
    // one slot for every key, so that the multi-key commands work on the cluster too
    def key(name: String): String = s"{jsonspec}:$label:$name"
    lazy val target: JsonCommands = commands()
    lazy val json = target.Json
    def fresh(name: String): String = {
      val k = key(name)
      json.del(k).futureValue
      k
    }
    def raw(text: String): CJson = _root_.io.circe.parser.parse(text).fold(e => throw e, j => j)

    s"Json commands on $label" when {
      "setting and getting" should {
        "set a record and get it back, as a value and as JSON" taggedAs (RedisJson) in {
          val k = fresh("set")
          json.set(k, "$", record).futureValue should be (true)
          json.get[JsonRecord](k).futureValue should be (Some(record))
          json.getRaw(k, "$.score").futureValue should be (Some(raw("[100]")))
          json.getRaw(k, "$.score", "$.name").futureValue should be (Some(raw("""{"$.score":[100],"$.name":["hello json"]}""")))
          json.get[JsonRecord](key("missing")).futureValue should be (None)
          json.getRaw(key("missing")).futureValue should be (None)
        }
        "keep non-ASCII text, also when searching an array for it" taggedAs (RedisJson) in {
          val k = fresh("unicode")
          val unicode = record.copy(name = "HelloWorld!虫àéç蟲")
          json.set(k, "$", unicode).futureValue should be (true)
          json.get[JsonRecord](k).futureValue should be (Some(unicode))
          json.set(k, "$.words", List("虫", "àéç", "蟲")).futureValue should be (true)
          json.arrIndex(k, "$.words", "蟲").futureValue should be (List(Some(2L)))
          // the module counts the bytes of the UTF-8 text: 11 + 3 + 6 + 3 + 3
          json.strAppend(k, "$.name", "蟲").futureValue should be (List(Some(26L)))
          json.strLen(k, "$.name").futureValue should be (List(Some(26L)))
        }
        "use the root path by default" taggedAs (RedisJson) in {
          val k = fresh("default-path")
          json.set(k, value = record).futureValue should be (true)
          json.get[JsonRecord](k).futureValue should be (Some(record))
        }
        "set a string" taggedAs (RedisJson) in {
          val k = fresh("string")
          json.set(k, "$", "test").futureValue should be (true)
          json.get[String](k).futureValue should be (Some("test"))
        }
        "setRaw with a circe Json and with a String writer" taggedAs (RedisJson) in {
          val k = fresh("setraw")
          json.setRaw(k, "$", raw("""{"a":1}""")).futureValue should be (true)
          json.getRaw(k).futureValue should be (Some(raw("""{"a":1}""")))
          import scredis.serialization.Implicits.stringWriter
          json.setRaw(k, "$.b", """[true,null]""").futureValue should be (true)
          json.getRaw(k).futureValue should be (Some(raw("""{"a":1,"b":[true,null]}""")))
        }
        "setNX only when the key does not exist" taggedAs (RedisJson) in {
          val k = fresh("setnx")
          json.setNX(k, "$", record).futureValue should be (true)
          json.setNX(k, "$", record.copy(score = 1)).futureValue should be (false)
          json.get[JsonRecord](k).futureValue.map(_.score) should be (Some(100))
        }
        "setRawNX only when the key does not exist" taggedAs (RedisJson) in {
          val k = fresh("setrawnx")
          json.setRawNX(k, "$", raw("""{"a":1}""")).futureValue should be (true)
          json.setRawNX(k, "$", raw("""{"a":2}""")).futureValue should be (false)
          json.getRaw(k).futureValue should be (Some(raw("""{"a":1}""")))
        }
        "setXX only when the key exists" taggedAs (RedisJson) in {
          val k = fresh("setxx")
          json.setXX(k, "$", record).futureValue should be (false)
          json.getRaw(k).futureValue should be (None)
          json.set(k, "$", record).futureValue should be (true)
          json.setXX(k, "$", record.copy(score = 7)).futureValue should be (true)
          json.get[JsonRecord](k).futureValue.map(_.score) should be (Some(7))
        }
        "setRawXX only when the key exists" taggedAs (RedisJson) in {
          val k = fresh("setrawxx")
          json.setRawXX(k, "$", raw("""{"a":1}""")).futureValue should be (false)
          json.setRaw(k, "$", raw("""{"a":1}""")).futureValue should be (true)
          json.setRawXX(k, "$", raw("""{"a":2}""")).futureValue should be (true)
          json.getRaw(k).futureValue should be (Some(raw("""{"a":2}""")))
        }
      }

      "merging" should {
        "merge a value (merge) and raw JSON (mergeRaw)" taggedAs (RedisJson) in {
          val k = fresh("merge")
          json.set(k, "$", record).futureValue should be (true)
          json.merge(k, "$", Level(99)).futureValue should be (true)
          json.getRaw(k, "$.level").futureValue should be (Some(raw("[99]")))
          json.mergeRaw(k, "$", raw("""{"score":5,"flag":null}""")).futureValue should be (true)
          val merged = json.getRaw(k).futureValue.get
          merged.hcursor.get[Int]("score") should be (Right(5))
          merged.hcursor.downField("flag").succeeded should be (false)
        }
      }

      "deleting, toggling, clearing and typing" should {
        "del" taggedAs (RedisJson) in {
          val k = fresh("del")
          json.set(k, "$", record).futureValue should be (true)
          json.del(k, "$.optValue").futureValue should be (1L)
          json.del(k, "$.optValue").futureValue should be (0L)
          json.get[JsonRecord](k).futureValue.map(_.optValue) should be (Some(None))
          json.del(k).futureValue should be (1L)
          json.del(k).futureValue should be (0L)
        }
        "toggle" taggedAs (RedisJson) in {
          val k = fresh("toggle")
          json.set(k, "$", record).futureValue should be (true)
          json.toggle(k, "$.flag").futureValue should be (List(Some(0L)))
          json.toggle(k, "$.flag").futureValue should be (List(Some(1L)))
          json.toggle(k, "$.name").futureValue should be (List(None))
        }
        "clear" taggedAs (RedisJson) in {
          val k = fresh("clear")
          json.set(k, "$", record).futureValue should be (true)
          json.clear(k, "$.score").futureValue should be (1L)
          json.get[JsonRecord](k).futureValue.map(_.score) should be (Some(0))
          json.clear(k, "$.arr").futureValue should be (1L)
          json.get[JsonRecord](k).futureValue.map(_.arr) should be (Some(Nil))
        }
        "types" taggedAs (RedisJson) in {
          val k = fresh("types")
          json.set(k, "$", record).futureValue should be (true)
          json.types(k, "$.score").futureValue should be (List(Some("integer")))
          json.types(k, "$.name").futureValue should be (List(Some("string")))
          json.types(k, "$.arr").futureValue should be (List(Some("array")))
          json.types(k).futureValue should be (List(Some("object")))
          json.types(key("types-missing")).futureValue should be (Nil)
        }
      }

      "working on strings" should {
        "strLen" taggedAs (RedisJson) in {
          val k = fresh("strlen")
          json.set(k, "$", record).futureValue should be (true)
          json.strLen(k, "$.name").futureValue should be (List(Some(10L)))
          json.strLen(k, "$.name2").futureValue should be (Nil)
          json.strLen(k, "$.score").futureValue should be (List(None))
        }
        "strAppend, also with quotes and backslashes in the value" taggedAs (RedisJson) in {
          val k = fresh("strappend")
          json.set(k, "$", record).futureValue should be (true)
          json.strAppend(k, "$.name", "!!").futureValue should be (List(Some(12L)))
          json.strAppend(k, "$.score", "!!").futureValue should be (List(None))
          json.strAppend(k, "$.name", "\"quote\"").futureValue should be (List(Some(19L)))
          json.strAppend(k, "$.name", "a\\b").futureValue should be (List(Some(22L)))
          json.strAppend(k, "$.name", "tail\\").futureValue should be (List(Some(27L)))
          json.strAppend(k, "$.name", "x\ny").futureValue should be (List(Some(30L)))
          json.get[JsonRecord](k).futureValue.map(_.name) should be (Some("hello json!!\"quote\"a\\btail\\x\ny"))
        }
      }

      "working on numbers" should {
        "numIncBy" taggedAs (RedisJson) in {
          val k = fresh("numincby")
          json.set(k, "$", record).futureValue should be (true)
          json.numIncBy(k, "$.score", 10).futureValue should be (List(Some(110L)))
          json.numIncBy(k, "$.score", -20).futureValue should be (List(Some(90L)))
          json.numIncBy(k, "$.name", 10).futureValue should be (List(None))
        }
        "numIncBy with a legacy path" taggedAs (RedisJson) in {
          val k = fresh("numincby-legacy")
          json.set(k, "$", record).futureValue should be (true)
          json.numIncBy(k, ".score", 5).futureValue should be (List(Some(105L)))
        }
        "numIncBy fails when the new value is not an integer" taggedAs (RedisJson) in {
          val k = fresh("numincby-fraction")
          json.set(k, "$", record).futureValue should be (true)
          json.numIncBy(k, "$.score", 0.5).failed.futureValue shouldBe a [scredis.exceptions.RedisProtocolException]
        }
      }

      "setting and getting several keys" should {
        "mSet and mSetRaw" taggedAs (RedisJson) in {
          val a = fresh("mset-a")
          val b = fresh("mset-b")
          json.mSet((a, "$", record), (b, "$", record.copy(score = 2))).futureValue should be (true)
          json.get[JsonRecord](a).futureValue should be (Some(record))
          json.get[JsonRecord](b).futureValue.map(_.score) should be (Some(2))
          json.mSet((a, "$.score", 1000), (a, "$.optValue", 999)).futureValue should be (true)
          json.get[JsonRecord](a).futureValue.map(r => (r.score, r.optValue)) should be (Some((1000, Some(999))))
          json.mSetRaw((b, "$.name", raw("\"raw\""))).futureValue should be (true)
          json.get[JsonRecord](b).futureValue.map(_.name) should be (Some("raw"))
        }
        "mGet and mGetRaw" taggedAs (RedisJson) in {
          val a = fresh("mget-a")
          val b = fresh("mget-b")
          json.set(a, "$", record.copy(score = 1000)).futureValue should be (true)
          json.set(b, "$", record.copy(optValue = None)).futureValue should be (true)
          json.mGet[Int](List(a, b, key("mget-missing")), "$.score").futureValue should be (
            List(Some(List(Some(1000))), Some(List(Some(100))), None)
          )
          json.mGet[Int](List(a, b), "$.optValue").futureValue should be (List(Some(List(Some(-1))), Some(List(None))))
          json.mGetRaw(List(a, key("mget-missing")), "$.score").futureValue should be (List(Some(raw("[1000]")), None))
        }
      }

      "working on objects" should {
        "objLen and objKeys" taggedAs (RedisJson) in {
          val k = fresh("obj")
          json.set(k, "$", record).futureValue should be (true)
          json.objLen(k).futureValue should be (List(Some(6L)))
          json.objLen(k, "$.name").futureValue should be (List(None))
          json.objKeys(k).futureValue.map(_.map(_.sorted)) should be (
            List(Some(List("arr", "emptyArr", "flag", "name", "optValue", "score")))
          )
          json.objKeys(k, "$.name").futureValue should be (List(None))
        }
      }

      "working on arrays" should {
        "arrAppend and arrAppendRaw" taggedAs (RedisJson) in {
          val k = fresh("arrappend")
          json.set(k, "$", record).futureValue should be (true)
          json.arrAppend(k, "$.arr", 4, 5, 6).futureValue should be (List(Some(6L)))
          json.get[JsonRecord](k).futureValue.map(_.arr) should be (Some(List(1, 2, 3, 4, 5, 6)))
          json.arrAppend(k, "$.name", 4, 5, 6).futureValue should be (List(None))
          json.arrAppendRaw(k, "$.arr", raw("7"), raw("8")).futureValue should be (List(Some(8L)))
        }
        "arrLen" taggedAs (RedisJson) in {
          val k = fresh("arrlen")
          json.set(k, "$", record).futureValue should be (true)
          json.arrLen(k, "$.arr").futureValue should be (List(Some(3L)))
          json.arrLen(k, "$.emptyArr").futureValue should be (List(Some(0L)))
          json.arrLen(k, "$.name").futureValue should be (List(None))
        }
        "arrInsert and arrInsertRaw" taggedAs (RedisJson) in {
          val k = fresh("arrinsert")
          json.set(k, "$", record).futureValue should be (true)
          json.arrInsert(k, "$.arr", 3, 10, 11, 12).futureValue should be (List(Some(6L)))
          json.get[JsonRecord](k).futureValue.map(_.arr) should be (Some(List(1, 2, 3, 10, 11, 12)))
          json.arrInsertRaw(k, "$.arr", 0, raw("0")).futureValue should be (List(Some(7L)))
          json.get[JsonRecord](k).futureValue.map(_.arr) should be (Some(List(0, 1, 2, 3, 10, 11, 12)))
        }
        "arrIndex and arrIndexRaw" taggedAs (RedisJson) in {
          val k = fresh("arrindex")
          json.set(k, "$", record).futureValue should be (true)
          json.arrIndex(k, "$.arr", 3).futureValue should be (List(Some(2L)))
          json.arrIndex(k, "$.arr", 99).futureValue should be (List(Some(-1L)))
          json.arrIndexRaw(k, "$.arr", raw("2")).futureValue should be (List(Some(1L)))
          // start is inclusive, stop is exclusive (0 = to the end)
          json.arrIndex(k, "$.arr", 1, 1, 0).futureValue should be (List(Some(-1L)))
          json.arrIndex(k, "$.arr", 3, 1, 0).futureValue should be (List(Some(2L)))
          json.arrIndex(k, "$.arr", 3, 0, 2).futureValue should be (List(Some(-1L)))
          json.arrIndex(k, "$.arr", 3, 0, 3).futureValue should be (List(Some(2L)))
          // arrIndexRaw writes the value with its Writer
          json.arrIndexRaw(k, "$.arr", Marker(3)).futureValue should be (List(Some(2L)))
          json.arrIndex(k, "$.name", 3).futureValue should be (List(None))
        }
        "arrPop and arrPopRaw" taggedAs (RedisJson) in {
          val k = fresh("arrpop")
          json.set(k, "$", record).futureValue should be (true)
          json.arrPop[Int](k, "$.arr").futureValue should be (List(Some(3)))
          json.arrPop[Int](k, "$.arr", 0).futureValue should be (List(Some(1)))
          json.arrPopRaw(k, "$.arr").futureValue should be (List(Some(raw("2"))))
          json.arrPop[Int](k, "$.arr").futureValue should be (List(None))
          json.arrPop[Int](k, "$.emptyArr").futureValue should be (List(None))
          json.arrPop[Int](k, "$.name").futureValue should be (List(None))
        }
        "arrTrim" taggedAs (RedisJson) in {
          val k = fresh("arrtrim")
          json.set(k, "$", record.copy(arr = List(10, 11, 12, 13, 14))).futureValue should be (true)
          json.arrTrim(k, "$.arr", 1, 3).futureValue should be (List(Some(3L)))
          json.get[JsonRecord](k).futureValue.map(_.arr) should be (Some(List(11, 12, 13)))
          json.arrTrim(k, "$.emptyArr", 2, 4).futureValue should be (List(Some(0L)))
          json.arrTrim(k, "$.name", 2, 4).futureValue should be (List(None))
        }
      }

      "reporting memory" should {
        "debugMemory" taggedAs (RedisJson) in {
          val k = fresh("debug")
          json.set(k, "$", record).futureValue should be (true)
          json.debugMemory(k).futureValue.size should be (1)
          json.debugMemory(k).futureValue.head should be > 0L
          json.debugMemory(k, "$.unknown").futureValue should be (Nil)
          json.debugMemory(key("debug-missing")).futureValue should be (Nil)
        }
      }
    }
  }
}
