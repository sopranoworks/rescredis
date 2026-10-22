// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
package scredis.protocol.requests

import io.circe._
import io.circe.parser._
import scredis.protocol.{Decoder, _}
import scredis.serialization.{UTF8StringReader, UTF8StringWriter, Writer}

object JsonRequests {
  import scredis.serialization.Implicits.{longReader, stringReader}

  object JsonSet extends Command("JSON.SET")
  object JsonSetNX extends Command("JSON.SET")
  object JsonSetXX extends Command("JSON.SET")
  object JsonGet extends Command("JSON.GET")
  object JsonDel extends Command("JSON.DEL")
  object JsonMerge extends Command("JSON.MERGE")
  object JsonToggle extends Command("JSON.TOGGLE")
  object JsonClear extends Command("JSON.CLEAR")
  object JsonType extends Command("JSON.TYPE")

  object JsonMSet extends Command("JSON.MSET")
  object JsonMGet extends Command("JSON.MGET")

  object JsonStrLen extends Command("JSON.STRLEN")
  object JsonStrAppend extends Command("JSON.STRAPPEND")

  object JsonNumIncBy extends Command("JSON.NUMINCRBY")
  object JsonNumMulBy extends Command("JSON.NUMMULTBY")

  object JsonObjKeys extends Command("JSON.OBJKEYS")
  object JsonObjLen extends Command("JSON.OBJLEN")

  object JsonArrAppend extends Command("JSON.ARRAPPEND")
  object JsonArrIndex extends Command("JSON.ARRINDEX")
  object JsonArrInsert extends Command("JSON.ARRINSERT")
  object JsonArrLen extends Command("JSON.ARRLEN")
  object JsonArrPop extends Command("JSON.ARRPOP")
  object JsonArrTrim extends Command("JSON.ARRTRIM")

  object JsonDebug extends Command("JSON.DEBUG")

  trait BooleanDecoder extends Request[Boolean] {
    override def decode: Decoder[Boolean] = {
      case SimpleStringResponse(_)  => true
      case BulkStringResponse(None) => false
    }
  }

  trait LongDecoder extends Request[Long] {
    override def decode: Decoder[Long] = {
      case IntegerResponse(value) => value
    }
  }

  trait JsonDecoder extends Request[Option[Json]] {
    override def decode: Decoder[Option[Json]] = {
      case BulkStringResponse(optBytes) =>
        optBytes.flatMap {
          bytes =>
            parse(UTF8StringReader.read(bytes)) match {
              case Right(json) => Some(json)
              case Left(e) =>
                None
            }
        }
    }
  }

  trait LongArrayDecoder extends Request[List[Long]] {
    override def decode: Decoder[List[Long]] = {
      case arr : ArrayResponse =>
        arr.parsed[Long, List] {
          case IntegerResponse(value) => value
        }
    }
  }

  trait OptLongArrayDecoder extends Request[List[Option[Long]]] {
    override def decode: Decoder[List[Option[Long]]] = {
      case arr : ArrayResponse =>
        arr.parsed[Option[Long], List] {
          case IntegerResponse(value) => Some(value)
          case BulkStringResponse(None) => None
        }
    }
  }

  trait OptStringArrayDecoder extends Request[List[Option[String]]] {
    override def decode: Decoder[List[Option[String]]] = {
      // JSON.TYPE answers nil, not an empty array, for a key that does not exist
      case BulkStringResponse(None) => Nil
      case arr : ArrayResponse =>
        arr.parsed[Option[String], List] {
          case b: BulkStringResponse => b.flattenedOpt[String]
        }
    }
  }

  trait OptJsonArrayDecoder extends Request[List[Option[Json]]] {
    override def decode: Decoder[List[Option[Json]]] = {
      case arr : ArrayResponse =>
        arr.parsed[Option[Json], List] {
          case BulkStringResponse(optBytes) =>
            optBytes.flatMap {
              bytes =>
                parse(UTF8StringReader.read(bytes)) match {
                  case Right(json) => Some(json)
                  case Left(e) =>
                    None
                }
            }
        }
    }
  }

  case class JsonSet[W](key:String, path:String, value:W, option:Option[String])(implicit writer: Writer[W]) extends Request[Boolean](
    JsonSet, (List(key, path, writer.write(value)) ++ option.map(List(_)).getOrElse(Nil)):_*
  ) with Key with BooleanDecoder

  case class JsonGet(key:String, paths:String*) extends Request[Option[Json]](
    JsonGet, key +: paths:_*
  ) with Key with JsonDecoder

  case class JsonMerge[W](key:String, path:String, value:W)(implicit writer: Writer[W]) extends Request[Boolean](
    JsonMerge, key, path, writer.write(value)
  ) with Key with BooleanDecoder

  case class JsonDel(key:String, path:String) extends Request[Long](
    JsonDel, key, path
  ) with Key with LongDecoder

  case class JsonToggle(key:String, path:String) extends Request[List[Option[Long]]](
      JsonToggle, key, path
    ) with Key with OptLongArrayDecoder

  case class JsonClear(key:String, path:String) extends Request[Long](
    JsonClear, key, path
  ) with Key with LongDecoder

  case class JsonType(key:String, path:String) extends Request[List[Option[String]]](
    JsonType, key, path
  ) with Key with OptStringArrayDecoder


  case class JsonStrLen(key:String, path:String) extends Request[List[Option[Long]]](
    JsonStrLen, key, path
  ) with Key with OptLongArrayDecoder

  case class JsonStrAppend(key:String, path:String, value:String) extends Request[List[Option[Long]]](
    JsonStrAppend, key, path, value
  ) with Key with OptLongArrayDecoder


  case class JsonNumIncBy[W](key:String, path:String, value:W)(implicit writer: Writer[W]) extends Request[Option[Json]](
    JsonNumIncBy, key, path, writer.write(value)
  ) with Key with JsonDecoder

  case class JsonNumMulBy[W](key:String, path:String, value:W)(implicit writer: Writer[W]) extends Request[Option[Json]](
    JsonNumMulBy, key, path, writer.write(value)
  ) with Key with JsonDecoder


  case class JsonMGet(keys:Seq[String], path:String) extends Request[List[Option[Json]]](
    JsonMGet, (keys ++ List(path)).map(UTF8StringWriter.write):_*
  ) with SameSlotKeys with OptJsonArrayDecoder {
    // in a cluster the request goes to the node of the first key, so all keys must be in one slot; the server
    // does not check that (it knows only the first key), RedisCluster does
    override val key: String = keys.head
  }

  case class JsonMSet[W](keyPathValue:(String, String, W)*)(implicit writer: Writer[W]) extends Request[Boolean](
    JsonMSet, keyPathValue.flatMap(kpv => List[Any](kpv._1, kpv._2, writer.write(kpv._3))):_*
  ) with Key with BooleanDecoder {
    // like MSET: in a cluster the request goes to the node of the first key, so all keys must be in one slot
    override val key: String = keyPathValue.head._1
  }


  case class JsonObjLen(key:String, path:String) extends Request[List[Option[Long]]](
    JsonObjLen, key, path
  ) with Key with OptLongArrayDecoder

  case class JsonObjKeys(key:String, path:String) extends Request[List[Option[List[String]]]](
    JsonObjKeys, key, path
  ) with Key {
    override def decode: Decoder[List[Option[List[String]]]] = {
      case arr : ArrayResponse =>
        arr.parsed[Option[List[String]], List] {
          case keys : ArrayResponse =>
            Some(keys.parsed[String, List] {
              case b : BulkStringResponse => b.flattened[String]
            })
          case BulkStringResponse(None) => None
        }
    }
  }

  
  case class JsonArrAppend[W](key:String, path:String, values:W*)(implicit writer: Writer[W]) extends Request[List[Option[Long]]](
    JsonArrAppend, key +: path +: values.map(v => writer.write(v)):_*
  ) with Key with OptLongArrayDecoder

  case class JsonArrIndex[W](key:String, path:String, value:W, start:Int, stop:Int)(implicit writer: Writer[W]) extends Request[List[Option[Long]]](
    JsonArrIndex, key, path, writer.write(value), start, stop
  ) with Key with OptLongArrayDecoder

  case class JsonArrInsert[W](key:String, path:String, index:Int, values:W*)(implicit writer: Writer[W]) extends Request[List[Option[Long]]](
    JsonArrInsert, key +: path +: index +: values.map(v => writer.write(v)):_*
  ) with Key with OptLongArrayDecoder

  case class JsonArrLen(key:String, path:String) extends Request[List[Option[Long]]](
    JsonArrLen, key, path
  ) with Key with OptLongArrayDecoder

  case class JsonArrPop(key:String, path:String, index:Int) extends Request[List[Option[Json]]](
    JsonArrPop, key, path, index
  ) with Key with OptJsonArrayDecoder

  case class JsonArrTrim(key:String, path:String, start:Int, stop:Int) extends Request[List[Option[Long]]](
    JsonArrTrim, key, path, start, stop
  ) with Key with OptLongArrayDecoder

  case class JsonDebugMemory(key:String, path:String) extends Request[List[Long]](
    JsonDebug, "MEMORY", key, path
  ) with Key with LongArrayDecoder
}
