// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
package scredis.protocol.requests

import scredis.exceptions.RedisProtocolException
import scredis.protocol._
import scredis.serialization.{Reader, Writer}
import scredis.{StreamEntry, StreamTrim}

import scala.collection.mutable.ListBuffer

object StreamRequests {

  import scredis.serialization.Implicits.stringReader

  object XAdd extends Command("XADD") with WriteCommand
  object XDel extends Command("XDEL") with WriteCommand
  object XLen extends Command("XLEN")
  object XRange extends Command("XRANGE")
  object XRead extends Command("XREAD")
  object XRevRange extends Command("XREVRANGE")
  object XTrim extends Command("XTRIM") with WriteCommand

  /** An entry: `[id, [field, value, ...]]`. A nil field list (an entry deleted under a consumer group) is empty. */
  private def entryDecoder[R: Reader]: Decoder[StreamEntry[R]] = {
    case a: ArrayResponse if a.length == 2 =>
      val id = Protocol.decode(a.buffer) match {
        case b: BulkStringResponse => b.flattened[String]
        case x => throw RedisProtocolException(s"Unexpected stream entry id: $x")
      }
      val fields = Protocol.decode(a.buffer) match {
        case f: ArrayResponse => f.parsedAsPairs[String, R, List] {
          case b: BulkStringResponse => b.flattened[String]
        } {
          case b: BulkStringResponse => b.flattened[R]
        }
        case x => throw RedisProtocolException(s"Unexpected stream entry fields: $x")
      }
      StreamEntry(id, fields)
  }

  private def entriesDecoder[R: Reader]: Decoder[List[StreamEntry[R]]] = {
    case a: ArrayResponse => a.parsed[StreamEntry[R], List](entryDecoder[R])
  }

  /** The reply of XREAD: nil, or `[[key, entries], ...]` */
  private def streamsDecoder[R: Reader]: Decoder[Option[List[(String, List[StreamEntry[R]])]]] = {
    case ArrayResponse(-1, _) => None
    case a: ArrayResponse => Some(a.parsed[(String, List[StreamEntry[R]]), List] {
      case s: ArrayResponse if s.length == 2 =>
        val key = Protocol.decode(s.buffer) match {
          case b: BulkStringResponse => b.flattened[String]
          case x => throw RedisProtocolException(s"Unexpected stream key: $x")
        }
        val entries = Protocol.decode(s.buffer) match {
          case e: ArrayResponse => entriesDecoder[R].apply(e)
          case x => throw RedisProtocolException(s"Unexpected stream entries: $x")
        }
        (key, entries)
    })
  }

  private[scredis] def xReadArgs(count: Option[Long], blockMillis: Option[Long], streams: Seq[(String, String)]): List[Any] = {
    val args = ListBuffer[Any]()
    count.foreach(c => args += "COUNT" += c)
    blockMillis.foreach(b => args += "BLOCK" += b)
    args += "STREAMS"
    args ++= streams.map(_._1)
    args ++= streams.map(_._2)
    args.toList
  }

  case class XAdd[W](
    key: String, id: String, fields: Seq[(String, W)], noMkStream: Boolean, trim: Option[StreamTrim]
  )(implicit writer: Writer[W]) extends Request[Option[String]](
    XAdd,
    (List(key) ++ (if (noMkStream) List("NOMKSTREAM") else Nil) ++ trim.toList.flatMap(_.args) ++ List(id) ++
      unpair(fields.map { case (field, value) => (field, writer.write(value)) })): _*
  ) with Key {
    override def decode = {
      case b: BulkStringResponse => b.parsed[String]
    }
  }

  case class XDel(key: String, ids: String*) extends Request[Long](XDel, key +: ids: _*) with Key {
    override def decode = {
      case IntegerResponse(x) => x
    }
  }

  case class XLen(key: String) extends Request[Long](XLen, key) with Key {
    override def decode = {
      case IntegerResponse(x) => x
    }
  }

  case class XRange[R: Reader](key: String, start: String, end: String, count: Option[Long])
    extends Request[List[StreamEntry[R]]](
      XRange, (List(key, start, end) ++ count.toList.flatMap(c => List("COUNT", c))): _*
    ) with Key {
    override def decode = entriesDecoder[R]
  }

  case class XRevRange[R: Reader](key: String, end: String, start: String, count: Option[Long])
    extends Request[List[StreamEntry[R]]](
      XRevRange, (List(key, end, start) ++ count.toList.flatMap(c => List("COUNT", c))): _*
    ) with Key {
    override def decode = entriesDecoder[R]
  }

  /**
   * XREAD; with `blockMillis` it blocks (BLOCK). Routed by its first key; keys of several slots go to Redis as
   * they are (Redis answers CROSSSLOT).
   */
  case class XRead[R: Reader](count: Option[Long], blockMillis: Option[Long], streams: Seq[(String, String)])
    extends Request[Option[List[(String, List[StreamEntry[R]])]]](XRead, xReadArgs(count, blockMillis, streams): _*)
      with Key {
    override def decode = streamsDecoder[R]
    override val key: String = streams.head._1
  }

  case class XTrim(key: String, trim: StreamTrim) extends Request[Long](XTrim, (key :: trim.args): _*) with Key {
    override def decode = {
      case IntegerResponse(x) => x
    }
  }

}
