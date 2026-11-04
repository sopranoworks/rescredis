// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
package scredis.commands

import scredis.io.{Connection, NonBlockingConnection}
import scredis.protocol.requests.StreamRequests._
import scredis.serialization.{Reader, Writer}
import scredis.{StreamEntry, StreamTrim}

import scala.concurrent.Future

/**
 * This trait implements stream commands (the non-blocking ones; XREAD with BLOCK is in
 * [[scredis.commands.BlockingStreamCommands]]). Consumer groups are not implemented yet.
 *
 * Entry ids are `String`s, sent and returned as Redis writes them: an id argument can be any form Redis
 * accepts for the command (`*`, `<ms>-*`, `<ms>-<seq>`, `<ms>`, `-`, `+`, `(<id>`, `$`, ...).
 *
 * @define e [[scredis.exceptions.RedisErrorResponseException]]
 * @define none `None`
 */
trait StreamCommands { self: Connection with NonBlockingConnection =>

  /**
   * Appends an entry to a stream (XADD).
   *
   * `XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold [LIMIT count]] *|id field value [field value ...]`
   *
   * @param key stream key
   * @param fields the field-value pairs of the entry
   * @param id the entry id: `*` (Redis generates it, the default), `<ms>-*` (since 7.0.0) or `<ms>-<seq>`
   * @param noMkStream `NOMKSTREAM` (since 6.2.0): do not create the stream if `key` does not exist
   * @param trim the trimming of the stream after the entry is added (`MINID` and `LIMIT` since 6.2.0)
   * @return the id of the added entry, or $none if `noMkStream` is set and the key does not exist
   * @throws $e if key contains a value that is not a stream, or the id is not greater than the stream's last id
   *
   * @since 5.0.0
   */
  def xAdd[W: Writer](
    key: String,
    fields: Seq[(String, W)],
    id: String = "*",
    noMkStream: Boolean = false,
    trim: Option[StreamTrim] = None
  ): Future[Option[String]] = send(XAdd(key, id, fields, noMkStream, trim))

  /**
   * Removes entries from a stream (XDEL).
   *
   * `XDEL key id [id ...]`
   *
   * @param key stream key
   * @param ids the ids of the entries to remove
   * @return the number of entries removed
   * @throws $e if key contains a value that is not a stream
   *
   * @since 5.0.0
   */
  def xDel(key: String, ids: String*): Future[Long] = send(XDel(key, ids: _*))

  /**
   * Returns the number of entries of a stream (XLEN).
   *
   * `XLEN key`
   *
   * @param key stream key
   * @return the number of entries, 0 if the key does not exist
   * @throws $e if key contains a value that is not a stream
   *
   * @since 5.0.0
   */
  def xLen(key: String): Future[Long] = send(XLen(key))

  /**
   * Returns the entries of a stream with ids from `start` to `end`, in increasing order (XRANGE).
   *
   * `XRANGE key start end [COUNT count]`
   *
   * @param key stream key
   * @param start the lowest id: `-` (the first entry), an id, a millisecond time, or `(id` for an exclusive
   * bound (since 6.2.0)
   * @param end the highest id: `+` (the last entry), an id, a millisecond time, or `(id`
   * @param count `COUNT`: at most this number of entries
   * @return the entries
   * @throws $e if key contains a value that is not a stream
   *
   * @since 5.0.0
   */
  def xRange[R: Reader](
    key: String, start: String = "-", end: String = "+", count: Option[Long] = None
  ): Future[List[StreamEntry[R]]] = send(XRange[R](key, start, end, count))

  /**
   * Returns the entries of a stream with ids from `end` down to `start`, in decreasing order (XREVRANGE).
   *
   * `XREVRANGE key end start [COUNT count]`
   *
   * @param key stream key
   * @param end the highest id: `+` (the last entry), an id, a millisecond time, or `(id` (since 6.2.0)
   * @param start the lowest id: `-` (the first entry), an id, a millisecond time, or `(id`
   * @param count `COUNT`: at most this number of entries
   * @return the entries, the highest id first
   * @throws $e if key contains a value that is not a stream
   *
   * @since 5.0.0
   */
  def xRevRange[R: Reader](
    key: String, end: String = "+", start: String = "-", count: Option[Long] = None
  ): Future[List[StreamEntry[R]]] = send(XRevRange[R](key, end, start, count))

  /**
   * Reads the entries after the given ids from one or more streams, without blocking (XREAD).
   *
   * `XREAD [COUNT count] STREAMS key [key ...] id [id ...]`
   *
   * On a cluster, the request goes to the node of the first key; keys of several slots are sent as they are
   * and Redis answers `CROSSSLOT`.
   *
   * @param streams the stream keys, each with the id after which to read: an id, `0`, `$` (the stream's last
   * id: nothing is returned without blocking) or `+` (the last entry, since 7.4.0)
   * @param count `COUNT`: at most this number of entries per stream
   * @return per stream with entries, its key and entries, or $none if no stream has entries after its id
   * @throws $e if a key contains a value that is not a stream
   *
   * @since 5.0.0
   */
  def xRead[R: Reader](
    streams: Seq[(String, String)], count: Option[Long] = None
  ): Future[Option[List[(String, List[StreamEntry[R]])]]] = send(XRead[R](count, None, streams))

  /**
   * Trims a stream (XTRIM).
   *
   * `XTRIM key MAXLEN|MINID [=|~] threshold [LIMIT count]`
   *
   * @param key stream key
   * @param trim the trimming (`MINID` and `LIMIT` since 6.2.0)
   * @return the number of entries removed
   * @throws $e if key contains a value that is not a stream
   *
   * @since 5.0.0
   */
  def xTrim(key: String, trim: StreamTrim): Future[Long] = send(XTrim(key, trim))

}
