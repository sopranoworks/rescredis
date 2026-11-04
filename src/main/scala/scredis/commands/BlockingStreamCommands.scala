// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
package scredis.commands

import scredis.StreamEntry
import scredis.io.BlockingConnection
import scredis.protocol.requests.StreamRequests._
import scredis.serialization.Reader

import scala.concurrent.duration._
import scala.util.Try

/**
 * This trait implements the blocking stream command, XREAD with BLOCK, as
 * [[scredis.commands.BlockingListCommands]] implements the blocking list commands: it is sent on the
 * connection of a [[scredis.BlockingClient]], whose caller waits for the reply.
 *
 * @define e [[scredis.exceptions.RedisErrorResponseException]]
 * @define none `None`
 */
trait BlockingStreamCommands { self: BlockingConnection =>

  /** The caller waits 2 s longer than Redis blocks, or without limit for `BLOCK 0` (as BlockingListCommands) */
  private def blockToDuration(blockMillis: Long): Duration = if (blockMillis <= 0) {
    Duration.Inf
  } else {
    blockMillis.millis + 2.seconds
  }

  /**
   * Reads the entries after the given ids from one or more streams, blocking until one has an entry or the
   * time is over (XREAD with BLOCK).
   *
   * `XREAD [COUNT count] BLOCK milliseconds STREAMS key [key ...] id [id ...]`
   *
   * @param blockMillis `BLOCK`: the time to block in milliseconds; 0 blocks without limit
   * @param streams the stream keys, each with the id after which to read: an id, `0`, `$` (only entries added
   * after the command) or `+` (the last entry, since 7.4.0)
   * @param count `COUNT`: at most this number of entries per stream
   * @return per stream with entries, its key and entries, or $none if the time is over
   * @throws $e if a key contains a value that is not a stream
   *
   * @since 5.0.0
   */
  def xRead[R: Reader](
    blockMillis: Long, streams: Seq[(String, String)], count: Option[Long] = None
  ): Try[Option[List[(String, List[StreamEntry[R]])]]] = sendBlocking(
    XRead[R](count, Some(blockMillis), streams)
  )(blockToDuration(blockMillis))

}
