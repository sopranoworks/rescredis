// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
package scredis.util

/**
 * Scripts that keep a Redis server busy for a while, with a loop count calibrated on the server (inside a script
 * Redis 7 keeps the time of the script's start, so a loop cannot wait on TIME).
 */
object Spin {
  /** Loops ARGV[1] times */
  val Script = "for i = 1, tonumber(ARGV[1]) do end return 1"

  /** Increments KEYS[1], then loops ARGV[1] times: a write whose reply comes late */
  val IncrThenSpin = "redis.call('INCR', KEYS[1]) for i = 1, tonumber(ARGV[1]) do end return 1"

  /** The loop count that keeps the server at `port` busy for about `millis` ms */
  def iterationsFor(port: Int, millis: Long): Long = {
    val n = 20000000L
    val start = System.nanoTime()
    RawRedis.withConnection("127.0.0.1", port)(_.command("EVAL", Script, "0", n.toString))
    val ms = math.max(1L, (System.nanoTime() - start) / 1000000L)
    math.max(1L, n * millis / ms)
  }
}
