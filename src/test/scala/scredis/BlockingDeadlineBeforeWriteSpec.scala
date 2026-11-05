// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
package scredis

import java.util.concurrent.TimeoutException

import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec
import scredis.ConnectionState.{Connected, Reconnecting}
import scredis.exceptions._
import scredis.util.RawRedis

import scala.util.{Failure, Success, Try}

/**
 * The deadline of a blocking call (the block time + 2 s) passing before its request is written, on 6379. The
 * connection of a named client is killed, and in the same transaction `CLIENT PAUSE … ALL` holds back every command:
 * the client reconnects at once, and its `CLIENT SETNAME` is answered only when the pause ends, so a call made
 * meanwhile waits in the listener's queue. Whether Redis executed a BLPOP is read from `INFO commandstats` (the
 * calls of BLPOP), and from the element pushed before the pause.
 */
class BlockingDeadlineBeforeWriteSpec extends AnyWordSpec with Matchers with ScalaFutures with Eventually {

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(20, Seconds), interval = Span(20, Millis))

  private val L = "deadline-before-write-spec:list"
  private val Other = "deadline-before-write-spec:other"

  private def timed[A](f: => A): (A, Long) = {
    val start = System.nanoTime()
    val a = f
    (a, (System.nanoTime() - start) / 1000000L)
  }

  private def raw[A](f: RawRedis => A): A = RawRedis.withConnection("127.0.0.1", 6379)(f)

  /** The ids of the connections named `name` */
  private def ids(name: String): List[String] =
    raw(_.string("CLIENT", "LIST")).split("\n").toList.filter(_.contains(s" name=$name ")).flatMap { line =>
      line.split(" ").find(_.startsWith("id=")).map(_.stripPrefix("id="))
    }

  /** The id of the connection named `name` whose last command was BLPOP: the blocking one */
  private def blockingId(name: String): String = {
    val lines = raw(_.string("CLIENT", "LIST")).split("\n").toList
      .filter(line => line.contains(s" name=$name ") && line.contains(" cmd=blpop "))
    lines should have size 1
    lines.head.split(" ").find(_.startsWith("id=")).get.stripPrefix("id=")
  }

  /** The age in seconds of the connection `id` */
  private def ageOf(id: String): Long =
    raw(_.string("CLIENT", "LIST")).split("\n").find(_.startsWith(s"id=$id ")).get.split(" ")
      .find(_.startsWith("age=")).get.stripPrefix("age=").toLong

  /** The number of BLPOPs Redis has executed */
  private def blpopCalls(): Long =
    raw(_.string("INFO", "commandstats")).split("\r\n").find(_.startsWith("cmdstat_blpop:")) match {
      case Some(line) => line.split("[:,]").find(_.startsWith("calls=")).get.stripPrefix("calls=").toLong
      case None => 0L
    }

  /** Pushes `value` (if any), then kills the connection `id` and pauses every command for `ms`, in one transaction */
  private def pushKillPause(value: Option[String], id: String, ms: Int): Unit = raw { r =>
    r.command("DEL", L)
    value.foreach(v => r.command("RPUSH", L, v))
    r.command("MULTI")
    r.command("CLIENT", "KILL", "ID", id)
    r.command("CLIENT", "PAUSE", ms.toString, "ALL")
    r.command("EXEC") match {
      case List(1L, "OK") =>
      case other => fail(s"MULTI CLIENT KILL / CLIENT PAUSE: $other")
    }
  }

  "Redis.blockingClient" should {
    "fail a call whose deadline passes while it waits for the connection as not sent, take it out of the queue, and make the next call on the same connection" in {
      val name = "deadline-before-write-spec-queued"
      val redis = Redis(port = 6379, nameOpt = Some(name))
      try {
        raw(_.command("DEL", L))
        redis.blockingClient.blPop[String](1, L) shouldBe Success(None)
        val before = blockingId(name)
        val calls = blpopCalls()
        val killed = System.nanoTime()
        pushKillPause(Some("kept"), before, 6000)
        eventually { redis.blockingClient.health.state shouldBe a [Reconnecting] }
        val (result, ms) = timed(redis.blockingClient.blPop[String](1, L))
        info(s"the call returned after $ms ms: $result")
        val e = result match {
          case Failure(e: RedisNotSentException) => e
          case other => fail(s"expected a RedisNotSentException, got $other")
        }
        e.getCause shouldBe a [TimeoutException]
        ms should be >= 2900L
        ms should be < 5000L
        e.availability.isAvailable shouldBe false
        // the pause ends: the client's connection is initialised, and the request it had queued is not written
        eventually { e.availability.isAvailable shouldBe true }
        redis.blockingClient.health.state shouldBe Connected
        raw(_.command("LLEN", L)) shouldBe 1L
        blpopCalls() shouldBe calls
        val (next, ms2) = timed(redis.blockingClient.blPop[String](5, L))
        info(s"the next call returned $next after $ms2 ms")
        next shouldBe Success(Some((L, "kept")))
        blpopCalls() shouldBe calls + 1
        // the next call is made on the connection the timed-out call waited for: the first one made after the kill (the
        // client reconnects at once; the connection of the `Redis` itself may be replaced later, its idle PING
        // unanswered during the pause), as old as the time since the kill
        val after = blockingId(name)
        val sinceKill = (System.nanoTime() - killed) / 1000000000L
        val age = ageOf(after)
        info(s"blocking connection id $before before the kill, $after after it (age $age s, $sinceKill s since the kill)")
        after should not be before
        after.toLong shouldBe ids(name).map(_.toLong).filter(_ > before.toLong).min
        age should be >= (sinceKill - 1)
        raw(_.command("LLEN", L)) shouldBe 0L
      } finally {
        Try(raw(_.command("CLIENT", "UNPAUSE")))
        Try(redis.quit().futureValue)
      }
    }

    "end a call whose deadline passes about when it is written in exactly one of not sent (never executed) and outcome unknown" in {
      val name = "deadline-before-write-spec-tie"
      val redis = Redis(port = 6379, nameOpt = Some(name))
      // the pause ends (and the queued call is written) from before to after the call's deadline (3 s); the list is
      // empty, so a call written blocks in Redis past its deadline
      val pauses = (2800 to 3200 by 20).toList
      try {
        raw(_.command("DEL", Other))
        redis.blockingClient.blPop[String](1, Other) shouldBe Success(None)
        val outcomes = pauses.map { pause =>
          // the blocking connection: the last command of the one before was the BLPOP made after the previous run
          val id = eventually {
            redis.blockingClient.health.state shouldBe Connected
            blockingId(name)
          }
          val calls = blpopCalls()
          pushKillPause(None, id, pause)
          eventually { redis.blockingClient.health.state shouldBe a [Reconnecting] }
          val (result, ms) = timed(redis.blockingClient.blPop[String](1, L))
          // the connection the next call is made on is initialised: a call written meanwhile was written before it
          eventually { redis.blockingClient.health.state shouldBe Connected }
          redis.blockingClient.blPop[String](1, Other) shouldBe Success(None)
          val executed = blpopCalls() - calls - 1
          val outcome = result match {
            case Failure(e: RedisNotSentException) if e.getCause.isInstanceOf[TimeoutException] =>
              // not sent: never executed, then or later
              executed shouldBe 0L
              "not sent"
            case Failure(e: RedisOutcomeUnknownException) if e.getCause.isInstanceOf[TimeoutException] =>
              "outcome unknown"
            case other => fail(s"pause $pause ms: expected not sent or outcome unknown, got $other")
          }
          ms should be < 8000L
          info(s"pause $pause ms: $outcome after $ms ms (BLPOP executed $executed time(s) besides the next call)")
          outcome
        }
        val notSent = outcomes.count(_ == "not sent")
        val unknown = outcomes.count(_ == "outcome unknown")
        info(s"${pauses.size} runs: $notSent not sent, $unknown outcome unknown")
        (notSent + unknown) shouldBe pauses.size
      } finally {
        Try(raw(_.command("CLIENT", "UNPAUSE")))
        Try(redis.quit().futureValue)
      }
    }
  }

  "The deprecated Redis.blocking" should {
    "leave a call whose deadline passes while it waits for the connection in the queue, as in 1.0.0: it is written and executed later" in {
      val name = "deadline-before-write-spec-deprecated"
      val redis = Redis(port = 6379, nameOpt = Some(name))
      try {
        raw(_.command("DEL", L))
        redis.blocking.blPop[String](1, L) shouldBe Success(None)
        val before = blockingId(name)
        val calls = blpopCalls()
        pushKillPause(Some("taken"), before, 6000)
        eventually { redis.blocking.health.state shouldBe a [Reconnecting] }
        val (result, ms) = timed(redis.blocking.blPop[String](1, L))
        info(s"the call returned after $ms ms: $result")
        result match {
          case Failure(e) => e.getClass shouldBe classOf[TimeoutException]
          case other => fail(s"expected a TimeoutException, got $other")
        }
        ms should be >= 2900L
        // the pause ends: the request it had queued is written and executed; its reply reaches nobody
        eventually { blpopCalls() shouldBe calls + 1 }
        eventually { raw(_.command("LLEN", L)) shouldBe 0L }
        redis.blocking.health.state shouldBe Connected
      } finally {
        Try(raw(_.command("CLIENT", "UNPAUSE")))
        Try(redis.quit().futureValue)
      }
    }
  }
}
