// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
package scredis

import java.util.concurrent.TimeoutException

import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec
import scredis.exceptions._
import scredis.util.RawRedis

import scala.util.{Failure, Success, Try}

/**
 * The deadline of a blocking call (the block time + 2 s) on 6379: `Redis.blockingClient` replaces the connection the
 * call was made on, the deprecated `Redis.blocking` keeps it (as 1.0.0). The reply comes late through
 * `CLIENT PAUSE … WRITE`: Redis takes the BLPOP and executes it only when the pause ends, after the client's deadline.
 * An element pushed before the pause is popped by whichever BLPOP Redis executes first.
 */
class BlockingDeadlineSpec extends AnyWordSpec with Matchers with ScalaFutures with Eventually {

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(20, Seconds), interval = Span(20, Millis))

  private val L = "blocking-deadline-spec:list"

  private def timed[A](f: => A): (A, Long) = {
    val start = System.nanoTime()
    val a = f
    (a, (System.nanoTime() - start) / 1000000L)
  }

  private def failureOf(f: => Try[_]): Throwable = Try(f).flatten match {
    case Failure(e) => e
    case Success(v) => fail(s"expected a failure, got $v")
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

  /** Pushes `value`, then pauses the write commands (BLPOP among them) for `ms` */
  private def pushThenPause(value: String, ms: Int): Unit = raw { r =>
    r.command("DEL", L)
    r.command("RPUSH", L, value)
    r.command("CLIENT", "PAUSE", ms.toString, "WRITE")
  }

  private def unpause(): Unit = Try(raw(_.command("CLIENT", "UNPAUSE")))

  "Redis.blockingClient" should {
    "replace its connection when a call's deadline passes: the next call is made on a new connection, and the late reply reaches no caller" in {
      val name = "blocking-deadline-spec-new"
      val redis = Redis(port = 6379, nameOpt = Some(name))
      try {
        raw(_.command("DEL", L))
        redis.blockingClient.blPop[String](1, L) shouldBe Success(None)
        val before = blockingId(name)
        pushThenPause("late", 4000)
        val (deadline, ms) = timed(failureOf(redis.blockingClient.blPop[String](1, L)))
        info(s"the call failed after $ms ms: $deadline")
        deadline shouldBe a [RedisOutcomeUnknownException]
        deadline.getCause shouldBe a [TimeoutException]
        ms should be >= 2900L
        // the old connection is gone before Redis executes its BLPOP, which pops nothing: the next call pops the
        // element pushed before the pause, on a new connection
        eventually { ids(name) should not contain before }
        val (next, ms2) = timed(redis.blockingClient.blPop[String](5, L))
        info(s"the next call returned $next after $ms2 ms")
        next shouldBe Success(Some((L, "late")))
        val after = blockingId(name)
        info(s"blocking connection id $before before the deadline, $after after it")
        after should not be before
        raw(_.command("LLEN", L)) shouldBe 0L
        redis.blockingClient.blPop[String](1, L) shouldBe Success(None)
        blockingId(name) shouldBe after
      } finally {
        unpause()
        Try(redis.quit().futureValue)
      }
    }
  }

  "The deprecated Redis.blocking" should {
    "keep its connection when a call's deadline passes, as in 1.0.0: the late BLPOP is executed on it" in {
      val name = "blocking-deadline-spec-deprecated"
      val redis = Redis(port = 6379, nameOpt = Some(name))
      try {
        raw(_.command("DEL", L))
        redis.blocking.blPop[String](1, L) shouldBe Success(None)
        val before = blockingId(name)
        pushThenPause("late", 4000)
        val (deadline, ms) = timed(failureOf(redis.blocking.blPop[String](1, L)))
        info(s"the call failed after $ms ms: $deadline")
        deadline.getClass shouldBe classOf[TimeoutException]
        ms should be >= 2900L
        ids(name) should contain (before)
        // the late BLPOP of the timed-out call pops the element when the pause ends; its reply completes that call,
        // which nobody waits for, and the next call, behind it on the same connection, finds the list empty
        val (next, ms2) = timed(redis.blocking.blPop[String](2, L))
        info(s"the next call returned $next after $ms2 ms")
        next shouldBe Success(None)
        blockingId(name) shouldBe before
        raw(_.command("LLEN", L)) shouldBe 0L
      } finally {
        unpause()
        Try(redis.quit().futureValue)
      }
    }
  }
}
