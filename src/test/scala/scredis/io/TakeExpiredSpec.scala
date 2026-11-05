// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
package scredis.io

import java.util

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scredis.Transaction
import scredis.protocol.Request
import scredis.protocol.requests.StringRequests.Get
import scredis.serialization.Implicits.stringReader

import scala.jdk.CollectionConverters._

/**
 * The rule by which the queue timeout takes requests out of a queue of requests not written yet (the listener's,
 * the IOActor's): those that waited since before the cutoff; a transaction whole, and only when its MULTI is in the
 * queue too. And how they are then removed from the requests the listener sent, from the end.
 */
class TakeExpiredSpec extends AnyWordSpec with Matchers {

  private val Multi = Transaction.MultiRequest

  private def get(key: String, since: Long = Request.NotWaiting): Get[String] = {
    val r = Get[String](key)
    r.waitingSince = since
    r
  }

  /** A transaction of `keys` whose EXEC has waited since `since`: MULTI, the GETs, EXEC */
  private def transaction(since: Long, keys: String*): List[Request[_]] = {
    val t = Transaction(keys.map(k => Get[String](k)))
    t.execRequest.waitingSince = since
    (Multi :: t.requests.toList) :+ t.execRequest
  }

  private def list(requests: Request[_]*): util.LinkedList[Request[_]] = new util.LinkedList[Request[_]](requests.asJava)

  "takeExpired" should {
    "take out the requests that waited since before the cutoff, in order, and keep the others in order" in {
      val (a, b, c, d) = (get("a", 10), get("b", 30), get("c", 15), get("d"))
      val queue = list(a, b, c, d)
      ListenerActor.takeExpired(queue, 20) shouldBe List(a, c)
      queue.asScala.toList shouldBe List(b, d)
    }
    "take nothing when nothing waited that long" in {
      val (a, b) = (get("a", 30), get("b"))
      val queue = list(a, b)
      ListenerActor.takeExpired(queue, 20) shouldBe Nil
      queue.asScala.toList shouldBe List(a, b)
    }
    "take a transaction out whole when its MULTI is in the queue" in {
      val x = get("x", 30)
      val t = transaction(10, "t1", "t2")
      val queue = list((x :: t): _*)
      ListenerActor.takeExpired(queue, 20) shouldBe t
      queue.asScala.toList shouldBe List(x)
    }
    "leave a transaction whose MULTI has been written (only its rest is in the queue)" in {
      val t = transaction(10, "t1", "t2")
      val y = get("y", 5)
      val rest = t.tail :+ y
      val queue = list(rest: _*)
      ListenerActor.takeExpired(queue, 20) shouldBe List(y)
      queue.asScala.toList shouldBe t.tail
    }
    "take one of two transactions" in {
      val t1 = transaction(30, "a1")
      val t2 = transaction(10, "b1", "b2")
      val queue = list((t1 ++ t2): _*)
      ListenerActor.takeExpired(queue, 20) shouldBe t2
      queue.asScala.toList shouldBe t1
    }
  }

  "removeFromEnd" should {
    "remove the requests taken from the end of the requests sent, MULTI where it belongs" in {
      val written = transaction(Request.NotWaiting, "w1")
      val kept = transaction(30, "k1")
      val z = get("z", 10)
      val taken = transaction(10, "t1")
      val sent = list((written ++ List(z) ++ kept ++ taken): _*)
      ListenerActor.removeFromEnd(sent, z :: taken)
      sent.asScala.toList.map(System.identityHashCode) shouldBe (written ++ kept).map(System.identityHashCode)
    }
  }
}
