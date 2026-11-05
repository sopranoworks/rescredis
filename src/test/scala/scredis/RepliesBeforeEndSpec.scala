// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
package scredis

import akka.actor.ActorSystem
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec
import scredis.ConnectionState.Connected
import scredis.util.ScriptedRedis

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Success, Try}

/**
 * Replies that are in the last data read from a connection that then ends (closed by the server, reset, closed after
 * QUIT) reach their requests: the end of the connection is handled only once they have. Against [[ScriptedRedis]],
 * which writes many replies in one write and ends the connection right after it, so that the end arrives while the
 * replies are still being decoded; every case is run many times and every lost reply is listed.
 */
class RepliesBeforeEndSpec extends AnyWordSpec with Matchers with ScalaFutures with Eventually with BeforeAndAfterAll {

  private implicit val system: ActorSystem = ActorSystem("replies-before-end-spec")
  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(20, Seconds), interval = Span(10, Millis))

  override def afterAll(): Unit = system.terminate()

  /** Replies in the last write before the end */
  private val Batch = 200

  private def result[A](future: Future[A]): Try[A] = Try(Await.result(future, 10.seconds))

  /** The GETs of a batch and of `last`, each answered with its key: those that were not, with what they got */
  private def lost(futures: Seq[(String, Future[Option[String]])]): Seq[String] = futures.flatMap { case (key, future) =>
    result(future) match {
      case Success(Some(`key`)) => None
      case other => Some(s"$key: $other")
    }
  }

  private def report(rounds: Int, failures: Seq[(Int, Seq[String])]): Unit = {
    info(s"$rounds rounds, ${failures.size} with a lost reply (${failures.map(_._2.size).sum} replies)")
    failures.take(5).foreach { case (round, l) => info(s"round $round: ${l.size} lost, e.g. ${l.head}") }
  }

  /** Rounds on one client: a batch of GETs whose replies come in one write, then the connection ends (`end`) */
  private def endAfterBatch(end: String, rounds: Int): Unit = {
    val server = new ScriptedRedis()
    val client = Client(port = server.port)
    try {
      val failures = (1 to rounds).flatMap { round =>
        // the end of the previous round's connection has been handled: this round's is a new one
        eventually {
          server.accepted should be >= round
          client.health.state shouldBe Connected
        }
        val keys = (1 to Batch).map(i => s"batch:$round-$i") :+ s"$end:$round"
        val l = lost(keys.map(key => key -> client.get[String](key)))
        if (l.isEmpty) None else Some(round -> l)
      }
      report(rounds, failures)
      eventually { server.accepted should be > rounds }
      failures.map(_._1) shouldBe empty
    } finally {
      Try(client.quit().futureValue)
      server.stop()
    }
  }

  "The replies in the last data of a connection" when {
    "the server closes the connection after them" should {
      "all reach their requests (300 rounds of 201 replies)" in endAfterBatch("end-fin", 300)
    }
    "the server resets the connection 200 ms after them" should {
      "all reach their requests (100 rounds of 201 replies)" in endAfterBatch("end-rst", 100)
    }
    "the server closes the connection after QUIT" should {
      "all reach their requests, and quit() completes (200 clients, 200 replies and +OK)" in {
        val server = new ScriptedRedis()
        try {
          val rounds = 200
          val failures = (1 to rounds).flatMap { round =>
            val client = Client(port = server.port)
            eventually { client.health.state shouldBe Connected }
            val gets = (1 to Batch).map(i => s"batch:$round-$i").map(key => key -> client.get[String](key))
            val quit = client.quit()
            val l = lost(gets) ++ (result(quit) match {
              case Success(()) => Nil
              case other => List(s"QUIT: $other")
            })
            if (l.isEmpty) None else Some(round -> l)
          }
          report(rounds, failures)
          failures.map(_._1) shouldBe empty
        } finally server.stop()
      }
    }
  }

  "The replies in the last data of a subscriber connection" when {
    "the server closes the connection after them" should {
      "all reach the SUBSCRIBE (200 rounds of a SUBSCRIBE of 100 channels)" in {
        val server = new ScriptedRedis()
        val subscriber = SubscriberClient(_ => (), port = server.port)
        try {
          val rounds = 200
          val failures = (1 to rounds).flatMap { round =>
            eventually {
              server.accepted should be >= round
              subscriber.health.state shouldBe Connected
            }
            val channels = (1 until 100).map(i => s"sub:$round-$i") :+ s"end-fin:$round"
            result(subscriber.subscribe(channels: _*)) match {
              case Success(_) => None
              case other => Some(round -> List(s"SUBSCRIBE: $other"))
            }
          }
          report(rounds, failures)
          eventually { server.accepted should be > rounds }
          failures.map(_._1) shouldBe empty
        } finally {
          Try(subscriber.quit().futureValue)
          server.stop()
        }
      }
    }
    "the server closes the connection after QUIT" should {
      "complete quit() (200 subscribers, each subscribed to a channel)" in {
        val server = new ScriptedRedis()
        try {
          val rounds = 200
          val failures = (1 to rounds).flatMap { round =>
            val subscriber = SubscriberClient(_ => (), port = server.port)
            eventually { subscriber.health.state shouldBe Connected }
            val l = (result(subscriber.subscribe(s"quit:$round")) match {
              case Success(1) => Nil
              case other => List(s"SUBSCRIBE: $other")
            }) ++ (result(subscriber.quit()) match {
              case Success(()) => Nil
              case other => List(s"QUIT: $other")
            })
            if (l.isEmpty) None else Some(round -> l)
          }
          report(rounds, failures)
          failures.map(_._1) shouldBe empty
        } finally server.stop()
      }
    }
  }
}
