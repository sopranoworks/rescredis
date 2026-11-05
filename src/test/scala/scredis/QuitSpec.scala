// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
package scredis

import java.net.ServerSocket

import akka.actor.ActorSystem
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec
import scredis.ConnectionState.{Closed, Connected}
import scredis.exceptions.RedisNotSentException
import scredis.util.ScriptedRedis

import scala.concurrent.duration._

/**
 * `quit()` completes successfully once the client is closed: QUIT written and its connection ended with or without
 * `+OK` (closed, reset, or closed by the client after the receive timeout), and QUIT not written because there is no
 * connection (none can be made, or the one it waited for could not be initialised). The client is then closed: no
 * new connection is made. Against [[ScriptedRedis]].
 */
class QuitSpec extends AnyWordSpec with Matchers with ScalaFutures with Eventually with BeforeAndAfterAll {

  private implicit val system: ActorSystem = ActorSystem("quit-spec")
  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(10, Seconds), interval = Span(10, Millis))

  override def afterAll(): Unit = system.terminate()

  /** A port nothing listens on */
  private def refusedPort(): Int = {
    val s = new ServerSocket(0)
    try s.getLocalPort finally s.close()
  }

  /** The client stays closed: no new connection to `server` within a second */
  private def staysClosed(server: ScriptedRedis, health: ConnectionHealth, accepted: Int): Unit = {
    health.state shouldBe Closed
    Thread.sleep(1000)
    server.accepted shouldBe accepted
    health.state shouldBe Closed
  }

  "quit()" when {
    Seq("close" -> "closes the connection without +OK", "reset" -> "resets the connection without +OK").foreach {
      case (quit, what) =>
        s"QUIT was written and the server $what" should {
          "complete, and the client is closed" in {
            val server = new ScriptedRedis(quit = quit)
            val client = Client(port = server.port)
            try {
              eventually { client.health.state shouldBe Connected }
              client.quit().futureValue shouldBe (())
              staysClosed(server, client.health, 1)
              client.ping().failed.futureValue shouldBe a [RedisNotSentException]
            } finally server.stop()
          }
        }
    }
    "QUIT was written and is not answered within the receive timeout" should {
      "complete when the client has closed the connection, and the client is closed" in {
        val server = new ScriptedRedis(quit = "silent")
        val client = Client(port = server.port, receiveTimeoutOpt = Some(1.second))
        try {
          eventually { client.health.state shouldBe Connected }
          val start = System.nanoTime()
          client.quit().futureValue shouldBe (())
          val ms = (System.nanoTime() - start) / 1000000
          info(s"quit() completed after $ms ms")
          ms should be >= 900L
          staysClosed(server, client.health, 1)
        } finally server.stop()
      }
    }
    "there is no connection (nothing listens)" should {
      "complete, and the client is closed" in {
        val client = Client(port = refusedPort())
        client.quit().futureValue shouldBe (())
        client.health.state shouldBe Closed
        client.ping().failed.futureValue shouldBe a [RedisNotSentException]
      }
    }
    "QUIT waits for a connection that then cannot be initialised" should {
      "complete without being written, and the client is closed" in {
        val server = new ScriptedRedis(answerSetName = false)
        val client = Client(port = server.port, nameOpt = Some("quit-spec"))
        try {
          eventually { server.accepted shouldBe 1 }
          Thread.sleep(200)
          val quit = client.quit()
          Thread.sleep(200)
          // CLIENT SETNAME is not answered: QUIT waits in the queue
          quit.isCompleted shouldBe false
          server.closeAll()
          quit.futureValue shouldBe (())
          staysClosed(server, client.health, 1)
        } finally server.stop()
      }
    }
  }

  "SubscriberClient.quit()" when {
    "QUIT was written and the server closes the connection without +OK" should {
      "complete, and the subscriber is closed" in {
        val server = new ScriptedRedis(quit = "close")
        val subscriber = SubscriberClient(_ => (), port = server.port)
        try {
          eventually { subscriber.health.state shouldBe Connected }
          subscriber.subscribe("quit-spec").futureValue shouldBe 1
          subscriber.quit().futureValue shouldBe (())
          staysClosed(server, subscriber.health, 1)
        } finally server.stop()
      }
    }
    "there is no connection (nothing listens)" should {
      "complete, and the subscriber is closed" in {
        val subscriber = SubscriberClient(_ => (), port = refusedPort())
        subscriber.quit().futureValue shouldBe (())
        subscriber.health.state shouldBe Closed
      }
    }
  }
}
