// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
package scredis

import java.io.{BufferedInputStream, IOException, InputStream}
import java.net.{InetAddress, InetSocketAddress, ServerSocket, Socket}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.{ConcurrentLinkedQueue, TimeoutException}

import akka.actor.ActorSystem
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec
import scredis.ConnectionState.{Connected, Reconnecting}
import scredis.exceptions._
import scredis.util.RawRedis

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

/**
 * The queue timeout (`scredis.io.queue-timeout`; not set in the tests, so each client's receive timeout, here 3 s and
 * 4 s): a request that waited unwritten longer is taken out and fails as not sent, and is never written. Waiting in
 * the listener's queue: on 6379, the client's connection is killed and every command paused in one transaction (as in
 * `BlockingDeadlineBeforeWriteSpec`), so that its new connection is not initialised for the pause; whether Redis
 * executed a command is read from `INFO commandstats`. Waiting in the IOActor's queue: behind a write that a server
 * which stopped reading does not take ([[QueueTimeoutSpec.BackpressureServer]]).
 */
class QueueTimeoutSpec extends AnyWordSpec with Matchers with ScalaFutures with Eventually with BeforeAndAfterAll {

  import QueueTimeoutSpec._

  private implicit val system: ActorSystem = ActorSystem("queue-timeout-spec")
  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(20, Seconds), interval = Span(20, Millis))

  override def afterAll(): Unit = system.terminate()

  private val L = "queue-timeout-spec:list"
  private val QueueTimeout = 3.seconds

  private def timed[A](f: => A): (A, Long) = {
    val start = System.nanoTime()
    val a = f
    (a, (System.nanoTime() - start) / 1000000L)
  }

  private def raw[A](f: RawRedis => A): A = RawRedis.withConnection("127.0.0.1", 6379)(f)

  /** The id of the one connection named `name` */
  private def idOf(name: String): String = {
    val lines = raw(_.string("CLIENT", "LIST")).split("\n").toList.filter(_.contains(s" name=$name "))
    lines should have size 1
    lines.head.split(" ").find(_.startsWith("id=")).get.stripPrefix("id=")
  }

  /** The number of times Redis has executed `command` */
  private def calls(command: String): Long =
    raw(_.string("INFO", "commandstats")).split("\r\n").find(_.startsWith(s"cmdstat_$command:")) match {
      case Some(line) => line.split("[:,]").find(_.startsWith("calls=")).get.stripPrefix("calls=").toLong
      case None => 0L
    }

  /** Kills the connection `id` and pauses every command for `ms`, in one transaction */
  private def killPause(id: String, ms: Int): Unit = raw { r =>
    r.command("MULTI")
    r.command("CLIENT", "KILL", "ID", id)
    r.command("CLIENT", "PAUSE", ms.toString, "ALL")
    r.command("EXEC") match {
      case List(1L, "OK") =>
      case other => fail(s"MULTI CLIENT KILL / CLIENT PAUSE: $other")
    }
  }

  /** A client named `name` whose queue timeout is its receive timeout, 3 s; connected, with "kept" in L */
  private def withClient(name: String)(body: Client => Unit): Unit = {
    val client = Client(port = 6379, nameOpt = Some(name), receiveTimeoutOpt = Some(QueueTimeout))
    try {
      raw { r =>
        r.command("DEL", L)
        r.command("RPUSH", L, "kept")
      }
      eventually { client.health.state shouldBe Connected }
      body(client)
    } finally {
      Try(raw(_.command("CLIENT", "UNPAUSE")))
      Try(client.quit().futureValue)
    }
  }

  private def result[A](future: Future[A]): Try[A] = Try(Await.result(future, 15.seconds))

  "A request that waits in the client's queue" when {
    "it waits longer than the queue timeout" should {
      "fail as not sent, and not be executed when the connection is back; the next request is made" in {
        val name = "queue-timeout-spec-long"
        withClient(name) { client =>
          val before = calls("lpop")
          killPause(idOf(name), 8000)
          eventually { client.health.state shouldBe a [Reconnecting] }
          val (r, ms) = timed(result(client.lPop[String](L)))
          info(s"the request ended after $ms ms: $r")
          val e = r match {
            case Failure(e: RedisNotSentException) => e
            case other => fail(s"expected a RedisNotSentException, got $other")
          }
          e.getCause shouldBe a [TimeoutException]
          e.availability.isAvailable shouldBe false
          ms should be >= 2900L
          ms should be < 4500L
          // the pause ends: the connection is initialised, the request is not written
          eventually { client.health.state shouldBe Connected }
          Thread.sleep(500)
          raw(_.command("LLEN", L)) shouldBe 1L
          calls("lpop") shouldBe before
          result(client.lPop[String](L)) shouldBe Success(Some("kept"))
          calls("lpop") shouldBe before + 1
        }
      }
      "fail a transaction as not sent, whole: none of it is executed" in {
        val name = "queue-timeout-spec-transaction"
        withClient(name) { client =>
          val (multi, exec, lpop) = (calls("multi"), calls("exec"), calls("lpop"))
          killPause(idOf(name), 8000)
          eventually { client.health.state shouldBe a [Reconnecting] }
          val (r, ms) = timed(result(client.inTransaction(b => b.lPop[String](L))))
          info(s"the transaction ended after $ms ms: $r")
          r match {
            case Failure(e: RedisNotSentException) => e.getCause shouldBe a [TimeoutException]
            case other => fail(s"expected a RedisNotSentException, got $other")
          }
          eventually { client.health.state shouldBe Connected }
          Thread.sleep(500)
          raw(_.command("LLEN", L)) shouldBe 1L
          // the MULTI and EXEC of killPause itself, none of the client's
          (calls("multi"), calls("exec"), calls("lpop")) shouldBe ((multi + 1, exec + 1, lpop))
        }
      }
    }
    "it is written within the queue timeout" should {
      "complete normally" in {
        val name = "queue-timeout-spec-short"
        withClient(name) { client =>
          val before = calls("lpop")
          killPause(idOf(name), 1000)
          eventually { client.health.state shouldBe a [Reconnecting] }
          val (r, ms) = timed(result(client.lPop[String](L)))
          info(s"the request ended after $ms ms: $r")
          r shouldBe Success(Some("kept"))
          calls("lpop") shouldBe before + 1
        }
      }
    }
    "the connection is initialised about when the queue timeout passes" should {
      "end in exactly one of not sent (never executed) and written (completed, executed once)" in {
        val name = "queue-timeout-spec-tie"
        // the pause ends from before to after the queue timeout (3 s, checked every 300 ms)
        val pauses = (2700 to 3700 by 50).toList
        withClient(name) { client =>
          val outcomes = pauses.map { pause =>
            raw { r =>
              r.command("DEL", L)
              r.command("RPUSH", L, "kept")
            }
            val id = eventually {
              client.health.state shouldBe Connected
              idOf(name)
            }
            val before = calls("lpop")
            killPause(id, pause)
            eventually { client.health.state shouldBe a [Reconnecting] }
            val (r, ms) = timed(result(client.lPop[String](L)))
            eventually { client.health.state shouldBe Connected }
            Thread.sleep(300)
            val executed = calls("lpop") - before
            val length = raw(_.command("LLEN", L))
            val outcome = r match {
              case Failure(e: RedisNotSentException) if e.getCause.isInstanceOf[TimeoutException] =>
                (executed, length) shouldBe ((0L, 1L))
                "not sent"
              case Success(Some("kept")) =>
                (executed, length) shouldBe ((1L, 0L))
                "written"
              case other => fail(s"pause $pause ms: expected not sent or written, got $other")
            }
            info(s"pause $pause ms: $outcome after $ms ms (LPOP executed $executed time(s))")
            outcome
          }
          val notSent = outcomes.count(_ == "not sent")
          val written = outcomes.count(_ == "written")
          info(s"${pauses.size} runs: $notSent not sent, $written written")
          (notSent + written) shouldBe pauses.size
        }
      }
    }
  }

  "A request that waits in the IOActor's queue, behind a write the server does not take" when {
    "it waits longer than the queue timeout" should {
      "be taken out and fail as not sent, never written; the next request is made" in {
        val server = new BackpressureServer
        // queue timeout and receive timeout 4 s; the reply to `slow` after 3 s keeps the receive timeout off
        val client = Client(port = server.port, receiveTimeoutOpt = Some(4.seconds))
        try {
          eventually { client.health.state shouldBe Connected }
          val slow = client.get[String]("slow:3000")
          val stall = client.get[String]("stall:6000")
          val big = client.set("big", "x" * (32 * 1024 * 1024))
          val (queued, ms) = timed(result(client.get[String]("queued")))
          info(s"the request behind the write ended after $ms ms: $queued")
          queued match {
            case Failure(e: RedisNotSentException) => e.getCause shouldBe a [TimeoutException]
            case other => fail(s"expected a RedisNotSentException, got $other")
          }
          ms should be >= 3900L
          ms should be < 5500L
          result(slow) shouldBe Success(Some("slow:3000"))
          result(stall) shouldBe Success(Some("stall:6000"))
          result(big) shouldBe Success(true)
          result(client.get[String]("next")) shouldBe Success(Some("next"))
          server.keys should not contain "queued"
          server.keys should contain ("next")
          server.accepted shouldBe 1
        } finally {
          Try(client.quit().futureValue)
          server.stop()
        }
      }
    }
  }
}

object QueueTimeoutSpec {

  /**
   * A server for one client: `GET k` → k; `GET slow:<ms>` answered after ms; after `GET stall:<ms>` it stops reading
   * for ms (what the client writes meanwhile stays in the TCP buffers, and a large write is not taken); `SET` → OK;
   * PING → PONG. Replies are written in order. It records the keys it has read.
   */
  final class BackpressureServer {
    private val server = new ServerSocket()
    server.setReuseAddress(true)
    server.bind(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
    val port: Int = server.getLocalPort

    @volatile var accepted = 0
    private val received = new ConcurrentLinkedQueue[String]()
    @volatile private var sockets = List.empty[Socket]

    def keys: List[String] = received.toArray.toList.map(_.toString)

    def stop(): Unit = {
      try server.close() catch { case _: IOException => }
      sockets.foreach(s => try s.close() catch { case _: IOException => })
    }

    daemon {
      try {
        while (true) {
          val socket = server.accept()
          accepted += 1
          sockets = socket :: sockets
          daemon(serve(socket))
        }
      } catch {
        case _: IOException =>
      }
    }

    private def daemon(body: => Unit): Unit = {
      val t = new Thread(new Runnable { def run(): Unit = body })
      t.setDaemon(true)
      t.start()
    }

    private def bulk(s: String): String = s"$$${s.getBytes(UTF_8).length}\r\n$s\r\n"

    private def serve(socket: Socket): Unit = {
      val in = new BufferedInputStream(socket.getInputStream)
      val out = socket.getOutputStream
      // replies in order, each not before its time
      val replies = new java.util.concurrent.LinkedBlockingQueue[(Long, String)]()
      daemon {
        try {
          while (true) {
            val (due, reply) = replies.take()
            val wait = (due - System.nanoTime()) / 1000000L
            if (wait > 0) Thread.sleep(wait)
            out.write(reply.getBytes(UTF_8))
            out.flush()
          }
        } catch {
          case _: IOException | _: InterruptedException =>
        }
      }
      def reply(s: String, afterMs: Long = 0): Unit = replies.put((System.nanoTime() + afterMs * 1000000L, s))
      try {
        var command = readCommand(in)
        while (command.isDefined) {
          val c = command.get
          c.head.toUpperCase match {
            case "PING" => reply("+PONG\r\n")
            case "SET" =>
              received.add(c(1))
              reply("+OK\r\n")
            case "GET" =>
              val key = c(1)
              received.add(key)
              if (key.startsWith("slow:")) reply(bulk(key), key.stripPrefix("slow:").toLong)
              else reply(bulk(key))
              if (key.startsWith("stall:")) Thread.sleep(key.stripPrefix("stall:").toLong)
            case "QUIT" => reply("+OK\r\n")
            case other => reply(s"-ERR unknown command '$other'\r\n")
          }
          command = readCommand(in)
        }
      } catch {
        case _: IOException =>
      }
    }

    private def readLine(in: InputStream): Option[String] = {
      val line = new StringBuilder
      var c = in.read()
      while (c != -1 && c != '\n') {
        if (c != '\r') line.append(c.toChar)
        c = in.read()
      }
      if (c == -1) None else Some(line.toString)
    }

    private def readCommand(in: InputStream): Option[Seq[String]] = readLine(in).map { header =>
      (1 to header.drop(1).toInt).map { _ =>
        val length = readLine(in).get.drop(1).toInt
        val bytes = new Array[Byte](length)
        var read = 0
        while (read < length) {
          val n = in.read(bytes, read, length - read)
          if (n < 0) throw new IOException("closed")
          read += n
        }
        readLine(in)
        new String(bytes, UTF_8)
      }
    }
  }
}
