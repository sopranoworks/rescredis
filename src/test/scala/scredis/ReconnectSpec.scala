// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
package scredis

import java.util.concurrent.ConcurrentLinkedQueue

import akka.actor.{Actor, ActorSystem, Props}
import akka.event.Logging
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec
import scredis.util.TcpProxy

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try

import ReconnectSpec._

/**
 * Reconnecting with a backoff (`scredis.io.reconnect`): while a server is down the client makes a bounded number
 * of connection attempts, logs the loss and the return of its connection once each (without stack traces), and
 * works afterwards. The server is behind a proxy ([[scredis.util.TcpProxy]]) that accepts connections and closes
 * them at once (as a forwarder in front of a dead server does), or refuses them.
 */
object ReconnectSpec {
  /** The log events of one system: level, source, message, and whether a cause (a stack trace) came with it */
  final case class Logged(level: Int, source: String, message: String, withCause: Boolean)

  class LogCollector(events: ConcurrentLinkedQueue[Logged]) extends Actor {
    def receive: Receive = {
      case e: Logging.Error => events.add(Logged(e.level.asInt, e.logSource, String.valueOf(e.message), e.cause != Logging.Error.NoCause))
      case e: Logging.LogEvent => events.add(Logged(e.level.asInt, e.logSource, String.valueOf(e.message), withCause = false))
    }
  }
}

class ReconnectSpec extends AnyWordSpec with Matchers with ScalaFutures with Eventually {

  private implicit val ec: ExecutionContext = ExecutionContext.global
  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(20, Seconds), interval = Span(50, Millis))

  private val minBackoff = RedisConfigDefaults.IO.Reconnect.MinBackoff
  private val maxBackoff = RedisConfigDefaults.IO.Reconnect.MaxBackoff
  private val connectTimeout = RedisConfigDefaults.IO.ConnectTimeout

  /** The wait before attempt n + 1 after n failed attempts */
  private def backoff(n: Int): Long = math.min(maxBackoff.toMillis, minBackoff.toMillis * (1L << (n - 1)))

  /**
   * The most attempts in `downMillis`: the first at once (the lost connection had been answered on), each later
   * one after the backoff, plus one under way when the server is back
   */
  private def attemptsBound(downMillis: Long): Int = {
    var n = 0
    var elapsed = 0L
    while (elapsed + backoff(n + 1) <= downMillis) {
      n += 1
      elapsed += backoff(n)
    }
    1 + n + 1
  }

  private def withSystem[A](f: (ActorSystem, ConcurrentLinkedQueue[Logged]) => A): A = {
    val system = ActorSystem(s"reconnect-spec-${System.nanoTime()}")
    val events = new ConcurrentLinkedQueue[Logged]()
    val collector = system.actorOf(Props(new LogCollector(events)))
    system.eventStream.subscribe(collector, classOf[Logging.LogEvent])
    try f(system, events)
    finally Await.result(system.terminate(), 10.seconds)
  }

  private def logsOf(events: ConcurrentLinkedQueue[Logged], port: Int): List[Logged] = {
    import scala.jdk.CollectionConverters._
    events.asScala.toList.filter(_.source.contains(s"127.0.0.1-$port-"))
  }

  /** Sends a PING every 10 ms for `millis` (requests keep coming while the server is down); the futures with their send times */
  private def pingFor(client: Client, millis: Long): List[(Long, Future[String])] = {
    val end = System.nanoTime() + millis * 1000000L
    var sent = List.empty[(Long, Future[String])]
    while (System.nanoTime() < end) {
      sent = (System.nanoTime(), client.ping()) :: sent
      Thread.sleep(10)
    }
    sent.reverse
  }

  private def worksAfterwards(client: Client): Long = {
    val start = System.nanoTime()
    eventually {
      Await.result(client.ping(), 1.second) shouldBe "PONG"
    }
    (1 to 100).map(i => client.echo(s"back-$i").map(_ == s"back-$i")).foreach(_.futureValue shouldBe true)
    (System.nanoTime() - start) / 1000000L
  }

  /** Every request sent while the server was down completed (failed or answered) within the backoff and a connect timeout */
  private def noneWaitedLonger(sent: List[(Long, Future[String])]): Unit = {
    val limit = maxBackoff + connectTimeout + 2.seconds
    sent.foreach { case (_, f) => Try(Await.ready(f, limit)).isSuccess shouldBe true }
  }

  "A client whose server is down behind a forwarder that closes every connection for 4 s" should {
    "make no more connection attempts than the backoff allows, log the loss and the return once, and work afterwards" in withSystem { (system, events) =>
      val proxy = new TcpProxy("127.0.0.1", 6379)
      val client = Client(host = "127.0.0.1", port = proxy.port)(system)
      try {
        client.ping().futureValue shouldBe "PONG"
        val before = proxy.accepted
        proxy.closeConnectionsOnAccept(true)
        proxy.cutAll()
        val downMillis = 4000L
        val sent = pingFor(client, downMillis)
        val attempts = proxy.accepted - before
        proxy.closeConnectionsOnAccept(false)
        val backAfter = worksAfterwards(client)
        noneWaitedLonger(sent)
        val logs = logsOf(events, proxy.port)
        val lost = logs.filter(_.message.startsWith(s"Connection to 127.0.0.1/<unresolved>:${proxy.port} lost"))
        val back = logs.filter(_.message.startsWith(s"Connection to 127.0.0.1/<unresolved>:${proxy.port} is back"))
        info(s"$attempts connection attempts in $downMillis ms (bound ${attemptsBound(downMillis)}, backoff $minBackoff to $maxBackoff); " +
          s"working again $backAfter ms after the server was back; ${sent.size} PINGs sent meanwhile")
        info(s"logged: ${(lost ++ back).map(_.message).mkString(" | ")}")
        attempts should be <= attemptsBound(downMillis)
        attempts should be >= 2
        lost should have size 1
        back should have size 1
        logs.filter(_.withCause) shouldBe empty
        backAfter should be < (maxBackoff + connectTimeout + 2.seconds).toMillis
      } finally {
        Try(Await.result(client.quit(), 10.seconds))
        proxy.stop()
      }
    }
  }

  "A client whose server refuses connections for 3 s" should {
    "reconnect with the backoff, log the loss and the return once, and work afterwards" in withSystem { (system, events) =>
      val proxy = new TcpProxy("127.0.0.1", 6379)
      val client = Client(host = "127.0.0.1", port = proxy.port)(system)
      try {
        client.ping().futureValue shouldBe "PONG"
        proxy.refuse()
        proxy.cutAll()
        val sent = pingFor(client, 3000)
        proxy.listen()
        val backAfter = worksAfterwards(client)
        noneWaitedLonger(sent)
        val logs = logsOf(events, proxy.port)
        val attemptsLogged = logs.count(_.message.startsWith("Connecting to"))
        info(s"working again $backAfter ms after the server was back; ${sent.size} PINGs sent meanwhile; " +
          s"${logs.size} log events from the connection: ${logs.map(_.message.take(90)).mkString(" | ")}")
        logs.count(_.message.contains(" lost (")) shouldBe 1
        logs.count(_.message.contains(" is back after ")) shouldBe 1
        attemptsLogged shouldBe 1 // the first connection only; attempts are not logged at info level
        logs.filter(_.withCause) shouldBe empty
        backAfter should be < (maxBackoff + connectTimeout + 2.seconds).toMillis
      } finally {
        Try(Await.result(client.quit(), 10.seconds))
        proxy.stop()
      }
    }
  }
}
