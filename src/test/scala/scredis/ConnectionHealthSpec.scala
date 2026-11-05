// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
package scredis

import java.time.Instant
import java.util.concurrent.ConcurrentLinkedQueue

import akka.actor.ActorSystem
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec
import scredis.ConnectionState.{Closed, Connected, Reconnecting}
import scredis.exceptions.RedisNotSentException
import scredis.protocol.AuthConfig
import scredis.util.{CapturingLogger, RawRedis, StallingProxy, TcpProxy}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.CollectionConverters._
import scala.util.Try

/**
 * The health of a connection (state, subscription, idle PING) and its availability, on the standalone clients: a
 * [[scredis.Client]], a [[scredis.BlockingClient]] and a [[scredis.SubscriberClient]] reach Redis through a proxy
 * that refuses or cuts connections ([[scredis.util.TcpProxy]]) or stalls them ([[scredis.util.StallingProxy]]).
 */
class ConnectionHealthSpec extends AnyWordSpec with Matchers with ScalaFutures with Eventually with BeforeAndAfterAll {

  private implicit val system: ActorSystem = ActorSystem("connection-health-spec")
  private implicit val ec: ExecutionContext = ExecutionContext.global
  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(20, Seconds), interval = Span(20, Millis))

  /** Runs callbacks on the thread that completes the future */
  private val sameThread: ExecutionContext = ExecutionContext.fromExecutor((r: Runnable) => r.run())

  private val idleInterval = RedisConfigDefaults.IO.Health.IdlePingIntervalOpt.get
  private val pingTimeout = RedisConfigDefaults.IO.Health.IdlePingTimeout

  override def afterAll(): Unit = {
    system.terminate()
  }

  private def record(health: ConnectionHealth): ConcurrentLinkedQueue[ConnectionState] = {
    val seen = new ConcurrentLinkedQueue[ConnectionState]()
    health.subscribe { state => seen.add(state); () }
    seen
  }

  /** The kinds of the states seen, consecutive repetitions (attempts of one outage) collapsed */
  private def kinds(seen: ConcurrentLinkedQueue[ConnectionState]): List[String] =
    seen.asScala.toList.map {
      case Connected => "connected"
      case _: Reconnecting => "reconnecting"
      case Closed => "closed"
    }.foldRight(List.empty[String]) { (k, acc) => if (acc.headOption.contains(k)) acc else k :: acc }

  /** CLIENT LIST of a server, one map per client */
  private def clientList(port: Int, password: Option[String] = None): List[Map[String, String]] =
    RawRedis.withConnection("127.0.0.1", port) { r =>
      password.foreach(p => r.command("AUTH", p))
      r.string("CLIENT", "LIST").split("\n").toList.filter(_.trim.nonEmpty).map { line =>
        line.trim.split(" ").toList.flatMap { field =>
          field.split("=", 2) match {
            case Array(k, v) => List(k -> v)
            case _ => Nil
          }
        }.toMap
      }
    }

  private def namedClients(port: Int, name: String, password: Option[String] = None) =
    clientList(port, password).filter(_.get("name").contains(name))

  "The health of a Client" should {
    "be Connected, Reconnecting (attempt, since, cause) while the server cannot be reached, Connected again, and Closed after quit" in {
      val proxy = new TcpProxy("127.0.0.1", 6379)
      val client = Client(port = proxy.port)
      try {
        val seen = record(client.health)
        client.ping().futureValue shouldBe "PONG"
        eventually { client.health.state shouldBe Connected }
        client.availability.isAvailable shouldBe true
        client.availability.whenAvailable.isCompleted shouldBe true
        client.availability.unavailableSince shouldBe None

        val before = Instant.now()
        proxy.refuse()
        proxy.cutAll()
        eventually {
          client.health.state match {
            case Reconnecting(attempt, _, _) => attempt should be >= 3
            case other => fail(s"not reconnecting: $other")
          }
        }
        val (since, cause) = client.health.state match {
          case Reconnecting(_, since, cause) => (since, cause)
          case other => fail(s"not reconnecting: $other")
        }
        info(s"state while refused: ${client.health.state}")
        since.isBefore(before.minusSeconds(1)) shouldBe false
        cause should not be empty
        val outage = client.availability
        outage.isAvailable shouldBe false
        outage.whenAvailable.isCompleted shouldBe false
        outage.unavailableSince shouldBe Some(since)
        outage.cause should not be empty
        // one object per outage, shared: asked again, and carried by the requests that fail meanwhile
        client.availability shouldBe theSameInstanceAs (outage)
        client.ping().failed.futureValue match {
          case e: RedisNotSentException => e.availability shouldBe theSameInstanceAs (outage)
          case other => fail(s"not a RedisNotSentException: $other")
        }

        proxy.listen()
        outage.whenAvailable.futureValue shouldBe (())
        client.health.state shouldBe Connected
        client.ping().futureValue shouldBe "PONG"
        client.availability.isAvailable shouldBe true

        // the next outage has a new object
        proxy.refuse()
        proxy.cutAll()
        eventually { client.availability.isAvailable shouldBe false }
        val second = client.availability
        second should not be theSameInstanceAs (outage)
        proxy.listen()
        second.whenAvailable.futureValue shouldBe (())

        client.quit().futureValue
        eventually { client.health.state shouldBe Closed }
        client.availability.whenAvailable.failed.futureValue shouldBe a [scredis.exceptions.RedisIOException]
        val k = kinds(seen)
        info(s"states seen: ${k.mkString(" -> ")}")
        k.dropWhile(_ == "reconnecting") shouldBe List("connected", "reconnecting", "connected", "reconnecting", "connected", "closed")
      } finally proxy.stop()
    }

    "complete whenAvailable only once the connection is back and initialised (AUTH, SELECT, CLIENT SETNAME)" in {
      val proxy = new TcpProxy("127.0.0.1", 6380)
      val name = "health-spec-init"
      val client = Client(port = proxy.port, authOpt = Some(AuthConfig(None, "foobar")), database = 2, nameOpt = Some(name))
      try {
        client.ping().futureValue shouldBe "PONG"
        eventually { namedClients(6380, name, Some("foobar")) should have size 1 }
        val oldId = namedClients(6380, name, Some("foobar")).head("id")
        proxy.refuse()
        proxy.cutAll()
        eventually { client.availability.isAvailable shouldBe false }
        eventually { namedClients(6380, name, Some("foobar")) shouldBe empty }
        val outage = client.availability
        // what the server has at the moment the availability completes
        val atCompletion = Promise[List[Map[String, String]]]()
        outage.whenAvailable.onComplete(_ => atCompletion.complete(Try(namedClients(6380, name, Some("foobar")))))(sameThread)
        Thread.sleep(500)
        proxy.listen()
        val named = atCompletion.future.futureValue
        info(s"at completion: $named")
        named should have size 1
        named.head("id") should not be oldId
        named.head("db") shouldBe "2"
        client.health.state shouldBe Connected
        client.quit().futureValue
      } finally proxy.stop()
    }

    "send a PING on an idle connection, whose reply reaches no caller, and find a connection that died silently" in {
      val proxy = new StallingProxy("127.0.0.1", 6379)
      val name = "health-spec-idle"
      val client = Client(port = proxy.port, nameOpt = Some(name))
      try {
        client.echo("last-request").futureValue shouldBe "last-request"
        Thread.sleep(idleInterval.toMillis * 2 + 500)
        // the client's own PING was the last command on the connection
        namedClients(6379, name).map(_("cmd")) shouldBe List("ping")
        // the replies to the idle PINGs reach nobody: every reply is the request's own
        Future.sequence((1 to 200).map(i => client.echo(s"echo-$i"))).futureValue shouldBe (1 to 200).map(i => s"echo-$i")
        Thread.sleep(idleInterval.toMillis * 2 + 500)
        Future.sequence((1 to 200).map(i => client.echo(s"again-$i"))).futureValue shouldBe (1 to 200).map(i => s"again-$i")

        // the connection dies silently: nothing is closed, nothing more is answered; no request is made
        val stalledAt = System.nanoTime()
        proxy.stall(true)
        eventually(timeout(Span(15, Seconds))) { client.health.state shouldBe a [Reconnecting] }
        val foundAfter = (System.nanoTime() - stalledAt) / 1000000L
        val cause = client.health.state.asInstanceOf[Reconnecting].lastCause
        info(s"found $foundAfter ms after the stall, cause: $cause")
        cause should include ("PING")
        // idle for one interval (seen at the next tick, at most one more interval), then the PING's timeout
        foundAfter should be <= (idleInterval * 2 + pingTimeout).toMillis + 1000L
        proxy.stall(false)
        client.availability.whenAvailable.futureValue shouldBe (())
        client.echo("after").futureValue shouldBe "after"
        client.quit().futureValue
      } finally proxy.stop()
    }
  }

  "The health of a BlockingClient" should {
    "follow its connection, and its availability complete when it is back" in {
      val proxy = new TcpProxy("127.0.0.1", 6379)
      val blocking = BlockingClient(port = proxy.port)
      try {
        blocking.blPop[String](1, "health-spec-blocking").get shouldBe None
        eventually { blocking.health.state shouldBe Connected }
        proxy.refuse()
        proxy.cutAll()
        eventually { blocking.health.state shouldBe a [Reconnecting] }
        val outage = blocking.availability
        outage.isAvailable shouldBe false
        proxy.listen()
        outage.whenAvailable.futureValue shouldBe (())
        blocking.blPop[String](1, "health-spec-blocking").get shouldBe None
        blocking.quit()(5.seconds)
        eventually { blocking.health.state shouldBe Closed }
      } finally proxy.stop()
    }
  }

  "The health of a SubscriberClient" should {
    "answer the idle PING while subscribed as the PING's own reply: the subscription sees only its messages" in {
      val received = new ConcurrentLinkedQueue[PubSubMessage]()
      val name = "health-spec-subscriber"
      val subscriber = SubscriberClient((m: PubSubMessage) => received.add(m), port = 6379, nameOpt = Some(name))
      val publisher = Client(port = 6379)
      try {
        subscriber.subscribe("health-spec-channel").futureValue shouldBe 1
        Thread.sleep(idleInterval.toMillis * 2 + 500)
        namedClients(6379, name).map(_("cmd")) shouldBe List("ping")
        subscriber.health.state shouldBe Connected
        publisher.publish("health-spec-channel", "after-pings").futureValue shouldBe 1L
        eventually {
          received.asScala.toList.collect { case PubSubMessage.Message(c, m) => (c, new String(m, "UTF-8")) } shouldBe
            List(("health-spec-channel", "after-pings"))
        }
        Thread.sleep(idleInterval.toMillis * 2 + 500)
        received.asScala.toList.map(_.getClass.getSimpleName) shouldBe List("Subscribe", "Message")
        CapturingLogger.messagesFrom("decoder-actor").filter(_.contains("Could not decode")) shouldBe empty
        subscriber.subscribe("health-spec-channel-2").futureValue shouldBe 2
        subscriber.quit().futureValue
        eventually { subscriber.health.state shouldBe Closed }
      } finally publisher.quit().futureValue
    }
  }
}
