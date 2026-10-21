// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
// Tests the Akka configuration compatibility of the Pekko variant.
package scredis

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.pekko.actor.{Actor, ActorIdentity, ActorSystem, Identify, Props}
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scredis.io.DecoderActor
import scredis.util.AkkaConfigCompat

import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.duration._

class AkkaConfigCompatSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  private implicit val timeout: Timeout = Timeout(10.seconds)

  private val systems = ListBuffer.empty[ActorSystem]

  /** Config as the application would provide it: given settings over the usual loaded config. */
  private def applicationConfig(hocon: String): Config =
    ConfigFactory.parseString(hocon).withFallback(ConfigFactory.load()).resolve()

  private def systemFor(hocon: String): ActorSystem = {
    val system = AkkaConfigCompat.createActorSystem("compat-test", applicationConfig(hocon))
    systems.synchronized { systems += system }
    system
  }

  private def scredisConfig(hocon: String): RedisConfig =
    new RedisConfig(ConfigFactory.parseString(hocon))

  /** Kind of the dispatcher that an actor started with `path` really runs on. */
  private def dispatcherKind(system: ActorSystem, path: String): String = {
    val actor = system.actorOf(Props(new Actor {
      def receive = {
        case _ => sender() ! context.dispatcher.getClass.getSimpleName
      }
    }).withDispatcher(path))
    Await.result((actor ? "kind").mapTo[String], 10.seconds)
  }

  private def ioPath(system: ActorSystem): String =
    new RedisConfig(system.settings.config.getConfig("scredis")).IO.Akka.IODispatcherPath

  private def decoderPath(system: ActorSystem): String =
    new RedisConfig(system.settings.config.getConfig("scredis")).IO.Akka.DecoderDispatcherPath

  override def afterAll(): Unit = {
    systems.foreach(s => Await.result(s.terminate(), 10.seconds))
  }

  "RedisConfig" when {
    "given a scredis.io.akka override only" should {
      "take the akka values" in {
        val config = scredisConfig(
          """io.akka {
            |  actor-system-name = "from-akka"
            |  io-dispatcher-path = "akka-io-path"
            |}""".stripMargin
        )
        config.IO.Akka.ActorSystemName should be("from-akka")
        config.IO.Akka.IODispatcherPath should be("akka-io-path")
        config.IO.Akka.ListenerDispatcherPath should be("scredis.io.pekko.listener-dispatcher")
      }
    }

    "given a scredis.io.pekko override only" should {
      "take the pekko values" in {
        val config = scredisConfig(
          """io.pekko {
            |  actor-system-name = "from-pekko"
            |  io-dispatcher-path = "pekko-io-path"
            |}""".stripMargin
        )
        config.IO.Akka.ActorSystemName should be("from-pekko")
        config.IO.Akka.IODispatcherPath should be("pekko-io-path")
        config.IO.Akka.ListenerDispatcherPath should be("scredis.io.pekko.listener-dispatcher")
      }
    }

    "given neither" should {
      "use the reference defaults" in {
        val config = new RedisConfig(ConfigFactory.empty())
        config.IO.Akka.ActorSystemName should be("scredis")
        config.IO.Akka.IODispatcherPath should be("scredis.io.pekko.io-dispatcher")
        config.IO.Akka.ListenerDispatcherPath should be("scredis.io.pekko.listener-dispatcher")
        config.IO.Akka.DecoderDispatcherPath should be("scredis.io.pekko.decoder-dispatcher")
      }
    }

    "given the same key under akka and pekko" should {
      "take the akka value" in {
        val config = scredisConfig(
          """io.akka.actor-system-name = "from-akka"
            |io.pekko.actor-system-name = "from-pekko"""".stripMargin
        )
        config.IO.Akka.ActorSystemName should be("from-akka")
      }
    }
  }

  "The ActorSystem created by scredis" when {
    "the application overrides a scredis dispatcher under scredis.io.akka only" should {
      "run actors on that dispatcher" in {
        val system = systemFor("scredis.io.akka.io-dispatcher.type = Dispatcher")
        dispatcherKind(system, ioPath(system)) should be("Dispatcher")
      }
    }

    "the application overrides a scredis dispatcher under scredis.io.pekko only" should {
      "run actors on that dispatcher" in {
        val system = systemFor(
          """scredis.io.pekko.decoder-dispatcher {
            |  type = PinnedDispatcher
            |  executor = "thread-pool-executor"
            |}""".stripMargin
        )
        dispatcherKind(system, decoderPath(system)) should be("PinnedDispatcher")
      }
    }

    "the application overrides nothing" should {
      "run actors on the reference dispatchers" in {
        val system = systemFor("")
        dispatcherKind(system, ioPath(system)) should be("PinnedDispatcher")
        dispatcherKind(system, decoderPath(system)) should be("Dispatcher")
      }
    }

    "the application sets a top-level akka key only" should {
      "apply it as the pekko key" in {
        val system = systemFor("akka.loglevel = \"ERROR\"")
        system.settings.LogLevel should be("ERROR")
      }
    }

    "the application sets a top-level pekko key only" should {
      "apply it" in {
        val system = systemFor("pekko.loglevel = \"WARNING\"")
        system.settings.LogLevel should be("WARNING")
      }
    }

    "the application sets neither" should {
      "use the configured default" in {
        val system = systemFor("")
        system.settings.LogLevel should be("INFO")
      }
    }
  }

  "The decoder dispatcher" should {
    "name a Pekko bounded mailbox" in {
      ConfigFactory.defaultReference()
        .getString("scredis.io.pekko.decoder-dispatcher.mailbox-type") should be(
          "org.apache.pekko.dispatch.BoundedMailbox"
        )
    }

    "start DecoderActors with the default config" in {
      val system = systemFor("")
      val actor = system.actorOf(Props(new DecoderActor(None)).withDispatcher(decoderPath(system)))
      val identity = Await.result((actor ? Identify("decoder")).mapTo[ActorIdentity], 10.seconds)
      identity.ref should be(Some(actor))
    }
  }

}
