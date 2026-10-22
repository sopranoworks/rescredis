// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
package scredis

import java.net.ServerSocket
import java.util.UUID

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec
import scredis.exceptions.RedisIOException
import scredis.util.CapturingLogger

import scala.concurrent.Await
import scala.concurrent.duration._

/**
 * Every public way to construct a client must hand its parameters to the parameter of the same
 * meaning. The values are distinctive and are observed where they are used: the buffer sizes in the
 * options the IO actor connects with (it reports them), the dispatchers in the names of the threads
 * that the actors of the connection run on, `failCommandOnConnecting` in the answer of a client that
 * cannot connect.
 */
class ConstructorParametersSpec extends AnyWordSpec with Matchers with ScalaFutures with Eventually {

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(10, Seconds), interval = Span(100, Millis))

  private val SendHint = 262145
  private val ReceiveHint = 131075
  private val Expected = s"tcp send buffer $SendHint bytes, receive buffer $ReceiveHint bytes"

  private def uniqueName(prefix: String): String = s"$prefix-${UUID.randomUUID().toString.take(8)}"

  /** The connections that the actors of the system `systemName` opened, as the IO actors report them. */
  private def connectionsOf(systemName: String, expectedCount: Int): List[String] = eventually {
    val connecting = CapturingLogger.messagesFrom(s"//$systemName").filter(_.startsWith("Connecting to"))
    connecting.size should be >= expectedCount
    connecting
  }

  private def shouldUseHints(systemName: String, expectedCount: Int): Unit =
    all(connectionsOf(systemName, expectedCount)) should include (Expected)

  private def withSystem[A](f: (ActorSystem, String) => A): A = {
    val name = uniqueName("params-hints")
    val system = ActorSystem(name)
    try f(system, name)
    finally Await.result(system.terminate(), 10.seconds)
  }

  private def configWithHints: RedisConfig = new RedisConfig(ConfigFactory.parseString(
    s"""io.tcp-send-buffer-size-hint = $SendHint
       |io.tcp-receive-buffer-size-hint = $ReceiveHint""".stripMargin
  ))

  "tcp buffer size hints" should {
    "reach the IO actor of Client" in withSystem { (system, name) =>
      val client = Client(port = 6379, tcpSendBufferSizeHint = SendHint, tcpReceiveBufferSizeHint = ReceiveHint)(system)
      client.ping().futureValue
      shouldUseHints(name, 1)
    }

    "reach the IO actor of Client built from a RedisConfig" in withSystem { (system, name) =>
      val client = Client(configWithHints)(system)
      client.ping().futureValue
      shouldUseHints(name, 1)
    }

    "reach the IO actor of BlockingClient" in withSystem { (system, name) =>
      val client = BlockingClient(port = 6379, tcpSendBufferSizeHint = SendHint, tcpReceiveBufferSizeHint = ReceiveHint)(system)
      shouldUseHints(name, 1)
      client.select(0)(5.seconds).get
    }

    "reach the IO actor of SubscriberClient" in withSystem { (system, name) =>
      val client = SubscriberClient(
        port = 6379, tcpSendBufferSizeHint = SendHint, tcpReceiveBufferSizeHint = ReceiveHint
      )(system)
      client.subscribe("params-hints").futureValue
      shouldUseHints(name, 1)
    }

    "reach the IO actors of Redis.apply, of its blocking client and of its subscriber" in {
      val name = uniqueName("params-apply")
      val redis = Redis(
        port = 6379, tcpSendBufferSizeHint = SendHint, tcpReceiveBufferSizeHint = ReceiveHint, actorSystemName = name
      )
      try {
        redis.ping().futureValue
        redis.blocking.select(0)(5.seconds).get
        redis.subscriber.subscribe("params-hints").futureValue
        shouldUseHints(name, 3)
      } finally Await.result(redis.quit(), 10.seconds)
    }

    "reach the IO actors of new Redis(...), of its blocking client and of its subscriber" in {
      val name = uniqueName("params-new")
      val redis = new Redis(
        port = 6379, tcpSendBufferSizeHint = SendHint, tcpReceiveBufferSizeHint = ReceiveHint, actorSystemName = name
      )
      try {
        redis.ping().futureValue
        redis.blocking.select(0)(5.seconds).get
        redis.subscriber.subscribe("params-hints").futureValue
        shouldUseHints(name, 3)
      } finally Await.result(redis.quit(), 10.seconds)
    }

    "reach the IO actors of Redis.withActorSystem, of its blocking client and of its subscriber" in
      withSystem { (system, name) =>
        val redis = Redis.withActorSystem(
          port = 6379, tcpSendBufferSizeHint = SendHint, tcpReceiveBufferSizeHint = ReceiveHint
        )(system)
        redis.ping().futureValue
        redis.blocking.select(0)(5.seconds).get
        redis.subscriber.subscribe("params-hints").futureValue
        shouldUseHints(name, 3)
      }

    "reach the IO actors of Redis.withActorSystem built from a RedisConfig" in withSystem { (system, name) =>
      val redis = Redis.withActorSystem(configWithHints)(system)
      redis.ping().futureValue
      redis.blocking.select(0)(5.seconds).get
      redis.subscriber.subscribe("params-hints").futureValue
      shouldUseHints(name, 3)
    }

    "reach the IO actors of RedisCluster.apply" in withSystem { (system, name) =>
      val cluster = RedisCluster(
        nodes = Seq(Server("localhost", 7000)),
        tcpSendBufferSizeHint = SendHint,
        tcpReceiveBufferSizeHint = ReceiveHint,
        systemOpt = Some(system)
      )
      cluster.clusterInfo().futureValue
      shouldUseHints(name, 1)
    }

    "reach the IO actors of RedisCluster built from a RedisConfig" in withSystem { (system, name) =>
      val config = new RedisConfig(ConfigFactory.parseString(
        s"""io.tcp-send-buffer-size-hint = $SendHint
           |io.tcp-receive-buffer-size-hint = $ReceiveHint
           |redis.cluster-nodes = ["localhost:7000"]""".stripMargin
      ))
      val cluster = RedisCluster(config, Some(system))
      cluster.clusterInfo().futureValue
      shouldUseHints(name, 1)
    }
  }

  private val IoPath = "scredis-test-io"
  private val ListenerPath = "scredis-test-listener"
  private val DecoderPath = "scredis-test-decoder"

  /** Names of the threads of the system that run the dispatcher `path`. */
  private def threadsOf(systemName: String, path: String): Set[String] = {
    val prefix = s"$systemName-$path"
    val threads = Thread.getAllStackTraces.keySet.toArray(new Array[Thread](0))
    threads.map(_.getName).filter(_.contains(prefix)).toSet
  }

  private def shouldRunOnTheDispatchers(systemName: String): Unit = eventually {
    withClue(s"threads of the IO dispatcher: ") { threadsOf(systemName, IoPath) should not be empty }
    withClue(s"threads of the listener dispatcher: ") { threadsOf(systemName, ListenerPath) should not be empty }
    withClue(s"threads of the decoder dispatcher: ") { threadsOf(systemName, DecoderPath) should not be empty }
  }

  "dispatcher paths" should {
    "be used by the actors of Client" in withSystem { (system, name) =>
      val client = Client(
        akkaIODispatcherPath = IoPath, akkaListenerDispatcherPath = ListenerPath, akkaDecoderDispatcherPath = DecoderPath
      )(system)
      client.ping().futureValue
      shouldRunOnTheDispatchers(name)
    }

    "be used by the actors of BlockingClient" in withSystem { (system, name) =>
      val client = BlockingClient(
        akkaIODispatcherPath = IoPath, akkaListenerDispatcherPath = ListenerPath, akkaDecoderDispatcherPath = DecoderPath
      )(system)
      client.select(0)(5.seconds).get
      shouldRunOnTheDispatchers(name)
    }

    "be used by the actors of SubscriberClient" in withSystem { (system, name) =>
      val client = SubscriberClient(
        akkaIODispatcherPath = IoPath, akkaListenerDispatcherPath = ListenerPath, akkaDecoderDispatcherPath = DecoderPath
      )(system)
      client.subscribe("params-dispatchers").futureValue
      shouldRunOnTheDispatchers(name)
    }

    "be used by the actors of Redis.apply, of its blocking client and of its subscriber" in {
      val name = uniqueName("params-dispatchers")
      val redis = Redis(
        actorSystemName = name,
        akkaIODispatcherPath = IoPath, akkaListenerDispatcherPath = ListenerPath, akkaDecoderDispatcherPath = DecoderPath
      )
      try {
        redis.ping().futureValue
        redis.blocking.select(0)(5.seconds).get
        redis.subscriber.subscribe("params-dispatchers").futureValue
        shouldRunOnTheDispatchers(name)
      } finally Await.result(redis.quit(), 10.seconds)
    }

    "be used by the actors of Redis.withActorSystem" in withSystem { (system, name) =>
      val redis = Redis.withActorSystem(
        akkaIODispatcherPath = IoPath, akkaListenerDispatcherPath = ListenerPath, akkaDecoderDispatcherPath = DecoderPath
      )(system)
      redis.ping().futureValue
      shouldRunOnTheDispatchers(name)
    }

    "be used by the actors of RedisCluster.apply" in withSystem { (system, name) =>
      val cluster = RedisCluster(
        nodes = Seq(Server("localhost", 7000)),
        akkaIODispatcherPath = IoPath, akkaListenerDispatcherPath = ListenerPath, akkaDecoderDispatcherPath = DecoderPath,
        systemOpt = Some(system)
      )
      cluster.clusterInfo().futureValue
      shouldRunOnTheDispatchers(name)
    }
  }

  "failCommandOnConnecting" should {
    /** A port nobody listens on. */
    def closedPort(): Int = {
      val socket = new ServerSocket(0)
      try socket.getLocalPort finally socket.close()
    }

    "reach the blocking client of Redis" in withSystem { (system, _) =>
      val redis = Redis.withActorSystem(port = closedPort(), failCommandOnConnecting = true)(system)
      val started = System.nanoTime()
      val failure = redis.blocking.blPop(1, "params-fail-fast").failed.get
      failure shouldBe a [RedisIOException]
      failure.getMessage should include ("Trying to connect")
      (System.nanoTime() - started) should be < 1.second.toNanos
    }
  }
}
