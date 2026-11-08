// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
package scredis

import akka.actor.ActorSystem
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec
import scredis.ConnectionState.Connected
import scredis.protocol.ClusterCRC16

import scala.concurrent.{ExecutionContext, Future, Promise}

/**
 * A new connection without initialisation requests (no AUTH, database 0, no CLIENT SETNAME) is initialised by a PING
 * of the client's own, and the requests given to it are sent only after its reply: when the first command's future
 * completes, the health already says Connected. Read in a callback on the thread that completes the future, on many
 * fresh connections: a [[scredis.Client]] on 6379 and the node connections of a [[scredis.RedisCluster]] on 7000.
 */
class FirstAnswerHealthSpec extends AnyWordSpec with Matchers with ScalaFutures with BeforeAndAfterAll {

  private implicit val system: ActorSystem = ActorSystem("first-answer-health-spec")
  private implicit val ec: ExecutionContext = ExecutionContext.global
  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(20, Seconds), interval = Span(20, Millis))

  /** Runs callbacks on the thread that completes the future */
  private val sameThread: ExecutionContext = ExecutionContext.fromExecutor((r: Runnable) => r.run())

  private val connections = 500

  override def afterAll(): Unit = {
    system.terminate()
  }

  /** `read` when `answer` completes, on the completing thread */
  private def atCompletion[T](answer: Future[_])(read: => T): Future[T] = {
    val p = Promise[T]()
    answer.onComplete(_ => p.success(read))(sameThread)
    p.future
  }

  "The first answer on a new connection without initialisation requests" should {
    s"find the Client's health Connected (${connections} fresh connections)" in {
      val states = (1 to connections).map { i =>
        val client = Client(port = 6379)
        try {
          val answer = client.get(s"first-answer-health-$i")
          val state = atCompletion(answer)(client.health.state).futureValue
          answer.futureValue shouldBe None
          state
        } finally client.quit().futureValue
      }
      states.filterNot(_ == Connected) shouldBe empty
    }

    s"find the slot's master connection of a RedisCluster Connected (${connections} fresh clients)" in {
      val states = (1 to connections).map { i =>
        val cluster = RedisCluster(nodes = Seq(Server("localhost", 7000)), systemOpt = Some(system))
        try {
          val key = s"first-answer-health-$i"
          val answer = cluster.get(key)
          val reachable = atCompletion(answer)(cluster.health.isReachable(ClusterCRC16.getSlot(key))).futureValue
          answer.futureValue shouldBe None
          reachable
        } finally cluster.quit().futureValue
      }
      states.count(!_) shouldBe 0
    }
  }
}
