// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
package scredis

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import akka.actor.ActorSystem
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec
import scredis.protocol.{Decoder, IntegerResponse, Protocol}
import scredis.protocol.requests.StringRequests.Get
import scredis.util.{Spin, TcpProxy}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/**
 * The permits of `scredis.global.max-concurrent-requests` (300 in the test configuration): a permit is taken when a
 * request is sent and given back exactly once, when it completes, also when it fails; requests the library sends
 * itself hold none. Counted with requests that note when they get their permit and when they complete.
 */
class PermitsSpec extends AnyWordSpec with Matchers with ScalaFutures with Eventually with BeforeAndAfterAll {

  private implicit val system: ActorSystem = ActorSystem("permits-spec")
  private implicit val ec: ExecutionContext = ExecutionContext.global
  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(30, Seconds), interval = Span(20, Millis))

  private implicit val long: Decoder[Long] = { case IntegerResponse(value) => value }

  private val limit = RedisConfigDefaults.Global.MaxConcurrentRequestsOpt.get

  override def afterAll(): Unit = system.terminate()

  /** The requests that hold a permit now, and the most that ever did */
  private val inFlight = new AtomicInteger(0)
  private val maxInFlight = new AtomicInteger(0)

  /** A GET that counts itself in flight from when it gets its permit until it completes */
  private final class CountedGet(key: String) extends Get[String](key) {
    private val held = new AtomicBoolean(false)
    override private[scredis] def holdPermit(): Unit = {
      super.holdPermit()
      held.set(true)
      val n = inFlight.incrementAndGet()
      maxInFlight.accumulateAndGet(n, (a: Int, b: Int) => math.max(a, b))
    }
    private def completed(): Unit = if (held.compareAndSet(true, false)) inFlight.decrementAndGet()
    override private[scredis] def success(value: Any): Unit = { completed(); super.success(value) }
    override private[scredis] def failure(throwable: Throwable): Unit = { completed(); super.failure(throwable) }
  }

  "max-concurrent-requests" should {
    "lose no permit and gain none when requests fail, in transactions, on reconnects, on the subscriber and the cluster" in {
      eventually { Protocol.availablePermits shouldBe Some(limit) }

      // transactions (MULTI is one object shared by every transaction)
      val client = Client(port = 6379)
      (1 to 50).foreach { i =>
        client.inTransaction { t =>
          t.set(s"permits-spec:tx-$i", "x")
          t.get[String](s"permits-spec:tx-$i")
          t.del(s"permits-spec:tx-$i")
        }.futureValue should have size 3
      }

      // a client with a database and a name (SELECT and CLIENT SETNAME on every connection), whose connection is cut
      // under load, then refused: requests fail as outcome unknown and as not sent, transactions too
      val proxy = new TcpProxy("127.0.0.1", 6379)
      val named = Client(port = proxy.port, database = 1, nameOpt = Some("permits-spec"))
      named.ping().futureValue
      (1 to 10).foreach { _ =>
        val fs = (1 to 100).map(i => named.get[String](s"permits-spec:k$i").map(_ => ()).recover { case _ => () })
        proxy.cutAll()
        Future.sequence(fs).futureValue
        named.availability.whenAvailable.futureValue
      }
      proxy.refuse()
      proxy.cutAll()
      eventually { named.availability.isAvailable shouldBe false }
      (1 to 20).foreach { i =>
        named.get[String](s"permits-spec:k$i").failed.futureValue shouldBe a [scredis.exceptions.RedisIOException]
        Try(named.inTransaction(_.get[String]("permits-spec:k")).failed.futureValue)
      }
      proxy.listen()
      named.availability.whenAvailable.futureValue

      // the subscriber's requests
      val subscriber = SubscriberClient((_: PubSubMessage) => (), port = 6379)
      (1 to 20).foreach { i =>
        subscriber.subscribe(s"permits-spec:c$i").futureValue shouldBe 1
        subscriber.unsubscribe(s"permits-spec:c$i").futureValue shouldBe 0
      }

      // the cluster: requests sent again after MOVED (a copy each), and the slots read again
      val cluster = RedisCluster(Server("127.0.0.1", 7000))
      (1 to 50).foreach { i =>
        val key = s"permits-spec:cluster-$i"
        val slot = protocol.ClusterCRC16.getSlot(key)
        val wrong = cluster.health.nodes.keys.find(s => !cluster.masterOf(slot).contains(s)).get
        cluster.slotMoved(slot, wrong)
        cluster.get[String](key).futureValue shouldBe None
      }

      subscriber.quit().futureValue
      named.quit().futureValue
      client.quit().futureValue
      cluster.quit().futureValue
      proxy.stop()
      eventually { Protocol.availablePermits shouldBe Some(limit) }
    }

    "never be exceeded under failures: at most that many requests are in flight, and every permit comes back" in {
      eventually { Protocol.availablePermits shouldBe Some(limit) }
      val direct = Client(port = 6379)
      val proxy = new TcpProxy("127.0.0.1", 6379)
      val proxied = Client(port = proxy.port)
      direct.ping().futureValue
      proxied.ping().futureValue
      inFlight.set(0)
      maxInFlight.set(0)
      @volatile var running = true
      val maxAvailable = new AtomicInteger(0)
      val sampler = new Thread(new Runnable {
        def run(): Unit = while (running) {
          Protocol.availablePermits.foreach(a => maxAvailable.accumulateAndGet(a, (x: Int, y: Int) => math.max(x, y)))
          Thread.sleep(1)
        }
      })
      sampler.setDaemon(true)
      sampler.start()
      val cutter = new Thread(new Runnable {
        def run(): Unit = while (running) {
          Thread.sleep(150)
          proxy.cutAll()
        }
      })
      cutter.setDaemon(true)
      cutter.start()
      try {
        // Redis is busy for a while again and again: the requests pile up until the permits run out
        val iterations = Spin.iterationsFor(6379, 700)
        val busy = Future.sequence((1 to 3).map(_ => direct.eval[Long, String, String](Spin.Script, Nil, Seq(iterations.toString))))
        val sent = new AtomicInteger(0)
        val threads = (1 to 8).map { t =>
          val thread = new Thread(new Runnable {
            def run(): Unit = (1 to 150).foreach { i =>
              val client = if (i % 2 == 0) direct else proxied
              client.send(new CountedGet(s"permits-spec:$t-$i"))
              sent.incrementAndGet()
            }
          })
          thread.setDaemon(true)
          thread.start()
          thread
        }
        threads.foreach(_.join(60000))
        threads.exists(_.isAlive) shouldBe false
        Try(busy.futureValue)
        eventually { inFlight.get shouldBe 0 }
        info(s"${sent.get} requests; at most ${maxInFlight.get} in flight; at most ${maxAvailable.get} permits free (limit $limit)")
        maxInFlight.get should be <= limit
        maxInFlight.get should be >= limit - 8
        maxAvailable.get should be <= limit
      } finally {
        running = false
        // no more cuts once the clients are closed
        cutter.join()
        sampler.join()
        direct.quit().futureValue
        proxied.quit().futureValue
        proxy.stop()
      }
      eventually { Protocol.availablePermits shouldBe Some(limit) }
    }
  }
}
