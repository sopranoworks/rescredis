// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
package scredis

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, Semaphore, TimeUnit}

import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec
import scredis.exceptions.RedisProtocolException
import scredis.protocol.{BulkStringResponse, ClusterCRC16, Decoder}
import scredis.util.{FailoverCluster, RawRedis, TcpProxy}
import scredis.util.TestUtils._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
 * A reply completes only the request it belongs to, also when the connection dies: pipelined requests whose
 * replies identify them (ECHO of a unique payload, GET of a unique value, a script that writes and returns a
 * unique value) while connections are cut (a proxy, see [[scredis.util.TcpProxy]]), closed by the server
 * (CLIENT KILL) or lost with a cluster master (DEBUG SEGFAULT on the failover cluster 7100 - 7105, see
 * testing.md). Every reply that arrives must be the request's own, and every request must get its own reply or
 * fail; afterwards the same client must work.
 */
class ReplyPairingSpec extends AnyWordSpec with Matchers with ScalaFutures with BeforeAndAfterAll {

  private implicit val ec: ExecutionContext = ExecutionContext.global
  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(30, Seconds), interval = Span(20, Millis))

  private implicit val bulkString: Decoder[String] = {
    case b: BulkStringResponse => b.flattened[String]
  }

  private val Keys = 300
  private def key(i: Int): String = s"pairing-spec:k$i"
  private def writeKey(i: Int): String = s"pairing-spec:w$i"
  // values of different sizes, up to 4 KB, so that a cut often falls inside a reply
  private def value(i: Int): String = s"v$i-" + ("x" * ((i * 37) % 4000))
  private val WriteScript = "redis.call('SET', KEYS[1], ARGV[1]) return ARGV[1]"

  private val admin = Client(port = 6379)
  private lazy val adminCluster = RedisCluster(Server("127.0.0.1", 7100))

  override def beforeAll(): Unit = {
    (0 until Keys).foreach(i => admin.set(key(i), value(i)).!)
  }

  override def afterAll(): Unit = {
    admin.del((0 until Keys).map(key) ++ (0 until Keys).map(writeKey): _*).!
    admin.quit().!
    if (clusterUsed) {
      (0 until Keys).foreach { i => adminCluster.del(key(i)).!; adminCluster.del(writeKey(i)).! }
      adminCluster.quit().!
    }
  }

  @volatile private var clusterUsed = false

  /**
   * Keeps `window` requests outstanding until stopped. `next(n)` sends request n and says what its reply must
   * be. Counts replies that are their request's own, replies that are not (or do not decode as the request's
   * reply type), failures by kind, and requests that never complete.
   */
  private final class PairingLoad(window: Int, next: Long => (Future[Any], Any, String)) {
    private val permits = new Semaphore(window)
    private val outstanding = new ConcurrentHashMap[Long, String]()
    private val counter = new AtomicLong(0)
    val own = new AtomicLong(0)
    val foreign = new AtomicLong(0)
    val failed = new AtomicLong(0)
    private val foreignSamples = new ConcurrentLinkedQueue[String]()
    private val failures = new ConcurrentHashMap[String, AtomicLong]()
    @volatile private var running = true
    private val error = new AtomicReference[Throwable](null)

    private def violation(sample: String): Unit = {
      foreign.incrementAndGet()
      if (foreignSamples.size < 10) foreignSamples.add(sample)
    }

    private val thread = new Thread(new Runnable {
      def run(): Unit = try {
        while (running) {
          if (permits.tryAcquire(100, TimeUnit.MILLISECONDS)) {
            val n = counter.incrementAndGet()
            val (future, expected, description) = next(n)
            outstanding.put(n, description)
            future.onComplete { result =>
              result match {
                case Success(v) if v == expected => own.incrementAndGet()
                case Success(v) =>
                  violation(s"$description got ${v.toString.take(60)}")
                case Failure(e: RedisProtocolException) =>
                  // a reply that does not decode as this request's reply: another request's reply
                  violation(s"$description failed with ${e.getMessage.take(120)}")
                case Failure(e) =>
                  failed.incrementAndGet()
                  val kind = s"${e.getClass.getSimpleName}: ${Option(e.getMessage).getOrElse("").take(50)}"
                  failures.computeIfAbsent(kind, _ => new AtomicLong(0)).incrementAndGet()
              }
              outstanding.remove(n)
              permits.release()
            }
          }
        }
      } catch {
        case e: Throwable => error.set(e)
      }
    }, "pairing-load")
    thread.setDaemon(true)
    thread.start()

    def sent: Long = counter.get()

    /** Stops sending; waits up to `drainMillis` for the outstanding requests; returns those that never completed */
    def stop(drainMillis: Long): Int = {
      running = false
      thread.join()
      val deadline = System.nanoTime() + drainMillis * 1000000L
      while (!outstanding.isEmpty && System.nanoTime() < deadline) Thread.sleep(50)
      Option(error.get()).foreach(e => throw e)
      outstanding.size
    }

    def summary(hung: Int): String = {
      import scala.jdk.CollectionConverters._
      s"sent ${counter.get()}, own reply ${own.get()}, another request's reply ${foreign.get()}, failed ${failed.get()}, " +
        s"never completed $hung; failures: ${failures.asScala.map { case (k, v) => s"$k x${v.get}" }.mkString("; ")}" +
        (if (foreignSamples.isEmpty) "" else s"; samples: ${foreignSamples.asScala.mkString(" | ")}")
    }
  }

  private def check(load: PairingLoad, hung: Int): Unit = {
    val s = load.summary(hung)
    info(s)
    withClue(s) {
      load.foreign.get() shouldBe 0
      hung shouldBe 0
      load.own.get() should be > 0L
    }
  }

  /** A standalone load: ECHO of a unique payload and GET of a unique value, alternately */
  private def standaloneLoad(client: Client, window: Int): PairingLoad = new PairingLoad(window, { n =>
    if (n % 2 == 0) {
      val payload = s"echo-$n"
      (client.echo(payload), payload, s"ECHO $payload")
    } else {
      val i = (n % Keys).toInt
      (client.get[String](key(i)), Some(value(i)), s"GET ${key(i)}")
    }
  })

  /** After a fault the same client works: every request gets its own reply */
  private def worksAfterwards(client: Client): Unit = {
    val replies = (1 to 200).map { n =>
      if (n % 2 == 0) client.echo(s"after-$n").map(_ == s"after-$n")
      else client.get[String](key(n)).map(_ == Some(value(n)))
    }
    Future.sequence(replies).futureValue should contain only true
  }

  private def awaitConnected(client: Client): Unit = {
    val deadline = System.nanoTime() + 20.seconds.toNanos
    var ok = false
    while (!ok && System.nanoTime() < deadline) {
      ok = Try(Await.result(client.ping(), 2.seconds)).isSuccess
      if (!ok) Thread.sleep(100)
    }
  }

  /** Kills the master of `key(0)`'s slot under pipelined load (`kill` does it) and checks the same client */
  private def clusterFailover(kill: Int => String): Unit = {
    clusterUsed = true
    FailoverCluster.awaitWhole()
    (0 until Keys).foreach(i => adminCluster.set(key(i), value(i)).!)
    val cluster = RedisCluster(Server("127.0.0.1", 7100))
    try {
      val load = new PairingLoad(100, { n =>
        val i = (n % Keys).toInt
        if (n % 2 == 0) (cluster.get[String](key(i)), Some(value(i)), s"GET ${key(i)}")
        else {
          val unique = s"w-$n"
          (cluster.eval[String, String, String](WriteScript, Seq(writeKey(i)), Seq(unique)), unique, s"EVAL ${writeKey(i)} $unique")
        }
      })
      Thread.sleep(1000)
      val slot = ClusterCRC16.getSlot(key(0))
      val victim = FailoverCluster.masterOf(slot)
      val killedAt = System.nanoTime()
      val how = kill(victim.port)
      val promoted = FailoverCluster.awaitFailover(victim.port, slot)
      info(s"killed master ${victim.port}$how; ${promoted.port} is master of slot $slot ${(System.nanoTime() - killedAt) / 1000000} ms after the kill")
      Thread.sleep(2000)
      val hung = load.stop(30000)
      check(load, hung)
      // the same client: every key from its current master, and writes
      (0 until Keys).foreach { i =>
        withClue(s"GET ${key(i)} after the failover: ") {
          cluster.get[String](key(i)).futureValue shouldBe Some(value(i))
        }
        cluster.eval[String, String, String](WriteScript, Seq(writeKey(i)), Seq(s"after-$i")).futureValue shouldBe s"after-$i"
      }
    } finally {
      // the outcome of the test is not hidden by a quit that does not complete
      Try(Await.result(cluster.quit(), 30.seconds)).failed.foreach(e => info(s"quit did not complete: $e"))
      FailoverCluster.awaitWhole()
    }
  }

  "A standalone client" when {
    "the connection is cut in the middle of a reply" should {
      "complete each later request with its own reply" in {
        val proxy = new TcpProxy("127.0.0.1", 6379)
        val client = Client(host = "127.0.0.1", port = proxy.port)
        try {
          client.echo("warm-up").futureValue shouldBe "warm-up"
          proxy.cutInNextReply(100)
          // the reply of 5000 bytes arrives only up to 100 bytes, then the connection is cut
          Try(Await.result(client.echo("y" * 5000), 10.seconds)).isFailure shouldBe true
          // the next requests go out on the new connection, pipelined
          val payloads = (1 to 60).map(n => s"after-cut-$n-" + ("z" * 200))
          val results = payloads.map(p => client.echo(p).transform(r => Success(r)))
          val got = Future.sequence(results).futureValue
          val foreign = payloads.zip(got).collect {
            case (p, Success(v)) if v != p => s"ECHO ${p.take(15)} got ${v.take(15)}"
            case (p, Failure(e: RedisProtocolException)) => s"ECHO ${p.take(15)} failed with ${e.getMessage.take(60)}"
          }
          info(s"${got.count(_.isSuccess)} of 60 answered, ${foreign.size} with another request's reply ${foreign.take(3).mkString(" | ")}")
          foreign shouldBe empty
          worksAfterwards(client)
        } finally {
          Try(Await.result(client.quit(), 10.seconds))
          proxy.stop()
        }
      }
    }

    "its connections are cut repeatedly under pipelined load" should {
      "never complete a request with another request's reply" in {
        val proxy = new TcpProxy("127.0.0.1", 6379)
        val client = Client(host = "127.0.0.1", port = proxy.port)
        try {
          client.ping().futureValue
          val load = standaloneLoad(client, 100)
          Thread.sleep(500)
          (1 to 15).foreach { _ =>
            proxy.cutAll()
            Thread.sleep(300)
          }
          Thread.sleep(1000)
          val hung = load.stop(15000)
          check(load, hung)
          awaitConnected(client)
          worksAfterwards(client)
        } finally {
          Try(Await.result(client.quit(), 10.seconds))
          proxy.stop()
        }
      }
    }

    "the server closes its connection (CLIENT KILL) under pipelined load" should {
      "never complete a request with another request's reply" in {
        val name = s"pairing-${System.nanoTime()}"
        val client = Client(host = "127.0.0.1", port = 6379, nameOpt = Some(name))
        try {
          client.ping().futureValue
          val load = standaloneLoad(client, 100)
          Thread.sleep(500)
          (1 to 10).foreach { _ =>
            RawRedis.withConnection("127.0.0.1", 6379) { r =>
              r.string("CLIENT", "LIST").split("\n").filter(_.contains(s" name=$name ")).foreach { line =>
                val id = line.split(" ").find(_.startsWith("id=")).get.drop(3)
                r.command("CLIENT", "KILL", "ID", id)
              }
            }
            Thread.sleep(300)
          }
          Thread.sleep(1000)
          val hung = load.stop(15000)
          check(load, hung)
          awaitConnected(client)
          worksAfterwards(client)
        } finally {
          Try(Await.result(client.quit(), 10.seconds))
        }
      }
    }
    "its server is down behind a forwarder that accepts connections and closes them, under pipelined load" should {
      "never complete a request with another request's reply" in {
        val proxy = new TcpProxy("127.0.0.1", 6379)
        val client = Client(host = "127.0.0.1", port = proxy.port)
        try {
          client.ping().futureValue
          val load = standaloneLoad(client, 100)
          Thread.sleep(500)
          (1 to 3).foreach { _ =>
            proxy.closeConnectionsOnAccept(true)
            proxy.cutAll()
            Thread.sleep(2000)
            proxy.closeConnectionsOnAccept(false)
            Thread.sleep(1000)
          }
          val hung = load.stop(15000)
          info(s"the forwarder accepted ${proxy.accepted} connections")
          check(load, hung)
          awaitConnected(client)
          worksAfterwards(client)
        } finally {
          Try(Await.result(client.quit(), 10.seconds))
          proxy.stop()
        }
      }
    }
  }

  "A RedisCluster client" when {
    "a master is killed under pipelined load" should {
      "never complete a request with another request's reply, and work afterwards" in {
        clusterFailover { port => FailoverCluster.kill(port); "" }
      }
    }

    "a master is killed and its port answered by a forwarder that closes connections, under pipelined load" should {
      "never complete a request with another request's reply, and work afterwards" in {
        clusterFailover { port =>
          val accepted = FailoverCluster.killBehindForwarder(port, 4000)
          s" (the forwarder on its port accepted and closed $accepted connections for 4 s)"
        }
      }
    }
  }
}
