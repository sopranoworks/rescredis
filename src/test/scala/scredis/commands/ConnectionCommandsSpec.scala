// Modified by Sopranoworks: adapted to Redis 7.4; tests of auth as given and clearCredentials.
package scredis.commands

import org.scalatest._
import org.scalatest.concurrent._
import scredis._
import scredis.exceptions._
import scredis.protocol.AuthConfig
import scredis.protocol.requests.ConnectionRequests._
import scredis.tags._
import scredis.util.CapturingLogger
import scredis.util.TestUtils._
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import java.util.UUID
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class ConnectionCommandsSpec extends AnyWordSpec
  with GivenWhenThen
  with BeforeAndAfterAll
  with Matchers
  with Eventually
  with ScalaFutures {
  
  private val client = Client()
  private val clientToQuit = Client()
  private val clientWithPassword = Client(port = 6380)
  private val CorrectPassword = "foobar"
  
  Auth.toString when {
    "the server has no password" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.auth("foo").!
        }
      }
    }
    "the password is invalid" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy {
          clientWithPassword.auth("foo").!
        }
      }
    }
    "the password is correct" should {
      "authenticate to the server" taggedAs (V100) in {
        clientWithPassword.auth(CorrectPassword).futureValue should be (())
        clientWithPassword.ping().futureValue should be ("PONG")
      }
    }
    "re-authenticating with a wrong password" should {
      "keep client authorized with previous one" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy {
          clientWithPassword.auth("foo").!
        }
        clientWithPassword.ping().!
      }
    }
    "re-authenticating with a correct password" should {
      "authenticate back to the server" taggedAs (V100) in {
        clientWithPassword.auth(CorrectPassword).futureValue should be (())
        clientWithPassword.ping().futureValue should be ("PONG")
      }
    }
  }

  // AUTH is sent as given, and the credentials of a connection are what its constructor was given or
  // the last AUTH that the server accepted; clearCredentials() removes them. Every class that can authenticate.
  private case class Subject(
    label: String,
    name: String,
    auth: String => Future[Unit],
    clearCredentials: () => Unit,
    probe: () => Future[Any],
    quit: () => Future[Any]
  )

  private def uniqueName(prefix: String): String = s"$prefix-${UUID.randomUUID().toString.take(8)}"

  private val subjectFactories: List[(String, (Int, Option[AuthConfig]) => Subject)] = List(
    "Client" -> { (port, authOpt) =>
      val name = uniqueName("creds-client")
      val c = Client(port = port, authOpt = authOpt, nameOpt = Some(name))
      Subject("Client", name, p => c.auth(p), () => c.clearCredentials(), () => c.ping(), () => c.quit())
    },
    "Redis" -> { (port, authOpt) =>
      val name = uniqueName("creds-redis")
      val r = Redis(port = port, authOpt = authOpt, nameOpt = Some(name))
      Subject("Redis", name, p => r.auth(p), () => r.clearCredentials(), () => r.ping(), () => r.quit())
    },
    "BlockingClient" -> { (port, authOpt) =>
      val name = uniqueName("creds-blocking")
      val c = BlockingClient(port = port, authOpt = authOpt, nameOpt = Some(name))
      Subject(
        "BlockingClient", name,
        p => Future.fromTry(c.auth(p)(5.seconds)),
        () => c.clearCredentials(),
        () => Future(c.select(0)(5.seconds).get),
        () => Future(c.quit()(5.seconds).get)
      )
    },
    "SubscriberClient" -> { (port, authOpt) =>
      val name = uniqueName("creds-subscriber")
      val c = SubscriberClient(port = port, authOpt = authOpt, nameOpt = Some(name))
      Subject("SubscriberClient", name, p => c.auth(p), () => c.clearCredentials(), () => c.clientSetName(name), () => c.quit())
    }
  )

  private def authCalls(admin: Client): Long =
    admin.info("commandstats").futureValue.get("cmdstat_auth")
      .map(_.split(',').head.stripPrefix("calls=").toLong).getOrElse(0L)

  private def killConnection(admin: Client, name: String): Unit = {
    val id = admin.clientList().futureValue.find(_.get("name") == Some(name)).get("id").toInt
    admin.clientKillWithFilters(idOpt = Some(id)).futureValue should be (1)
  }

  private def shouldReconnect(subject: Subject): Unit =
    eventually(timeout(Span(15, Seconds)), interval(Span(200, Millis))) {
      subject.probe().futureValue
    }

  subjectFactories.foreach { case (label, factory) =>
    s"${Auth.toString} on $label" when {
      "the server has no password and AUTH is given the empty string" should {
        "return the Redis error and keep the credentials" taggedAs (V100) in {
          val admin = Client()
          val subject = factory(6379, None)
          try {
            subject.probe().futureValue
            val calls = authCalls(admin)
            val failure = subject.auth("").failed.futureValue
            failure shouldBe an [RedisErrorResponseException]
            failure.getMessage should include ("without any password configured")
            // AUTH "" was sent as it is, once; a reconnect sends no AUTH: no credentials were stored
            killConnection(admin, subject.name)
            shouldReconnect(subject)
            authCalls(admin) - calls should be (1L)
          } finally {
            subject.quit().futureValue
            admin.quit().futureValue
          }
        }
      }
      "the password is wrong" should {
        "return the Redis error; a reconnect still authenticates with the previous credentials" taggedAs (V100) in {
          val admin = Client(port = 6380, authOpt = Some(AuthConfig(None, CorrectPassword)))
          val subject = factory(6380, Some(AuthConfig(None, CorrectPassword)))
          try {
            subject.probe().futureValue
            subject.auth("wrong-password").failed.futureValue shouldBe an [RedisErrorResponseException]
            killConnection(admin, subject.name)
            shouldReconnect(subject)
          } finally {
            subject.quit().futureValue
            admin.quit().futureValue
          }
        }
      }
      "the password is accepted" should {
        "authenticate again with it when the connection reconnects" taggedAs (V100) in {
          val admin = Client(port = 6380, authOpt = Some(AuthConfig(None, CorrectPassword)))
          val subject = factory(6380, Some(AuthConfig(None, CorrectPassword)))
          try {
            subject.probe().futureValue
            admin.configSet("requirepass", "rotated-password").futureValue should be (())
            subject.auth("rotated-password").futureValue should be (())
            // the old password is refused by the server now: the reconnect only works with the new credentials
            killConnection(admin, subject.name)
            shouldReconnect(subject)
          } finally {
            admin.configSet("requirepass", CorrectPassword).futureValue should be (())
            subject.quit().futureValue
            admin.quit().futureValue
          }
        }
      }
      "clearCredentials() has been called" should {
        "send no AUTH when the connection reconnects" taggedAs (V100) in {
          val admin = Client(port = 6380, authOpt = Some(AuthConfig(None, CorrectPassword)))
          val subject = factory(6380, Some(AuthConfig(None, CorrectPassword)))
          try {
            subject.probe().futureValue
            admin.configSet("requirepass", "").futureValue should be (())
            subject.clearCredentials()
            val calls = authCalls(admin)
            killConnection(admin, subject.name)
            shouldReconnect(subject)
            authCalls(admin) should be (calls)
          } finally {
            admin.configSet("requirepass", CorrectPassword).futureValue should be (())
            subject.quit().futureValue
            admin.quit().futureValue
          }
        }
      }
    }
  }

  "A connection built with AuthConfig(None, \"\") against a server without password" should {
    "report the Redis error, as given, and work as the server allows" taggedAs (V100) in {
      val name = uniqueName("creds-constructor")
      val c = Client(authOpt = Some(AuthConfig(None, "")), nameOpt = Some(name))
      try {
        c.ping().futureValue should be ("PONG")
        eventually {
          val errors = CapturingLogger.messagesFrom(name).filter(_.startsWith("Could not authenticate"))
          errors should not be empty
          errors.head should include ("without any password configured")
        }
      } finally {
        c.quit().futureValue
      }
    }
  }

  Echo.toString should {
    "echo back the message" taggedAs (V100) in {
      client.echo("Hello World -> 虫àéç蟲").futureValue should be ("Hello World -> 虫àéç蟲")
    }
  }

  Ping.toString should {
    "receive PONG" taggedAs (V100) in {
      client.ping().futureValue should be ("PONG")
    }
  }

  Quit.toString when {
    "quiting" should {
      "close the connection" taggedAs (V100) in {
        clientToQuit.quit().futureValue should be (())
        a [RedisIOException] should be thrownBy {
          clientToQuit.ping().!
        }
      }
    }
  }

  Select.toString when {
    "database index is negative" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.select(-1).!
        }
      }
    }
    "database index is too large" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy { 
          client.select(Integer.MAX_VALUE).!
        }
      }
    }
    "database index is valid" should {
      "succeed" taggedAs (V100) in {
        client.select(1).futureValue should be (())
      }
    }
  }

  override def afterAll(): Unit = {
    client.quit().!
    clientWithPassword.quit().!
  }
  
}
