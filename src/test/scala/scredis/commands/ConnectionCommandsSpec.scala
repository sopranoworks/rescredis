// Modified by Sopranoworks: adapted to Redis 7.4.
package scredis.commands

import org.scalatest._
import org.scalatest.concurrent._
import scredis._
import scredis.exceptions._
import scredis.protocol.AuthConfig
import scredis.protocol.requests.ConnectionRequests._
import scredis.tags._
import scredis.util.TestUtils._
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

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

  s"${Auth.toString} with the empty string" when {
    "the server has no password" should {
      "succeed, clear the stored credentials and survive a reconnect" taggedAs (V100) in {
        val admin = Client(port = 6380, authOpt = Some(AuthConfig(None, CorrectPassword)))
        val victim = Client(port = 6380, authOpt = Some(AuthConfig(None, CorrectPassword)))
        try {
          victim.clientSetName("auth-empty").futureValue should be (())
          admin.configSet("requirepass", "").futureValue should be (())

          // `AUTH ""` is refused by Redis >= 6.2 when no password is set; "no password" must work
          victim.auth("").futureValue should be (())
          victim.ping().futureValue should be ("PONG")

          // force a reconnect: it only succeeds if no AUTH with the old password is sent
          val id = admin.clientList().futureValue.find(_.get("name") == Some("auth-empty")).get("id").toInt
          admin.clientKillWithFilters(idOpt = Some(id)).futureValue should be (1)
          eventually(timeout(Span(15, Seconds)), interval(Span(200, Millis))) {
            victim.ping().futureValue should be ("PONG")
          }
        } finally {
          admin.configSet("requirepass", CorrectPassword).futureValue should be (())
          victim.quit().futureValue
          admin.quit().futureValue
        }
      }
    }
    "the server has a password" should {
      "return an error" taggedAs (V100) in {
        val c = Client(port = 6380, authOpt = Some(AuthConfig(None, CorrectPassword)))
        try {
          a [RedisErrorResponseException] should be thrownBy {
            c.auth("").!
          }
        } finally {
          c.quit().futureValue
        }
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
