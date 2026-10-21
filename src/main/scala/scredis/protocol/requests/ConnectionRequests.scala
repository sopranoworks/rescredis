// Modified by Sopranoworks: Scala 3 compatibility; adapted to Redis 7.4 (auth).
package scredis.protocol.requests

import scredis.protocol._
import scredis.stringReader

object ConnectionRequests {
  
  object Auth extends Command("AUTH")
  object Echo extends Command("ECHO")
  object Ping extends ZeroArgCommand("PING")
  object Quit extends ZeroArgCommand("QUIT")
  object Select extends Command("SELECT")
  
  case class Auth(password: String, username: Option[String])
      extends Request[Unit](Auth, authParams(username, password): _*) {
    override def decode = {  
      case SimpleStringResponse(value) => ()
    }
  }
  
  case class Echo(message: String) extends Request[String](Echo, message) {
    override def decode = {  
      case b: BulkStringResponse => b.flattened[String]
    }
  }
  
  case class Ping() extends Request[String](Ping) {
    override def decode = {  
      case SimpleStringResponse(value) => value
    }
  }
  
  case class Quit() extends Request[Unit](Quit) {
    override def decode = {  
      case SimpleStringResponse(value) => ()
    }
  }
  
  case class Select(database: Int) extends Request[Unit](Select, database) {
    override def decode = {  
      case SimpleStringResponse(value) => ()
    }
  }

  /**
   * The arguments of AUTH. The empty password without a username means "no password" and is sent
   * as `AUTH default ""`: the single-argument `AUTH ""` is refused by Redis 6.2 and later when the
   * server has no password, whereas the two-argument form is accepted for the default user when it
   * has none (and fails with WRONGPASS when the server does require a password).
   */
  private def authParams(username: Option[String], password: String): List[String] = {
    if (username.isEmpty && password.isEmpty) List("default", password)
    else List(username, Some(password)).flatten
  }
}