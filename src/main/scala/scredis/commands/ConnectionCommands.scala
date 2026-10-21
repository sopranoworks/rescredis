// Modified by Sopranoworks: adapted to Redis 7.4.
package scredis.commands

import scredis.io.NonBlockingConnection
import scredis.protocol.requests.ConnectionRequests._

import scala.concurrent.Future

/**
 * This trait implements connection commands.
 *
 * @define e [[scredis.exceptions.RedisErrorResponseException]]
 * @define none `None`
 * @define true '''true'''
 * @define false '''false'''
 */
trait ConnectionCommands { self: NonBlockingConnection =>
  
  /**
   * Authenticates to the server.
   * 
   * @note Use the empty string (without a username) to re-authenticate with no password. The request is
   *       sent as `AUTH default ""`, which succeeds when the server has no password set and fails when it
   *       has one, and the stored credentials are cleared, so that a reconnect does not send `AUTH`.
   *       (`AUTH ""` is refused by Redis 6.2 and later when no password is set.)
   *
   * @param password the server password
   * @throws $e if authentication failed
   *
   * @since 1.0.0
   */
  def auth(password: String, username: Option[String] = None): Future[Unit] = send(Auth(password, username))
  
  /**
   * Echoes the given string on the server.
   *
   * @param message	the message to echo
   * @return the echoed message
   *
   * @since 1.0.0
   */
  def echo(message: String): Future[String] = send(Echo(message))

  /**
   * Pings the server. This command is often used to test if a connection is still alive,
   * or to measure latency.
   *
   * @return PONG
   *
   * @since 1.0.0
   */
  def ping(): Future[String] = send(Ping())

  /**
   * Closes the connection.
   *
   * @since 1.0.0
   */
  def quit(): Future[Unit] = send(Quit())

  /**
   * Changes the selected database on the current connection.
   *
   * @param database database index
   * @throws $e if the database index is invalid
   *
   * @since 1.0.0
   */
  def select(database: Int): Future[Unit] = send(Select(database))
  
}