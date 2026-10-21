// Modified by Sopranoworks: adapted to Redis 7.4.
package scredis.protocol

case class AuthConfig(username: Option[String], password: String)

object AuthConfig {

  /**
   * The credentials to remember after an AUTH request: none for the empty password without a
   * username, which means "no password", so that a reconnect does not send AUTH.
   */
  private[scredis] def afterAuth(username: Option[String], password: String): Option[AuthConfig] =
    if (username.isEmpty && password.isEmpty) None else Some(AuthConfig(username, password))
}
