// Modified by Sopranoworks: adapted to Redis 7.4; no rewriting of credentials.
package scredis.protocol

case class AuthConfig(username: Option[String], password: String)
