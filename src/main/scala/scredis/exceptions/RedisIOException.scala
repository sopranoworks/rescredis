// Modified by Sopranoworks: no longer final; the base of RedisNotSentException and RedisOutcomeUnknownException.
package scredis.exceptions

/**
 * Wraps all IO exceptions.
 *
 * A request that fails because of its connection fails with one of its two subclasses, which say whether Redis may
 * have executed it: [[RedisNotSentException]] (certainly not) and [[RedisOutcomeUnknownException]] (unknown).
 */
case class RedisIOException(
  message: String = null,
  cause: Throwable = null
) extends RedisException(message, cause)
