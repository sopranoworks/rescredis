// Modified by Sopranoworks: ported from Akka to Apache Pekko and Scala 3.
package scredis.exceptions

/**
 * Exception resulting from a transaction being aborted due to watched key(s).
 */
case object RedisTransactionAbortedException extends RedisException(
  "Transaction aborted due to watched key(s)"
)
