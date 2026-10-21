// Modified by Sopranoworks: Scala 3 compatibility.
package scredis.exceptions

/**
 * Exception resulting from a transaction being aborted due to watched key(s).
 */
case object RedisTransactionAbortedException extends RedisException(
  "Transaction aborted due to watched key(s)"
)
