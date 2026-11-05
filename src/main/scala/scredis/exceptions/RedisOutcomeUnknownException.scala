// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
package scredis.exceptions

import scredis.Availability

/**
 * The request was written, and the connection was lost (or its reply did not arrive within the receive timeout or a
 * blocking call's deadline) before its reply arrived: Redis may or may not have executed it. The library does not
 * send it again; send it again only if executing it twice is harmless.
 *
 * @param availability completes when the connection, or the master of the key's slot, is usable again
 *
 * @since 1.1.0
 */
final class RedisOutcomeUnknownException(
  message: String,
  val availability: Availability,
  cause: Throwable = null
) extends RedisIOException(message, cause)
