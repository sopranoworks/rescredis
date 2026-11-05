// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
package scredis.exceptions

import scredis.Availability

/**
 * The request was not sent to Redis: it has certainly not been executed, and sending it again is safe.
 *
 * A request fails with it when there is no connection (the connection could not be made, or the client waits for
 * its next reconnect attempt), when it is addressed to a node or a slot's master that is known to be down, with
 * `fail-command-on-connecting` while connecting, after the connection was closed (QUIT), and on a cluster after
 * `CLUSTERDOWN` / `TRYAGAIN` replies when the retries are used up.
 *
 * @param availability completes when the connection, or the master of the key's slot, is usable again
 *
 * @since 1.1.0
 */
final class RedisNotSentException(
  message: String,
  val availability: Availability,
  cause: Throwable = null
) extends RedisIOException(message, cause)
