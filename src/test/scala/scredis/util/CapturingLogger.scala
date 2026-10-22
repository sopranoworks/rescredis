// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
package scredis.util

import java.util.concurrent.ConcurrentLinkedQueue

import akka.actor.Actor
import akka.event.Logging
import akka.event.Logging.{InitializeLogger, LoggerInitialized}

/**
 * Event stream logger that keeps the info and error messages of the actors, enabled by `loggers` in the test
 * `application.conf`. Lets a test observe what an actor reports about the way it was configured.
 */
class CapturingLogger extends Actor {
  def receive: Receive = {
    case InitializeLogger(_) => sender() ! LoggerInitialized
    case info: Logging.Info => CapturingLogger.record(info.logSource, String.valueOf(info.message))
    case error: Logging.Error =>
      val cause = Option(error.cause).map(c => s": ${c.getMessage}").getOrElse("")
      CapturingLogger.record(error.logSource, s"${error.message}$cause")
  }
}

object CapturingLogger {
  final case class Entry(source: String, message: String)

  private val entries = new ConcurrentLinkedQueue[Entry]()

  private def record(source: String, message: String): Unit = entries.add(Entry(source, message))

  /** The messages logged by actors whose path contains `part`, oldest first. */
  def messagesFrom(part: String): List[String] =
    entries.toArray(new Array[Entry](0)).toList.filter(_.source.contains(part)).map(_.message)
}
