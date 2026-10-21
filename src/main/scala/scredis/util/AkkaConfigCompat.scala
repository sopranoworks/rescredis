// Modified by Sopranoworks: added for the Akka configuration compatibility of the Pekko port.
package scredis.util

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.pekko.actor.ActorSystem

/**
 * Accepts configuration written for Akka wherever scredis receives configuration from outside.
 *
 * The canonical keys are `pekko` and `scredis.io.pekko`. A subtree written under `akka` (top level)
 * or `scredis.io.akka` is moved to the `pekko` path and laid over the given config, so the
 * `akka` value takes effect over reference defaults. Values are not rewritten.
 *
 * Used by [[scredis.RedisConfig]] and by the two places where scredis creates its own ActorSystem.
 */
private[scredis] object AkkaConfigCompat {

  /** Normalises a config rooted at `scredis` (as read by [[scredis.RedisConfig]]). */
  def normaliseScredis(config: Config): Config = moveSubtree(config, "io.akka", "io.pekko")

  /** Normalises a config rooted at the top level (as loaded for an ActorSystem). */
  def normaliseRoot(config: Config): Config =
    moveSubtree(moveSubtree(config, "akka", "pekko"), "scredis.io.akka", "scredis.io.pekko")

  /** Creates an ActorSystem owned by scredis, from the normalised application config. */
  def createActorSystem(name: String, root: Config = ConfigFactory.load()): ActorSystem =
    ActorSystem(name, normaliseRoot(root))

  /** B.withFallback(A), where A is `config` and B is the `from` subtree moved to `to`. */
  private def moveSubtree(config: Config, from: String, to: String): Config =
    if (config.hasPath(from)) {
      ConfigFactory.empty().withValue(to, config.getValue(from)).withFallback(config)
    } else {
      config
    }
}
