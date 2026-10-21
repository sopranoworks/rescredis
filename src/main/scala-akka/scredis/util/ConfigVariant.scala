package scredis.util

import akka.actor.ActorSystem
import com.typesafe.config.Config

/**
 * What differs between the Akka and the Pekko build of rescredis in the way configuration is read.
 * This is the Akka build: the canonical keys are `akka` and `scredis.io.akka`, and the configuration
 * is used as it is.
 */
private[scredis] object ConfigVariant {

  /** Name of the actor system block below `scredis.io`. */
  val IoKey: String = "akka"

  /** Normalises a config rooted at `scredis` (as read by [[scredis.RedisConfig]]). */
  def normaliseScredis(config: Config): Config = config

  /** Creates an ActorSystem owned by scredis. */
  def createActorSystem(name: String): ActorSystem = ActorSystem(name)
}
