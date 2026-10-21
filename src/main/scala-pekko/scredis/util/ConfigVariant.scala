// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
// Pekko variant of ConfigVariant; the Akka build has a pass-through of the same name.
package scredis.util

import com.typesafe.config.Config
import org.apache.pekko.actor.ActorSystem

/**
 * What differs between the Akka and the Pekko build of rescredis in the way configuration is read.
 * This is the Pekko build: the canonical keys are `pekko` and `scredis.io.pekko`; configuration
 * written for Akka is accepted through [[AkkaConfigCompat]].
 */
private[scredis] object ConfigVariant {

  /** Name of the actor system block below `scredis.io`. */
  val IoKey: String = "pekko"

  /** Normalises a config rooted at `scredis` (as read by [[scredis.RedisConfig]]). */
  def normaliseScredis(config: Config): Config = AkkaConfigCompat.normaliseScredis(config)

  /** Creates an ActorSystem owned by scredis. */
  def createActorSystem(name: String): ActorSystem = AkkaConfigCompat.createActorSystem(name)
}
