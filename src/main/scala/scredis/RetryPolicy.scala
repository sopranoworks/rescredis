// Copyright 2026 Sopranoworks
// SPDX-License-Identifier: Apache-2.0
package scredis

import java.util.concurrent.{Executors, ScheduledExecutorService, ThreadFactory, TimeUnit}

import scredis.exceptions.{RedisNotSentException, RedisOutcomeUnknownException}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

/**
 * What a retry helper (`withRetry` on [[scredis.Client]], [[scredis.Redis]], [[scredis.RedisCluster]],
 * [[scredis.BlockingClient]] and [[scredis.ClusterBlockingClient]]) retries, and for how long.
 *
 * An attempt that fails with a retryable error is made again once **that error's** availability
 * (`availability.whenAvailable`) has completed: the helper waits on it, it does not poll and does not sleep. It waits
 * at most for what is left of `maxWaitingDuration`, counted on the wall clock from the first attempt; when that is
 * used up, when the availability fails (the client was closed), or when `maxRetries` attempts have been retried, the
 * helper fails with the last error. An availability that is already completed when the error arrives (the
 * connection came back meanwhile) is retried at once.
 *
 * Retryable:
 *  - [[scredis.exceptions.RedisNotSentException]]: certainly not executed. Always.
 *  - [[scredis.exceptions.RedisOutcomeUnknownException]]: maybe executed. Only with `retryOutcomeUnknown`, which is
 *    the caller's statement that the attempt is idempotent (executing it twice is harmless).
 *  - Redis error replies ([[scredis.exceptions.RedisErrorResponseException]], ...) and every other error: never.
 *
 * @param maxWaitingDuration the longest the helper waits, in all, from its first attempt; `Duration.Inf`: no limit
 * @param retryOutcomeUnknown retry an attempt whose outcome is unknown too (the attempt is idempotent)
 * @param maxRetries the most retries; `None`: no limit but `maxWaitingDuration`
 *
 * @since 1.1.0
 */
final case class RetryPolicy(
  maxWaitingDuration: Duration = Duration.Inf,
  retryOutcomeUnknown: Boolean = false,
  maxRetries: Option[Int] = None
) {

  /** This policy, retrying an attempt whose outcome is unknown too: the caller's statement that it is idempotent */
  def idempotent: RetryPolicy = copy(retryOutcomeUnknown = true)

  /** True when an attempt that failed with `e` is retried */
  def isRetryable(e: Throwable): Boolean = e match {
    case _: RedisNotSentException => true
    case _: RedisOutcomeUnknownException => retryOutcomeUnknown
    case _ => false
  }

  private def availabilityOf(e: Throwable): Option[Availability] = e match {
    case n: RedisNotSentException => Some(n.availability)
    case u: RedisOutcomeUnknownException => Some(u.availability)
    case _ => None
  }

  /** What is left of `maxWaitingDuration` after `start` (`None`: no limit) */
  private def left(start: Long): Option[FiniteDuration] = maxWaitingDuration match {
    case limit: FiniteDuration => Some((limit.toNanos - (System.nanoTime() - start)).nanos)
    case _ => None
  }

  private def mayRetry(e: Throwable, retries: Int): Boolean = isRetryable(e) && maxRetries.forall(retries < _)

  /**
   * Runs `attempt` until it succeeds or fails with an error that is not retried (see the class documentation).
   *
   * @param attempt makes one attempt; it is called again for every retry
   */
  def retry[T](attempt: () => Future[T])(implicit ec: ExecutionContext): Future[T] = {
    val start = System.nanoTime()
    def run(retries: Int): Future[T] = Future.fromTry(Try(attempt())).flatten.recoverWith {
      case e if mayRetry(e, retries) => availabilityOf(e) match {
        case Some(availability) => left(start) match {
          case Some(time) if time <= Duration.Zero => Future.failed(e)
          case limit => RetryPolicy.within(availability.whenAvailable, limit).flatMap { available =>
            if (available) run(retries + 1) else Future.failed(e)
          }
        }
        case None => Future.failed(e)
      }
    }
    run(0)
  }

  /**
   * Runs `attempt` until it succeeds or fails with an error that is not retried (see the class documentation), on
   * the calling thread: between attempts the thread waits on the error's availability. An exception thrown by
   * `attempt` counts as its failure.
   *
   * @param attempt makes one attempt; it is called again for every retry
   */
  def retryBlocking[T](attempt: () => Try[T]): Try[T] = {
    val start = System.nanoTime()
    var retries = 0
    var result: Option[Try[T]] = None
    while (result.isEmpty) {
      Try(attempt()).flatten match {
        case Failure(e) if mayRetry(e, retries) && availabilityOf(e).isDefined =>
          val waited = left(start) match {
            case Some(time) if time <= Duration.Zero => false
            case limit => Try(Await.ready(availabilityOf(e).get.whenAvailable, limit.getOrElse(Duration.Inf))) match {
              case Success(ready) => ready.value.exists(_.isSuccess)
              case Failure(_) => false // Await's TimeoutException: the waiting duration is used up
            }
          }
          if (waited) retries += 1 else result = Some(Failure(e))
        case other => result = Some(other)
      }
    }
    result.get
  }
}

/** @since 1.1.0 */
object RetryPolicy {

  /** Retries what was not sent, with no limit on the waiting */
  val Default: RetryPolicy = RetryPolicy()

  private lazy val timer: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory {
    def newThread(r: Runnable): Thread = {
      val t = new Thread(r, "rescredis-retry-timer")
      t.setDaemon(true)
      t
    }
  })

  /** True once `f` has succeeded, false when it fails or `limit` passes first */
  private def within(f: Future[Unit], limit: Option[FiniteDuration])(implicit ec: ExecutionContext): Future[Boolean] =
    limit match {
      case None => f.transform(r => Success(r.isSuccess))
      case Some(time) =>
        val p = Promise[Boolean]()
        val timeout = timer.schedule(new Runnable { def run(): Unit = { p.trySuccess(false); () } }, time.toNanos, TimeUnit.NANOSECONDS)
        f.onComplete { r =>
          timeout.cancel(false)
          p.trySuccess(r.isSuccess)
        }
        p.future
    }
}
