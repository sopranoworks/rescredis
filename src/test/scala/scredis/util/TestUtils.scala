// Modified by Sopranoworks: ported from Akka to Apache Pekko and Scala 3.
package scredis.util

import org.apache.pekko.actor.ActorSystem

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object TestUtils {
  implicit val system: ActorSystem = ActorSystem("test")
  
  final implicit class RichFuture[A](future: Future[A]) {
    def ! : A = Await.result(future, Duration.Inf)
  }
  
}
