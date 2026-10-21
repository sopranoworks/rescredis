// Modified by Sopranoworks: added for Scala 3 (non-local returns replaced).
package scredis.util

import org.scalactic.Equality
import org.scalatest.enablers.Sequencing
import scredis.util.LinkedHashSet

import scala.util.boundary

object LinkedHashSetSequencing {
  implicit def linkedHashSetSequencing[A](implicit equality: Equality[A]): Sequencing[LinkedHashSet[A]] = new Sequencing[LinkedHashSet[A]] {
    override def containsInOrder(sequence: LinkedHashSet[A], eles: collection.Seq[Any]): Boolean = {
      val it = eles.iterator
      boundary[Boolean] {
        for (e <- sequence) {
          it.takeWhile(_ != e)
          if (it.hasNext) it.next()
          else boundary.break(false)
        }
        true
      }
    }

    override def containsInOrderOnly(sequence: LinkedHashSet[A], eles: collection.Seq[Any]): Boolean = {
      sequence.toList == eles.toList
    }

    override def containsTheSameElementsInOrderAs(leftSequence: LinkedHashSet[A], rightSequence: Iterable[Any]): Boolean =
      leftSequence.toList == rightSequence.toList
  }

}
