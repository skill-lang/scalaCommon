package de.ust.skill.common.scala.internal

import scala.annotation.tailrec
import de.ust.skill.common.scala.api.SkillObject

/**
 * Iterates efficiently over dynamic instances of a pool.
 * @author Timm Felden
 */
final class DynamicNewInstancesIterator[T <: B, B <: SkillObject](p : StoragePool[T, B]) extends Iterator[T] {
  private var ts = new TypeHierarchyIterator(p)
  private var is : Iterator[T] = {
    var r : Iterator[T] = null
    while (null == r && ts.hasNext) {
      val t = ts.next
      if (t.newObjects.size != 0)
        r = t.newObjects.iterator
    }
    r
  }

  @inline
  def hasNext : Boolean = null != is

  @inline
  def next() : T = {
    val result = is.next.asInstanceOf[T]
    if (!is.hasNext) {
      var r : Iterator[T] = null
      while (null == r && ts.hasNext) {
        val t = ts.next
        if (t.newObjects.size != 0)
          r = t.newObjects.iterator
      }
      is = r
    }

    result
  }
}