package de.ust.skill.common.scala.internal

import scala.annotation.tailrec
import de.ust.skill.common.scala.api.SkillObject

/**
 * Iterates efficiently over dynamic instances of a pool.
 * @author Timm Felden
 */
final class TypeOrderIterator[T <: B, B <: SkillObject](p : StoragePool[T, B]) extends Iterator[T] {
  private val ts = new TypeHierarchyIterator(p)
  private var is : StaticDataIterator[T] = {
    var r : StaticDataIterator[T] = null
    while (null == r && ts.hasNext) {
      val t = ts.next
      if (t.staticSize > t.deletedCount)
        r = new StaticDataIterator(t)
    }
    r
  }

  @inline
  def hasNext : Boolean = null != is

  @inline
  def next() : T = {
    val result = is.next.asInstanceOf[T]
    if (!is.hasNext) {
      var r : StaticDataIterator[T] = null
      while (null == r && ts.hasNext) {
        val t = ts.next
        if (t.staticSize > t.deletedCount)
          r = new StaticDataIterator(t)
      }
      is = r
    }

    result
  }
}