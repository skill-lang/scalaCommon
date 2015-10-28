package de.ust.skill.common.scala.internal

import scala.annotation.tailrec
import de.ust.skill.common.scala.api.SkillObject
import scala.collection.mutable.ArrayBuffer

/**
 * Iterates efficiently over the type hierarchy
 *
 * @author Timm Felden
 */
final class TypeHierarchyIterator[T <: B, B <: SkillObject](base : StoragePool[T, B]) extends Iterator[StoragePool[T, B]] {
  private val endParent = base.superPool
  private var current : StoragePool[_, B] = base

  @inline
  def hasNext : Boolean = null != current

  {
    val n = current.nextPool
    null != n && endParent != n.superPool
  }

  @inline
  def next() : StoragePool[T, B] = {
    val r = current
    val n = current.nextPool
    if (null != n && endParent != n.superPool)
      current = n
    else
      current = null
    r.asInstanceOf[StoragePool[T, B]]
  }
}