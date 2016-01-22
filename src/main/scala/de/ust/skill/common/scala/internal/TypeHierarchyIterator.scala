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
  private val endHeight = base.typeHierarchyHeight
  private var current : StoragePool[_, B] = base

  @inline
  def hasNext : Boolean = null != current

  @inline
  def next() : StoragePool[T, B] = {
    val r = current
    val n = current.nextPool
    if (null != n && endHeight < n.typeHierarchyHeight)
      current = n
    else
      current = null
    r.asInstanceOf[StoragePool[T, B]]
  }
}