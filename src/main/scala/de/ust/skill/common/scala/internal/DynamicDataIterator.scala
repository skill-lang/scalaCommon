package de.ust.skill.common.scala.internal

import scala.annotation.tailrec
import de.ust.skill.common.scala.api.SkillObject

/**
 * Iterates efficiently over dynamic instances of a pool.
 * @author Timm Felden
 */
final class DynamicDataIterator[T <: B, B <: SkillObject](val p : StoragePool[T, B]) extends Iterator[T] {
  private var ts = new TypeHierarchyIterator(p)
  private var secondIndex = 0
  private val lastBlock = p.blocks.size
  private var index = 0
  private var end = 0
  while (index == end && secondIndex < lastBlock) {
    val b = p.blocks(secondIndex)
    index = b.bpo
    end = index + b.dynamicCount
    secondIndex += 1
  }
  private var newObjects : Iterator[T] = null
  // mode switch, if there is no other block
  if (index == end) {
    secondIndex = 0
    while (ts.hasNext && null == newObjects) {
      val t = ts.next
      if (t.newObjects.size != 0)
        newObjects = t.newObjects.iterator
    }
    if (null == newObjects)
      ts = null
  }

  @inline
  def hasNext : Boolean = null != ts

  @inline
  def next() : T = {
    if (index < end) {
      val r = p.data(index)
      index += 1
      while (index == end && secondIndex < lastBlock) {
        val b = p.blocks(secondIndex)
        index = b.bpo
        end = index + b.dynamicCount
        secondIndex += 1
      }
      // mode switch, if there is no other block
      if (index == end) {
        secondIndex = 0
        while (ts.hasNext && null == newObjects) {
          val t = ts.next
          if (t.newObjects.size != 0)
            newObjects = t.newObjects.iterator
        }
        if (null == newObjects)
          ts = null
      }
      r.asInstanceOf[T]
    } else {
      val r = newObjects.next
      if (!newObjects.hasNext) {
        newObjects = null
        while (ts.hasNext && null == newObjects) {
          val t = ts.next
          if (t.newObjects.size != 0)
            newObjects = t.newObjects.iterator
        }
        if (null == newObjects)
          ts = null
      }
      r.asInstanceOf[T]
    }
  }
}