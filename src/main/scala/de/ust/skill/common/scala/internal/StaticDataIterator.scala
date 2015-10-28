package de.ust.skill.common.scala.internal

import scala.annotation.tailrec

/**
 * Iterates efficiently over static instances of a pool.
 *
 * @author Timm Felden
 */
final class StaticDataIterator[T](private val p : StoragePool[T, _]) extends Iterator[T] {
  private var secondIndex = 0
  private val lastBlock = p.blocks.size
  private var index = 0
  private var end = 0
  while (index == end && secondIndex < lastBlock) {
    val b = p.blocks(secondIndex)
    index = b.bpo
    end = index + b.staticCount
    secondIndex += 1
  }
  // mode switch, if there is no other block
  if (index == end)
    secondIndex = 0

  @inline
  def hasNext : Boolean = (index < end || secondIndex < p.newObjects.size);

  @inline
  def next() : T = {
    if (index < end) {
      val r = p.data(index)
      index += 1
      while (index == end && secondIndex < lastBlock) {
        val b = p.blocks(secondIndex)
        index = b.bpo
        end = index + b.staticCount
        secondIndex += 1
      }
      // mode switch, if there is no other block
      if (index == end)
        secondIndex = 0
      r.asInstanceOf[T]
    } else {
      val r = p.newObjects(secondIndex)
      secondIndex += 1
      r.asInstanceOf[T]
    }
  }
}