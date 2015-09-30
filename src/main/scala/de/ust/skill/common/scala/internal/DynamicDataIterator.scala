package de.ust.skill.common.scala.internal

import scala.annotation.tailrec

/**
 * Iterates efficiently over dynamic instances of a pool.
 * @author Timm Felden
 */
final class DynamicDataIterator[T](private val p : StoragePool[T, _]) extends Iterator[T] {
  var blockIndex = 0
  val lastBlock = p.blocks.size - 1
  var index = 0
  var end = 0
  for (b ← p.blocks.headOption) {
    index = b.bpo
    end = index + b.dynamicCount
  }

  @tailrec
  @inline
  def hasNext : Boolean = {
    if (index < end)
      true
    else if (blockIndex < lastBlock) {
      blockIndex += 1
      index = p.blocks(blockIndex).bpo
      end = index + p.blocks(blockIndex).dynamicCount
      hasNext
    } else
      false
  }

  @inline
  def next() : T = {
    val r = p.data(index)
    index += 1
    r
  }
}