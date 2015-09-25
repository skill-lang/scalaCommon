package de.ust.skill.common.scala.internal

import de.ust.skill.common.scala.api

/**
 * @author Timm Felden
 */
abstract class FieldDeclaration[T] extends api.FieldDeclaration[T] {

  def addChunk(c : Chunk) : Unit = ???
}