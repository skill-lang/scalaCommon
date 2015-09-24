package de.ust.skill.common.scala.internal

import de.ust.skill.common.scala.api.Access
import de.ust.skill.common.scala.api.SkillObject
import de.ust.skill.common.scala.api.FieldDeclaration
import de.ust.skill.common.scala.SkillID

/**
 * @author Timm Felden
 */
class StoragePool[T <: B, B <: SkillObject](override val typeID : Int) extends Access[T] {
  ???

  def all: Iterator[T] = {
    ???
  }

  def allFields: Iterator[FieldDeclaration[_]] = {
    ???
  }

  def allInTypeOrder: Iterator[T] = {
    ???
  }

  def get(index: SkillID): String = {
    ???
  }

  def iterator: Iterator[T] = {
    ???
  }

  val name: String = ???

  val superName: Option[String] = ???
}