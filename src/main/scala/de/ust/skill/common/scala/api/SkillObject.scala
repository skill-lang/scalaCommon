package de.ust.skill.common.scala.api

import de.ust.skill.common.scala.SkillID
import de.ust.skill.common.scala.SkillID

/**
 * The top of a skill-user-type hierarchy.
 *
 * @author Timm Felden
 */
abstract class SkillObject(protected var _skillID : SkillID) {

  /**
   * bindings may choose to reveal skill IDs
   */
  final protected[scala] def skillID : SkillID = _skillID

  /**
   * skill IDs are managed by code inside of this library
   */
  final private[scala] def skillID_=(v : SkillID) : Unit = _skillID = v

  /**
   * marks an object for deletion; it will be deleted on the next flush operation
   */
  final def delete : Unit = skillID = 0

  /**
   * checks for a deleted mark
   */
  final def markedForDeletion : Boolean = 0 == skillID

  /**
   * return the skill name of the skill type
   */
  def getTypeName : String

  /**
   * provides a pretty representation of this
   */
  def prettyString : String

  /**
   * reflective setter
   *
   * @param field a field declaration instance as obtained from the storage pools iterator
   * @param value the new value of the field
   *
   * @note if field is not a distributed field of this type, then anything may happen
   */
  final def set[@specialized T](field : FieldDeclaration[T], value : T) {
    field.setR(this, value)
  }

  /**
   * reflective getter
   *
   * @param field a field declaration instance as obtained from the storage pools iterator
   *
   * @note if field is not a distributed field of this type, then anything may happen
   */
  final def get[@specialized T](field : FieldDeclaration[T]) : T = field.getR(this)
}

/**
 * these objects store a type name in each instance
 * @author Timm Felden
 */
trait UnknownObject[T <: SkillObject] extends SkillObject {
  def owner : Access[_ <: T]
}

/**
 * This type is used, if we known nothing about a base type.
 * @author Timm Felden
 */
final class CompletelyUnknownObject(
  _skillID : SkillID,
  val owner : Access[CompletelyUnknownObject]) extends SkillObject(_skillID)
    with UnknownObject[CompletelyUnknownObject] {

  final def getTypeName : String = owner.name

  final def prettyString : String = s"$getTypeName#$skillID"

  final override def toString : String = s"unknown($getTypeName)#$skillID"
}