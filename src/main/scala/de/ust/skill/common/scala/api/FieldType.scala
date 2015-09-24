package de.ust.skill.common.scala.api

/**
 * Field types as used in reflective access.
 *
 * @author Timm Felden
 *
 * @param <T>
 *            (boxed) runtime type of target objects
 */
trait FieldType[T] {

  /**
   * @return the ID of this type (respective to the state in which it lives)
   */
  def typeID : Int

  /**
   * @return yielding the skill representation of the type
   */
  override def toString : String;
}