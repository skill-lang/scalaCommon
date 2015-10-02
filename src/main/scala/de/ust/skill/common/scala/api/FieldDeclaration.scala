package de.ust.skill.common.scala.api

/**
 * @author Timm Felden
 */
abstract class FieldDeclaration[@specialized T] {
  def t : FieldType[T]
  def name : String

  /**
   * reflective get
   * @note it is silently assumed, that owner.contains(i)
   * @note known fields provide .get methods that are generally faster, because they exist without boxing
   */
  def getR(i : SkillObject) : T;

  /**
   * reflective set
   * @note it is silently assumed, that owner.contains(i)
   * @note known fields provide .get methods that are generally faster, because they exist without boxing
   */
  def setR(i : SkillObject, v : T) : Unit;

  /**
   * pretty to string method
   */
  override def toString : String = s"$t $name"
}