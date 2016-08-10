package de.ust.skill.common.scala.api

import scala.reflect.ClassTag
import de.ust.skill.common.scala.SkillID

/**
 * @author Timm Felden
 */
trait Access[T <: SkillObject] extends IndexedSeq[T] with FieldType[T] {
  /**
   * the SKilL name of T
   */
  val name : String

  /**
   * the SKilL name of the super type of T, if any
   */
  val superName : Option[String]

  /**
   * allocate a new instance via reflection
   */
  def reflectiveAllocateInstance : T

  /**
   * get n'th element of dynamic type T
   *
   * @note in general this is unrelated to the skill id of the object
   */
  def apply(index : Int) : T

  /**
   * @return iterator over all instances of T
   */
  def all : Iterator[T]
  /**
   * just for convenience
   */
  def iterator : Iterator[T]
  /**
   * @return a type ordered Container iterator over all instances of T
   */
  def allInTypeOrder : Iterator[T]

  /**
   * @return an iterator over all field declarations, even those provided by the binary skill file
   */
  def fields : Iterator[FieldDeclaration[_]]

  /**
   * @return an iterator over all field declarations, even those provided by the binary skill file, including fields
   * declared in super types
   */
  def allFields : Iterator[FieldDeclaration[_]]

  override def length : Int
  final override def foreach[U](f : T ⇒ U) {
    for (x ← all)
      f(x)
  }
}

trait StringAccess extends Iterable[String] with FieldType[String] {
  def get(index : SkillID) : String

  /**
   * adds a string to the pool; using the result instead of the argument may improve overall performance
   */
  def add(string : String) : Unit
  /**
   * @note the iterator will not cause lazy strings to be unpacked!
   */
  def iterator : Iterator[String]
  def size : Int
}