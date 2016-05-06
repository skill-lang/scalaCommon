package de.ust.skill.common.scala.internal

import de.ust.skill.common.scala.api.SkillObject
import de.ust.skill.common.scala.internal.fieldTypes.UserType
import de.ust.skill.common.scala.api.ThrowException
import de.ust.skill.common.scala.api.ReplaceByNull
import de.ust.skill.common.scala.api.RecursiveInsert
import de.ust.skill.common.scala.api.ClosureException

/**
 * Holds interface instances.
 * Serves as an API realization.
 * Ensures correctness of reflective type system.
 *
 * @author Timm Felden
 */
final class InterfacePool[T <: B, B <: SkillObject](
  final val name : String,
  final val superPool : StoragePool[_ >: T <: B, B],
  final val realizations : Array[StoragePool[_ <: T, _ <: B]])
    extends UserType[T](-1) {

  def all : Iterator[T] = realizations.map(_.all).reduce(_ ++ _)
  def allInTypeOrder : Iterator[T] = realizations.map(_.allInTypeOrder.asInstanceOf[Iterator[T]]).reduce(_ ++ _)

  def fields : Iterator[de.ust.skill.common.scala.api.FieldDeclaration[_]] = ???
  def allFields : Iterator[de.ust.skill.common.scala.api.FieldDeclaration[_]] = ???

  def apply(index : Int) : T =
    if (null != superPool) superPool(index).asInstanceOf[T]
    else throw new NoSuchMethodError("One cannot access an unrooted interface by index. Forgot \".all\"? ")

  override def length : Int = realizations.map(_.length).reduce(_ + _)

  def reflectiveAllocateInstance : T = throw new NoSuchMethodError("One cannot create an instance of an interface.")
  val superName : Option[String] =
    if (null == superPool) None
    else Some(superPool.name)

  // Members declared in de.ust.skill.common.scala.internal.fieldTypes.FieldType  
  def closure(
    sf : de.ust.skill.common.scala.internal.SkillState,
    i : T, mode : de.ust.skill.common.scala.api.ClosureMode) : scala.collection.mutable.ArrayBuffer[de.ust.skill.common.scala.api.SkillObject] =
    throw new NoSuchMethodError("One cannot create a closure of an interface.")
  def requiresClosure : Boolean = false

  def offset(target : T) : Long = throw new NoSuchMethodError("Interfaces do not participate in serialization.")
  def read(in : de.ust.skill.common.jvm.streams.InStream) : T = throw new NoSuchMethodError("Interfaces do not participate in serialization.")
  def write(target : T, out : de.ust.skill.common.jvm.streams.MappedOutStream) : Unit = throw new NoSuchMethodError("Interfaces do not participate in serialization.")
}