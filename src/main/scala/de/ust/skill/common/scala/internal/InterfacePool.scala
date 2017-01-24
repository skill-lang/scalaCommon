package de.ust.skill.common.scala.internal

import de.ust.skill.common.scala.api.SkillObject
import de.ust.skill.common.scala.internal.fieldTypes.UserType
import de.ust.skill.common.scala.api.ThrowException
import de.ust.skill.common.scala.api.ReplaceByNull
import de.ust.skill.common.scala.api.RecursiveInsert
import de.ust.skill.common.scala.api.ClosureException
import de.ust.skill.common.scala.internal.fieldTypes.AnnotationType

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
    extends UserType[T](superPool.typeID) {

  def all : Iterator[T] = realizations.map(_.all).reduce(_ ++ _)
  def allInTypeOrder : Iterator[T] = realizations.map(_.allInTypeOrder.asInstanceOf[Iterator[T]]).reduce(_ ++ _)

  def fields : Iterator[de.ust.skill.common.scala.api.FieldDeclaration[_]] = ???
  def allFields : Iterator[de.ust.skill.common.scala.api.FieldDeclaration[_]] = ???

  def apply(index : Int) : T = {
    var idx = index
    for (p ← realizations) {
      val size = p.size
      if (size > idx)
        return p(idx)
      else
        idx -= size;
    }
    null.asInstanceOf[T]
  }

  override def length : Int = realizations.map(_.length).reduce(_ + _)

  def reflectiveAllocateInstance : T = throw new NoSuchMethodError("One cannot create an instance of an interface.")
  val superName : Option[String] = Some(superPool.name)

  // Members declared in de.ust.skill.common.scala.internal.fieldTypes.FieldType  
  def closure(
    sf : de.ust.skill.common.scala.internal.SkillState,
    i : T, mode : de.ust.skill.common.scala.api.ClosureMode) : scala.collection.mutable.ArrayBuffer[de.ust.skill.common.scala.api.SkillObject] =
    throw new NoSuchMethodError("One cannot create a closure of an interface.")

  def requiresClosure : Boolean = false

  def offset(target : T) : Long = superPool.offset(target)
  def read(in : de.ust.skill.common.jvm.streams.InStream) : T = superPool.read(in).asInstanceOf[T]
  def write(target : T, out : de.ust.skill.common.jvm.streams.OutStream) : Unit = superPool.write(target, out)
}

/**
 * Same as interface Pool, but holds interfaces without static super classes.
 *
 * @author Timm Felden
 */
final class UnrootedInterfacePool[T <: SkillObject](
  final val name : String,
  final val superPool : AnnotationType,
  final val realizations : Array[StoragePool[_ <: T, _ <: SkillObject]])
    extends UserType[T](superPool.typeID) {

  def all : Iterator[T] = realizations.map(_.all).reduce(_ ++ _)
  def allInTypeOrder : Iterator[T] = realizations.map(_.allInTypeOrder.asInstanceOf[Iterator[T]]).reduce(_ ++ _)

  def fields : Iterator[de.ust.skill.common.scala.api.FieldDeclaration[_]] = ???
  def allFields : Iterator[de.ust.skill.common.scala.api.FieldDeclaration[_]] = ???

  def apply(index : Int) : T = {
    var idx = index
    for (p ← realizations) {
      val size = p.size
      if (size > idx)
        return p(idx)
      else
        idx -= size;
    }
    null.asInstanceOf[T]
  }

  override def length : Int = realizations.map(_.length).reduce(_ + _)

  def reflectiveAllocateInstance : T = throw new NoSuchMethodError("One cannot create an instance of an interface.")
  val superName : Option[String] = None

  // Members declared in de.ust.skill.common.scala.internal.fieldTypes.FieldType  
  def closure(
    sf : de.ust.skill.common.scala.internal.SkillState,
    i : T, mode : de.ust.skill.common.scala.api.ClosureMode) : scala.collection.mutable.ArrayBuffer[de.ust.skill.common.scala.api.SkillObject] =
    throw new NoSuchMethodError("One cannot create a closure of an interface.")
  def requiresClosure : Boolean = false

  def offset(target : T) : Long = superPool.offset(target)
  def read(in : de.ust.skill.common.jvm.streams.InStream) : T = superPool.read(in).asInstanceOf[T]
  def write(target : T, out : de.ust.skill.common.jvm.streams.OutStream) : Unit = superPool.write(target, out)
}