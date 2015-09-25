package de.ust.skill.common.scala.internal

import scala.collection.mutable.ArrayBuffer
import de.ust.skill.common.scala.SkillID
import de.ust.skill.common.scala.api
import de.ust.skill.common.scala.api.Access
import de.ust.skill.common.scala.api.SkillObject
import de.ust.skill.common.scala.api.FieldType
import de.ust.skill.common.scala.internal.fieldTypes.UserType
import de.ust.skill.common.jvm.streams.MappedOutStream
import de.ust.skill.common.jvm.streams.MappedInStream
import de.ust.skill.common.scala.internal.fieldTypes.V64

/**
 * @author Timm Felden
 */
sealed abstract class StoragePool[T <: B, B <: SkillObject](
  final val name : String,
  final val superPool : StoragePool[_ >: T <: B, B],
  _typeID : Int)
    extends UserType[T](_typeID) with Access[T] {

  /**
   * the index of this inside of the enclosing states types array
   */
  def poolIndex = typeID - 32

  def all : Iterator[T] = {
    ???
  }

  def allFields : Iterator[api.FieldDeclaration[_]] = {
    ???
  }

  def allInTypeOrder : Iterator[T] = {
    ???
  }

  def get(index : SkillID) : String = {
    ???
  }

  def iterator : Iterator[T] = {
    ???
  }

  val superName : Option[String] =
    if (null == superPool) None
    else Some(superPool.name)

  /**
   * the base pool of this type hierarchy
   */
  def basePool : BasePool[B]

  /**
   * the blocks that this type is involved in
   */
  val blocks = ArrayBuffer[Block]()

  /**
   * the cached dynamic size of this pool. Reliable if fixed, used by parser and writers.
   */
  var cachedSize : SkillID = 0
  /**
   * can be used to fix states, thereby making some operations (dynamic size) cacheable
   *
   * no instances can be added or deleted in a fixed state
   */
  var fixed = false

  /**
   * static fields of this type taking part in serialization
   */
  private[internal] val dataFields = ArrayBuffer[FieldDeclaration[_]]()

  def addField(fieldID : Int, t : FieldType[_], name : String) : FieldDeclaration[_] = ???

  def getById(id : SkillID) : T

  def read(in : MappedInStream) : T = getById(in.v64.toInt)

  def offset(target : T) : Long = V64.offset(target.skillID)

  def write(target : T, out : MappedOutStream) : Unit = out.v64(target.skillID)
}

class BasePool[B <: SkillObject](_name : String, _typeID : Int) extends StoragePool[B, B](_name, null, _typeID) {

  final override def basePool : BasePool[B] = this

  def getById(id : SkillID) : B = {
    ???
  }
}

class SubPool[T <: B, B <: SkillObject](_name : String, _superPool : StoragePool[_ >: T <: B, B], _typeID : Int)
    extends StoragePool[T, B](_name, _superPool, _typeID) {

  final override val basePool = superPool.basePool

  def getById(id : SkillID) : T = {
    ???
  }

}