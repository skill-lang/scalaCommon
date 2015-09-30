package de.ust.skill.common.scala.internal

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import de.ust.skill.common.jvm.streams.MappedInStream
import de.ust.skill.common.jvm.streams.MappedOutStream
import de.ust.skill.common.scala.api
import de.ust.skill.common.scala.api.Access
import de.ust.skill.common.scala.api.SkillObject
import de.ust.skill.common.scala.internal.fieldTypes.FieldType
import de.ust.skill.common.scala.internal.fieldTypes.UserType
import de.ust.skill.common.scala.internal.fieldTypes.V64
import de.ust.skill.common.scala.SkillID

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

  override def iterator : Iterator[T] = all
  def all : Iterator[T] = new DynamicDataIterator[T](this) ++ newDynamicInstances

  def staticInstances : Iterator[T] = new StaticDataIterator[T](this)

  def allInTypeOrder : Iterator[T] = subPools.foldLeft(staticInstances)(_ ++ _.allInTypeOrder)

  def allFields : Iterator[api.FieldDeclaration[_]] = {
    ???
  }

  def apply(index : Int) : T = {
    ???
  }

  /**
   * All stored objects, which have exactly the type T. Objects are stored as arrays of field entries. The types of the
   *  respective fields can be retrieved using the fieldTypes map.
   */
  protected val newObjects = new ArrayBuffer[T]()
  protected def newDynamicInstances : Iterator[T] = subPools.foldLeft(newObjects.iterator)(_ ++ _.newDynamicInstances)

  val superName : Option[String] =
    if (null == superPool) None
    else Some(superPool.name)

  /**
   * the base pool of this type hierarchy
   */
  def basePool : BasePool[B]

  /**
   * the sub pools are constructed during construction of all storage pools of a state
   */
  private[internal] val subPools = new ArrayBuffer[SubPool[_ <: T, B]];
  // update sub-pool relation
  this match {
    case t : SubPool[T, B] ⇒
      // @note: we drop the super type, because we can not know what it is in general
      superPool.subPools.asInstanceOf[ArrayBuffer[SubPool[_, B]]] += t
    case _ ⇒
  }

  /**
   * create a sub pool for an unknown sub type
   */
  def makeSubPool(name : String, typeId : Int) : SubPool[_ <: T, B]

  /**
   * the blocks that this type is involved in
   */
  final val blocks = new ArrayBuffer[Block]()

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
   * number of static instances stored in data
   */
  protected[internal] var staticDataInstnaces : SkillID = 0
  override def length : Int = {
    if (fixed)
      cachedSize
    else {
      subPools.foldLeft(staticSize)(_ + _.length)
    }
  }

  @inline
  protected def staticSize : SkillID = {
    staticDataInstnaces + newObjects.length
  }

  /**
   * static fields of this type not taking any part in serialization
   *
   * TODO provide a sane implementation
   */
  private[internal] val autoFields = new ArrayBuffer[AutoField[_, T]]()

  /**
   * static fields of this type taking part in serialization
   */
  private[internal] val dataFields = new ArrayBuffer[FieldDeclaration[_, T]]()

  /**
   * add a field to the pool, known fields are treated specially
   */
  def addField[R : Manifest](fieldID : Int, t : FieldType[R], name : String) : FieldDeclaration[R, T] = {
    val f = new LazyField[R, T](t, name, fieldID, this);
    // TODO field restrictions
    //        for (FieldRestriction<?> r : restrictions)
    //            f.addRestriction(r);
    dataFields.append(f);
    return f;
  }

  /**
   * that took part in serialization
   * already
   * @note this is in fact an array of [B], but all sane access will be type correct :)
   */
  final var data : Array[T] = _
  /**
   * a total function, that will either return the correct object or null
   */
  @inline final private[internal] def getById(id : SkillID) : T = {
    if (id < 1 || data.length <= id)
      null.asInstanceOf[T]
    else
      data(id - 1)
  }

  /**
   * allocate a new instance via reflection
   */
  def reflectiveAllocateInstance : T

  /**
   * ensure that data is set correctly
   */
  def allocateData : Unit

  /**
   * ensure that instances are created correctly
   *
   * @note will parallelize over blocks and can be invoked in parallel
   */
  def allocateInstances : Unit

  final def read(in : MappedInStream) : T = getById(in.v64.toInt)

  final def offset(target : T) : Long = V64.offset(target.skillID)

  final def write(target : T, out : MappedOutStream) : Unit = out.v64(target.skillID)

  /**
   * override stupid inherited equals method
   */
  final override def equals(obj : Any) : Boolean = obj match {
    case o : StoragePool[_, _] ⇒ eq(o)
    case _                     ⇒ false
  }
  /**
   * override hashcode as well
   */
  final override def hashCode : Int = this.typeID

  /**
   * override to string, such that it produces skill types
   */
  final override def toString : String = name
}

abstract class BasePool[B <: SkillObject](_name : String, _typeID : Int) extends StoragePool[B, B](_name, null, _typeID) {

  final override def basePool : BasePool[B] = this
}

abstract class SubPool[T <: B, B <: SkillObject](_name : String, _superPool : StoragePool[_ >: T <: B, B], _typeID : Int)
    extends StoragePool[T, B](_name, _superPool, _typeID) {

  final override val basePool = superPool.basePool

  final override def allocateData : Unit = this.data = basePool.data.asInstanceOf[Array[T]]
}