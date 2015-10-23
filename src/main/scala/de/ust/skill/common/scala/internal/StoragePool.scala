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
import de.ust.skill.common.jvm.streams.InStream
import de.ust.skill.common.scala.internal.restrictions.FieldRestriction
import scala.collection.mutable.HashSet
import de.ust.skill.common.scala.internal.restrictions.FieldRestriction
import java.util.Arrays
import java.util.concurrent.ConcurrentHashMap

/**
 * @author Timm Felden
 */
sealed abstract class StoragePool[T <: B, B <: SkillObject](
  final val name : String,
  final val superPool : StoragePool[_ >: T <: B, B],
  _typeID : Int)
    extends UserType[T](_typeID.ensuring(_ >= 32, "user types have IDs larger then 32")) {

  def getInstanceClass : Class[T]

  /**
   * the index of this inside of the enclosing states types array
   */
  def poolIndex = typeID - 32
  private[internal] final def typeID_=(id : Int) = __typeID = id

  override def iterator : Iterator[T] = all
  final def all : Iterator[T] = new DynamicDataIterator[T](this) ++ newDynamicInstances

  final def staticInstances : Iterator[T] = new StaticDataIterator[T](this) ++ newObjects.iterator

  final def allInTypeOrder : Iterator[T] = subPools.foldLeft(staticInstances)(_ ++ _.allInTypeOrder)

  final def fields : Iterator[api.FieldDeclaration[_]] = autoFields.iterator ++ dataFields.iterator
  final def allFields : Iterator[api.FieldDeclaration[_]] = if (null != superPool)
    superPool.allFields ++ autoFields.iterator ++ dataFields.iterator
  else fields

  @inline
  final def apply(idx : Int) : T = {
    var index = idx
    for (b ← blocks) {
      if (index < b.dynamicCount)
        return data(b.bpo + index).asInstanceOf[T]
      else
        index -= b.dynamicCount
    }
    for (sub ← typeHierarchy) {
      val n = sub.newObjects
      if (index < n.size)
        return n(index)
      else
        index -= n.size
    }
    throw new IndexOutOfBoundsException
  }

  /**
   * All stored objects, which have exactly the type T. Objects are stored as arrays of field entries. The types of the
   *  respective fields can be retrieved using the fieldTypes map.
   */
  final protected var newObjects = new ArrayBuffer[T]()
  final private[internal] def newObjectsSize = newObjects.size
  final protected def newDynamicInstances : Iterator[T] = subPools.foldLeft(newObjects.iterator)(_ ++ _.newDynamicInstances)
  final protected def newDynamicInstancesSize : Int = subPools.foldLeft(newObjects.size)(_ + _.newDynamicInstancesSize)

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
   * returns an iterator over this Pool and all subPools
   */
  protected def typeHierarchy : Seq[StoragePool[_ <: T, B]] = subPools.foldLeft(
    Seq[StoragePool[_ <: T, B]](this))(_ ++ _.typeHierarchy)

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
  private var fixed = false

  /**
   * sets fixed for this pool; automatically adjusts sub/super pools
   *
   * @note takes deletedCount into account, thus the size may decrease by fixing
   */
  def fix(setFixed : Boolean) {
    // only do something if there is action required
    if (setFixed != fixed) {
      fixed = setFixed
      if (fixed) {
        subPools.foreach(_.fix(true))
        cachedSize = subPools.foldLeft(staticSize)(_ + _.cachedSize) - deletedCount
      } else if (superPool != null) {
        superPool.fix(false)
      }
    }
  }

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
  protected[internal] def staticSize : SkillID = {
    staticDataInstnaces + newObjects.length
  }

  /**
   * number of deleted instancances currently stored in this pool
   */
  private var deletedCount = 0;
  /**
   * Delete shall only be called from skill state
   *
   * @param target
   *            the object to be deleted
   * @note we type target using the erasure directly, because the Java type system is too weak to express correct
   *       typing, when taking the pool from a map
   */
  @inline
  private[internal] final def delete(target : SkillObject) {
    // @note we do not need null check or 0 check, because both happen in SkillState
    target.skillID = 0
    deletedCount += 1
  }

  /**
   * static fields of this type not taking any part in serialization
   */
  protected[internal] val autoFields = new ArrayBuffer[AutoField[_, T]]()

  /**
   * static fields of this type taking part in serialization
   */
  protected[internal] val dataFields = new ArrayBuffer[FieldDeclaration[_, T]]()

  /**
   * add a field to the pool, known fields are treated specially
   */
  def addField[R : Manifest](fieldID : Int,
                             t : FieldType[R],
                             name : String,
                             restrictions : HashSet[FieldRestriction]) : FieldDeclaration[R, T] = {
    val f = new LazyField[R, T](t, name, fieldID, this);
    f.restrictions ++= restrictions
    dataFields.append(f);
    return f;
  }

  /**
   * ensures that all known fields are present in respective arrays
   * @note internal use only
   */
  def ensureKnownFields(state : SkillState) : Unit

  /**
   * that took part in serialization
   * already
   * @note this is in fact an array of [B], but all sane access will be type correct :)
   * @note internal use only!
   */
  final var data : Array[B] = _
  /**
   * a total function, that will either return the correct object or null
   */
  @inline final private[internal] def getById(id : SkillID) : T = {
    if (id < 1 || data.length < id)
      null.asInstanceOf[T]
    else
      data(id - 1).asInstanceOf[T]
  }

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

  /**
   * update blocks to reflect actual layout of data
   */
  protected final def updateAfterCompress(lbpoMap : Array[Int]) {
    // update pointer to data
    this.data = basePool.data
    blocks.clear()
    // pools without instances wont be written to disk
    if (0 != cachedSize) {
      blocks.append(new Block(0, lbpoMap(poolIndex), staticSize, cachedSize))
      subPools.foreach(_.updateAfterCompress(lbpoMap))

      val ds = dataFields.iterator

      // reset data chunks
      while (ds.hasNext) {
        val d = ds.next
        d.dataChunks.clear
        d.dataChunks += new BulkChunk(-1, -1, cachedSize, 1)
      }
    }
  }

  /**
   * called after a prepare append operation to write empty the new objects buffer and to set blocks correctly
   */
  protected final def updateAfterPrepareAppend(chunkMap : Array[Array[Chunk]]) {
    val newInstances = newDynamicInstances.hasNext;
    val newPool = blocks.isEmpty;
    val newField = dataFields.exists(_.dataChunks.isEmpty)

    // allocate an additional slot so that we can directly use index
    chunkMap(poolIndex) = new Array[Chunk](1 + dataFields.size)

    if (newPool || newInstances || newField) {

      // build block chunk
      val lcount = newDynamicInstancesSize
      // //@ note this is the index into the data array and NOT the written lbpo
      val lbpo = if (0 == lcount) 0 else (newDynamicInstances.next.skillID - 1);

      blocks.append(new Block(blocks.size, lbpo, newObjects.size, lcount));

      // @note: if this does not hold for p; then it will not hold for p.subPools either!
      if (newInstances || !newPool) {
        // build field chunks
        for (f ← dataFields) {
          val c = if (f.dataChunks.isEmpty) {
            new BulkChunk(-1, -1, size, blocks.size);
          } else if (newInstances) {
            new SimpleChunk(-1, -1, lcount, lbpo);
          } else
            null

          if (c != null) {
            f.addChunk(c);
            chunkMap(poolIndex)(f.index) = c;
          }
        }
      }
    }
    // notify sub pools
    for (p ← subPools)
      p.updateAfterPrepareAppend(chunkMap);

    // remove new objects, because they are regular objects by now
    staticDataInstnaces += newObjects.size
    newObjects = new ArrayBuffer[T]
  }

  @inline
  final def read(in : InStream) : T = getById(in.v64.toInt)

  @inline
  final def offset(target : T) : Long = V64.offset(target.skillID)

  @inline
  final def write(target : T, out : MappedOutStream) : Unit = if (null == target) out.i8(0) else out.v64(target.skillID)

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

  @inline
  final override def foreach[U](f : T ⇒ U) {
    for (
      bs ← blocks;
      i ← bs.bpo until bs.bpo + bs.dynamicCount
    ) {
      f(data(i).asInstanceOf[T])
    }
    foreachNewInstance(f)
  }

  @inline
  final def foreachNewInstance[U](f : T ⇒ U) {
    newObjects.foreach(f)
    subPools.foreach(_.foreachNewInstance(f))
  }
}

object StoragePool {
  val noTypeRestrictions = new HashSet[restrictions.TypeRestriction]
  val noFieldRestrictions = new HashSet[restrictions.FieldRestriction]

  // creates map for all Bs
  final def makeLBPOMap[B <: SkillObject](p : StoragePool[_ <: B, B], lbpoMap : Array[Int], next : Int) : Int = {
    lbpoMap(p.poolIndex) = next
    var result = next + p.staticSize - p.deletedCount
    for (sub ← p.subPools)
      result = makeLBPOMap(sub, lbpoMap, result)

    result
  }
}

abstract class BasePool[B <: SkillObject](
  _typeID : Int,
  _name : String)
    extends StoragePool[B, B](_name, null, _typeID) {

  final override def basePool : BasePool[B] = this

  final def compress(lbpoMap : Array[Int]) {
    // create our part of the lbpo map
    StoragePool.makeLBPOMap(this, lbpoMap, 0);

    val tmp = data
    allocateData
    val d = data
    data = tmp
    var p = 0
    val xs = allInTypeOrder
    while (xs.hasNext) {
      val i = xs.next()
      if (0 != i.skillID) {
        d(p) = i
        p += 1
        i.skillID = p
      }
    }
    data = d
    updateAfterCompress(lbpoMap)
  }

  final def prepareAppend(chunkMap : Array[Array[Chunk]]) {
    val newInstances = newDynamicInstances.hasNext

    // check if we have to append at all
    if (!newInstances && !blocks.isEmpty && !dataFields.isEmpty && !dataFields.exists(_.dataChunks.isEmpty))
      return ;

    if (newInstances) {
      // we have to resize
      val d : Array[B] = Arrays.copyOf[B](data, cachedSize);
      var i = data.length;

      val is = newDynamicInstances;
      while (is.hasNext) {
        val instance = is.next;
        d(i) = instance;
        i += 1
        instance.skillID = i
      }
      data = d;
    }
    updateAfterPrepareAppend(chunkMap);
  }
}

abstract class SubPool[T <: B, B <: SkillObject](
  _typeID : Int,
  _name : String,
  _superPool : StoragePool[_ >: T <: B, B])
    extends StoragePool[T, B](_name, _superPool, _typeID) {

  final override val basePool : BasePool[B] = superPool.basePool

  final override def allocateData : Unit = this.data = basePool.data
}