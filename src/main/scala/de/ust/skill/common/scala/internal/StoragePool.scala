package de.ust.skill.common.scala.internal

import java.util.Arrays

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashSet

import de.ust.skill.common.jvm.streams.InStream
import de.ust.skill.common.jvm.streams.MappedOutStream
import de.ust.skill.common.scala.SkillID
import de.ust.skill.common.scala.api
import de.ust.skill.common.scala.api.ClosureException
import de.ust.skill.common.scala.api.ClosureMode
import de.ust.skill.common.scala.api.RecursiveInsert
import de.ust.skill.common.scala.api.ReplaceByNull
import de.ust.skill.common.scala.api.SkillObject
import de.ust.skill.common.scala.api.ThrowException
import de.ust.skill.common.scala.internal.fieldTypes.FieldType
import de.ust.skill.common.scala.internal.fieldTypes.UserType
import de.ust.skill.common.scala.internal.fieldTypes.V64
import de.ust.skill.common.scala.internal.restrictions.FieldRestriction
import de.ust.skill.common.jvm.streams.OutStream
import de.ust.skill.common.scala.api.TypeSystemError

/**
 * @author Timm Felden
 */
sealed abstract class StoragePool[T <: B, B <: SkillObject](
  final val name :      String,
  final val superPool : StoragePool[_ >: T <: B, B],
  _typeID :             Int)
  extends UserType[T](_typeID) {

  def getInstanceClass : Class[T]

  /**
   * the index of this inside of the enclosing states types array
   */
  def poolIndex = typeID - 32
  private[internal] final def typeID_=(id : Int) = __typeID = id

  override def iterator : Iterator[T] = all
  final def all : Iterator[T] = new DynamicDataIterator(this)

  final def staticInstances : StaticDataIterator[T] = new StaticDataIterator[T](this)

  final def allInTypeOrder : TypeOrderIterator[T, B] = new TypeOrderIterator(this)

  final def fields : Iterator[api.FieldDeclaration[_]] = autoFields.iterator ++ dataFields.iterator
  final def allFields : Iterator[api.FieldDeclaration[_]] = if (null != superPool)
    superPool.allFields ++ fields
  else fields

  @inline
  final def apply(idx : Int) : T = {
    var index = idx
    val bs = blocks.iterator
    while (bs.hasNext) {
      val b = bs.next
      if (index < b.dynamicCount)
        return data(b.bpo + index).asInstanceOf[T]
      else
        index -= b.dynamicCount
    }
    val subs = typeHierarchyIterator
    while (subs.hasNext) {
      val n = subs.next.newObjects
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
  final protected[internal] var newObjects = new ArrayBuffer[T]()
  final protected def newDynamicInstances : DynamicNewInstancesIterator[T, B] = new DynamicNewInstancesIterator(this)
  final protected def newDynamicInstancesSize : Int = {
    var r = newObjects.size
    val subs = subPools.iterator
    while (subs.hasNext)
      r += subs.next.newDynamicInstancesSize

    r
  }

  val superName : Option[String] =
    if (null == superPool) None
    else Some(superPool.name)

  /**
   * the base pool of this type hierarchy
   */
  final val basePool : BasePool[B] = if (null == superPool) this.asInstanceOf[BasePool[B]] else superPool.basePool

  /**
   * our distance to the base pool
   */
  final val typeHierarchyHeight : Int = if (null == superPool) 0 else superPool.typeHierarchyHeight + 1

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
   * a pointer to the next pool in a type order traversal
   */
  var nextPool : StoragePool[_ <: B, B] = null
  /**
   * returns an iterator over this Pool and all subPools
   */
  protected[internal] def typeHierarchyIterator : Iterator[StoragePool[_ <: T, B]] =
    new TypeHierarchyIterator(this)

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
        cachedSize = staticSize - deletedCount
        val subs = subPools.iterator
        while (subs.hasNext) {
          val s = subs.next
          s.fix(true)
          cachedSize += s.cachedSize
        }
      } else if (superPool != null) {
        superPool.fix(false)
      }
    }
  }

  /**
   * number of static instances stored in data
   */
  protected[internal] var staticDataInstances : SkillID = 0
  override def length : Int = {
    if (fixed)
      cachedSize
    else {
      var r = staticSize
      val sub = subPools.iterator
      while (sub.hasNext)
        r += sub.next.length

      r
    }
  }

  /**
   * the number of instances of exactly this type, excluding sub-types
   *
   * @return size excluding subtypes
   */
  @inline
  def staticSize : SkillID = {
    staticDataInstances + newObjects.length
  }

  /**
   * number of deleted instances currently stored in this pool
   */
  private[internal] var deletedCount = 0;
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
  def addField[R : Manifest](
    fieldID :      Int,
    t :            FieldType[R],
    name :         String,
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
   * a total function, that will either return the correct object or
   * null
   * @return the instance matching argument skill id
   */
  @inline final def getById(id : SkillID) : T = {
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
      blocks.append(new Block(0, lbpoMap(poolIndex), staticSize - deletedCount, cachedSize))
      val s = subPools.iterator
      while (s.hasNext)
        s.next.updateAfterCompress(lbpoMap)

      val ds = dataFields.iterator

      // reset data chunks
      while (ds.hasNext) {
        val d = ds.next
        d.dataChunks.clear
        d.dataChunks += new BulkChunk(-1, -1, cachedSize, 1)
      }

      staticDataInstances += (newObjects.size - deletedCount)
      this.newObjects = new ArrayBuffer[T]()
      this.deletedCount = 0
    }
  }

  /**
   * called after a prepare append operation to write empty the new objects buffer and to set blocks correctly
   */
  protected final def updateAfterPrepareAppend(chunkMap : Array[Array[Chunk]], bpoMap : Array[Int]) {
    this.data = basePool.data
    val newInstances = newDynamicInstances.hasNext;
    val newPool = blocks.isEmpty;
    val newField = dataFields.exists(_.dataChunks.isEmpty)

    // allocate an additional slot so that we can directly use index
    chunkMap(poolIndex) = new Array[Chunk](1 + dataFields.size)

    if (newPool || newInstances || newField) {

      // build block chunk
      val lcount = newDynamicInstancesSize
      val bpo = bpoMap(poolIndex);

      blocks.append(new Block(blocks.size, bpo, newObjects.size, lcount));

      // @note: if this does not hold for p; then it will not hold for p.subPools either!
      if (newInstances || !newPool) {
        // build field chunks
        val fs = dataFields.iterator
        while (fs.hasNext) {
          val f = fs.next
          val c = if (f.dataChunks.isEmpty) {
            new BulkChunk(-1, -1, size, blocks.size);
          } else if (newInstances) {
            new SimpleChunk(-1, -1, lcount, bpo);
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
    val subs = subPools.iterator
    while (subs.hasNext)
      subs.next.updateAfterPrepareAppend(chunkMap, bpoMap);

    // remove new objects, because they are regular objects by now
    staticDataInstances += newObjects.size
    if (newObjects.size != 0)
      newObjects = new ArrayBuffer[T]
  }

  @inline
  final def read(in : InStream) : T = getById(in.v64.toInt)

  override final def requiresClosure = true

  override final def closure(sf : SkillState, i : T, mode : ClosureMode) : ArrayBuffer[SkillObject] = {
    if (null == i) return null;

    val id = i.skillID

    if (0 == id ||
      (
        ((id - 1) >= 0) &&
        (data.length > (id - 1)) &&
        (i == data(id - 1)))) {
      null
    } else {
      val n = basePool.owner.typesByName(i.getTypeName).newObjects
      if (id < 0 && n.length >= -id && i == n((-id) - 1)) {
        null
      } else {
        mode match {
          case ThrowException  ⇒ throw new ClosureException(i)
          case RecursiveInsert ⇒ ???
          case ReplaceByNull   ⇒ ???
        }
      }
    }
  }

  @inline
  final def offset(target : T) : Long = if (null == target) 1 else V64.offset(target.skillID)

  @inline
  final def write(target : T, out : OutStream) : Unit = if (null == target) out.i8(0) else out.v64(target.skillID)

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

object StoragePool {
  val noTypeRestrictions = new HashSet[restrictions.TypeRestriction]
  val noFieldRestrictions = new HashSet[restrictions.FieldRestriction]

  // calculate sequence of type ordered pools
  private final def getNextSeq[B <: SkillObject](p : StoragePool[_ <: B, B]) : Seq[StoragePool[_ <: B, B]] =
    p.subPools.foldLeft(Seq[StoragePool[_ <: B, B]](p))(_ ++ getNextSeq(_))

  // calculate next references
  final def setNextPools[B <: SkillObject](base : StoragePool[_ <: SkillObject, _ <: SkillObject]) {
    val ps : Iterator[StoragePool[_ <: B, B]] = getNextSeq(base.asInstanceOf[StoragePool[_ <: B, B]]).iterator
    // we know that the sequence is nonempty
    var p : StoragePool[_ <: B, B] = ps.next
    while (ps.hasNext) {
      p.nextPool = ps.next
      p = p.nextPool
    }
  }
}

/**
 * Singletons instantiate this trait to improve the API.
 *
 * @author Timm Felden
 */
trait SingletonStoragePool[T <: B, B <: SkillObject] extends StoragePool[T, B] {

  final lazy val theInstance = reflectiveAllocateInstance
  final def get = theInstance

  final override def allocateInstances {
    for (b ← blocks.par) {
      var i : SkillID = b.bpo
      val last = i + b.staticCount
      while (i < last) {
        if (-1 == theInstance.skillID) {
          data(i) = theInstance
          this.theInstance.skillID = i + 1
          // instance is not a new object (it may have been accessed already)
          this.newObjects.clear()
        } else {
          throw TypeSystemError(s"found a second instance of a singleton in type $name. First: ${theInstance.skillID}, Second: $i")
        }
        i += 1
      }
    }
  }
}

abstract class BasePool[B <: SkillObject](
  _typeID : Int,
  _name :   String)
  extends StoragePool[B, B](_name, null, _typeID) {

  protected[internal] var _owner : SkillState = _
  def owner : SkillState = _owner

  final def compress(lbpoMap : Array[Int]) {
    // create our part of the lbpo map
    locally {
      val ts = typeHierarchyIterator
      var next = 0
      while (ts.hasNext) {
        val p = ts.next
        lbpoMap(p.poolIndex) = next
        next += p.staticSize - p.deletedCount
      }
    }

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

  final def prepareAppend(chunkMap : Array[Array[Chunk]], bpoMap : Array[Int]) {

    // fix this pool
    fix(true)

    val newInstances = newDynamicInstances.hasNext

    // check if we have to append at all
    if (!newInstances && !blocks.isEmpty && !dataFields.isEmpty
      && typeHierarchyIterator.forall(!_.dataFields.exists(_.dataChunks.isEmpty)))
      return ;

    // calculate our part of the lbpo map
    locally {
      val ts = typeHierarchyIterator
      // new objects will be appended after existing ones
      var next = data.size
      while (ts.hasNext) {
        val p = ts.next
        bpoMap(p.poolIndex) = next
        next += p.newObjects.size
      }
    }

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
    updateAfterPrepareAppend(chunkMap, bpoMap);
  }
}

abstract class SubPool[T <: B, B <: SkillObject](
  _typeID :    Int,
  _name :      String,
  _superPool : StoragePool[_ >: T <: B, B])
  extends StoragePool[T, B](_name, _superPool, _typeID) {

  final override def allocateData : Unit = this.data = basePool.data
}