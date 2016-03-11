package de.ust.skill.common.scala.internal

import java.nio.BufferUnderflowException
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import de.ust.skill.common.jvm.streams.MappedInStream
import de.ust.skill.common.jvm.streams.MappedOutStream
import de.ust.skill.common.scala.api
import de.ust.skill.common.scala.api.PoolSizeMissmatchError
import de.ust.skill.common.scala.api.SkillObject
import de.ust.skill.common.scala.internal.fieldTypes.FieldType
import de.ust.skill.common.scala.internal.restrictions.CheckableFieldRestriction
import de.ust.skill.common.scala.internal.restrictions.FieldRestriction
import de.ust.skill.common.scala.api.ClosureMode

/**
 * runtime representation of fields
 *
 * @author Timm Felden
 */
sealed abstract class FieldDeclaration[T, Obj <: SkillObject](
  final override val t : FieldType[T],
  final override val name : String,
  /**
   * index of the field inside of dataFields; this may change, if fields get reordered by a write operation
   */
  protected[internal] final var index : Int,
  final val owner : StoragePool[Obj, _ >: Obj <: SkillObject])
    extends api.FieldDeclaration[T] {

  protected[internal] final val dataChunks = new ArrayBuffer[Chunk]

  @inline final def addChunk(c : Chunk) : Unit = dataChunks.append(c)

  /**
   * Restriction handling.
   */
  val restrictions = HashSet[FieldRestriction]();
  def addRestriction(r : FieldRestriction) = restrictions += r
  def check {
    var rs = restrictions.collect { case r : CheckableFieldRestriction[T] ⇒ r }
    if (!rs.isEmpty)
      owner.foreach { x ⇒ rs.foreach(_.check(x.get(this))) }
  }

  /**
   * Read data from a mapped input stream and set it accordingly. This is invoked at the very end of state
   * construction and done massively in parallel.
   */
  def read(in : MappedInStream, target : Chunk) : Unit;

  /**
   * Perform a closure operation on this field for all instances of owner.
   *
   * @return list of new objects added to the state or null if none were added
   */
  protected[internal] def closure(sf : SkillState, mode : ClosureMode) : ArrayBuffer[SkillObject] = {
    for (i ← owner)
      t.closure(sf, getR(i), mode)

    null
  }

  /**
   * offset calculation as preparation of writing data belonging to the owners last block
   */
  def offset : Unit;

  /**
   * offsets are stored in fields, so that file writer does not have to allocate futures
   */
  final protected[internal] var cachedOffset : Long = 0L

  /**
   * write data into a map at the end of a write/append operation
   *
   * @note this will always write the last chunk, as, in contrast to read, it is impossible to write to fields in
   *       parallel
   * @note only called, if there actually is field data to be written
   */
  def write(out : MappedOutStream) : Unit;
}

/**
 * This class marks known fields.
 */
abstract class KnownField[T, Obj <: SkillObject](
  _t : FieldType[T],
  _name : String,
  _index : Int,
  _owner : StoragePool[Obj, _ >: Obj <: SkillObject]) extends FieldDeclaration[T, Obj](_t, _name, _index, _owner);

/**
 * This trait marks auto fields, i.e. fields that wont be touched by serialization.
 * @note an auto field must be known
 */
abstract class AutoField[T, Obj <: SkillObject](
  _t : FieldType[T],
  _name : String,
  _index : Int,
  _owner : StoragePool[Obj, _ >: Obj <: SkillObject])
    extends KnownField[T, Obj](_t, _name, _index, _owner) {

  final override def read(in : MappedInStream, target : Chunk) : Unit = throw new NoSuchMethodError("one can not read auto fields!")
  final override def offset = throw new NoSuchMethodError("one can not write auto fields!")
  final override def write(out : MappedOutStream) : Unit = throw new NoSuchMethodError("one can not write auto fields!")
}

/**
 * This trait marks ignored fields.
 */
trait IgnoredField[T, Obj <: SkillObject] extends KnownField[T, Obj];

/**
 * The fields data is distributed into an array (for now its a hash map) holding its instances.
 */
class DistributedField[@specialized(Boolean, Byte, Char, Double, Float, Int, Long, Short) T : Manifest, Obj <: SkillObject](
  _t : FieldType[T],
  _name : String,
  _index : Int,
  _owner : StoragePool[Obj, _ >: Obj <: SkillObject])
    extends FieldDeclaration[T, Obj](_t, _name, _index, _owner) {

  // data held as in storage pools
  // @note see paper notes for O(1) implementation
  protected var data = new HashMap[Obj, T]() //Array[T]()
  protected var newData = new HashMap[Obj, T]()

  override def read(in : MappedInStream, lastChunk : Chunk) {
    val d = owner.data.asInstanceOf[Array[Obj]]

    val firstPosition = in.position
    try {
      lastChunk match {
        case c : SimpleChunk ⇒
          val low = c.bpo.toInt
          val high = (c.bpo + c.count).toInt
          for (i ← low until high) {
            data(d(i)) = t.read(in)
          }
        case bci : BulkChunk ⇒
          for (
            bi ← owner.blocks;
            i ← bi.bpo.toInt until (bi.bpo + bi.dynamicCount).toInt
          ) {
            data(d(i)) = t.read(in)
          }
      }
    } catch {
      case e : BufferUnderflowException ⇒
        val lastPosition = in.position
        throw PoolSizeMissmatchError(dataChunks.size - 1, lastChunk.begin, lastChunk.end, this, lastPosition)
    }
    val lastPosition = in.position
    if (lastPosition - firstPosition != lastChunk.end - lastChunk.begin)
      throw PoolSizeMissmatchError(dataChunks.size - 1, lastChunk.begin, lastChunk.end, this, lastPosition)
  }
  override def offset : Unit = {
    // compress data
    data ++= newData
    newData.clear()

    val target = owner.data
    var result = 0L
    dataChunks.last match {
      case c : SimpleChunk ⇒
        var i = c.bpo.toInt
        val high = i + c.count
        while (i != high) {
          result += t.offset(data(target(i).asInstanceOf[Obj]))
          i += 1
        }
      case bci : BulkChunk ⇒
        val blocks = owner.blocks
        var blockIndex = 0
        while (blockIndex < bci.blockCount) {
          val b = blocks(blockIndex)
          blockIndex += 1
          var i = b.bpo
          val end = i + b.dynamicCount
          while (i != end) {
            result += t.offset(data(target(i).asInstanceOf[Obj]))
            i += 1
          }
        }
    }
    cachedOffset = result
  }

  override def write(out : MappedOutStream) : Unit = {
    val target = owner.data
    dataChunks.last match {
      case c : SimpleChunk ⇒
        var i = c.bpo.toInt
        val high = i + c.count
        while (i != high) {
          t.write(data(target(i).asInstanceOf[Obj]), out)
          i += 1
        }
      case bci : BulkChunk ⇒
        val blocks = owner.blocks
        var blockIndex = 0
        while (blockIndex < bci.blockCount) {
          val b = blocks(blockIndex)
          blockIndex += 1
          var i = b.bpo
          val end = i + b.dynamicCount
          while (i != end) {
            t.write(data(target(i).asInstanceOf[Obj]), out)
            i += 1
          }
        }
    }
  }

  override def getR(ref : SkillObject) : T = {
    if (-1 == ref.skillID)
      return newData(ref.asInstanceOf[Obj])
    else
      return data(ref.asInstanceOf[Obj])
  }
  override def setR(ref : SkillObject, value : T) {
    if (-1 == ref.skillID)
      newData.put(ref.asInstanceOf[Obj], value)
    else
      data(ref.asInstanceOf[Obj]) = value
  }

  def iterator = data.iterator ++ newData.valuesIterator
}

/**
 * The field is distributed and loaded on demand.
 * Unknown fields are lazy as well.
 *
 * @note implementation abuses a distributed field that can be accessed iff there are no data chunks to be processed
 */
final class LazyField[T : Manifest, Obj <: SkillObject](
  _t : FieldType[T],
  _name : String,
  _index : Int,
  _owner : StoragePool[Obj, _ >: Obj <: SkillObject])
    extends DistributedField[T, Obj](_t, _name, _index, _owner) {

  // pending parts that have to be loaded
  private var parts = new HashMap[Chunk, MappedInStream]
  private def isLoaded = null == parts

  // executes pending read operations
  private def load {
    val d = owner.data.asInstanceOf[Array[Obj]]

    var chunkIndex = 0
    while (chunkIndex < dataChunks.size) {
      val chunk = dataChunks(chunkIndex)
      chunkIndex += 1
      val in = parts(chunk).view(chunk.begin.toInt, chunk.end.toInt)
      try {
        chunk match {
          case c : SimpleChunk ⇒
            var i = c.bpo.toInt
            val high = i + c.count
            while (i != high) {
              data(d(i)) = t.read(in)
              i += 1
            }
          case bci : BulkChunk ⇒
            val blocks = owner.blocks
            var blockIndex = 0
            while (blockIndex < bci.blockCount) {
              val b = blocks(blockIndex)
              blockIndex += 1
              var i = b.bpo
              val end = i + b.dynamicCount
              while (i != end) {
                data(d(i)) = t.read(in)
                i += 1
              }
            }
        }
      } catch {
        case e : BufferUnderflowException ⇒
          val lastPosition = in.position
          throw new PoolSizeMissmatchError(
            dataChunks.size - parts.size,
            parts(chunk).position() + chunk.begin,
            parts(chunk).position() + chunk.end, this, lastPosition)
      }
      if (in.asByteBuffer().remaining() != 0)
        throw new PoolSizeMissmatchError(
          dataChunks.size - parts.size,
          parts(chunk).position() + chunk.begin,
          parts(chunk).position() + chunk.end,
          this, in.position())
    }
    parts = null
  }

  /**
   * ensures that the data has been loaded from disk
   */
  def ensureIsLoaded {
    if (!isLoaded)
      load
  }

  override def read(part : MappedInStream, target : Chunk) {
    this.synchronized {
      parts(target) = part
    }
  }

  override def getR(ref : SkillObject) : T = {
    if (-1 == ref.skillID)
      return newData(ref.asInstanceOf[Obj])

    if (!isLoaded)
      load

    return data(ref.asInstanceOf[Obj])
  }

  override def setR(ref : SkillObject, v : T) {
    if (-1 == ref.skillID)
      newData(ref.asInstanceOf[Obj]) = v
    else {

      if (!isLoaded)
        load

      return data(ref.asInstanceOf[Obj]) = v
    }
  }

  override def iterator = {
    if (!isLoaded)
      load

    super.iterator
  }
}
