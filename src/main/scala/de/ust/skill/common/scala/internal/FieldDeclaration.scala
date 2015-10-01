package de.ust.skill.common.scala.internal

import de.ust.skill.common.scala.api
import de.ust.skill.common.scala.api.SkillObject
import de.ust.skill.common.scala.internal.fieldTypes.FieldType
import scala.collection.mutable.ArrayBuffer
import de.ust.skill.common.jvm.streams.MappedInStream
import de.ust.skill.common.jvm.streams.MappedOutStream
import scala.collection.mutable.HashMap
import scala.collection.mutable.WrappedArray
import scala.collection.mutable.ListBuffer
import java.nio.BufferUnderflowException
import de.ust.skill.common.scala.api.PoolSizeMissmatchError

/**
 * runtime representation of fields
 *
 * @author Timm Felden
 */
sealed abstract class FieldDeclaration[T, Obj <: SkillObject](
  final override val t : FieldType[T],
  final override val name : String,
  protected[internal] final val index : Int,
  final val owner : StoragePool[Obj, _ >: Obj <: SkillObject])
    extends api.FieldDeclaration[T] {

  private[internal] final val dataChunks = new ArrayBuffer[Chunk]

  @inline final def addChunk(c : Chunk) : Unit = dataChunks.append(c)

  /**
   * Read data from a mapped input stream and set it accordingly. This is invoked at the very end of state
   * construction and done massively in parallel.
   */
  def read(in : MappedInStream, target : Chunk) : Unit;

  /**
   * offset calculation as preparation of writing data belonging to the owners last block
   */
  def offset : Long;

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
trait IgnoredField[T, Obj <: SkillObject] extends FieldDeclaration[T, Obj];

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
  protected var data = new HashMap[SkillObject, T]() //Array[T]()
  protected var newData = new HashMap[SkillObject, T]()

  override def read(in : MappedInStream, lastChunk : Chunk) {
    val d = owner match {
      case p : BasePool[Obj]   ⇒ p.data
      case p : SubPool[Obj, _] ⇒ p.data
    }

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
        throw new PoolSizeMissmatchError(dataChunks.size - 1, lastChunk.begin, lastChunk.end, this)
    }
    val lastPosition = in.position
    if (lastPosition - firstPosition != lastChunk.end - lastChunk.begin)
      throw new PoolSizeMissmatchError(dataChunks.size - 1, lastChunk.begin, lastChunk.end, this)
  }
  override def offset : Long = ???
  override def write(out : MappedOutStream) : Unit = ???

  override def getR(ref : SkillObject) : T = {
    if (-1 == ref.skillID)
      return newData(ref)
    else
      return data(ref)
  }
  override def setR(ref : SkillObject, value : T) {
    if (-1 == ref.skillID)
      newData.put(ref, value)
    else
      data(ref) = value
  }

  def iterator = data.iterator ++ newData.valuesIterator
}

/**
 * The field is distributed and loaded on demand.
 * Unknown fields are lazy as well.
 *
 * @note implementation abuses a distributed field that can be accessed iff there are no data chunks to be processed
 */
class LazyField[T : Manifest, Obj <: SkillObject](
  _t : FieldType[T],
  _name : String,
  _index : Int,
  _owner : StoragePool[Obj, _ >: Obj <: SkillObject])
    extends DistributedField[T, Obj](_t, _name, _index, _owner) {

  // pending parts that have to be loaded
  private var parts = new ListBuffer[MappedInStream]()
  private def isLoaded = parts.isEmpty

  // executes pending read operations
  private def load {
    val d = owner match {
      case p : BasePool[Obj]   ⇒ p.data
      case p : SubPool[Obj, _] ⇒ p.data
    }

    for (chunk ← dataChunks) {
      val in = parts.head
      val firstPosition = in.position
      try {
        parts.remove(0)
        chunk match {
          case c : SimpleChunk ⇒
            val low = c.bpo.toInt
            val high = (c.bpo + c.count).toInt
            for (i ← low until high) {
              data(d(i)) = t.read(in)
            }
          case bci : BulkChunk ⇒
            var count = bci.count;
            for (
              bi ← owner.blocks; if ({ count -= bi.dynamicCount; count >= 0 });
              i ← bi.bpo.toInt until (bi.bpo + bi.dynamicCount).toInt
            ) {
              data(d(i)) = t.read(in)
            }
        }
      } catch {
        case e : BufferUnderflowException ⇒
          val lastPosition = in.position
          throw new PoolSizeMissmatchError(dataChunks.size - parts.size - 1, chunk.begin, chunk.end, this)
      }
      val lastPosition = in.position
      if (lastPosition - firstPosition != chunk.end - chunk.begin)
        throw new PoolSizeMissmatchError(dataChunks.size - parts.size - 1, chunk.begin, chunk.end, this)
    }
  }

  override def read(part : MappedInStream, target : Chunk) {
    parts += part
  }

  override def getR(ref : SkillObject) : T = {
    if (-1 == ref.skillID)
      return newData(ref)

    if (!isLoaded)
      load

    return super.getR(ref)
  }

  override def setR(ref : SkillObject, v : T) {
    if (-1 == ref.skillID)
      newData(ref) = v
    else {

      if (!isLoaded)
        load

      return super.setR(ref, v)
    }
  }

  override def iterator = {
    if (!isLoaded)
      load

    super.iterator
  }
}
