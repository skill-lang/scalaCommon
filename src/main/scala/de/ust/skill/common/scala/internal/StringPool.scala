package de.ust.skill.common.scala.internal

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import de.ust.skill.common.jvm.streams.FileInputStream
import de.ust.skill.common.jvm.streams.FileOutputStream
import de.ust.skill.common.jvm.streams.InStream
import de.ust.skill.common.jvm.streams.MappedOutStream
import de.ust.skill.common.scala.SkillID
import de.ust.skill.common.scala.api.StringAccess
import de.ust.skill.common.scala.internal.fieldTypes.StringType
import java.nio.ByteBuffer
import de.ust.skill.common.jvm.streams.OutStream
import de.ust.skill.common.scala.internal.fieldTypes.V64

/**
 * @author Timm Felden
 */
final class StringPool(val in : FileInputStream)
    extends StringType with StringAccess {
  /**
   * the set of known strings, including new strings
   *
   * @note having all strings inside of the set instead of just the new ones, we can optimize away some duplicates,
   * while not having any additional cost, because the duplicates would have to be checked upon serialization anyway.
   * Furthermore, we can unify type and field names, thus we do not have to have duplicate names laying around,
   * improving the performance of hash containers and name checks:)
   */
  private[internal] var knownStrings = new HashSet[String];

  /**
   * ID ⇀ (absolute offset, length)
   *
   * will be used if idMap contains a null reference
   *
   * @note there is a fake entry at ID 0
   */
  private[internal] var stringPositions = ArrayBuffer[StringPosition](new StringPosition(-1L, -1));

  /**
   * get string by ID
   */
  private[internal] var idMap = ArrayBuffer[String](null)

  /**
   * map used to write strings
   */
  private val serializationIDs = new HashMap[String, Int]

  /**
   * returns the string, that should be used
   *
   * @note if result is null, nothing will happen and null will be returned
   */
  def add(result : String) {
    if (result != null)
      knownStrings.add(result)
  }

  /**
   * search a string by id it had inside of the read file, may block if the string has not yet been read
   */
  def get(index : SkillID) : String = {
    if (index <= 0) null
    else {
      var result = idMap(index)
      if (null == result) {
        this.synchronized {
          // read result
          val off = stringPositions(index.toInt)
          in.push(off.absoluteOffset)
          var chars = in.bytes(off.length)
          in.pop
          result = new String(chars, "UTF-8")

          // ensure that the string is known
          knownStrings.add(result)

          idMap(index.toInt) = result
          result
        }
      } else
        result
    }
  }

  def iterator : Iterator[String] = knownStrings.iterator

  def read(in : InStream) : String = get(in.v64.toInt)

  def offset(target : String) : Long = {
    if (null == target) 1L
    else V64.offset(serializationIDs(target))
  }

  def write(target : String, out : OutStream) : Unit = {
    if (null == target) out.i8(0)
    else out.v64(serializationIDs(target))
  }
  override def write(target : String, out : MappedOutStream) : Unit = {
    if (null == target) out.i8(0)
    else out.v64(serializationIDs(target))
  }

  final override def toString = "string"

  private def prepareSerialization {
    // ensure all strings are present
    var i = stringPositions.length - 1
    while (i != 0) {
      get(i)
      i -= 1
    }

    // create inverse map
    serializationIDs.clear()
    i = idMap.length - 1
    while (i != 0) {
      serializationIDs(idMap(i)) = i
      i -= 1
    }
  }

  def prepareAndWrite(out : FileOutputStream) {
    prepareSerialization

    // Insert new strings to the map;
    // this is where duplications with lazy strings will be detected and eliminated
    val ks = knownStrings.iterator
    while (ks.hasNext) {
      val s = ks.next
      if (!serializationIDs.contains(s)) {
        serializationIDs.put(s, idMap.size);
        idMap += s
      }
    }

    val count = idMap.length - 1
    out.v64(count)

    // write block, if nonempty
    if (0 != count) {
      val end = out.mapBlock(4 * count).buffer().asIntBuffer();
      var off = 0;
      var i = 1
      while (i <= count) {
        val data = idMap(i).getBytes()
        off += data.length;
        end.put(off)
        out.put(data)
        i += 1
      }
    }
  }

  def prepareAndAppend(out : FileOutputStream) {
    prepareSerialization

    val todo = new ArrayBuffer[Array[Byte]]

    // Insert new strings to the map;
    // this is the place where duplications with lazy strings will be detected and eliminated
    // this is also the place, where new instances are appended to the output file
    val ks = knownStrings.iterator
    while (ks.hasNext) {
      val s = ks.next
      if (!serializationIDs.contains(s)) {
        serializationIDs.put(s, idMap.size);
        idMap += s
        todo += s.getBytes
      }
    }

    // count
    val count = todo.size;
    out.v64(count);
    // write block, if nonempty
    if (0 != count) {
      val end = out.mapBlock(4 * count).buffer().asIntBuffer();
      var off = 0;
      var i = 0
      while (i < count) {
        val data = todo(i)
        off += data.length;
        end.put(off)
        out.put(data)
        i += 1
      }
    }
  }

  private[internal] def clearSearilizationIDs = {
    serializationIDs.clear()
    serializationIDs.sizeHint(0)
  }
}

/**
 * Used to get rid of position tuple.
 * @author Timm Felden
 */
final class StringPosition(val absoluteOffset : Long, val length : Int);