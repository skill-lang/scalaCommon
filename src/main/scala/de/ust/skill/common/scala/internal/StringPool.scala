package de.ust.skill.common.scala.internal

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashSet
import de.ust.skill.common.jvm.streams.FileInputStream
import de.ust.skill.common.jvm.streams.MappedInStream
import de.ust.skill.common.jvm.streams.MappedOutStream
import de.ust.skill.common.scala.SkillID
import de.ust.skill.common.scala.api.StringAccess
import de.ust.skill.common.scala.internal.fieldTypes.StringType
import de.ust.skill.common.jvm.streams.InStream

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
   * returns the string, that should be used
   */
  def add(result : String) : String = {
    knownStrings.find(_.equals(result)).getOrElse {
      knownStrings.add(result)
      result
    }
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

          // unify string and mark it read
          result = knownStrings.find(_.equals(result)).getOrElse {
            knownStrings.add(result)
            result
          }

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
    ???
  }

  def write(target : String, out : MappedOutStream) : Unit = {
    ???
  }

  final override def toString = "string"
}

/**
 * Used to get rid of position tuple.
 * @author Timm Felden
 */
final case class StringPosition(val absoluteOffset : Long, val length : Int);