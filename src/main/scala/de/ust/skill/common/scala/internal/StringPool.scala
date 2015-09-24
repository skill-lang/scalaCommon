package de.ust.skill.common.scala.internal

import de.ust.skill.common.scala.api.StringAccess
import de.ust.skill.common.scala.SkillID
import de.ust.skill.common.jvm.streams.FileInputStream
import scala.collection.mutable.HashSet
import scala.collection.mutable.ArrayBuffer

/**
 * @author Timm Felden
 */
class StringPool(val in : FileInputStream) extends StringAccess {
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
  private[internal] var stringPositions = ArrayBuffer[(Long, Int)]((-1L, -1));

  /**
   * get string by ID
   */
  private[internal] var idMap = ArrayBuffer[String](null)

  /**
   * returns the string, that should be used
   */
  def add(string : String) : String = {
    ???
  }

  def get(index : SkillID) : String = {
    ???
    
//    if read add to known String and drop, if required
  }

  def iterator : Iterator[String] = {
    ???
  }

  def typeID : Int = {
    ???
  }
}