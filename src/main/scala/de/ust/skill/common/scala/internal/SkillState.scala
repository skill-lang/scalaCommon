package de.ust.skill.common.scala.internal

import java.nio.file.Files
import java.nio.file.Path
import scala.collection.mutable.ArrayBuffer
import de.ust.skill.common.scala.api.Access
import de.ust.skill.common.scala.api.Append
import de.ust.skill.common.scala.api.IllegalOperation
import de.ust.skill.common.scala.api.ReadOnly
import de.ust.skill.common.scala.api.SkillFile
import de.ust.skill.common.scala.api.Write
import de.ust.skill.common.scala.api.WriteMode
import de.ust.skill.common.scala.api.SkillObject
import scala.collection.mutable.HashMap

/**
 * @author Timm Felden
 */
class SkillState(
  /**
   * path used for flush/close operations
   */
  var path : Path,

  /**
 *  current write mode
 */
  var mode : WriteMode,

  /**
 * strings stored in this file
 */
  override val String : StringPool,

  /**
 * annotation type used for annotations RTTI
 */
  private[internal] val annotationType : fieldTypes.AnnotationType,

  /**
 * types stored in this file
 */
  protected[internal] val types : ArrayBuffer[StoragePool[_ <: SkillObject, _ <: SkillObject]],

  /**
 * types by skill name
 */
  protected[internal] val typesByName : HashMap[String, StoragePool[_ <: SkillObject, _ <: SkillObject]])
    extends SkillFile {

  /**
   * ensures that instances get allocated and all eager fields are read
   */
  final def finalizePools : Unit = ???

  final def changeMode(newMode : WriteMode) : Unit = {
    // pointless
    if (mode == newMode)
      return ;

    // check read only
    if (ReadOnly == mode)
      throw IllegalOperation("can not change mode of a read only file")

    // write -> append
    if (Append == newMode)
      throw IllegalOperation(
        "Cannot change write mode from Write to Append, try to use open(<path>, Create, Append) instead.");

    mode = newMode
  }

  final def changePath(path : Path) : Unit = mode match {
    case ReadOnly ⇒ throw IllegalOperation("can not change path of a read only file")
    case Write    ⇒ this.path = path
    case Append ⇒
      // catch erroneous behavior
      if (this.path.equals(path))
        return ;
      Files.deleteIfExists(path);
      Files.copy(this.path, path);
      this.path = path
  }

  def check : Unit = {
    ???
  }

  def flush : Unit = {
    ???
  }

  def close : Unit = {
    flush
    changeMode(ReadOnly)
  }

  def iterator : Iterator[Access[_ <: de.ust.skill.common.scala.api.SkillObject]] = types.iterator
}

object SkillState {
  def open = {
    ???
  }
}