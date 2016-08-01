package de.ust.skill.common.scala.internal

import java.nio.file.Files
import java.nio.file.Path
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import de.ust.skill.common.jvm.streams.FileOutputStream
import de.ust.skill.common.scala.api.Access
import de.ust.skill.common.scala.api.Append
import de.ust.skill.common.scala.api.IllegalOperation
import de.ust.skill.common.scala.api.ReadOnly
import de.ust.skill.common.scala.api.RestrictionCheckFailed
import de.ust.skill.common.scala.api.SkillException
import de.ust.skill.common.scala.api.SkillFile
import de.ust.skill.common.scala.api.SkillObject
import de.ust.skill.common.scala.api.Write
import de.ust.skill.common.scala.api.WriteMode
import de.ust.skill.common.jvm.streams.FileInputStream
import de.ust.skill.common.scala.api.ClosureMode
import de.ust.skill.common.scala.api.NoClosure
import de.ust.skill.common.scala.api.ThrowException

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
  protected[internal] val annotationType : fieldTypes.AnnotationType,

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
   * a file input stream keeping the handle to a file for potential write operations
   *
   * @note this is a consequence of the retarded windows file system
   */
  private var input : FileInputStream = String.in;

  // set types owners to this
  for (t ← types if t.isInstanceOf[BasePool[_]]) {
    t.asInstanceOf[BasePool[_]]._owner = this
  }

  /**
   * set to true, if an object is deleted from this state to cause a state transition before trying to append to a file
   */
  private var dirty = false

  override final def delete(target : SkillObject) {
    if (target != null && target.skillID != 0) {
      dirty |= target.skillID > 0
      typesByName(target.getTypeName).delete(target)
    }
  }

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
    // TODO a more efficient solution would be helpful
    // TODO lacks type restrictions
    // @note this should be more like, each pool is checking its type restriction, aggergating its field restrictions,
    // and if there are any, then they will all be checked using (hopefully) overridden check methods
    for (p ← types.par; f ← p.dataFields) try { f.check } catch {
      case e : SkillException ⇒ throw RestrictionCheckFailed(s"check failed in ${p.name}.${f.name}:\n  ${e.getMessage}", e)
    }
  }

  def closure(mode : ClosureMode) {
    mode match {
      case NoClosure ⇒ // done
      case ThrowException ⇒
        for (
          p ← types.par;
          f ← p.dataFields.par;
          if f.t.requiresClosure
        ) f.closure(this, ThrowException)
    }
  }

  /**
   * @return the file input stream matching our current status
   */
  private def makeInStream : FileInputStream = {
    if (null == input || !path.equals(input.path()))
      input = FileInputStream.open(path, false);

    input;
  }

  final def flush(closureMode : ClosureMode = NoClosure) {
    closure(closureMode)
    mode match {
      case Write ⇒ FileWriters.write(this, FileOutputStream.write(makeInStream))
      case Append if dirty ⇒
        changeMode(Write);
        FileWriters.write(this, FileOutputStream.write(makeInStream))
      case Append   ⇒ FileWriters.append(this, FileOutputStream.append(makeInStream))
      case ReadOnly ⇒ throw IllegalOperation("can not flush a read only file")
    }
  }

  final def close : Unit = {
    flush()
    changeMode(ReadOnly)
  }

  final override def iterator : Iterator[Access[_ <: de.ust.skill.common.scala.api.SkillObject]] = types.iterator

  // access type by index
  final override def apply(idx : Int) : Access[_ <: de.ust.skill.common.scala.api.SkillObject] = types(idx)

  // access type by name
  final def apply(name : String) : StoragePool[_ <: de.ust.skill.common.scala.api.SkillObject, _ <: de.ust.skill.common.scala.api.SkillObject] = typesByName(name)

  final override def length : Int = types.length
}