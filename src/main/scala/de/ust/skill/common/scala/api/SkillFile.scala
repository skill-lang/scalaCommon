package de.ust.skill.common.scala.api

import java.nio.file.Path

/**
 * The public interface to the in-memory representation of a SKilL file.
 * This class provides access to instances of types stored in a file as well as state management capabilities.
 *
 * @author Timm Felden
 */
trait SkillFile extends IndexedSeq[Access[_ <: SkillObject]] {

  /**
   * strings stored in this file
   */
  def String : StringAccess

  /**
   * iterator over all user types known by this file
   */
  def iterator : Iterator[Access[_ <: SkillObject]]

  /**
   * changes output path
   * @note in append mode, the old file will be copied to the new path; this may take some time
   */
  def changePath(path : Path) : Unit
  /**
   * change mode
   * @note currently only append -> write is supported; if you want to change write -> append, you are probably looking
   * for open(Create, Append) instead
   */
  def changeMode(writeMode : WriteMode) : Unit

  /**
   * Checks restrictions in types. Restrictions are checked before write/append, where an error is raised if they do not
   * hold.
   */
  def check : Unit

  /**
   * Check consistency and write changes to disk.
   * @note this will not sync the file to disk, but it will block until all in-memory changes are written to buffers.
   * @note if check fails, then the state is guaranteed to be unmodified compared to the state before flush
   */
  def flush : Unit

  /**
   * Same as flush, but will also sync and close file, thus the state is not usable afterwards.
   */
  def close : Unit
}

/**
 * Modes for file handling.
 */
sealed abstract class Mode;
sealed abstract class ReadMode extends Mode;
sealed abstract class WriteMode extends Mode;
object Create extends ReadMode;
object Read extends ReadMode;
object Write extends WriteMode;
object Append extends WriteMode;
/**
 * can not be written at all; read only is permanent and must not be changed with a change mode
 */
object ReadOnly extends WriteMode;