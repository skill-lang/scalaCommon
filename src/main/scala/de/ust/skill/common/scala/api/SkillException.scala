package de.ust.skill.common.scala.api

import de.ust.skill.common.jvm.streams.InStream
import de.ust.skill.common.scala.internal

/**
 * Top level skill exception class.
 * All skill related exceptions will inherit from this one.
 *
 * @author Timm Felden
 */
sealed abstract class SkillException private[scala] (msg : String, cause : Throwable = null)
  extends Exception(msg, cause);

/**
 * thrown if an operation can not be performed in general, message will explain the problem
 */
final case class IllegalOperation(msg : String) extends SkillException(msg);

/**
 * thrown if the loaded type system contradicts our specification
 */
final case class TypeSystemError(msg : String) extends SkillException(msg);

/**
 * This exception is used if byte stream related errors occur.
 *
 * @author Timm Felden
 */
final case class ParseException(in : InStream, block : Int, msg : String, cause : Throwable = null)
  extends SkillException(
    s"In block ${block + 1} @0x${in.position.toHexString}: $msg",
    cause
  );

/**
 * Thrown, if field deserialization consumes more or less bytes then specified by the header.
 *
 * @author Timm Felden
 */
final class PoolSizeMissmatchError(
  block : Int,
  begin : Long,
  end : Long,
  field : internal.FieldDeclaration[_, _ <: SkillObject],
  last : Long)
    extends SkillException(
      s"""Corrupted data chunk in block ${block + 1} between 0x${begin.toHexString} and 0x${end.toHexString}
 Field ${field.owner.name}.${field.name} of type: ${field.t.toString}
 Last position: ${last.toHexString}""")
object PoolSizeMissmatchError {
  def apply(
    block : Int,
    begin : Long,
    end : Long,
    field : internal.FieldDeclaration[_, _ <: SkillObject],
    last : Long) = new PoolSizeMissmatchError(
    block : Int,
    begin : Long,
    end : Long,
    field : internal.FieldDeclaration[_, _ <: SkillObject],
    last : Long)
}

/**
 * Thrown in case of a type miss-match on a field type.
 *
 * @author Timm Felden
 */
final class TypeMissmatchError(t : FieldType[_], expected : String, fieldName : String, poolName : String)
  extends SkillException(s"""During construction of $poolName.$fieldName.
Encountered incompatible type "$t" (expected: $expected)""")
object TypeMissmatchError {
  def apply(
    t : FieldType[_],
    expected : String,
    fieldName : String,
    poolName : String) = new TypeMissmatchError(
    t : FieldType[_],
    expected : String,
    fieldName : String,
    poolName : String)
}

/**
 * Thrown if a restriction fails its check.
 *
 * @author Timm Felden
 */
final case class RestrictionCheckFailed(msg : String, cause : Throwable = null) extends SkillException(msg, cause);