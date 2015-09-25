package de.ust.skill.common.scala.api

import de.ust.skill.common.jvm.streams.InStream

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
final case class ParseException(in : InStream, block : Int, msg : String, cause : Throwable = null) extends SkillException(
  s"In block ${block + 1} @0x${in.position.toHexString}: $msg",
  cause
);