/*  ___ _  ___ _ _                                                            *\
 * / __| |/ (_) | |       Your SKilL Scala Binding                            *
 * \__ \ ' <| | | |__     <<debug>>                                           *
 * |___/_|\_\_|_|____|    by: <<some developer>>                              *
\*                                                                            */
package de.ust.skill.common.scala.internal.restrictions

import de.ust.skill.common.scala.api.RestrictionCheckFailed
import de.ust.skill.common.scala.api.SkillObject
import de.ust.skill.common.scala.api.RestrictionCheckFailed
import de.ust.skill.common.scala.api.RestrictionCheckFailed
import de.ust.skill.common.scala.internal.StoragePool

/**
 * Marker trait for restrictions applicable to types.
 *
 * @author Timm Felden
 */
sealed abstract class TypeRestriction;

/**
 * restrictions that require runtime checks
 */
sealed abstract class CheckableTypeRestriction
    extends TypeRestriction {
  def check(pool : StoragePool[_, _]) : Unit;
}

final case object Unique extends CheckableTypeRestriction {
  override def check(pool : StoragePool[_, _]) {
    // the problem with unique is that we did not define what equality actually is :-/
    ???
  }
}

final case object Singleton extends CheckableTypeRestriction {
  override def check(pool : StoragePool[_, _]) {
    if (pool.size > 1)
      throw new RestrictionCheckFailed(s"$pool is not a singleton, it has ${pool.size} instances.")
  }
}

/**
 * This restriction should be treated by a generator somehow.
 */
final case object Monotone extends TypeRestriction;

/**
 * Abstract types must not have static instances.
 */
final case object Abstract extends CheckableTypeRestriction {
  override def check(pool : StoragePool[_, _]) {
    if (pool.staticSize != 0)
      throw new RestrictionCheckFailed(s"$pool is abstract, but it has ${pool.staticSize} static instances.")
  }
}
/**
 * Default can be applied to both fields and types, will not check anything
 */
final class DefaultTypeRestriction(val typeId : Int) extends TypeRestriction;
object DefaultTypeRestriction {
  def apply(typeId : Int) = new DefaultTypeRestriction(typeId)
}

/**
 * Marker trait for restrictions applicable to fields.
 *
 * @author Timm Felden
 */
sealed abstract class FieldRestriction(val id : Long) {}

/**
 * restrictions that require runtime checks
 */
sealed abstract class CheckableFieldRestriction[@specialized(Boolean, Byte, Char, Double, Float, Int, Long, Short) T](_id : Long)
    extends FieldRestriction(_id) {
  def check(value : T) : Unit;
}

/**
 * A nonnull restricition. It will ensure that field data is non null.
 */
object NonNull {
  val theNonNull = new NonNull[AnyRef]

  def apply[T <: AnyRef] : NonNull[T] = theNonNull.asInstanceOf[NonNull[T]]
}
/**
 * A nonnull restricition. It will ensure that field data is non null.
 */
final class NonNull[T <: AnyRef] private () extends CheckableFieldRestriction[T](0) {

  override def check(value : T) {
    if (value == null)
      throw RestrictionCheckFailed("Null value violates @NonNull.")
  }
}

/**
 * Default can be applied to both fields and types, will not check anything
 */
final case class DefaultRestriction[T](val value : T) extends FieldRestriction(1);

/**
 * manual specialization because Scala wont help us
 */
object Range {
  final case class RangeI8(min : Byte, max : Byte) extends CheckableFieldRestriction[Byte](3) {
    override def check(value : Byte) {
      if (value < min || max < value) throw RestrictionCheckFailed(s"$value is not in Range($min, $max)")
    }
  }
  def apply(min : Byte, max : Byte) = new RangeI8(min, max)

  final case class RangeI16(min : Short, max : Short) extends CheckableFieldRestriction[Short](3) {
    override def check(value : Short) {
      if (value < min || max < value) throw RestrictionCheckFailed(s"$value is not in Range($min, $max)")
    }
  }
  def apply(min : Short, max : Short) = new RangeI16(min, max)

  final case class RangeI32(min : Int, max : Int) extends CheckableFieldRestriction[Int](3) {
    override def check(value : Int) {
      if (value < min || max < value) throw RestrictionCheckFailed(s"$value is not in Range($min, $max)")
    }
  }
  def apply(min : Int, max : Int) = new RangeI32(min, max)

  final case class RangeI64(min : Long, max : Long) extends CheckableFieldRestriction[Long](3) {
    override def check(value : Long) {
      if (value < min || max < value) throw RestrictionCheckFailed(s"$value is not in Range($min, $max)")
    }
  }
  def apply(min : Long, max : Long) = new RangeI64(min, max)

  final case class RangeF32(min : Float, max : Float) extends CheckableFieldRestriction[Float](3) {
    override def check(value : Float) {
      if (min <= value && value <= max) {
        return
      }

      throw RestrictionCheckFailed(s"$value is not in Range($min, $max)")
    }
  }
  def apply(min : Float, max : Float) = new RangeF32(min, max)

  final case class RangeF64(min : Double, max : Double) extends CheckableFieldRestriction[Double](3) {
    override def check(value : Double) {
      println(s"checking value: $value")
      if (min <= value && value <= max) {
        return
      }

      throw RestrictionCheckFailed(s"$value is not in Range($min, $max)")
    }
  }
  def apply(min : Double, max : Double) = new RangeF64(min, max)
}

/**
 * custom coding
 */
final case class Coding(val kind : String) extends FieldRestriction(5);

/**
 * This restriction disables variable length coding of references.
 */
case object ConstantLengthPointer extends FieldRestriction(7) {
  def apply = this
}

/**
 * ensures that instances of a field are in fact instances of just some sub types of the specified type
 */
final case class OneOf[T <: SkillObject](types : Array[Class[_ <: SkillObject]]) extends CheckableFieldRestriction[T](9) {
  override def check(value : T) {
    if (null != value && !types.exists(_.isInstance(value)))
      throw RestrictionCheckFailed(s"$value is not one of ${types.map(_.getName).mkString}")
  }
}
