package de.ust.skill.common.scala.internal.fieldTypes;

import scala.collection.mutable.ArrayBuffer
import de.ust.skill.common.scala.api.SkillObject
import de.ust.skill.common.scala.internal.StoragePool
import de.ust.skill.common.scala.api
import de.ust.skill.common.jvm.streams.InStream
import de.ust.skill.common.jvm.streams.MappedOutStream
import scala.collection.mutable.HashSet
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap
import de.ust.skill.common.scala.api.Access

/**
 * the top of the actual field type hierarchy
 *
 * @author Timm
 * Felden
 *
 * @param typeID the skill type ID as obtained from the read file or as it would appear in the to be written file
 * @param <T> the scala type to represent instances of this field type
 */
sealed abstract class FieldType[@specialized T](override val typeID : Int) extends api.FieldType[T] {
  def toString : String

  /**
   * read a singe instance
   */
  def read(in : InStream) : T

  /**
   * offset of a single instance
   */
  def offset(target : T) : Long

  /**
   * write a single instance
   */
  def write(target : T, out : MappedOutStream) : Unit

  override def equals(obj : Any) = obj match {
    case o : FieldType[_] ⇒ o.typeID == typeID
    case _                ⇒ false
  }
  override def hashCode = typeID.toInt
}

sealed abstract class ConstantInteger[T](_typeID : Int) extends FieldType[T](_typeID) {
  def value : T

  final def read(in : InStream) : T = value

  final def offset(target : T) : Long = 0

  final def write(target : T, out : MappedOutStream) {}
}

final case class ConstantI8(value : Byte) extends ConstantInteger[Byte](0) {
  override def toString() : String = "const i8 = "+("%02X" format value)
  override def equals(obj : Any) = obj match {
    case ConstantI8(v) ⇒ v == value
    case _             ⇒ false
  }
}
final case class ConstantI16(value : Short) extends ConstantInteger[Short](1) {
  override def toString() : String = "const i16 = "+("%04X" format value)
  override def equals(obj : Any) = obj match {
    case ConstantI16(v) ⇒ v == value
    case _              ⇒ false
  }
}
final case class ConstantI32(value : Int) extends ConstantInteger[Int](2) {
  override def toString() : String = "const i32 = "+value.toHexString
  override def equals(obj : Any) = obj match {
    case ConstantI32(v) ⇒ v == value
    case _              ⇒ false
  }
}
final case class ConstantI64(value : Long) extends ConstantInteger[Long](3) {
  override def toString() : String = "const i64 = "+value.toHexString
  override def equals(obj : Any) = obj match {
    case ConstantI64(v) ⇒ v == value
    case _              ⇒ false
  }
}
final case class ConstantV64(value : Long) extends ConstantInteger[Long](4) {
  override def toString() : String = "const v64 = "+value.toHexString
  override def equals(obj : Any) = obj match {
    case ConstantV64(v) ⇒ v == value
    case _              ⇒ false
  }
}

final class AnnotationType(
  val types : ArrayBuffer[StoragePool[_ <: SkillObject, _ <: SkillObject]],
  val typesByName : HashMap[String, StoragePool[_ <: SkillObject, _ <: SkillObject]])
    extends FieldType[SkillObject](5) {

  override def read(in : InStream) : SkillObject = {
    val t = in.v64
    if (0 == t) {
      in.v64
      null
    } else {
      types(t.toInt - 1).getById(in.v64.toInt)
    }
  }

  def offset(target : SkillObject) : Long = {
    if (null == target) 2L
    else {
      val p = typesByName(target.getTypeName).asInstanceOf[StoragePool[SkillObject, SkillObject]]
      V64.offset(p.poolIndex + 1) + V64.offset(target.skillID)
    }
  }

  def write(target : SkillObject, out : MappedOutStream) : Unit = {
    if (null == target) out.i16(0)
    else {
      val p = typesByName(target.getTypeName).asInstanceOf[StoragePool[SkillObject, SkillObject]]
      out.v64(p.poolIndex + 1)
      out.v64(target.skillID)
    }
  }

  override def toString : String = "annotation"
}

final case object BoolType extends FieldType[Boolean](6) {
  override def read(in : InStream) = in.i8 != 0

  override def offset(target : Boolean) : Long = 1

  override def write(target : Boolean, out : MappedOutStream) : Unit = if (target) out.i8(-1) else out.i8(0)

  override def toString() : String = "bool"
}

sealed abstract class IntegerType[T](_typeID : Int) extends FieldType[T](_typeID);

final case object I8 extends IntegerType[Byte](7) {
  override def read(in : InStream) = in.i8
  override def offset(target : Byte) : Long = 1
  override def write(target : Byte, out : MappedOutStream) : Unit = out.i8(target)

  override def toString() : String = "i8"
}

final case object I16 extends IntegerType[Short](8) {
  override def read(in : InStream) = in.i16
  override def offset(target : Short) : Long = 2
  override def write(target : Short, out : MappedOutStream) : Unit = out.i16(target)

  override def toString() : String = "i16"
}
final case object I32 extends IntegerType[Int](9) {
  override def read(in : InStream) = in.i32
  override def offset(target : Int) : Long = 4
  override def write(target : Int, out : MappedOutStream) : Unit = out.i32(target)

  override def toString() : String = "i32"
}
final case object I64 extends IntegerType[Long](10) {
  override def read(in : InStream) = in.i64
  override def offset(target : Long) : Long = 8
  override def write(target : Long, out : MappedOutStream) : Unit = out.i64(target)

  override def toString() : String = "i64"
}
final case object V64 extends IntegerType[Long](11) {
  @inline
  override def read(in : InStream) = in.v64
  @inline
  override final def offset(v : Long) : Long = if (0L == (v & 0xFFFFFFFFFFFFFF80L)) {
    1L
  } else if (0L == (v & 0xFFFFFFFFFFFFC000L)) {
    2
  } else if (0L == (v & 0xFFFFFFFFFFE00000L)) {
    3
  } else if (0L == (v & 0xFFFFFFFFF0000000L)) {
    4
  } else if (0L == (v & 0xFFFFFFF800000000L)) {
    5
  } else if (0L == (v & 0xFFFFFC0000000000L)) {
    6
  } else if (0L == (v & 0xFFFE000000000000L)) {
    7
  } else if (0L == (v & 0xFF00000000000000L)) {
    8
  } else {
    9
  }
  @inline
  override final def write(target : Long, out : MappedOutStream) : Unit = out.v64(target)

  override def toString() : String = "v64"
}

final case object F32 extends FieldType[Float](12) {
  override def read(in : InStream) = in.f32
  override def offset(target : Float) : Long = 4
  override def write(target : Float, out : MappedOutStream) : Unit = out.f32(target)

  override def toString() : String = "f32"
}
final case object F64 extends FieldType[Double](13) {
  override def read(in : InStream) = in.f64
  override def offset(target : Double) : Long = 8
  override def write(target : Double, out : MappedOutStream) : Unit = out.f64(target)

  override def toString() : String = "f64"
}

/**
 * This type is only used to have field types sealed.
 */
abstract class StringType extends FieldType[String](14);

/**
 * container types
 */
sealed abstract class CompoundType[T](_typeID : Int) extends FieldType[T](_typeID);
sealed abstract class SingleBaseTypeContainer[T <: Iterable[Base], Base](_typeID : Int)
    extends CompoundType[T](_typeID) {
  def groundType : FieldType[Base]

  override def offset(target : T) : Long =
    target.foldLeft(V64.offset(target.size)) { case (r, i) ⇒ r + groundType.offset(i) }

  override def write(target : T, out : MappedOutStream) : Unit = {
    out.v64(target.size)
    target.foreach(groundType.write(_, out))
  }
}

final case class ConstantLengthArray[T](val length : Int, val groundType : FieldType[T])
    extends SingleBaseTypeContainer[ArrayBuffer[T], T](15) {

  override def read(in : InStream) = (for (i ← 0 until length) yield groundType.read(in)).to

  override def offset(target : ArrayBuffer[T]) : Long = target.foldLeft(0L) { case (r, i) ⇒ r + groundType.offset(i) }

  override def write(target : ArrayBuffer[T], out : MappedOutStream) : Unit = target.foreach(groundType.write(_, out))

  override def toString() : String = groundType+"["+length+"]"
  override def equals(obj : Any) = obj match {
    case ConstantLengthArray(l, g) ⇒ l == length && g == groundType
    case _                         ⇒ false
  }
}

final case class VariableLengthArray[T](val groundType : FieldType[T])
    extends SingleBaseTypeContainer[ArrayBuffer[T], T](17) {

  override def read(in : InStream) = (for (i ← 0 until in.v64.toInt) yield groundType.read(in)).to

  override def toString() : String = groundType+"[]"
  override def equals(obj : Any) = obj match {
    case VariableLengthArray(g) ⇒ g == groundType
    case _                      ⇒ false
  }
}

final case class ListType[T](val groundType : FieldType[T])
    extends SingleBaseTypeContainer[ListBuffer[T], T](18) {

  override def read(in : InStream) = (for (i ← 0 until in.v64.toInt) yield groundType.read(in)).to

  override def toString() : String = "list<"+groundType+">"
  override def equals(obj : Any) = obj match {
    case ListType(g) ⇒ g == groundType
    case _           ⇒ false
  }
}
final case class SetType[T](val groundType : FieldType[T])
    extends SingleBaseTypeContainer[HashSet[T], T](19) {

  override def read(in : InStream) = (for (i ← 0 until in.v64.toInt) yield groundType.read(in)).to

  override def toString() : String = "set<"+groundType+">"
  override def equals(obj : Any) = obj match {
    case SetType(g) ⇒ g == groundType
    case _          ⇒ false
  }
}
final case class MapType[K, V](val keyType : FieldType[K], val valueType : FieldType[V])
    extends CompoundType[HashMap[K, V]](20) {

  override def read(in : InStream) : HashMap[K, V] = {
    val size = in.v64.toInt
    val r = new HashMap[K, V]
    r.sizeHint(size)
    for (i ← 0 until size)
      r(keyType.read(in)) = valueType.read(in)

    r
  }

  override def offset(target : HashMap[K, V]) : Long =
    target.foldLeft(V64.offset(target.size)) { case (r, (k, v)) ⇒ r + keyType.offset(k) + valueType.offset(v) }

  override def write(target : HashMap[K, V], out : MappedOutStream) : Unit = {
    out.v64(target.size)
    for ((k, v) ← target) {
      keyType.write(k, out)
      valueType.write(v, out)
    }
  }

  override def toString() : String = s"map<$keyType, $valueType>"
  override def equals(obj : Any) = obj match {
    case MapType(k, v) ⇒ k.equals(keyType) && v.equals(valueType)
    case _             ⇒ false
  }
}

/**
 * All user defined type instantiations inherit this. The purpose of this type is to enable useful error messages
 * in pattern matching over field types.
 */
abstract class UserType[T <: SkillObject](_typeID : Int) extends FieldType[T](_typeID) with Access[T];