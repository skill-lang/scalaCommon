package de.ust.skill.common.scala.internal

import java.nio.file.Path
import java.util.concurrent.ConcurrentLinkedQueue
import scala.annotation.switch
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.concurrent.ExecutionContext.Implicits.global
import de.ust.skill.common.jvm.streams.FileInputStream
import de.ust.skill.common.jvm.streams.MappedInStream
import de.ust.skill.common.scala.api.ParseException
import de.ust.skill.common.scala.api.SkillException
import de.ust.skill.common.scala.api.WriteMode
import de.ust.skill.common.scala.internal.fieldTypes.AnnotationType
import de.ust.skill.common.scala.internal.fieldTypes.BoolType
import de.ust.skill.common.scala.internal.fieldTypes.ConstantI16
import de.ust.skill.common.scala.internal.fieldTypes.ConstantI32
import de.ust.skill.common.scala.internal.fieldTypes.ConstantI64
import de.ust.skill.common.scala.internal.fieldTypes.ConstantI8
import de.ust.skill.common.scala.internal.fieldTypes.ConstantLengthArray
import de.ust.skill.common.scala.internal.fieldTypes.ConstantV64
import de.ust.skill.common.scala.internal.fieldTypes.F32
import de.ust.skill.common.scala.internal.fieldTypes.F64
import de.ust.skill.common.scala.internal.fieldTypes.FieldType
import de.ust.skill.common.scala.internal.fieldTypes.I16
import de.ust.skill.common.scala.internal.fieldTypes.I32
import de.ust.skill.common.scala.internal.fieldTypes.I64
import de.ust.skill.common.scala.internal.fieldTypes.I8
import de.ust.skill.common.scala.internal.fieldTypes.ListType
import de.ust.skill.common.scala.internal.fieldTypes.MapType
import de.ust.skill.common.scala.internal.fieldTypes.SetType
import de.ust.skill.common.scala.internal.fieldTypes.V64
import de.ust.skill.common.scala.internal.fieldTypes.VariableLengthArray
import de.ust.skill.common.scala.api.SkillObject

/**
 * @author Timm Felden
 */
trait SkillFileParser[SF <: SkillState] {

  final class LFEntry(val pool : StoragePool[_, _], val count : Int);

  def newPool(
    typeId : Int,
    name : String,
    superPool : StoragePool[_ <: SkillObject, _ <: SkillObject],
    rest : HashSet[restrictions.TypeRestriction]) : StoragePool[_ <: SkillObject, _ <: SkillObject]

  def makeState(path : Path,
                mode : WriteMode,
                String : StringPool,
                Annotation : AnnotationType,
                types : ArrayBuffer[StoragePool[_ <: SkillObject, _ <: SkillObject]],
                typesByName : HashMap[String, StoragePool[_ <: SkillObject, _ <: SkillObject]],
                dataList : ArrayBuffer[MappedInStream]) : SF

  /**
   * read a state from file
   */
  final def read(in : FileInputStream, mode : WriteMode) : SF = {
    // ERROR DETECTION
    var blockCounter = 0;
    val seenTypes = new HashSet[String]();

    // PARSE STATE
    val String = new StringPool(in)
    val types = new ArrayBuffer[StoragePool[_ <: SkillObject, _ <: SkillObject]]()
    val typesByName = new HashMap[String, StoragePool[_ <: SkillObject, _ <: SkillObject]]()
    val Annotation = new AnnotationType(types, typesByName)
    val dataList = new ArrayBuffer[MappedInStream]()

    /**
     * Turns a field type into a preliminary type information. In case of user types, the declaration of the respective
     *  user type may follow after the field declaration.
     */
    def parseFieldType : FieldType[_] = in.v64 match {
      case 0  ⇒ ConstantI8(in.i8)
      case 1  ⇒ ConstantI16(in.i16)
      case 2  ⇒ ConstantI32(in.i32)
      case 3  ⇒ ConstantI64(in.i64)
      case 4  ⇒ ConstantV64(in.v64)
      case 5  ⇒ Annotation
      case 6  ⇒ BoolType
      case 7  ⇒ I8
      case 8  ⇒ I16
      case 9  ⇒ I32
      case 10 ⇒ I64
      case 11 ⇒ V64
      case 12 ⇒ F32
      case 13 ⇒ F64
      case 14 ⇒ String
      case 15 ⇒ ConstantLengthArray(in.v64.toInt, parseFieldType)
      case 17 ⇒ VariableLengthArray(parseFieldType)
      case 18 ⇒ ListType(parseFieldType)
      case 19 ⇒ SetType(parseFieldType)
      case 20 ⇒ MapType(parseFieldType, parseFieldType)
      case i if i >= 32 ⇒ if (i - 32 < types.size) types(i.toInt - 32)
      else throw ParseException(in, blockCounter, s"inexistent user type ${i.toInt - 32} (user types: ${types.map { t ⇒ s"${t.poolIndex} -> ${t.name}" }.mkString(", ")})")
      case id ⇒ throw ParseException(in, blockCounter, s"Invalid type ID: $id")
    }

    // process stream
    while (!in.eof) {

      // string block
      try {
        val count = in.v64.toInt;

        if (0 != count) {
          val offsets = new Array[Int](count);
          var i = 0
          while (i < count) {
            offsets(i) = in.i32;
            i += 1
          }
          String.stringPositions.sizeHint(String.stringPositions.size + count)
          var last = 0
          i = 0
          while (i < count) {
            String.stringPositions.append(new StringPosition(in.position + last, offsets(i) - last))
            String.idMap += null
            last = offsets(i)
            i += 1
          }
          in.jump(in.position + last);
        }

      } catch {
        case e : Exception ⇒
          throw ParseException(in, blockCounter, "corrupted string block", e)
      }

      // type block
      try {
        var typeCount = in.v64.toInt

        // this barrier is strictly increasing inside of each block and reset to 0 at the beginning of each block
        var blockIDBarrier : Int = 0;

        // reset counters and queues
        seenTypes.clear
        val resizeQueue = new ArrayBuffer[StoragePool[_, _]](typeCount)

        // number of fields to expect for that type in this block
        val localFields = new ArrayBuffer[LFEntry](typeCount)

        // parse type definitions
        while (typeCount != 0) {
          typeCount -= 1
          val name = String.get(in.v64.toInt)

          // check null name
          if (null == name)
            throw ParseException(in, blockCounter, "Corrupted file, nullptr in typename")

          // check duplicate types
          if (seenTypes.contains(name))
            throw ParseException(in, blockCounter, s"Duplicate definition of type $name")

          seenTypes.add(name)

          val count = in.v64.toInt
          val definition = typesByName.get(name).getOrElse {

            // type restrictions
            var restrictionCount = in.v64.toInt
            val rest = new HashSet[restrictions.TypeRestriction]
            rest.sizeHint(restrictionCount)
            while (restrictionCount != 0) {
              restrictionCount -= 1

              rest += ((in.v64.toInt : @switch) match {
                case 0 ⇒ restrictions.Unique
                case 1 ⇒ restrictions.Singleton
                case 2 ⇒ restrictions.Monotone
                case 3 ⇒ restrictions.Abstract
                case 5 ⇒ restrictions.DefaultTypeRestriction(in.v64.toInt)

                case i ⇒ throw new ParseException(in, blockCounter,
                  s"Found unknown field restriction $i. Please regenerate your binding, if possible.", null)
              })
            }

            // super
            val superID = in.v64.toInt
            val superPool = if (0 == superID)
              null
            else {
              if (superID > types.size)
                throw ParseException(in, blockCounter, s"""Type $name refers to an ill-formed super type.
  found: $superID
  current number of types: ${types.size}""")
              else {
                val r = types(superID - 1)
                assert(r != null)
                r
              }
            }

            // allocate pool
            val r = newPool(types.size + 32, name, superPool, rest)
            types.append(r)
            typesByName.put(name, r)
            r
          }

          if (blockIDBarrier < definition.typeID)
            blockIDBarrier = definition.typeID;
          else
            throw new ParseException(in, blockCounter,
              s"Found unordered type block. Type $name has id ${definition.typeID}, barrier was $blockIDBarrier.");

          // in contrast to prior implementation, bpo is the position inside of data, even if there are no actual
          // instances. We need this behavior, because that way we can cheaply calculate the number of static instances
          val lbpo = if (null == definition.superPool)
            0
          else if (0 != count)
            in.v64().toInt
          else
            definition.superPool.blocks.last.bpo

          // static count and cached size are updated in the resize phase
          definition.blocks.append(Block(blockCounter, definition.basePool.cachedSize + lbpo, count, count))

          resizeQueue.append(definition)
          localFields.append(new LFEntry(definition, in.v64().toInt))
        }

        // resize pools, i.e. update cachedSize and staticCount
        for (i ← (0 until resizeQueue.size).par) {
          val p = resizeQueue(i)
          val b = p.blocks.last
          p.cachedSize += b.dynamicCount
          // calculate static count; if there is a next type, and it is one of our sub types,
          // then the difference between its bpo and ours is the number of static instances, otherwise, we already have
          // the correct number, as there are no subtypes in this block
          if (i + 1 != resizeQueue.size) {
            val q = resizeQueue(i + 1)
            if (q.superPool == p)
              b.staticCount = q.blocks.last.bpo - b.bpo
          }

          p.staticDataInstnaces += b.staticCount
        }

        // track offset information, so that we can create the block maps and jump to the next block directly after
        // parsing field information
        var dataEnd = 0L

        // parse fields
        val es = localFields.iterator
        while (es.hasNext) {
          val e = es.next()
          val p = e.pool
          var legalFieldIDBarrier = 1 + p.dataFields.size
          val block = p.blocks.last
          var localFieldCount = e.count
          while (0 != localFieldCount) {
            localFieldCount -= 1
            val id = in.v64.toInt
            if (id <= 0 || legalFieldIDBarrier < id)
              throw ParseException(in, blockCounter, s"Found an illegal field ID: $id")

            var endOffset : Long = 0
            if (id == legalFieldIDBarrier) {
              // new field
              legalFieldIDBarrier += 1
              val fieldName = String.get(in.v64.toInt)
              if (null == fieldName)
                throw ParseException(in, blockCounter, s"Field ${p.name}#$id has a nullptr as name.")

              val t = parseFieldType

              // parse field restrictions
              var fieldRestrictionCount = in.v64.toInt
              val rest = new HashSet[restrictions.FieldRestriction]
              rest.sizeHint(fieldRestrictionCount)
              while (fieldRestrictionCount != 0) {
                fieldRestrictionCount -= 1

                rest += ((in.v64.toInt : @switch) match {
                  case 0 ⇒ restrictions.NonNull.theNonNull
                  case 1 ⇒ restrictions.DefaultRestriction(t.read(in))
                  case 3 ⇒ t match {
                    case I8  ⇒ restrictions.Range(in.i8, in.i8)
                    case I16 ⇒ restrictions.Range(in.i16, in.i16)
                    case I32 ⇒ restrictions.Range(in.i32, in.i32)
                    case I64 ⇒ restrictions.Range(in.i64, in.i64)
                    case V64 ⇒ restrictions.Range(in.v64, in.v64)
                    case F32 ⇒ restrictions.Range(in.f32, in.f32)
                    case F64 ⇒ restrictions.Range(in.f64, in.f64)
                    case t   ⇒ throw new ParseException(in, blockCounter, s"Type $t can not be range restricted!", null)
                  }
                  case 5 ⇒ restrictions.Coding(String.get(in.v64.toInt))
                  case 7 ⇒ restrictions.ConstantLengthPointer
                  case 9 ⇒ restrictions.OneOf((0 until in.v64.toInt).map(i ⇒
                    parseFieldType match {
                      case t : StoragePool[_, _] ⇒ t.getInstanceClass
                      case t ⇒ throw new ParseException(in, blockCounter,
                        s"Found a one of restrictions that tries to restrict to non user type $t.", null)
                    }).toArray
                  )
                  case i ⇒ throw new ParseException(in, blockCounter,
                    s"Found unknown field restriction $i. Please regenerate your binding, if possible.", null)
                })
              }
              endOffset = in.v64

              val f = p.addField(id, t, fieldName, rest)
              f.addChunk(new BulkChunk(dataEnd, endOffset, p.cachedSize, p.blocks.size - 1))
            } else {
              // known field
              endOffset = in.v64
              p.dataFields(id - 1).addChunk(new SimpleChunk(dataEnd, endOffset, block.dynamicCount, block.bpo))
            }
            dataEnd = endOffset
          }
        }

        // jump over data and continue in the next block
        dataList.append(in.jumpAndMap(dataEnd.toInt))
      } catch {
        case e : SkillException ⇒ throw e
        case e : Exception      ⇒ throw ParseException(in, blockCounter, "unexpected foreign exception", e)
      }

      blockCounter += 1
      seenTypes.clear()
    }

    // note there still isn't a single instance
    makeState(in.path(), mode, String, Annotation, types, typesByName, dataList)
  }

  /**
   * has to be called by make state after instances have been allocated
   */
  final protected def triggerFieldDeserialization(
    types : ArrayBuffer[StoragePool[_ <: SkillObject, _ <: SkillObject]],
    dataList : ArrayBuffer[MappedInStream]) {
    val errors = new ConcurrentLinkedQueue[Throwable]
    val barrier = new Barrier
    val ts = types.iterator
    while (ts.hasNext) {
      val t = ts.next
      var fieldIndex = t.dataFields.size
      while (0 != fieldIndex) {
        fieldIndex -= 1
        val f = t.dataFields(fieldIndex)
        var blockIndex = 0
        val dcs = f.dataChunks.iterator
        while (dcs.hasNext) {
          val dc = dcs.next
          if (dc.isInstanceOf[BulkChunk]) {
            blockIndex = dc.asInstanceOf[BulkChunk].blockCount
            if (dc.count != 0)
              global.execute(new Job(barrier, f, dataList(blockIndex), dc, errors))
          } else {
            blockIndex += 1
            if (dc.count != 0)
              global.execute(new Job(barrier, f, dataList(blockIndex), dc, errors))
          }
        }
      }
    }

    barrier.await

    if (!errors.isEmpty()) {
      throw new InternalError(s"${errors.size()} Job(s) crashed. First error supplied below.", errors.peek())
    }
  }
}

final class Job(
    val barrier : Barrier,
    val field : FieldDeclaration[_, _],
    val in : MappedInStream,
    val target : Chunk,
    val errors : ConcurrentLinkedQueue[Throwable]) extends Runnable {

  barrier.begin

  override def run {
    try {
      field.read(in, target)
    } catch {
      case e : Throwable ⇒ errors.add(e)
    }
    barrier.end
  }
}
