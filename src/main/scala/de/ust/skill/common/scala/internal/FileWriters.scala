package de.ust.skill.common.scala.internal

import scala.annotation.switch
import de.ust.skill.common.jvm.streams.FileOutputStream
import de.ust.skill.common.jvm.streams.OutStream
import de.ust.skill.common.scala.internal.fieldTypes.ConstantI16
import de.ust.skill.common.scala.internal.fieldTypes.ConstantI32
import de.ust.skill.common.scala.internal.fieldTypes.ConstantI64
import de.ust.skill.common.scala.internal.fieldTypes.ConstantI8
import de.ust.skill.common.scala.internal.fieldTypes.ConstantLengthArray
import de.ust.skill.common.scala.internal.fieldTypes.ConstantV64
import de.ust.skill.common.scala.internal.fieldTypes.FieldType
import de.ust.skill.common.scala.internal.fieldTypes.MapType
import de.ust.skill.common.scala.internal.fieldTypes.SingleBaseTypeContainer
import de.ust.skill.common.scala.api.SkillObject
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.global
import scala.language.existentials

/**
 * Implementation of file writing and appending.
 */
final object FileWriters {
  private def writeType(t : FieldType[_], out : OutStream) {
    out.v64(t.typeID)

    (t.typeID : @switch) match {
      case 0 ⇒ out.i8(t.asInstanceOf[ConstantI8].value)
      case 1 ⇒ out.i16(t.asInstanceOf[ConstantI16].value)
      case 2 ⇒ out.i32(t.asInstanceOf[ConstantI32].value)
      case 3 ⇒ out.i64(t.asInstanceOf[ConstantI64].value)
      case 4 ⇒ out.v64(t.asInstanceOf[ConstantV64].value)

      case 15 ⇒
        val arr = t.asInstanceOf[ConstantLengthArray[_]]
        out.v64(arr.length)
        writeType(arr.groundType, out)

      case 17 | 18 | 19 ⇒
        writeType(t.asInstanceOf[SingleBaseTypeContainer[_, _]].groundType, out)

      case 20 ⇒
        val m = t.asInstanceOf[MapType[_, _]]
        writeType(m.keyType, out)
        writeType(m.valueType, out)

      case _ ⇒
    }
  }

  private def restrictions(t : StoragePool[_, _], out : OutStream) {
    out.i8(0)
    // TODO
  }

  private def restrictions(f : FieldDeclaration[_, _], out : OutStream) {
    out.i8(0)
    // TODO
  }

  /**
   * we require type quantification to solve type equation after partition
   */
  @inline
  private def writeRelevantFieldCount[T <: SkillObject](t : StoragePool[T, _ <: SkillObject], out : FileOutputStream) {
    // we drop all types and fields that refer to *unused* types
    val (rFields, irrFields) = t.dataFields.partition(f ⇒ f.t.typeID < 32 || f.t.asInstanceOf[StoragePool[_, _]].cachedSize != 0)
    out.v64(rFields.size)
    if (!irrFields.isEmpty) {
      // we have to reorder data fields, because some IDs may have changed and we have to write fields with
      // correct IDs
      var i = 1
      for (f ← rFields) {
        f.index = i
        t.dataFields.update(i - 1, f)
        i += 1
      }
      // irrelevant fields get higher IDs, so that they can be ignored automatically
      for (f ← irrFields) {
        f.index = i
        t.dataFields.update(i - 1, f)
        i += 1
      }
    }
  }

  final def write(state : SkillState, out : FileOutputStream) {
    // fix pools to make size operations constant time (happens in amortized constant time)
    state.types.par.foreach {
      case p : BasePool[_] ⇒
        p.fix(true)
      case _ ⇒
    }

    // find relevant types and fields
    val (rTypes, irrTypes) = state.types.partition(0 != _.cachedSize)
    val fieldQueue = new ArrayBuffer[FieldDeclaration[_, _]]

    // reorder types and assign new IDs
    if (!irrTypes.isEmpty) {
      var nextID = 32
      state.types.clear
      for (t ← rTypes) {
        t.typeID = nextID
        nextID += 1
        state.types += t
      }
      for (t ← irrTypes) {
        t.typeID = nextID
        nextID += 1
        state.types += t
      }
    }

    /**
     *  collect String instances from known string types; this is required,
     * because we use plain strings
     * @note this is a O(σ) operation:)
     * @note we do not use generation time type info, because we want to treat
     * generic fields as well
     *
     * @todo unify type and field names in string pool, so that it is no longer required to add them here (and
     * checks/searches should be faster that way)
     */
    val strings = state.String
    //////////////////////
    // PHASE 1: Collect //
    //////////////////////
    for (t ← rTypes) {
      strings.add(t.name)
      for (f ← t.dataFields if f.t.typeID < 32 || f.t.asInstanceOf[StoragePool[_, _]].cachedSize != 0) {
        fieldQueue.append(f)
        strings.add(f.name)
        (f.t.typeID : @switch) match {
          case 14 ⇒
            for (x ← t)
              strings.add(f.getR(x).asInstanceOf[String])

          case 15 | 17 | 18 | 19 if (f.t.asInstanceOf[SingleBaseTypeContainer[_, _]].groundType.typeID == 14) ⇒
            t.foreach {
              f.getR(_).asInstanceOf[Iterable[String]].foreach(strings.add)
            }

          // TODO maps

          case _ ⇒
        }
      }
    }

    //////////////////////////////
    // PHASE 2: Check & Reorder //
    //////////////////////////////

    // index → bpo
    //  @note pools.par would not be possible if it were an actual
    val lbpoMap = new Array[Int](state.types.size)

    //  check consistency of the state, now that we aggregated all instances
    state.check

    state.types.par.foreach {
      case p : BasePool[_] ⇒
        p.compress(lbpoMap)
      case _ ⇒
    }

    ////////////////////
    // PHASE 3: Write //
    ////////////////////

    //  write string block
    strings.prepareAndWrite(out)

    // Calculate Offsets
    // @note this has to happen after string IDs have been updated
    locally {
      val offsetBarrier = new Barrier
      offsetBarrier.begin
      global.execute(new Runnable { def run = try { fieldQueue.par.foreach(_.offset) } finally { offsetBarrier.end } })

      // write count of the type block
      out.v64(rTypes.length)

      for (t ← rTypes) {
        val lCount = t.blocks.head.dynamicCount
        strings.write(t.name, out)
        out.v64(lCount)
        restrictions(t, out)
        if (null == t.superPool) {
          out.i8(0)
        } else {
          out.v64(1 + t.superPool.poolIndex)
          if (0 != lCount) {
            out.v64(lbpoMap(t.poolIndex))
          }
        }

        writeRelevantFieldCount(t, out)
      }

      // await offsets before we can write fields
      offsetBarrier.await
    }

    // write fields
    var (offset, endOffset) = (0, 0)

    for (f ← fieldQueue) {
      // write field info
      out.v64(f.index)
      strings.write(f.name, out)
      writeType(f.t, out)
      restrictions(f, out)
      endOffset = offset + f.cachedOffset.toInt
      out.v64(endOffset)

      // update chunks and prepare write data
      f.dataChunks.clear()
      f.dataChunks.append(new BulkChunk(offset, endOffset, f.owner.cachedSize, 1))

      offset = endOffset
    }

    // map field data
    val map = out.mapBlock(offset)

    // write field data
    for (f ← fieldQueue.par) {
      val c = f.dataChunks.head
      f.write(map.clone(c.begin.toInt, c.end.toInt))
    }

    // we are done
    out.close()

    ///////////////////////
    // PHASE 4: Cleaning //
    ///////////////////////

    // release data structures
    state.String.clearSearilizationIDs;

    // unfix pools
    for (t ← state.types)
      t.fix(false)
  }

  def append(state : SkillState, out : FileOutputStream) {
    //    collect(state)
    ???
  }
}