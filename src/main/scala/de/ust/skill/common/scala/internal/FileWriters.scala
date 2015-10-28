package de.ust.skill.common.scala.internal

import scala.annotation.switch
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.global
import scala.language.existentials

import de.ust.skill.common.jvm.streams.FileOutputStream
import de.ust.skill.common.jvm.streams.OutStream
import de.ust.skill.common.scala.api.SkillObject
import de.ust.skill.common.scala.internal.fieldTypes.ConstantI16
import de.ust.skill.common.scala.internal.fieldTypes.ConstantI32
import de.ust.skill.common.scala.internal.fieldTypes.ConstantI64
import de.ust.skill.common.scala.internal.fieldTypes.ConstantI8
import de.ust.skill.common.scala.internal.fieldTypes.ConstantLengthArray
import de.ust.skill.common.scala.internal.fieldTypes.ConstantV64
import de.ust.skill.common.scala.internal.fieldTypes.FieldType
import de.ust.skill.common.scala.internal.fieldTypes.MapType
import de.ust.skill.common.scala.internal.fieldTypes.MapType
import de.ust.skill.common.scala.internal.fieldTypes.SingleBaseTypeContainer

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

  private final def keepField(t : FieldType[_]) : Boolean = {
    t.typeID < 15 || (t.typeID >= 32 && t.asInstanceOf[StoragePool[_, _]].cachedSize != 0) ||
      (t match {
        case t : SingleBaseTypeContainer[_, _] ⇒ keepField(t.groundType)
        case t : MapType[_, _]                 ⇒ keepField(t.keyType) && keepField(t.valueType)
        case _                                 ⇒ false // @note other cases can not happen, because they were caught above
      })
  }

  /**
   * we require type quantification to solve type equation after partition
   */
  @inline
  private def writeRelevantFieldCount[T <: SkillObject](t : StoragePool[T, _ <: SkillObject], out : FileOutputStream) {

    // we drop all types and fields that refer to *unused* types
    val (rFields, irrFields) = t.dataFields.partition(f ⇒ keepField(f.t))
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

  private final def writeFieldData(
    state : SkillState, out : FileOutputStream, offset : Int, fieldQueue : ArrayBuffer[FieldDeclaration[_, _]]) {
    // map field data
    val map = out.mapBlock(offset)

    // write field data
    for (f ← fieldQueue.par) {
      val c = f.dataChunks.last
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
      for (f ← t.dataFields if keepField(f.t)) {
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
            // we have to make absolute indices relative
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
      val c = f.dataChunks(0)
      c.begin = offset
      c.end = endOffset

      offset = endOffset
    }

    writeFieldData(state, out, offset, fieldQueue)
  }

  def append(state : SkillState, out : FileOutputStream) {

    // save the index of the first new pool
    val newPoolIndex = {
      val r = state.types.indexWhere(_.blocks.isEmpty)
      // fix -1 result, because we want to compare against this barrier
      if (-1 == r)
        Integer.MAX_VALUE
      else
        r
    }

    //////////////////////////////
    // PHASE 2: Check & Reorder //
    //////////////////////////////

    // @note: I know that phase 2 happens before phase 1 ;)
    // we implement it that way to keep 

    // make lbpsi map, update data map to contain dynamic instances and create serialization skill IDs for
    // serialization
    // index → bpsi
    val lbpoMap = new Array[Int](state.types.size);
    // poolOffset → fieldID → chunk
    val chunkMap = new Array[Array[Chunk]](state.types.size)
    state.types.par.foreach {
      case p : BasePool[_] ⇒
        p.prepareAppend(chunkMap, lbpoMap)
      case _ ⇒
    }

    val (rTypes, irrTypes) = state.types.partition { p ⇒
      // new index?
      if (p.typeID - 32 >= newPoolIndex)
        true
      // new instance or field?
      else if (p.size > 0) {
        val cm = chunkMap(p.poolIndex)
        if (cm != null && p.dataFields.exists(f ⇒ null != cm(f.index)))
          true
        else
          false
      } else
        false
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
      for (f ← t.dataFields if keepField(f.t)) {
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

    ////////////////////
    // PHASE 3: Write //
    ////////////////////

    // write string block
    strings.prepareAndAppend(out);

    // calculate offsets for relevant fields
    for (
      p ← state.types.par;
      if null != chunkMap(p.poolIndex);
      f ← p.dataFields.par;
      if null != chunkMap(p.poolIndex)(f.index)
    ) f.offset

    // write count of the type block
    out.v64(rTypes.size);

    // write headers
    val fieldQueue = new ArrayBuffer[FieldDeclaration[_, _]]
    for (p ← rTypes) {
      // generic append
      val newPool = p.poolIndex >= newPoolIndex;
      var fields = 0
      for (f ← p.dataFields if null != chunkMap(p.poolIndex)(f.index)) {
        fields += 1
        fieldQueue += f
      }

      strings.write(p.name, out);
      val count = p.blocks.last.dynamicCount
      out.v64(count);

      if (newPool) {
        restrictions(p, out);
        if (null == p.superPool) {
          out.i8(0);
        } else {
          out.v64(p.superPool.typeID - 31);
          if (0 != count)
            out.v64(lbpoMap(p.poolIndex));
        }
      } else if (null != p.superPool && 0 != count) {
        out.v64(lbpoMap(p.poolIndex) - lbpoMap(p.basePool.poolIndex));
      }

      out.v64(fields);
    }

    // write fields
    var offset = 0;
    val fs = fieldQueue.iterator
    while (fs.hasNext) {
      val f = fs.next
      out.v64(f.index)

      // a new field
      if (f.dataChunks.last.isInstanceOf[BulkChunk]) {
        strings.write(f.name, out)
        writeType(f.t, out)
        restrictions(f, out)
      }

      val end = offset + f.cachedOffset.toInt
      val c = f.dataChunks.last
      c.begin = offset
      c.end = end
      out.v64(end)
      offset = end
    }

    writeFieldData(state, out, offset, fieldQueue)
  }
}