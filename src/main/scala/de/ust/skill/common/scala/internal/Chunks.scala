package de.ust.skill.common.scala.internal

import de.ust.skill.common.scala.SkillID

/**
 * Chunks contain information on where field data can be found.
 *
 * @author Timm Felden
 * @note indices of recipient of the field data is not necessarily continuous;
 *       make use of staticInstances!
 * @note in contrast to other implementations, the offset is relative, because read will use a buffer over the whole
 * data segment, instead of buffering each segment itself
 *
 * @todo add abstract IO map to support parallel serialization?
 */
sealed abstract class Chunk(val begin : Long, val end : Long, val count : SkillID);

/**
 * A chunk used for regular appearances of fields.
 *
 * @author Timm Felden
 */
final class SimpleChunk(begin : Long, end : Long, count : SkillID, val bpo : SkillID) extends Chunk(begin, end, count);

/**
 * A chunk that is used iff a field is appended to a preexisting type in a
 * block.
 *
 * @author Timm Felden
 */
final class BulkChunk(begin : Long, end : Long, count : SkillID, val blockCount : Int) extends Chunk(begin, end, count);