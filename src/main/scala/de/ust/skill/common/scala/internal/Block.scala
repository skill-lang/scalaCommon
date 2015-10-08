package de.ust.skill.common.scala.internal

import de.ust.skill.common.scala.SkillID

/**
 * A block contains information about instances in a type. A StoragePool holds
 * blocks in order of appearance in a file with the invariant, that the latest
 * block in the list will be the latest block in the file. If a StoragePool
 * holds no block, then it has no instances in a file.
 *
 * @author Timm Felden
 * @note While writing a Pool to disk, the latest block is the block currently
 *       written.
 *
 * @param the index of the block inside of the file, starting from 0
 * @param bpo absolute offset relative to base pools first index, because this is required almost always
 * @param staticCount is a var, because the number of static instances can only be known after the sub type has been
 * read
 */
final case class Block(val blockIndex : Int, val bpo : SkillID, var staticCount : SkillID, val dynamicCount : SkillID);
