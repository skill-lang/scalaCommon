package de.ust.skill.common.scala.internal

import de.ust.skill.common.scala.api.SkillObject
import de.ust.skill.common.scala.api.CompletelyUnknownObject
import de.ust.skill.common.scala.SkillID
import de.ust.skill.common.scala.api.CompletelyUnknownObject

/**
 * @author Timm Felden
 */
final class UnknownBasePool(_name : String, _typeId : Int) extends BasePool[CompletelyUnknownObject](_name, _typeId) {

  def getInstanceClass : Class[CompletelyUnknownObject] = classOf[CompletelyUnknownObject]

  def allocateData : Unit = data = new Array[CompletelyUnknownObject](cachedSize)

  def allocateInstances : Unit = {
    for (b ← blocks.par) {
      var i : SkillID = b.bpo
      val last = i + b.staticCount
      while (i < last) {
        data(i) = new CompletelyUnknownObject(i + 1, this)
        i += 1
      }
    }
  }

  final class UnknownSubPool(
    _name : String,
    _superPool : StoragePool[CompletelyUnknownObject, CompletelyUnknownObject],
    _typeId : Int)
      extends SubPool[CompletelyUnknownObject, CompletelyUnknownObject](_name, _superPool, _typeId) {
    def getInstanceClass : Class[CompletelyUnknownObject] = classOf[CompletelyUnknownObject]

    def allocateInstances : Unit = {
      for (b ← blocks.par) {
        var i : SkillID = b.bpo
        val last = i + b.staticCount
        while (i < last) {
          data(i) = new CompletelyUnknownObject(i + 1, this)
          i += 1
        }
      }
    }

    def makeSubPool(name : String, typeId : Int) : SubPool[_ <: CompletelyUnknownObject, CompletelyUnknownObject] = {
      new UnknownSubPool(name, this, typeId)
    }

    def reflectiveAllocateInstance : CompletelyUnknownObject = {
      val r = new CompletelyUnknownObject(-1, this)
      this.newObjects.append(r)
      r
    }
  }

  def makeSubPool(name : String, typeId : Int) : SubPool[_ <: de.ust.skill.common.scala.api.CompletelyUnknownObject, CompletelyUnknownObject] = {
    new UnknownSubPool(name, this, typeId)
  }

  def reflectiveAllocateInstance : CompletelyUnknownObject = {
    val r = new CompletelyUnknownObject(-1, this)
    this.newObjects.append(r)
    r
  }
}

