package de.ust.skill.common.scala.internal

import java.util.concurrent.Semaphore

/**
 * A barrier for simple parallelization of tasks
 *
 * @author Timm Felden
 */
final class Barrier extends Semaphore(1) {
  def begin = reducePermits(1)
  def end = release(1)
  def await = acquire(1)
}