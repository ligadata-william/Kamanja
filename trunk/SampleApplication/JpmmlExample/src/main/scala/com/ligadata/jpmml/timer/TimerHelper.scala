package com.ligadata.jpmml.timer

/**
 *
 */
trait TimerHelper {

  def recordTime(time: Long, name: String)

  def timed[T](thunk: => T): T = {
    val startTime = System.nanoTime()
    val result = (thunk)
    val time = System.nanoTime() - startTime
    recordTime(time, result.getClass.getName)
    result
  }
}
