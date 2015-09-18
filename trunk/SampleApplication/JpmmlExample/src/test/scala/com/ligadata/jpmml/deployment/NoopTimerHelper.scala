package com.ligadata.jpmml.deployment

import com.ligadata.jpmml.timer.TimerHelper

/**
 *
 */
trait NoopTimerHelper extends TimerHelper {
  override def recordTime(time: Long, name: String): Unit = {}
}
