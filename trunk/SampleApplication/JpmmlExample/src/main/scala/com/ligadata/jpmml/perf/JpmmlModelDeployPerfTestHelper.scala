package com.ligadata.jpmml.perf

import com.ligadata.jpmml.concrete.JpmmlModelEvaluatorWired
import com.ligadata.jpmml.timer.TimerHelper

import scala.collection.mutable.ListBuffer

/**
 *
 */
class JpmmlModelDeployPerfTestHelper extends JpmmlModelEvaluatorWired with TimerHelper {
  val deployModelTimes = ListBuffer[Double]()

  override def recordTime(time: Long, name: String): Unit = {
    deployModelTimes += time/math.pow(10, 6)
  }

  def runTest(count: Int, path: String): Unit = {
    val name = "Dummy"
    val version = "1.0"
    deployModelTimes.clear()
    for (_ <- 0 until count) yield deployModelFromFileSystem(name, version, path)
    printStats(deployModelTimes)
  }

  private def printStats(deployTimes: Iterable[Double]): Unit = {
    if (deployTimes.isEmpty) {
      println("Count - 0")
    }
    else {
      println(s"Avg - ${deployTimes.sum / deployTimes.size} millis, Count - ${deployTimes.size}, Max - ${deployTimes.max}")
    }
  }
}