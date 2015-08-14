
package com.ligadata.KamanjaManager

import java.io.{File, FileInputStream}
import java.util
import java.util.jar.JarInputStream

import com.ligadata.Utils.KamanjaClassLoader
import org.apache.log4j.Logger
import com.ligadata.Exceptions.StackTrace

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

object ManagerUtils {
  private[this] val LOG = Logger.getLogger(getClass);

  def getComponentElapsedTimeStr(compName: String, UniqVal: String, readTmInNs: Long, compStartTimInNs: Long): String = {
    val curTmInNs = System.nanoTime
    var elapsedTimeFromRead = (curTmInNs - readTmInNs) / 1000
    if (elapsedTimeFromRead <= 0) elapsedTimeFromRead = 1
    var elapsedTimeForComp = (curTmInNs - compStartTimInNs) / 1000
    if (elapsedTimeForComp <= 0) elapsedTimeForComp = 1
    "ElapsedTimeCalc => UniqVal:%s, ElapsedTimeFromRead:%d, %sElapsedTime:%d".format(UniqVal, elapsedTimeFromRead, compName, elapsedTimeForComp)
  }
}
