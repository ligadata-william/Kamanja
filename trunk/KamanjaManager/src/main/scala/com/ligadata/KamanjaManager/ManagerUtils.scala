
package com.ligadata.KamanjaManager

import org.apache.log4j.Logger
import com.ligadata.Exceptions.StackTrace

object ManagerUtils {
  private[this] val LOG = Logger.getLogger(getClass);

  def getClasseNamesInJar(jarName: String): Array[String] = {
    try {
      val jarFile = new JarInputStream(new FileInputStream(jarName))
      val classes = new ArrayBuffer[String]
      val taillen = ".class".length()
      breakable {
        while (true) {
          val jarEntry = jarFile.getNextJarEntry();
          if (jarEntry == null)
            break;
          if (jarEntry.getName().endsWith(".class") && !jarEntry.isDirectory()) {
            val clsnm: String = jarEntry.getName().replaceAll("/", ".").trim // Replace / with .
            classes += clsnm.substring(0, clsnm.length() - taillen)
          }
        }
      }
      return classes.toArray
    } catch {
      case e: Exception =>
        val stackTrace = StackTrace.ThrowableTraceString(e)
        LOG.debug("StackTrace:"+stackTrace)
        return null
    }
  }

  def LoadJars(jars: Array[String], loadedJars: TreeSet[String], loader: KamanjaClassLoader): Boolean = {
    // Loading all jars
    for (j <- jars) {
      LOG.debug("Processing Jar " + j.trim)
      val fl = new File(j.trim)
      if (fl.exists) {
        try {
          if (loadedJars(fl.getPath())) {
            LOG.debug("Jar " + j.trim + " already loaded to class path.")
          } else {
            loader.addURL(fl.toURI().toURL())
            LOG.debug("Jar " + j.trim + " added to class path.")
            loadedJars += fl.getPath()
          }
        } catch {
          case e: Exception => {
            LOG.error("Jar " + j.trim + " failed added to class path. Reason:%s Message:%s".format(e.getCause, e.getMessage))
            return false
          }
        }
      } else {
        LOG.error("Jar " + j.trim + " not found")
        return false
      }
    }

    true
  }

  def isDerivedFrom(clz: Class[_], clsName: String): Boolean = {
    var isIt: Boolean = false

    val interfecs = clz.getInterfaces()
    LOG.debug("Interfaces => " + interfecs.length + ",isDerivedFrom: Class=>" + clsName)

    breakable {
      for (intf <- interfecs) {
        LOG.debug("Interface:" + intf.getName())
        if (intf.getName().equals(clsName)) {
          isIt = true
          break
        }
      }
    }

    isIt
  }

  def getComponentElapsedTimeStr(compName: String, UniqVal: String, readTmInNs: Long, compStartTimInNs: Long): String = {
    val curTmInNs = System.nanoTime
    var elapsedTimeFromRead = (curTmInNs - readTmInNs) / 1000
    if (elapsedTimeFromRead <= 0) elapsedTimeFromRead = 1
    var elapsedTimeForComp = (curTmInNs - compStartTimInNs) / 1000
    if (elapsedTimeForComp <= 0) elapsedTimeForComp = 1
    "ElapsedTimeCalc => UniqVal:%s, ElapsedTimeFromRead:%d, %sElapsedTime:%d".format(UniqVal, elapsedTimeFromRead, compName, elapsedTimeForComp)
  }
}
