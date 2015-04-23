
package com.ligadata.FatafatManager

import java.util.zip.GZIPInputStream
import java.nio.file.{ Paths, Files }
import java.io.{ InputStream, ByteArrayInputStream, FileInputStream, File }
import com.ligadata.Utils.Utils
import java.util.jar.JarInputStream
import scala.util.control.Breaks._
import scala.collection.mutable.TreeSet
import org.apache.log4j.Logger

import scala.collection.mutable.ArrayBuffer

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
        e.printStackTrace();
        return null
    }
  }

  def LoadJars(jars: Array[String], loadedJars: TreeSet[String], loader: FatafatClassLoader): Boolean = {
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

}
