package com.ligadata.pmml.support

import scala.collection.mutable.TreeSet
import java.net.URL
import java.net.URLClassLoader
import scala.reflect.runtime.{ universe => ru }
import java.io.{ File }

/**
 * PMMLClassLoader, PMMLLoaderInfo, and PMMLConfiguration together work to build the classpath
 * dynamically, based upon the metadata dependencies noted in the FunctionDef metadata etc.
 */

class PMMLClassLoader(urls: Array[URL], parent: ClassLoader) extends URLClassLoader(urls, parent) {
  override def addURL(url: URL) {
    super.addURL(url)
  }
}

class PMMLLoaderInfo {
  // class loader
  val loader: PMMLClassLoader = new PMMLClassLoader(ClassLoader.getSystemClassLoader().asInstanceOf[URLClassLoader].getURLs(), getClass().getClassLoader())

  // Loaded jars
  val loadedJars: TreeSet[String] = new TreeSet[String];

  // Get a mirror for reflection
  val mirror: reflect.runtime.universe.Mirror = ru.runtimeMirror(loader)
}

object PMMLConfiguration {
  var jarPaths: collection.immutable.Set[String] = _
  def GetValidJarFile(jarPaths: collection.immutable.Set[String], jarName: String): String = {
    if (jarPaths == null) return jarName // Returning base jarName if no jarpaths found
    jarPaths.foreach(jPath => {
      val fl = new File(jPath + "/" + jarName)
      if (fl.exists) {
        return fl.getPath
      }
    })
    return jarName // Returning base jarName if not found in jar paths
  }
}
