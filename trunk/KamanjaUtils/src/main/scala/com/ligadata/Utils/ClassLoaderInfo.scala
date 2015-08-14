package com.ligadata.Utils

import scala.collection.mutable.TreeSet
import scala.reflect.runtime.{ universe => ru }
import java.io.{ File }
import java.net.{ URL, URLClassLoader }

class KamanjaClassLoader(urls: Array[URL], parent: ClassLoader) extends URLClassLoader(urls, parent) {
  override def addURL(url: URL) {
    super.addURL(url)
  }
}

class KamanjaLoaderInfo(val parent: KamanjaLoaderInfo = null, useParentloadedJars: Boolean = false) {
  // Parent Loader
  val parentLoader: ClassLoader = if (parent != null)  parent.loader else getClass().getClassLoader();
  
  // Class Loader
  val loader = new KamanjaClassLoader(ClassLoader.getSystemClassLoader().asInstanceOf[URLClassLoader].getURLs(), parentLoader)

  // Loaded jars
  val loadedJars: TreeSet[String] = if (useParentloadedJars && parent != null) parent.loadedJars else new TreeSet[String] 

  // Get a mirror for reflection
  val mirror: reflect.runtime.universe.Mirror = ru.runtimeMirror(loader)
}

