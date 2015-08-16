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

class KamanjaLoaderInfo {
  // class loader
  val loader = new KamanjaClassLoader(ClassLoader.getSystemClassLoader().asInstanceOf[URLClassLoader].getURLs(), getClass().getClassLoader())

  // Loaded jars
  val loadedJars: TreeSet[String] = new TreeSet[String];

  // Get a mirror for reflection
  val mirror: reflect.runtime.universe.Mirror = ru.runtimeMirror(loader)
}

