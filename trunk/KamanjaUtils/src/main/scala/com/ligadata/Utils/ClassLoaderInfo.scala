package com.ligadata.Utils

import scala.collection.mutable.TreeSet
import scala.reflect.runtime.{ universe => ru }
import java.net.{ URL, URLClassLoader }
import org.apache.log4j.Logger

class KamanjaClassLoader(val systemClassLoader: URLClassLoader, val parent: KamanjaClassLoader, val currentClassClassLoader: ClassLoader, val parentLast: Boolean) extends URLClassLoader(if (systemClassLoader != null) systemClassLoader.getURLs() else Array[URL](), if (parentLast) currentClassClassLoader else parent) {
  private val LOG = Logger.getLogger(getClass)

  override def addURL(url: URL) {
    // Passing it to super classes
    LOG.info("Adding URL:" + url.getPath + " to default class loader")
    super.addURL(url)
  }

  override def findClass(name: String): Class[_] = {
    return super.findClass(name)
    // throw new ClassNotFoundException()
  }

  protected override def loadClass(className: String, resolve: Boolean): Class[_] = this.synchronized {
    LOG.info("Trying to load class:" + className + ", resolve:" + resolve)

    if (parentLast) {
      try {
        return super.loadClass(className, resolve) // First try in local
      } catch {
        case e: ClassNotFoundException => {
          if (parent != null)
            return parent.loadClass(className) // If not found, go to Parent
          else
            throw e
        }
      }
    } else {
      return super.loadClass(className, resolve)
    }
  }
}

class KamanjaLoaderInfo(val parent: KamanjaLoaderInfo = null, val useParentloadedJars: Boolean = false, val parentLast: Boolean = false) {
  // Parent class loader
  val parentKamanLoader: KamanjaClassLoader = if (parent != null) parent.loader else null

  // Class Loader
  val loader = new KamanjaClassLoader(ClassLoader.getSystemClassLoader().asInstanceOf[URLClassLoader], parentKamanLoader, getClass().getClassLoader(), parentLast)

  // Loaded jars
  val loadedJars: TreeSet[String] = if (useParentloadedJars && parent != null) parent.loadedJars else new TreeSet[String]

  // Get a mirror for reflection
  val mirror: reflect.runtime.universe.Mirror = ru.runtimeMirror(loader)
}

