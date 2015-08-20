package com.ligadata.Utils

import scala.collection.mutable.TreeSet
import scala.collection.mutable.Map
import scala.reflect.runtime.{ universe => ru }
import java.io.{ File }
import java.net.{ URL, URLClassLoader }
import java.util.jar.{ JarInputStream, JarFile, JarEntry }
import java.io.{ ByteArrayOutputStream, InputStream }

class KamanjaClassLoader(urls: Array[URL], parent: ClassLoader) extends URLClassLoader(urls, parent) {
  private var loadedClasses = Map[String, Class[_]]()

  private def ReadAllData(is: InputStream, length: Int): Array[Byte] = {
    val retVal = new Array[Byte](length)
    var off = 0
    var len = length
    while (len > 0) {
      val bytesRead = is.read(retVal, off, len)
      if (bytesRead < 0)
        throw new Exception("Failed to read data from InputStream")
      len -= bytesRead
      off += bytesRead
    }

    retVal
  }

  private def ReadClassData(ze: JarEntry, is: InputStream): Array[Byte] = {
    val size = ze.getSize.toInt
    if (size != -1) {
      return ReadAllData(is, size)
    }
    val data = new Array[Byte](1024)
    val os = new ByteArrayOutputStream(1024)
    var r: Int = 0
    while (r != -1) {
      r = is.read(data)
      if (r != -1)
        os.write(data, 0, r)
    }
    return os.toByteArray
  }

  private def LoadClassesFromURL(jarName: String): Unit = this.synchronized {
    println("Trying to load classes from Jar:" + jarName)
    val taillen = ".class".length()
    var jar: JarFile = null
    try {
      jar = new JarFile(jarName)
      val entries = jar.entries

      while (entries.hasMoreElements()) {
        val entry = entries.nextElement();
        val name = entry.getName();
        val clsnm: String = entry.getName().replaceAll("/", ".").trim // Replace / with .
        val className = clsnm.substring(0, clsnm.length() - taillen)

        val is = jar.getInputStream(entry)

        val data = ReadClassData(entry, is)
        if (data != null && data.length > 0) {
          val cls = defineClass(className, data, 0, data.length, null)
          loadedClasses(className) = cls
        }
      }
    } catch {
      case e: Exception => {
        // e.printStackTrace();
      }
    } finally {
      if (jar != null)
        jar.close
    }
  }

  override def addURL(url: URL) {
    LoadClassesFromURL(url.getPath)
    super.addURL(url) // If we are going to maintain our own classes in this, no need to call this addURL. Which is duplicate
  }

  override def findClass(name: String): Class[_] = {
    throw new ClassNotFoundException()
  }

  protected override def loadClass(className: String, resolve: Boolean): Class[_] = this.synchronized {
    println("Trying to load Class:" + className)
    try {
      val cls = loadedClasses.getOrElse(className, null)
      if (cls != null) {
        val fndClass = cls
        return fndClass
      }
      return super.loadClass(className, resolve)
    } catch {
      case e: ClassNotFoundException => {
        return super.loadClass(className, resolve)
      }
    }
  }
  // val loader = new ParentLastClassLoader(Thread.currentThread().getContextClassLoader, paths)
}

class KamanjaLoaderInfo(val parent: KamanjaLoaderInfo = null, useParentloadedJars: Boolean = false) {
  // Parent Loader
  val parentLoader: ClassLoader = if (parent != null) parent.loader else getClass().getClassLoader();

  // Class Loader
  val loader = new KamanjaClassLoader(ClassLoader.getSystemClassLoader().asInstanceOf[URLClassLoader].getURLs(), parentLoader)

  // Loaded jars
  val loadedJars: TreeSet[String] = if (useParentloadedJars && parent != null) parent.loadedJars else new TreeSet[String]

  // Get a mirror for reflection
  val mirror: reflect.runtime.universe.Mirror = ru.runtimeMirror(loader)
}

