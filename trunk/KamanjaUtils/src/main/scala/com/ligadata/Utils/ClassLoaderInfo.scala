package com.ligadata.Utils

import scala.collection.mutable.TreeSet
import scala.collection.mutable.Map
import scala.reflect.runtime.{ universe => ru }
import java.io.{ File }
import java.net.{ URL, URLClassLoader }
import java.util.jar.{ JarInputStream, JarFile, JarEntry }
import java.io.{ ByteArrayOutputStream, InputStream }
import org.apache.log4j.Logger
import scala.collection.mutable.ArrayBuffer

class KamanjaClassLoader(val systemClassLoader: URLClassLoader, parent: ClassLoader, val parentLast: Boolean = false) extends URLClassLoader(if (systemClassLoader != null) systemClassLoader.getURLs() else Array[URL](), parent) {
  private val LOG = Logger.getLogger(getClass)
  private var loadedRawClasses = Map[String, Array[Byte]]()
  private var loadedClasses = Map[String, Class[_]]()
  private var loadedJars = ArrayBuffer[URL]()

  // Read All Data of given length from InputStream
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

  // Read Full Class Data from the given Input Stream
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
    LOG.info("Trying to load classes from Jar:" + jarName)
    val taillen = ".class".length()
    var jar: JarFile = null
    try {
      jar = new JarFile(jarName)
      val entries = jar.entries

      while (entries.hasMoreElements()) {
        val entry = entries.nextElement();
        if (entry != null && entry.getName().endsWith(".class") && !entry.isDirectory()) {
          val tmpclsnm = entry.getName().replaceAll("/", ".").trim // Replace / with .
          val className = tmpclsnm.substring(0, tmpclsnm.length() - taillen)
          if (loadedRawClasses.contains(className) == false) {
            val is = jar.getInputStream(entry)
            val data = ReadClassData(entry, is)
            if (data != null && data.length > 0) {
              LOG.info("Loading Raw class:" + className)
              loadedRawClasses(className) = data
            }
          } else {
            LOG.info(className + " raw data already loaded")
          }
        }
      }
    } catch {
      case e: Exception => {
        LOG.error("Failed to Load classes from JAR:" + jarName + ". Reason:" + e.getCause + ". Message:" + e.getMessage())
      }
    } finally {
      if (jar != null)
        jar.close
    }
  }

  override def addURL(url: URL) {
    if (parentLast) {
      // First resolve in the class in local loader, that's why we load raw data  
      LoadClassesFromURL(url.getPath)
      LOG.info("Adding URL:" + url.getPath + " locally")
      loadedJars += url
    } else {
      // Passing it to super classes
      LOG.info("Adding URL:" + url.getPath + " to default class loader")
      super.addURL(url)
    }
  }

  override def findClass(name: String): Class[_] = {
    throw new ClassNotFoundException()

    /*
    // Checking the System Class Loader 
    if (systemClassLoader != null) {
      val sysCls = super.findClass(name)
      if (sysCls != null)
        return sysCls
    }

    if (parentLast) {
      LOG.info("findClass -- Not found from local loader :" + name)
      throw new ClassNotFoundException()
    } else {
      val cls = super.findClass(name)
      LOG.info("findClass -- asking super class to find the class :" + name + ", cls:" + cls)
      return cls
    }
*/
  }

  protected override def loadClass(className: String, resolve: Boolean): Class[_] = this.synchronized {
    var triedSuper = false
    LOG.info("Trying to load class:" + className + ", resolve:" + resolve)
    try {
      // Checking the System Class Loader 
      if (systemClassLoader != null) {
        try {
          LOG.info("Trying to find class:" + className + " in System Class Loader")
          val sysCls = systemClassLoader.loadClass(className)
          if (sysCls != null) {
            LOG.info("Found class:" + className + " in System Class Loader")
            return sysCls
          }
        } catch {
          case e: Exception => {}
        }
      }

      try {
        // Find in Loaded Classes
        LOG.info("Trying to find class:" + className + " in resolved classes")
        val cls = loadedClasses.getOrElse(className, null)
        if (cls != null) {
          LOG.info("Found class:" + className + " in Local Class Loader")
          return cls
        }
      } catch {
        case e: Exception => {}
      }

      // If not find in Loaded Classes, find in Raw data Loaded Classes
      try {
        LOG.info("Trying to find class:" + className + " in raw loaded classes")
        val rawClsData = loadedRawClasses.getOrElse(className, null)
        if (rawClsData != null && rawClsData.length > 0) {
          val cls = defineClass(className, rawClsData, 0, rawClsData.length, null)
          if (resolve) resolveClass(cls)
          loadedClasses(className) = cls
          loadedRawClasses.remove(className) // Removing RawClass data once we resolve class
          LOG.info("Found class:" + className + " in Raw data of Local Class Loader")
          return cls
        }
      } catch {
        case e: Exception => {}
      }

      LOG.info("Trying to find class:" + className + " in super class")
      triedSuper = true
      val cls2 = super.loadClass(className, resolve)
      LOG.info("Loading class from Super class:" + className + ", resolve:" + resolve + ", cls2:" + cls2)
      return cls2
    } catch {
      case e: ClassNotFoundException => {
        if (triedSuper == false)
          return super.loadClass(className, resolve)
        else
          throw e
      }
    }
  }
}

class KamanjaLoaderInfo(val parent: KamanjaLoaderInfo = null, val useParentloadedJars: Boolean = false, val parentLast: Boolean = false) {
  // Parent Loader
  val parentLoader: ClassLoader = if (parent != null) parent.loader else getClass().getClassLoader();

  // Class Loader
  val loader = new KamanjaClassLoader(ClassLoader.getSystemClassLoader().asInstanceOf[URLClassLoader], parentLoader, parentLast)

  // Loaded jars
  val loadedJars: TreeSet[String] = if (useParentloadedJars && parent != null) parent.loadedJars else new TreeSet[String]

  // Get a mirror for reflection
  val mirror: reflect.runtime.universe.Mirror = ru.runtimeMirror(loader)
}

