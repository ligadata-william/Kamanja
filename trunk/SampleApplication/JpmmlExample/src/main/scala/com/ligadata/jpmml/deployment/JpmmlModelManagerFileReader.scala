package com.ligadata.jpmml.deployment

import java.io.{File, FileInputStream, PushbackInputStream, InputStream}

/**
 * Read pmml from a file and deploy it
 */
trait JpmmlModelManagerFileReader {
  self: JpmmlModelManager =>

  /**
   * PMML file is available in the classpath
   */
  def deployModelFromClasspath(name: String, version: String, pathNodes: List[String]): Unit = {
    val path = pathNodes.mkString("/")
    val fileStream = Option {
      getClass.getClassLoader.getResourceAsStream(path)
    } getOrElse {
      throw new RuntimeException(s"Could not load file with $path, file not found")
    }
   deployModelFromFileStream(name, version, fileStream)
  }

  /**
   * PMML file is available in the file system
   */
  def deployModelFromFileSystem(name: String, version: String, path: String): Unit = {
    val fileStream = new FileInputStream(new File(path))
    deployModelFromFileStream(name, version, fileStream)
  }

  private def deployModelFromFileStream(name: String, version: String, fileStream: InputStream): Unit = {
    val is = new PushbackInputStream(fileStream)

    try {
      deployModel(name, version, is)
    }
    finally {
      is.close()
    }
  }
}
