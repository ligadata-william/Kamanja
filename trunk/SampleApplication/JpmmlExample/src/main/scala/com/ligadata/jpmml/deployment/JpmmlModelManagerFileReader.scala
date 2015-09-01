package com.ligadata.jpmml.deployment

import java.io.PushbackInputStream

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
    val is = new PushbackInputStream(fileStream)

    try {
      deployModel(name, version, is)
    }
    finally {
      is.close()
    }
  }
}
