package com.ligadata.automation.utils

import java.io.File

/**
 * Created by wtarver on 1/23/15.
 */
object FileManagement {
  val logger = org.apache.log4j.Logger.getLogger(this.getClass)
  def deleteFile(path: File): Unit = {
    if(path.exists()) {
      if (path.isDirectory) {
        Option(path.listFiles).map(_.toList).getOrElse(Nil).foreach(f => {
          if (f.isDirectory)
            deleteFile(f)
          logger.debug("Deleting file: " + f.getAbsolutePath)
          f.delete
        })
      }
      logger.debug("Deleting file: " + path)
      path.delete
    }
  }
}
