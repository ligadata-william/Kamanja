package com.ligadata.automation.unittests.api.setup

import java.io.{File, FileNotFoundException, IOException}
import java.net.ServerSocket

import scala.util.Random

/**
 * Created by wtarver on 4/23/15.
 */
object TestUtils {
  private val logger = org.apache.log4j.Logger.getLogger(this.getClass)
  private val random: Random = new Random()

  def constructTempDir(dirPrefix:String):File = {
    val file:File = new File(System.getProperty("java.io.tmpdir"), dirPrefix + random.nextInt(10000000))
    if(!file.mkdirs()){
      throw new RuntimeException("Could not create temp directory: " + file.getAbsolutePath)
    }
    file.deleteOnExit()
    return file
  }

  def getAvailablePort:Int = {
    try{
      val socket:ServerSocket = new ServerSocket(0)
      try {
        return socket.getLocalPort
      }
      finally {
        socket.close();
      }
    }
    catch {
      case e:IOException => throw new IllegalStateException("Cannot find available port: " + e.getMessage, e)
    }
  }

  @throws(classOf[FileNotFoundException])
  def deleteFile(path:File):Boolean = {
    if(!path.exists()){
      throw new FileNotFoundException(path.getAbsolutePath)
    }
    var ret = true
    if (path.isDirectory){
      for(f <- path.listFiles) {
        ret = ret && deleteFile(f)
      }
    }
    logger.debug("AUTOMATION-TESTUTILS: Deleting file '" + path + "'")
    return ret && path.delete()
  }
}
