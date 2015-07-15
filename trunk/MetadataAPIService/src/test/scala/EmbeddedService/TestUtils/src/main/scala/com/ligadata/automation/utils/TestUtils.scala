package com.ligadata.automation.utils

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
    println("Creating temp dir!")
    var file: File=null
    //val file:File = new File(System.getProperty("java.io.tmpdir"), dirPrefix + random.nextInt(10000000))
    try{
    //file= new File(getClass.getResource("/DataDirectories").getPath, dirPrefix + random.nextInt(10000000))
      file= new File("/Users/dhaval/"+dirPrefix + random.nextInt(10000000))
      println("Creating temp dir! 2")
    }catch {
      case e:Exception => throw new IllegalStateException("Cannot create a new dir: " + e.getMessage, e)
    }

    if(!file.mkdirs()){
      throw new RuntimeException("Could not create temp directory: " + file.getAbsolutePath)
    }
    println("AUTOMATION-TESTUTILS: File created: '" + file.getPath)
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
