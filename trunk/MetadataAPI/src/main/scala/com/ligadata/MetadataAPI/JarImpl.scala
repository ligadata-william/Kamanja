package com.ligadata.MetadataAPI

import org.apache.log4j._

import com.ligadata.Serialize._
import com.ligadata.Utils._
import com.ligadata.fatafat.metadata.ObjType._
import com.ligadata.fatafat.metadata._
import com.ligadata.fatafat.metadata.MdMgr._
import com.ligadata.Serialize._
import com.ligadata.keyvaluestore._
import com.ligadata.keyvaluestore.mapdb._
import com.ligadata.keyvaluestore.cassandra._

import java.io._

object JarImpl {
  
  lazy val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)  
  lazy val serializer = SerializerManager.GetSerializer("kryo")
  
  
  private def GetValidJarFile(jarPaths: collection.immutable.Set[String], jarName: String): String = {
    if (jarPaths == null) return jarName // Returning base jarName if no jarpaths found
    jarPaths.foreach(jPath => {
      val fl = new File(jPath + "/" + jarName)
      if (fl.exists) {
        return fl.getPath
      }
    })
    return jarName // Returning base jarName if not found in jar paths
  }
  
  def UploadJarsToDB(obj: BaseElemDef) {
    try {
      var keyList = new Array[String](0)
      var valueList = new Array[Array[Byte]](0)

      val jarPaths = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_PATHS").split(",").toSet
      
      keyList = keyList :+ obj.jarName
      var jarName = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_TARGET_DIR") + "/" + obj.jarName
      var value = GetJarAsArrayOfBytes(jarName)
      logger.debug("Update the jarfile (size => " + value.length + ") of the object: " + obj.jarName)
      valueList = valueList :+ value

      if (obj.DependencyJarNames != null) {
        obj.DependencyJarNames.foreach(j => {
          // do not upload if it already exist, minor optimization
          var loadObject = false
          if (j.endsWith(".jar")) {
            jarName = JarPathsUtils.GetValidJarFile(jarPaths, j)            
            value = GetJarAsArrayOfBytes(jarName)
            var mObj: IStorage = null
            try {
              mObj = DaoImpl.GetObject(j,  MetadataAPIImpl.getJarStore)
            } catch {
              case e: ObjectNotFoundException => {
                loadObject = true
              }
            }

            if (loadObject == false) {
              val ba = mObj.Value.toArray[Byte]
              val fs = ba.length
              if (fs != value.length) {
                logger.debug("A jar file already exists, but it's size (" + fs + ") doesn't match with the size of the Jar (" +
                  jarName + "," + value.length + ") of the object(" + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + ")")
                loadObject = true
              }
            }

            if (loadObject) {
              keyList = keyList :+ j
              logger.debug("Update the jarfile (size => " + value.length + ") of the object: " + j)
              valueList = valueList :+ value
            } else {
              logger.debug("The jarfile " + j + " already exists in DB.")
            }
          }
        })
      }
      if (keyList.length > 0) {
        DaoImpl.SaveObjectList(keyList, valueList,  MetadataAPIImpl.getJarStore)
      }
    } catch {
      case e: Exception => {
        throw new InternalErrorException("Failed to Update the Jar of the object(" + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + "): " + e.getMessage())
      }
    }
  }
  
  
  def UploadJarToDB(jarName: String) {
    try {
      val f = new File(jarName)
      if (f.exists()) {
        var key = f.getName()
        var value = GetJarAsArrayOfBytes(jarName)
        logger.debug("Update the jarfile (size => " + value.length + ") of the object: " + jarName)
        DaoImpl.SaveObject(key, value,  MetadataAPIImpl.getJarStore)
        var apiResult = new ApiResult(ErrorCodeConstants.Success, "UploadJarToDB", null, ErrorCodeConstants.Upload_Jar_Successful + ":" + jarName)
        apiResult.toString()

      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "UploadJarToDB", null, "Error : " + e.toString() + ErrorCodeConstants.Upload_Jar_Failed + ":" + jarName)
        apiResult.toString()
      }
    }
  }


  def UploadJarToDB(jarName:String,byteArray: Array[Byte]): String = {
    try {
        var key = jarName
        var value = byteArray
        logger.debug("Update the jarfile (size => " + value.length + ") of the object: " + jarName)
        DaoImpl.SaveObject(key, value,  MetadataAPIImpl.getJarStore)
        var apiResult = new ApiResult(ErrorCodeConstants.Success, "UploadJarToDB", null, ErrorCodeConstants.Upload_Jar_Successful + ":" + jarName)
        apiResult.toString()
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "UploadJarToDB", null, "Error : " + e.toString() + ErrorCodeConstants.Upload_Jar_Failed + ":" + jarName)
        apiResult.toString()
      }
    }
  }
  
  def IsDownloadNeeded(jar: String, obj: BaseElemDef): Boolean = {
    try {
      if (jar == null) {
        logger.debug("The object " + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + " has no jar associated with it. Nothing to download..")
        false
      }
      val jarPaths = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_PATHS").split(",").toSet
      val jarName = JarPathsUtils.GetValidJarFile(jarPaths, jar)
      val f = new File(jarName)
      if (f.exists()) {
        val key = jar
        val mObj = DaoImpl.GetObject(key, MetadataAPIImpl.getJarStore)
        val ba = mObj.Value.toArray[Byte]
        val fs = f.length()
        if (fs != ba.length) {
          logger.debug("A jar file already exists, but it's size (" + fs + ") doesn't match with the size of the Jar (" +
            jar + "," + ba.length + ") of the object(" + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + ")")
          true
        } else {
          logger.debug("A jar file already exists, and it's size (" + fs + ")  matches with the size of the existing Jar (" +
            jar + "," + ba.length + ") of the object(" + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + "), no need to download.")
          false
        }
      } else {
        logger.debug("The jar " + jarName + " is not available, download from database. ")
        true
      }
    } catch {
      case e: Exception => {
        throw new InternalErrorException("Failed to verify whether a download is required for the jar " + jar + " of the object(" + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + "): " + e.getMessage())
      }
    }
  }
  
  
  def GetDependantJars(obj: BaseElemDef): Array[String] = {
    try {
      var allJars = new Array[String](0)
      allJars = allJars :+ obj.JarName
      if (obj.DependencyJarNames != null) {
        obj.DependencyJarNames.foreach(j => {
          if (j.endsWith(".jar")) {
            allJars = allJars :+ j
          }
        })
      }
      allJars
    } catch {
      case e: Exception => {
        throw new InternalErrorException("Failed to get dependant jars for the given object (" + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + "): " + e.getMessage())
      }
    }
  }
  
  
  def DownloadJarFromDB(obj: BaseElemDef) {
    var curJar : String = ""
    try {
      //val key:String = (getObjectType(obj) + "." + obj.FullNameWithVer).toLowerCase
      if (obj.jarName == null) {
        logger.debug("The object " + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + " has no jar associated with it. Nothing to download..")
        return
      }
      var allJars = GetDependantJars(obj)
      logger.debug("Found " + allJars.length + " dependant jars ")
      if (allJars.length > 0) {
        val jarPaths = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_PATHS").split(",").toSet
        jarPaths.foreach(jardir => {
          val dir = new File(jardir)
          if (!dir.exists()) {
            // attempt to create the missing directory
            dir.mkdir();
          }
        })
        
        val dirPath = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_TARGET_DIR")
        val dir = new File(dirPath)
        if (!dir.exists()) {
          // attempt to create the missing directory
          dir.mkdir();
        }
        
        allJars.foreach(jar => {
          curJar = jar
          // download only if it doesn't already exists
          val b = IsDownloadNeeded(jar, obj)
          if (b == true) {
            val key = jar
            val mObj = DaoImpl.GetObject(key, MetadataAPIImpl.getJarStore )
            val ba = mObj.Value.toArray[Byte]
            val jarName = dirPath + "/" + jar
            PutArrayOfBytesToJar(ba, jarName)
          }
        })
      }
    } catch {
      case e: Exception => {
        logger.error("Failed to download the Jar of the object(" + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + "'s dep jar " + curJar + "): " + e.getMessage())
      }
    }
  }
  
  def GetJarAsArrayOfBytes(jarName: String): Array[Byte] = {
    try {
      val iFile = new File(jarName)
      if (!iFile.exists) {
        throw new FileNotFoundException("Jar file (" + jarName + ") is not found: ")
      }

      val bis = new BufferedInputStream(new FileInputStream(iFile));
      val baos = new ByteArrayOutputStream();
      var readBuf = new Array[Byte](1024) // buffer size

      // read until a single byte is available
      while (bis.available() > 0) {
        val c = bis.read();
        baos.write(c)
      }
      bis.close();
      baos.toByteArray()
    } catch {
      case e: IOException => {
        e.printStackTrace()
        throw new FileNotFoundException("Failed to Convert the Jar (" + jarName + ") to array of bytes: " + e.getMessage())
      }
      case e: Exception => {
        e.printStackTrace()
        throw new InternalErrorException("Failed to Convert the Jar (" + jarName + ") to array of bytes: " + e.getMessage())
      }
    }
  }
  
  def PutArrayOfBytesToJar(ba: Array[Byte], jarName: String) = {
    logger.debug("Downloading the jar contents into the file " + jarName)
    try {
      val iFile = new File(jarName)
      val bos = new BufferedOutputStream(new FileOutputStream(iFile));
      bos.write(ba)
      bos.close();
    } catch {
      case e: Exception => {
        logger.error("Failed to dump array of bytes to the Jar file (" + jarName + "):  " + e.getMessage())
      }
    }
  }
  
  // Upload Jars into system. Dependency jars may need to upload first. Once we upload the jar, if we retry to upload it will throw an exception.
  def UploadJar(jarPath: String): String = {
    try {
      val iFile = new File(jarPath)
      if (!iFile.exists) {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "UploadJar", null, ErrorCodeConstants.File_Not_Found + ":" + jarPath)
        apiResult.toString()
      } else {
        val jarName = iFile.getName()
        val jarObject = MdMgr.GetMdMgr.MakeJarDef(MetadataAPIImpl.sysNS, jarName, "100")
        var objectsAdded = new Array[BaseElemDef](0)
        objectsAdded = objectsAdded :+ jarObject
        UploadJarToDB(jarPath)
        val operations = for (op <- objectsAdded) yield "Add"
        MetadataSynchronizer.NotifyEngine(objectsAdded, operations)
        var apiResult = new ApiResult(ErrorCodeConstants.Success, "UploadJar", null, ErrorCodeConstants.Upload_Jar_Successful + ":" + jarPath)
        apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "UploadJar", null, "Error :" + e.toString() + ErrorCodeConstants.Upload_Jar_Failed + ":" + jarPath)
        apiResult.toString()
      }
    }
  }

}