package com.ligadata.MetadataAPI

import org.apache.log4j._

import com.ligadata.Serialize._
import com.ligadata.Utils._
import com.ligadata.fatafat.metadata.ObjType._
import com.ligadata.fatafat.metadata._
import com.ligadata.fatafat.metadata.MdMgr._


object ContainerImpl {
  lazy val serializer = SerializerManager.GetSerializer("kryo")
  def AddContainerDef(contDef: ContainerDef, recompile:Boolean = false): String = {
    var key = contDef.FullNameWithVer
    val dispkey = contDef.FullName + "." + MdMgr.Pad0s2Version(contDef.Version)
    try {
      DaoImpl.AddObjectToCache(contDef, MdMgr.GetMdMgr)
      JarImpl.UploadJarsToDB(contDef)
      var objectsAdded = MessageImpl.AddMessageTypes(contDef, MdMgr.GetMdMgr, recompile)
      objectsAdded = objectsAdded :+ contDef
      DaoImpl.SaveObjectList(objectsAdded, MetadataAPIImpl.getMetadataStore)
      val operations = for (op <- objectsAdded) yield "Add"
      MetadataSynchronizer.NotifyEngine(objectsAdded, operations)
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "AddContainerDef", null, ErrorCodeConstants.Add_Container_Successful + ":" + dispkey)
      apiResult.toString()
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddContainerDef", null, "Error :" + e.toString() + ErrorCodeConstants.Add_Container_Failed + ":" + dispkey)
        apiResult.toString()
      }
    }
  }
  
  def LoadAllContainersIntoCache {
    try {
      val contKeys = MetadataAPIImpl.GetAllKeys("ContainerDef")
      if (contKeys.length == 0) {
        logger.debug("No containers available in the Database")
        return
      }
      contKeys.foreach(key => {
        val obj = DaoImpl.GetObject(key.toLowerCase, MetadataAPIImpl.getContainerStore)
        val contDef = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
        DaoImpl.AddObjectToCache(contDef.asInstanceOf[ContainerDef], MdMgr.GetMdMgr)
      })
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }
   
  // Remove container with Container Name and Version Number
  def RemoveContainer(nameSpace: String, name: String, version: Long, zkNotify:Boolean = true): String = {
    var key = nameSpace + "." + name + "." + version
    val dispkey = nameSpace + "." + name + "." + MdMgr.Pad0s2Version(version)
    try {
      val o = MdMgr.GetMdMgr.Container(nameSpace.toLowerCase, name.toLowerCase, version, true)
      o match {
        case None =>
          None
          logger.debug("container not found => " + key)
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveContainer", null, ErrorCodeConstants.Remove_Container_Failed_Not_Found + ":" + dispkey)
          apiResult.toString()
        case Some(m) =>
          logger.debug("container found => " + m.asInstanceOf[ContainerDef].FullName + "." + MdMgr.Pad0s2Version(m.asInstanceOf[ContainerDef].Version))
          val contDef = m.asInstanceOf[ContainerDef]
          var objectsToBeRemoved = TypeImpl.GetAdditionalTypesAdded(contDef, MdMgr.GetMdMgr)
          // Also remove a type with same name as messageDef
          var typeName = name
          var typeDef = TypeImpl.GetType(nameSpace, typeName, version.toString, "JSON")
          if (typeDef != None) {
            objectsToBeRemoved = objectsToBeRemoved :+ typeDef.get
          }
          objectsToBeRemoved.foreach(typ => {
            MetadataAPIImpl.RemoveType(typ.nameSpace, typ.name, typ.ver)
          })
          // ContainerDef itself
          DaoImpl.DeleteObject(contDef)
          var allObjectsArray =  objectsToBeRemoved :+ contDef

          if( zkNotify ){
            val operations = for (op <- allObjectsArray) yield "Remove"
            MetadataSynchronizer.NotifyEngine(allObjectsArray, operations)
          }
          var apiResult = new ApiResult(ErrorCodeConstants.Success, "RemoveContainer", null, ErrorCodeConstants.Remove_Container_Successful + ":" + dispkey)
          apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveContainer", null, "Error :" + e.toString() + ErrorCodeConstants.Remove_Container_Failed + ":" + dispkey)
        apiResult.toString()
      }
    }
  }
  
  def LoadContainerIntoCache(key: String) {
    try {
      val obj = DaoImpl.GetObject(key.toLowerCase, MetadataAPIImpl.getContainerStore)
      val cont = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
      logger.debug("Get the jar from database ")
      val contDef = cont.asInstanceOf[ContainerDef]
      JarImpl.DownloadJarFromDB(contDef)
      DaoImpl.AddObjectToCache(contDef, MdMgr.GetMdMgr)
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }
  
  // All available containers(format JSON or XML) as a String
  def GetAllContainerDefs(formatType: String): String = {
    try {
      val msgDefs = MdMgr.GetMdMgr.Containers(true, true)
      msgDefs match {
        case None =>
          None
          logger.debug("No Containers found ")
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetAllContainerDefs", null, ErrorCodeConstants.Get_All_Containers_Failed_Not_Available)
          apiResult.toString()
        case Some(ms) =>
          val msa = ms.toArray
          var apiResult = new ApiResult(ErrorCodeConstants.Success, "GetAllContainerDefs", JsonSerializer.SerializeObjectListToJson("Containers", msa), ErrorCodeConstants.Get_All_Containers_Successful)
          apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetAllContainerDefs", null, "Error :" + e.toString() + ErrorCodeConstants.Get_All_Containers_Failed)
        apiResult.toString()
      }
    }
  }
  
  def RemoveContainerFromCache(zkMessage: ZooKeeperNotification) = {
    try {
      var key = zkMessage.NameSpace + "." + zkMessage.Name + "." + zkMessage.Version
      val dispkey = zkMessage.NameSpace + "." + zkMessage.Name + "." + MdMgr.Pad0s2Version(zkMessage.Version.toLong)
      val o = MdMgr.GetMdMgr.Container(zkMessage.NameSpace, zkMessage.Name, zkMessage.Version.toLong, true)
      o match {
        case None =>
          None
          logger.debug("Message not found, Already Removed? => " + dispkey)
        case Some(m) =>
          val msgDef = m.asInstanceOf[MessageDef]
          logger.debug("message found => " + msgDef.FullName + "." + MdMgr.Pad0s2Version(msgDef.Version))
          var typeName = zkMessage.Name
          MdMgr.GetMdMgr.RemoveType(zkMessage.NameSpace, typeName, zkMessage.Version.toLong)
          typeName = "arrayof" + zkMessage.Name
          MdMgr.GetMdMgr.RemoveType(zkMessage.NameSpace, typeName, zkMessage.Version.toLong)
          typeName = "sortedsetof" + zkMessage.Name
          MdMgr.GetMdMgr.RemoveType(zkMessage.NameSpace, typeName, zkMessage.Version.toLong)
          typeName = "arraybufferof" + zkMessage.Name
          MdMgr.GetMdMgr.RemoveType(zkMessage.NameSpace, typeName, zkMessage.Version.toLong)
          MdMgr.GetMdMgr.RemoveContainer(zkMessage.NameSpace, zkMessage.Name, zkMessage.Version.toLong)
      }
    } catch {
      case e: Exception => {
        logger.error("Failed to delete the Message from cache:" + e.toString)
      }
    }
  }
  
  def IsContainerAlreadyExists(contDef: ContainerDef): Boolean = {
    try {
      var key = contDef.nameSpace + "." + contDef.name + "." + contDef.ver
      val dispkey = contDef.nameSpace + "." + contDef.name + "." + MdMgr.Pad0s2Version(contDef.ver)
      val o = MdMgr.GetMdMgr.Container(contDef.nameSpace.toLowerCase,
        contDef.name.toLowerCase,
        contDef.ver,
        false)
      o match {
        case None =>
          None
          logger.debug("container not in the cache => " + dispkey)
          return false;
        case Some(m) =>
          logger.debug("container found => " + m.asInstanceOf[ContainerDef].FullName +  "." + MdMgr.Pad0s2Version(m.asInstanceOf[ContainerDef].ver))
          return true
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw new UnexpectedMetadataAPIException(e.getMessage())
      }
    }
  }
  
  // Get the latest container for a given FullName
  def GetLatestContainer(contDef: ContainerDef): Option[ContainerDef] = {
    try {
      var key = contDef.nameSpace + "." + contDef.name + "." + contDef.ver
      val dispkey = contDef.nameSpace + "." + contDef.name + "." + MdMgr.Pad0s2Version(contDef.ver)
      val o = MdMgr.GetMdMgr.Containers(contDef.nameSpace.toLowerCase,
        contDef.name.toLowerCase,
        false,
        true)
      o match {
        case None =>
          None
          logger.debug("container not in the cache => " + dispkey)
          None
        case Some(m) => 
          // We can get called from the Add Container path, and M could be empty.
          if (m.size == 0) return None
          logger.debug("container found => " + m.head.asInstanceOf[ContainerDef].FullName  + "." + MdMgr.Pad0s2Version(m.head.asInstanceOf[ContainerDef].ver))
          Some(m.head.asInstanceOf[ContainerDef])
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw new UnexpectedMetadataAPIException(e.getMessage())
      }
    }
  }
  
  // Specific container (format JSON or XML) as a String using containerName(with version) as the key
  def GetContainerDefFromCache(nameSpace: String, name: String, formatType: String, version: String): String = {
    var key = nameSpace + "." + name + "." + version.toLong
    val dispkey = nameSpace + "." + name + "." + MdMgr.Pad0s2Version(version.toLong)
    try {
      val o = MdMgr.GetMdMgr.Container(nameSpace.toLowerCase, name.toLowerCase, version.toLong, true)
      o match {
        case None =>
          None
          logger.debug("container not found => " + dispkey)
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetContainerDefFromCache", null, ErrorCodeConstants.Get_Container_From_Cache_Failed + ":" + dispkey)
          apiResult.toString()
        case Some(m) =>
          logger.debug("container found => " + m.asInstanceOf[ContainerDef].FullName + "." + MdMgr.Pad0s2Version(m.asInstanceOf[ContainerDef].Version))
          var apiResult = new ApiResult(ErrorCodeConstants.Success, "GetContainerDefFromCache", JsonSerializer.SerializeObjectToJson(m), ErrorCodeConstants.Get_Container_From_Cache_Successful)
          apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetContainerDefFromCache", null, "Error :" + e.toString() + ErrorCodeConstants.Get_Container_From_Cache_Failed + ":" + dispkey)
        apiResult.toString()
      }
    }
  }

  
  def GetAllContainersFromCache(active: Boolean): Array[String] = {
    var containerList: Array[String] = new Array[String](0)
    try {
      val contDefs = MdMgr.GetMdMgr.Containers(active, true)
      contDefs match {
        case None =>
          None
          logger.debug("No Containers found ")
          containerList
        case Some(ms) =>
          val msa = ms.toArray
          val contCount = msa.length
          containerList = new Array[String](contCount)
          for (i <- 0 to contCount - 1) {
            containerList(i) = msa(i).FullName + "." + MdMgr.Pad0s2Version(msa(i).Version)
          }
          containerList
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw new UnexpectedMetadataAPIException("Failed to fetch all the containers:" + e.toString)
      }
    }
  }
   

}