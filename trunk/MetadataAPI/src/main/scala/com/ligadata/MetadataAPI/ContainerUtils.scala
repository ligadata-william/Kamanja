package com.ligadata.MetadataAPI

import java.util.Properties
import java.io._
import scala.Enumeration
import scala.io._

import com.ligadata.olep.metadata.ObjType._
import com.ligadata.olep.metadata._
import com.ligadata.olep.metadata.MdMgr._

import com.ligadata.olep.metadataload.MetadataLoad

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Session
import com.datastax.driver.core.querybuilder.Insert
import com.datastax.driver.core.ResultSet

import com.ligadata.keyvaluestore._
import com.ligadata.keyvaluestore.mapdb._
import com.ligadata.keyvaluestore.cassandra._

import scala.util.parsing.json.JSON
import scala.util.parsing.json.{ JSONObject, JSONArray }
import scala.collection.immutable.Map
import scala.collection.immutable.HashMap
import scala.collection.mutable.HashMap

import com.ligadata.messagedef._

import scala.xml.XML
import org.apache.log4j._

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import com.ligadata.ZooKeeper._
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode

import com.ligadata.Serialize._
import com.ligadata.Utils._
import util.control.Breaks._

import java.util.Date

// The implementation class
object ContainerUtils {

  lazy val sysNS = "System" // system name space
  lazy val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)
  lazy val serializer = SerializerManager.GetSerializer("kryo")

  def GetMetadataAPIConfig: Properties = {
    MetadataAPIImpl.metadataAPIConfig
  }

  def SetLoggerLevel(level: Level) {
    logger.setLevel(level);
  }

  def AddContainerDef(contDef: ContainerDef): String = {
    var key = contDef.FullNameWithVer
    try {
      Utils.AddObjectToCache(contDef, MdMgr.GetMdMgr)
      DAOUtils.UploadJarsToDB(contDef)
      var objectsAdded = MessageUtils.AddMessageTypes(contDef, MdMgr.GetMdMgr)
      objectsAdded = objectsAdded :+ contDef
      DAOUtils.SaveObjectList(objectsAdded, MetadataAPIImpl.metadataStore)
      val operations = for (op <- objectsAdded) yield "Add"
      Utils.NotifyEngine(objectsAdded, operations)
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "AddContainerDef", null, ErrorCodeConstants.Add_Container_Successful + ":" + key)
      apiResult.toString()
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddContainerDef", null, "Error :" + e.toString() + ErrorCodeConstants.Add_Container_Failed + ":" + key)
        apiResult.toString()
      }
    }
  }

  def AddContainer(containerText: String, format: String): String = {
    MessageUtils.AddContainerOrMessage(containerText, format)
  }

  def AddContainer(containerText: String): String = {
    AddContainer(containerText, "JSON")
  }

  def UpdateContainer(messageText: String, format: String): String = {
    MessageUtils.UpdateMessage(messageText,format)
  }

  def UpdateContainer(messageText: String): String = {
    MessageUtils.UpdateMessage(messageText,"JSON")
  }

  // Remove container with Container Name and Version Number
  def RemoveContainer(nameSpace: String, name: String, version: Long,zkNotify:Boolean = true): String = {
    var key = nameSpace + "." + name + "." + version
    try {
      val o = MdMgr.GetMdMgr.Container(nameSpace.toLowerCase, name.toLowerCase, version, true)
      o match {
        case None =>
          None
          logger.debug("container not found => " + key)
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveContainer", null, ErrorCodeConstants.Remove_Container_Failed_Not_Found + ":" + key)
          apiResult.toString()
        case Some(m) =>
          logger.debug("container found => " + m.asInstanceOf[ContainerDef].FullNameWithVer)
          val contDef = m.asInstanceOf[ContainerDef]
          var objectsToBeRemoved = Utils.GetAdditionalTypesAdded(contDef, MdMgr.GetMdMgr)
          // Also remove a type with same name as messageDef
          var typeName = name
          var typeDef = TypeUtils.GetType(nameSpace, typeName, version.toString, "JSON")
          if (typeDef != None) {
            objectsToBeRemoved = objectsToBeRemoved :+ typeDef.get
          }
          objectsToBeRemoved.foreach(typ => {
            TypeUtils.RemoveType(typ.nameSpace, typ.name, typ.ver)
          })
          // ContainerDef itself
          DAOUtils.DeleteObject(contDef)
          objectsToBeRemoved = objectsToBeRemoved :+ contDef

	  if( zkNotify ){
            val operations = for (op <- objectsToBeRemoved) yield "Remove"
            Utils.NotifyEngine(objectsToBeRemoved, operations)
	  }
          var apiResult = new ApiResult(ErrorCodeConstants.Success, "RemoveContainer", null, ErrorCodeConstants.Remove_Container_Successful + ":" + key)
          apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveContainer", null, "Error :" + e.toString() + ErrorCodeConstants.Remove_Container_Failed + ":" + key)
        apiResult.toString()
      }
    }
  }

  // Remove container with Container Name and Version Number
  def RemoveContainer(containerName: String, version: Long): String = {
    RemoveContainer(sysNS, containerName, version)
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
            containerList(i) = msa(i).FullNameWithVer
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


  // Specific container (format JSON or XML) as a String using containerName(with version) as the key
  def GetContainerDefFromCache(nameSpace: String, name: String, formatType: String, version: String): String = {
    var key = nameSpace + "." + name + "." + version
    try {
      val o = MdMgr.GetMdMgr.Container(nameSpace.toLowerCase, name.toLowerCase, version.toInt, true)
      o match {
        case None =>
          None
          logger.debug("container not found => " + key)
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetContainerDefFromCache", null, ErrorCodeConstants.Get_Container_From_Cache_Failed + ":" + key)
          apiResult.toString()
        case Some(m) =>
          logger.debug("container found => " + m.asInstanceOf[ContainerDef].FullNameWithVer)
          var apiResult = new ApiResult(ErrorCodeConstants.Success, "GetContainerDefFromCache", JsonSerializer.SerializeObjectToJson(m), ErrorCodeConstants.Get_Container_From_Cache_Successful)
          apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetContainerDefFromCache", null, "Error :" + e.toString() + ErrorCodeConstants.Get_Container_From_Cache_Failed + ":" + key)
        apiResult.toString()
      }
    }
  }

  // Get the latest container for a given FullName
  def GetLatestContainer(contDef: ContainerDef): Option[ContainerDef] = {
    try {
      var key = contDef.nameSpace + "." + contDef.name + "." + contDef.ver
      val o = MdMgr.GetMdMgr.Containers(contDef.nameSpace.toLowerCase,
        contDef.name.toLowerCase,
        false,
        true)
      o match {
        case None =>
          None
          logger.debug("container not in the cache => " + key)
          None
        case Some(m) =>
          logger.debug("container found => " + m.head.asInstanceOf[ContainerDef].FullNameWithVer)
          Some(m.head.asInstanceOf[ContainerDef])
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw new UnexpectedMetadataAPIException(e.getMessage())
      }
    }
  }
  def IsContainerAlreadyExists(contDef: ContainerDef): Boolean = {
    try {
      var key = contDef.nameSpace + "." + contDef.name + "." + contDef.ver
      val o = MdMgr.GetMdMgr.Container(contDef.nameSpace.toLowerCase,
        contDef.name.toLowerCase,
        contDef.ver,
        false)
      o match {
        case None =>
          None
          logger.debug("container not in the cache => " + key)
          return false;
        case Some(m) =>
          logger.debug("container found => " + m.asInstanceOf[ContainerDef].FullNameWithVer)
          return true
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw new UnexpectedMetadataAPIException(e.getMessage())
      }
    }
  }

  def LoadContainerIntoCache(key: String) {
    try {
      val obj = DAOUtils.GetObject(key.toLowerCase, MetadataAPIImpl.containerStore)
      val cont = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
      logger.debug("Get the jar from database ")
      val contDef = cont.asInstanceOf[ContainerDef]
      DAOUtils.DownloadJarFromDB(contDef)
      Utils.AddObjectToCache(contDef, MdMgr.GetMdMgr)
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

  def LoadAllContainersIntoCache {
    try {
      val contKeys = Utils.GetAllKeys("ContainerDef")
      if (contKeys.length == 0) {
        logger.debug("No containers available in the Database")
        return
      }
      contKeys.foreach(key => {
        val obj = DAOUtils.GetObject(key.toLowerCase, MetadataAPIImpl.containerStore)
        val contDef = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
        Utils.AddObjectToCache(contDef.asInstanceOf[ContainerDef], MdMgr.GetMdMgr)
      })
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

  def GetContainerDef(objectName: String, formatType: String): String = {
    val nameSpace = MdMgr.sysNS
    GetContainerDefFromCache(nameSpace, objectName, formatType, "-1")
  }
  // Specific container (format JSON or XML) as a String using containerName(with version) as the key
  def GetContainerDef(nameSpace: String, objectName: String, formatType: String, version: String): String = {
    GetContainerDefFromCache(nameSpace, objectName, formatType, version)
  }

  // Specific container (format JSON or XML) as a String using containerName(with version) as the key
  def GetContainerDef(objectName: String, version: String, formatType: String): String = {
    val nameSpace = MdMgr.sysNS
    GetContainerDef(nameSpace, objectName, formatType, version)
  }

}
