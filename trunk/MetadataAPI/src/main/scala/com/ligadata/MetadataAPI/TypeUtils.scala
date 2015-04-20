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
object TypeUtils {

  lazy val sysNS = "System" // system name space
  lazy val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)
  lazy val serializer = SerializerManager.GetSerializer("kryo")

  private def GetMetadataAPIConfig: Properties = {
    MetadataAPIImpl.metadataAPIConfig
  }

  def SetLoggerLevel(level: Level) {
    logger.setLevel(level);
  }

  def AddType(typeText: String, format: String): String = {
    try {
      logger.debug("Parsing type object given as Json String..")
      val typ = JsonSerializer.parseType(typeText, "JSON")
      DAOUtils.SaveObject(typ, MdMgr.GetMdMgr)
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "AddType", null, ErrorCodeConstants.Add_Type_Successful + ":" + typeText)
      apiResult.toString()
    } catch {
      case e: AlreadyExistsException => {
        logger.warn("Failed to add the type, json => " + typeText + "\nError => " + e.getMessage())
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddType", null,  "Error :" + e.toString() + ErrorCodeConstants.Add_Type_Failed + ":" + typeText)
        apiResult.toString()
      }
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddType", null, "Error :" + e.toString() + ErrorCodeConstants.Add_Type_Failed + ":" + typeText)
        apiResult.toString()
      }
    }
  }

  private def DumpTypeDef(typeDef: ScalarTypeDef) {
    logger.debug("NameSpace => " + typeDef.nameSpace)
    logger.debug("Name => " + typeDef.name)
    logger.debug("Version => " + typeDef.ver)
    logger.debug("Description => " + typeDef.description)
    logger.debug("Implementation class name => " + typeDef.implementationNm)
    logger.debug("Implementation jar name => " + typeDef.jarName)
  }

  private def AddType(typeDef: BaseTypeDef): String = {
    try {
      var key = typeDef.FullNameWithVer
      var value = JsonSerializer.SerializeObjectToJson(typeDef);
      logger.debug("key => " + key + ",value =>" + value);
      DAOUtils.SaveObject(typeDef, MdMgr.GetMdMgr)
       var apiResult = new ApiResult(ErrorCodeConstants.Success, "AddType", null, ErrorCodeConstants.Add_Type_Successful + ":" + key)
      apiResult.toString()
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddType" , null, "Error :" + e.toString() + ErrorCodeConstants.Add_Type_Failed+ ":" + typeDef.FullNameWithVer)
        apiResult.toString()
      }
    }
  }

  def AddTypes(typesText: String, format: String): String = {
    try {
      if (format != "JSON") {
        var apiResult = new ApiResult(ErrorCodeConstants.Not_Implemented_Yet, "AddTypes", null, ErrorCodeConstants.Not_Implemented_Yet_Msg + ":" + typesText)
        apiResult.toString()
      } else {
        var typeList = JsonSerializer.parseTypeList(typesText, "JSON")
        if (typeList.length > 0) {
          logger.debug("Found " + typeList.length + " type objects ")
          typeList.foreach(typ => {
            DAOUtils.SaveObject(typ, MdMgr.GetMdMgr)
            logger.debug("Type object name => " + typ.FullNameWithVer)
          })
          /** Only report the ones actually saved... if there were others, they are in the log as "fail to add" due most likely to already being defined */
          val typesSavedAsJson : String = JsonSerializer.SerializeObjectListToJson(typeList)
          var apiResult = new ApiResult(ErrorCodeConstants.Success, "AddTypes" , null, ErrorCodeConstants.Add_Type_Successful + ":" + typesText + " Number of Types:${typeList.size}")
          apiResult.toString()
        } else {
          var apiResult = new ApiResult(ErrorCodeConstants.Warning, "AddTypes", null, "All supplied types are already available. No types to add.")
          apiResult.toString()
        }
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddTypes" , null, "Error :" + e.toString() + ErrorCodeConstants.Add_Type_Failed + ":" + typesText)
        apiResult.toString()
      }
    }
  }

  // Remove type for given TypeName and Version
  def RemoveType(typeNameSpace: String, typeName: String, version: Long): String = {
    val key = typeNameSpace + "." + typeName + "." + version
    try {
      val typ = MdMgr.GetMdMgr.Type(typeNameSpace, typeName, version, true)
      typ match {
        case None =>
          None
          logger.debug("Type " + key + " is not found in the cache ")
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveType", null, ErrorCodeConstants.Remove_Type_Not_Found + ":" + key)
          apiResult.toString()
        case Some(ts) =>
          DAOUtils.DeleteObject(ts.asInstanceOf[BaseElemDef])
         var apiResult = new ApiResult(ErrorCodeConstants.Success, "RemoveType", null, ErrorCodeConstants.Remove_Type_Successful + ":" + key)
          apiResult.toString()
      }
    } catch {
      case e: ObjectNolongerExistsException => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveType", null, "Error: " + e.toString() + ErrorCodeConstants.Remove_Type_Failed + ":" + key)
        apiResult.toString()
      }
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveType", null, "Error: " + e.toString() + ErrorCodeConstants.Remove_Type_Failed + ":" + key)
        apiResult.toString()
      }
    }
  }

  def UpdateType(typeJson: String, format: String): String = {
    implicit val jsonFormats: Formats = DefaultFormats
    val typeDef = JsonSerializer.parseType(typeJson, "JSON")
    val key = typeDef.nameSpace + "." + typeDef.name + "." + typeDef.ver
    var apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateType" , null, ErrorCodeConstants.Update_Type_Internal_Error + ":" + key)
    try {
      val fName = MdMgr.MkFullName(typeDef.nameSpace, typeDef.name)
      val latestType = MdMgr.GetMdMgr.Types(typeDef.nameSpace, typeDef.name, true, true)
      latestType match {
        case None =>
          None
          logger.debug("No types with the name " + fName + " are found ")
          apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateType", null, ErrorCodeConstants.Update_Type_Failed_Not_Found + ":" + key)
        case Some(ts) =>
          val tsa = ts.toArray
          logger.debug("Found " + tsa.length + " types ")
          if (tsa.length > 1) {
            apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateType", null, ErrorCodeConstants.Update_Type_Failed_More_Than_One_Found + ":" + key)
          } else {
            val latestVersion = tsa(0)
            if (latestVersion.ver > typeDef.ver) {
              RemoveType(latestVersion.nameSpace, latestVersion.name, latestVersion.ver)
              AddType(typeDef)
              apiResult = new ApiResult(ErrorCodeConstants.Success, "UpdateType", null, ErrorCodeConstants.Update_Type_Successful + ":" + key)
            } else {
              apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateType", null, ErrorCodeConstants.Update_Type_Failed_Higher_Version_Required + ":" + key)
            }
          }
      }
      apiResult.toString()
    }catch {
      case e: MappingException => {
        logger.debug("Failed to parse the type, json => " + typeJson + ",Error => " + e.getMessage())
        apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateType", null, "Error :" + e.toString() + ErrorCodeConstants.Update_Type_Failed + ":" + key)
        apiResult.toString()
      }
      case e: AlreadyExistsException => {
        logger.debug("Failed to update the type, json => " + typeJson + ",Error => " + e.getMessage())
        apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateType", null, "Error :" + e.toString() + ErrorCodeConstants.Update_Type_Failed + ":" + key)
        apiResult.toString()
      }
      case e: Exception => {
        logger.debug("Failed to up the type, json => " + typeJson + ",Error => " + e.getMessage())
        apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateType", null, "Error :" + e.toString() + ErrorCodeConstants.Update_Type_Failed + ":" + key)
        apiResult.toString()
      }
    }
  }

  def GetAllTypesFromCache(active: Boolean): Array[String] = {
    var typeList: Array[String] = new Array[String](0)
    try {
      val contDefs = MdMgr.GetMdMgr.Types(active, true)
      contDefs match {
        case None =>
          None
          logger.debug("No Types found ")
          typeList
        case Some(ms) =>
          val msa = ms.toArray
          val contCount = msa.length
          typeList = new Array[String](contCount)
          for (i <- 0 to contCount - 1) {
            typeList(i) = msa(i).FullNameWithVer
          }
          typeList
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw new UnexpectedMetadataAPIException("Failed to fetch all the types:" + e.toString)
      }
    }
  }


  def IsTypeObject(typeName: String): Boolean = {
    typeName match {
      case "scalartypedef" | "arraytypedef" | "arraybuftypedef" | "listtypedef" | "settypedef" | "treesettypedef" | "queuetypedef" | "maptypedef" | "immutablemaptypedef" | "hashmaptypedef" | "tupletypedef" | "structtypedef" | "sortedsettypedef" => {
        return true
      }
      case _ => {
        return false
      }
    }
  }

  def LoadTypeIntoCache(key: String) {
    try {
      logger.debug("Fetch the object " + key + " from database ")
      val obj = DAOUtils.GetObject(key.toLowerCase, MetadataAPIImpl.typeStore)
      logger.debug("Deserialize the object " + key)
      val typ = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
      if (typ != null) {
        logger.debug("Add the object " + key + " to the cache ")
        Utils.AddObjectToCache(typ, MdMgr.GetMdMgr)
      }
    } catch {
      case e: Exception => {
        logger.warn("Unable to load the object " + key + " into cache ")
      }
    }
  }


  // All available types(format JSON or XML) as a String
  def GetAllTypes(formatType: String): String = {
    try {
      val typeDefs = MdMgr.GetMdMgr.Types(true, true)
      typeDefs match {
        case None =>
          None
          logger.debug("No typeDefs found ")
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetAllTypes", null, ErrorCodeConstants.Get_All_Types_Failed_Not_Available)
          apiResult.toString()
        case Some(ts) =>
          val tsa = ts.toArray
          logger.debug("Found " + tsa.length + " types ")
          var apiResult = new ApiResult(ErrorCodeConstants.Success, "GetAllTypes", JsonSerializer.SerializeObjectListToJson("Types", tsa), ErrorCodeConstants.Get_All_Types_Successful)
          apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetAllTypes", null, "Error :" + e.toString() + ErrorCodeConstants.Get_All_Types_Failed)
        apiResult.toString()
      }
    }
  }

  // All available types(format JSON or XML) as a String
  def GetAllTypesByObjType(formatType: String, objType: String): String = {
    logger.debug("Find the types of type " + objType)
    try {
      val typeDefs = MdMgr.GetMdMgr.Types(true, true)
      typeDefs match {
        case None =>
          None
          logger.debug("No typeDefs found ")
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetAllTypesByObjType", null, ErrorCodeConstants.Get_All_Types_Failed_Not_Available + ":" + objType)
          apiResult.toString()
        case Some(ts) =>
          val tsa = ts.toArray.filter(t => { t.getClass.getName == objType })
          if (tsa.length == 0) {
            var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetAllTypesByObjType", null, ErrorCodeConstants.Get_All_Types_Failed_Not_Available + ":" + objType)
            apiResult.toString()
          } else {
            var apiResult = new ApiResult(ErrorCodeConstants.Success, "GetAllTypesByObjType", JsonSerializer.SerializeObjectListToJson("Types", tsa), ErrorCodeConstants.Get_All_Types_Successful + ":" + objType)
            apiResult.toString()
          }
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetAllTypesByObjType", null, "Error :" + e.toString() + ErrorCodeConstants.Get_All_Types_Failed + ":" + objType)
        apiResult.toString()
      }
    }
  }

  // Get types for a given name
  def GetType(objectName: String, formatType: String): String = {
    try {
      val typeDefs = MdMgr.GetMdMgr.Types(MdMgr.sysNS, objectName, false, false)
      typeDefs match {
        case None =>
          None
          logger.debug("No typeDefs found ")
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetType" , null, ErrorCodeConstants.Get_Type_Failed_Not_Available + ":" + objectName)
          apiResult.toString()
        case Some(ts) =>
          val tsa = ts.toArray
          logger.debug("Found " + tsa.length + " types ")
          var apiResult = new ApiResult(ErrorCodeConstants.Success, "GetType" , JsonSerializer.SerializeObjectListToJson("Types", tsa), ErrorCodeConstants.Get_Type_Successful + ":" + objectName)
          apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetType" , null, "Error :" + e.toString() + ErrorCodeConstants.Get_Type_Failed + ":" + objectName)
        apiResult.toString()
      }
    }
  }

  def GetTypeDef(nameSpace: String, objectName: String, formatType: String,version: String): String = {
    var key = nameSpace + "." + objectName + "." + version
    try {
      val typeDefs = MdMgr.GetMdMgr.Types(nameSpace, objectName,false,false)
      typeDefs match {
        case None =>
          None
          logger.debug("No typeDefs found ")
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetTypeDef", null, ErrorCodeConstants.Get_Type_Def_Failed_Not_Available + ":" + key)
          apiResult.toString()
        case Some(ts) =>
          val tsa = ts.toArray
          logger.debug("Found " + tsa.length + " types ")
          var apiResult = new ApiResult(ErrorCodeConstants.Success, "GetTypeDef", JsonSerializer.SerializeObjectListToJson("Types", tsa), ErrorCodeConstants.Get_Type_Def_Successful + ":" + key)
          apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetTypeDef", null, "Error :" + e.toString() + ErrorCodeConstants.Get_Type_Def_Failed + ":" + key)
        apiResult.toString()
      }
    }
  }

  def GetType(nameSpace: String, objectName: String, version: String, formatType: String): Option[BaseTypeDef] = {
    try {
      val typeDefs = MdMgr.GetMdMgr.Types(MdMgr.sysNS, objectName, false, false)
      typeDefs match {
        case None => None
        case Some(ts) =>
          val tsa = ts.toArray.filter(t => { t.ver == version.toInt })
          tsa.length match {
            case 0 => None
            case 1 => Some(tsa(0))
            case _ => {
              logger.debug("Found More than one type, Doesn't make sesne")
              Some(tsa(0))
            }
          }
      }
    } catch {
      case e: Exception => {
        logger.debug("Failed to fetch the typeDefs:" + e.getMessage())
        None
      }
    }
  }
}
