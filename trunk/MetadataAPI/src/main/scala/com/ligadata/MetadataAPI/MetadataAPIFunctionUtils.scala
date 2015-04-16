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
object MetadataAPIFunctionUtils {

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

  def AddFunction(functionDef: FunctionDef): String = {
     val key = functionDef.FullNameWithVer
    try {
      val value = JsonSerializer.SerializeObjectToJson(functionDef)

      logger.debug("key => " + key + ",value =>" + value);
      MetadataAPIDAOUtils.SaveObject(functionDef, MdMgr.GetMdMgr)
      logger.debug("Added function " + key + " successfully ")
      val apiResult = new ApiResult(ErrorCodeConstants.Success, "AddFunction" , null, ErrorCodeConstants.Add_Function_Successful + ":" + key)
      apiResult.toString()
    } catch {
      case e: Exception => {
        val apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddFunction" , null, ErrorCodeConstants.Add_Function_Failed + ":" + key)
        apiResult.toString()
      }
    }
  }

  def RemoveFunction(functionDef: FunctionDef): String = {
    var key = functionDef.typeString
    try {
      MetadataAPIDAOUtils.DeleteObject(functionDef)
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "RemoveFunction", null, ErrorCodeConstants.Remove_Function_Successfully + ":" + key)
      apiResult.toString()
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveFunction", null, "Error :" + e.toString() + ErrorCodeConstants.Remove_Function_Failed + ":" + key)
        apiResult.toString()
      }
    }
  }

  def RemoveFunction(nameSpace: String, functionName: String, version: Int): String = {
    var key = functionName + ":" + version
    try {
      MetadataAPIDAOUtils.DeleteObject(key, MetadataAPIImpl.functionStore)
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "RemoveFunction", null, ErrorCodeConstants.Remove_Function_Successfully + ":" + key)
      apiResult.toString()
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveFunction", null, "Error :" + e.toString() + ErrorCodeConstants.Remove_Function_Failed + ":" + key)
        apiResult.toString()
      }
    }
  }

  def DumpFunctionDef(funcDef: FunctionDef) {
    logger.debug("Name => " + funcDef.Name)
    for (arg <- funcDef.args) {
      logger.debug("arg_name => " + arg.name)
      logger.debug("arg_type => " + arg.Type.tType)
    }
    logger.debug("Json string => " + JsonSerializer.SerializeObjectToJson(funcDef))
  }


  def UpdateFunction(functionDef: FunctionDef): String = {
    val key = functionDef.typeString
    try {
      if (IsFunctionAlreadyExists(functionDef)) {
        functionDef.ver = functionDef.ver + 1
      }
      AddFunction(functionDef)
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "UpdateFunction", null, ErrorCodeConstants.Update_Function_Successful + ":" + key)
      apiResult.toString()
    } catch {
      case e: AlreadyExistsException => {
        logger.debug("Failed to update the function, key => " + key + ",Error => " + e.getMessage())
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateFunction", null, "Error :" + e.toString() + ErrorCodeConstants.Update_Function_Failed + ":" + key)
        apiResult.toString()
      }
      case e: Exception => {
        logger.debug("Failed to up the type, json => " + key + ",Error => " + e.getMessage())
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateFunction", null, "Error :" + e.toString() + ErrorCodeConstants.Update_Function_Failed + ":" + key)
        apiResult.toString()
      }
    }
  }

  def AddFunctions(functionsText: String, format: String): String = {
    try {
      if (format != "JSON") {
        var apiResult = new ApiResult(ErrorCodeConstants.Not_Implemented_Yet, "AddFunctions", null, ErrorCodeConstants.Not_Implemented_Yet_Msg + ":" + functionsText)
        apiResult.toString()
      } else {
        var funcList = JsonSerializer.parseFunctionList(functionsText, "JSON")
        funcList.foreach(func => {
          MetadataAPIDAOUtils.SaveObject(func, MdMgr.GetMdMgr)
        })
        var apiResult = new ApiResult(ErrorCodeConstants.Success, "AddFunctions", null, ErrorCodeConstants.Add_Function_Successful + ":" + functionsText)
        apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddFunctions", null, "Error :" + e.toString() + ErrorCodeConstants.Add_Function_Failed + ":" + functionsText)
        apiResult.toString()
      }
    }
  }

  def AddFunction(functionText: String, format: String): String = {
    try {
      if (format != "JSON") {
        var apiResult = new ApiResult(ErrorCodeConstants.Not_Implemented_Yet, "AddFunction", null, ErrorCodeConstants.Not_Implemented_Yet_Msg + ":" + functionText)
        apiResult.toString()
      } else {
        var func = JsonSerializer.parseFunction(functionText, "JSON")
        MetadataAPIDAOUtils.SaveObject(func, MdMgr.GetMdMgr)
        var apiResult = new ApiResult(ErrorCodeConstants.Success, "AddFunction", null, ErrorCodeConstants.Add_Function_Successful + ":" + functionText)
        apiResult.toString()
      }
    } catch {
      case e: MappingException => {
        logger.debug("Failed to parse the function, json => " + functionText + ",Error => " + e.getMessage())
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddFunction", null, "Error :" + e.toString() + ErrorCodeConstants.Add_Function_Failed + ":" + functionText)
        apiResult.toString()
      }
      case e: AlreadyExistsException => {
        logger.debug("Failed to add the function, json => " + functionText + ",Error => " + e.getMessage())
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddFunction", null, "Error :" + e.toString() + ErrorCodeConstants.Add_Function_Failed + ":" + functionText)
        apiResult.toString()
      }
      case e: Exception => {
        logger.debug("Failed to up the function, json => " + functionText + ",Error => " + e.getMessage())
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddFunction", null, "Error :" + e.toString() + ErrorCodeConstants.Add_Function_Failed + ":" + functionText)
        apiResult.toString()
      }
    }
  }

  def UpdateFunctions(functionsText: String, format: String): String = {
    try {
      if (format != "JSON") {
        var apiResult = new ApiResult(ErrorCodeConstants.Not_Implemented_Yet, "UpdateFunctions", null, ErrorCodeConstants.Not_Implemented_Yet_Msg + ":" + functionsText + ".Format not JSON.")
        apiResult.toString()
      } else {
        var funcList = JsonSerializer.parseFunctionList(functionsText, "JSON")
        funcList.foreach(func => {
          UpdateFunction(func)
        })
        var apiResult = new ApiResult(ErrorCodeConstants.Success, "UpdateFunctions", null, ErrorCodeConstants.Update_Function_Successful + ":" + functionsText)
        apiResult.toString()
      }
    } catch {
      case e: MappingException => {
        logger.debug("Failed to parse the function, json => " + functionsText + ",Error => " + e.getMessage())
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateFunctions", null, "Error :" + e.toString() + ErrorCodeConstants.Update_Function_Failed + ":" + functionsText)
        apiResult.toString()
      }
      case e: AlreadyExistsException => {
        logger.debug("Failed to add the function, json => " + functionsText + ",Error => " + e.getMessage())
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateFunctions", null, "Error :" + e.toString() + ErrorCodeConstants.Update_Function_Failed + ":" + functionsText)
        apiResult.toString()
      }
      case e: Exception => {
        logger.debug("Failed to up the function, json => " + functionsText + ",Error => " + e.getMessage())
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateFunctions", null, "Error :" + e.toString() + ErrorCodeConstants.Update_Function_Failed + ":" + functionsText)
        apiResult.toString()
      }
    }
  }

  def GetAllFunctionsFromCache(active: Boolean): Array[String] = {
    var functionList: Array[String] = new Array[String](0)
    try {
      val contDefs = MdMgr.GetMdMgr.Functions(active, true)
      contDefs match {
        case None =>
          None
          logger.debug("No Functions found ")
          functionList
        case Some(ms) =>
          val msa = ms.toArray
          val contCount = msa.length
          functionList = new Array[String](contCount)
          for (i <- 0 to contCount - 1) {
            functionList(i) = msa(i).FullNameWithVer
          }
          functionList
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw new UnexpectedMetadataAPIException("Failed to fetch all the functions:" + e.toString)
      }
    }
  }

  def IsFunctionAlreadyExists(funcDef: FunctionDef): Boolean = {
    try {
      var key = funcDef.typeString
      val o = MdMgr.GetMdMgr.Function(funcDef.nameSpace,
        funcDef.name,
        funcDef.args.toList.map(a => (a.aType.nameSpace, a.aType.name)),
        funcDef.ver,
        false)
      o match {
        case None =>
          None
          logger.debug("function not in the cache => " + key)
          return false;
        case Some(m) =>
          logger.debug("function found => " + m.asInstanceOf[FunctionDef].FullNameWithVer)
          return true
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw new UnexpectedMetadataAPIException(e.getMessage())
      }
    }
  }

  def LoadAllFunctionsIntoCache {
    try {
      val functionKeys = MetadataAPIUtils.GetAllKeys("FunctionDef")
      if (functionKeys.length == 0) {
        logger.debug("No functions available in the Database")
        return
      }
      functionKeys.foreach(key => {
        val obj = MetadataAPIDAOUtils.GetObject(key.toLowerCase, MetadataAPIImpl.functionStore)
        val function = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
        MetadataAPIUtils.AddObjectToCache(function.asInstanceOf[FunctionDef], MdMgr.GetMdMgr)
      })
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

  def LoadFunctionIntoCache(key: String) {
    try {
      val obj = MetadataAPIDAOUtils.GetObject(key.toLowerCase, MetadataAPIImpl.functionStore)
      val cont = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
      MetadataAPIUtils.AddObjectToCache(cont.asInstanceOf[FunctionDef], MdMgr.GetMdMgr)
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }


  // Answer count and dump of all available functions(format JSON or XML) as a String
  def GetAllFunctionDefs(formatType: String): (Int,String) = {
    try {
      val funcDefs = MdMgr.GetMdMgr.Functions(true, true)
      funcDefs match {
        case None =>
          None
          logger.debug("No Functions found ")
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetAllFunctionDefs", null, ErrorCodeConstants.Get_All_Functions_Failed_Not_Available)
          (0,apiResult.toString())
        case Some(fs) =>
          val fsa : Array[FunctionDef]= fs.toArray
          var apiResult = new ApiResult(ErrorCodeConstants.Success, "GetAllFunctionDefs", JsonSerializer.SerializeObjectListToJson("Functions", fsa), ErrorCodeConstants.Get_All_Functions_Successful)
          (fsa.size, apiResult.toString())
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetAllFunctionDefs", null, "Error :" + e.toString() + ErrorCodeConstants.Get_All_Functions_Failed)
        (0, apiResult.toString())
      }
    }
  }

  def GetFunctionDef(nameSpace: String, objectName: String, formatType: String): String = {
    try {
      val funcDefs = MdMgr.GetMdMgr.FunctionsAvailable(nameSpace, objectName)
      if (funcDefs == null) {
        logger.debug("No Functions found ")
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetFunctionDef", null, ErrorCodeConstants.Get_Function_Failed + ":" + nameSpace+"."+objectName)
        apiResult.toString()
      } else {
        val fsa = funcDefs.toArray
        var apiResult = new ApiResult(ErrorCodeConstants.Success, "GetFunctionDef", JsonSerializer.SerializeObjectListToJson("Functions", fsa), ErrorCodeConstants.Get_Function_Successful)
        apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetFunctionDef", null, "Error :" + e.toString() + ErrorCodeConstants.Get_Function_Failed + ":" + nameSpace+"."+objectName)
        apiResult.toString()
      }
    }
  }

  def GetFunctionDef(nameSpace: String, objectName: String, formatType: String, version:String): String = {
    GetFunctionDef(nameSpace,objectName,formatType)
  }

  // Specific messages (format JSON or XML) as a String using messageName(without version) as the key
  def GetFunctionDef(objectName: String, formatType: String): String = {
    val nameSpace = MdMgr.sysNS
    GetFunctionDef(nameSpace, objectName, formatType)
  }
}
