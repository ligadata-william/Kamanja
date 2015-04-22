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
object ModelUtils {

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

  // Remove model with Model Name and Version Number
  def DeactivateModel(nameSpace: String, name: String, version: Long): String = {
    var key = nameSpace + "." + name + "." + version
    try {
      val o = MdMgr.GetMdMgr.Model(nameSpace.toLowerCase, name.toLowerCase, version, true)
      o match {
        case None =>
          None
          logger.debug("No active model found => " + key)
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "Deactivate Model", null, ErrorCodeConstants.Deactivate_Model_Failed_Not_Active + ":" + key)
          apiResult.toString()
        case Some(m) =>
          logger.debug("model found => " + m.asInstanceOf[ModelDef].FullNameWithVer)
          DAOUtils.DeactivateObject(m.asInstanceOf[ModelDef])
          var objectsUpdated = new Array[BaseElemDef](0)
          objectsUpdated = objectsUpdated :+ m.asInstanceOf[ModelDef]
          val operations = for (op <- objectsUpdated) yield "Deactivate"
          Utils.NotifyEngine(objectsUpdated, operations)
          var apiResult = new ApiResult(ErrorCodeConstants.Success, "Deactivate Model", null, ErrorCodeConstants.Deactivate_Model_Successful + ":" + key)
          apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "Deactivate Model", null, "Error :" + e.toString() + ErrorCodeConstants.Deactivate_Model_Failed + ":" + key)
        apiResult.toString()
      }
    }
  }

  def ActivateModel(nameSpace: String, name: String, version: Long): String = {
    var key = nameSpace + "." + name + "." + version
    try {
      val o = MdMgr.GetMdMgr.Model(nameSpace.toLowerCase, name.toLowerCase, version, false)
      o match {
        case None =>
          None
          logger.debug("No active model found => " + key)
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "ActivateModel", null, ErrorCodeConstants.Activate_Model_Failed_Not_Active + ":" + key)
          apiResult.toString()
        case Some(m) =>
          logger.debug("model found => " + m.asInstanceOf[ModelDef].FullNameWithVer)
          DAOUtils.ActivateObject(m.asInstanceOf[ModelDef])
          var objectsUpdated = new Array[BaseElemDef](0)
          objectsUpdated = objectsUpdated :+ m.asInstanceOf[ModelDef]
          val operations = for (op <- objectsUpdated) yield "Activate"
          Utils.NotifyEngine(objectsUpdated, operations)
          var apiResult = new ApiResult(ErrorCodeConstants.Success, "ActivateModel", null, ErrorCodeConstants.Activate_Model_Successful + ":" + key)
          apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "ActivateModel", null, "Error :" + e.toString() + ErrorCodeConstants.Activate_Model_Failed + ":" + key)
        apiResult.toString()
      }
    }
  }

  // Remove model with Model Name and Version Number
  def RemoveModel(nameSpace: String, name: String, version: Long): String = {
    var key = nameSpace + "." + name + "." + version
    try {
      val o = MdMgr.GetMdMgr.Model(nameSpace.toLowerCase, name.toLowerCase, version, true)
      o match {
        case None =>
          None
          logger.debug("model not found => " + key)
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveModel", null, ErrorCodeConstants.Remove_Model_Failed_Not_Found + ":" + key)
          apiResult.toString()
        case Some(m) =>
          logger.debug("model found => " + m.asInstanceOf[ModelDef].FullNameWithVer)
          DAOUtils.DeleteObject(m.asInstanceOf[ModelDef])
          var objectsUpdated = new Array[BaseElemDef](0)
          objectsUpdated = objectsUpdated :+ m.asInstanceOf[ModelDef]
          var operations = for (op <- objectsUpdated) yield "Remove"
          Utils.NotifyEngine(objectsUpdated, operations)
          var apiResult = new ApiResult(ErrorCodeConstants.Success, "RemoveModel", null, ErrorCodeConstants.Remove_Model_Successful + ":" + key)
          apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveModel", null, "Error :" + e.toString() + ErrorCodeConstants.Remove_Model_Failed + ":" + key)
        apiResult.toString()
      }
    }
  }

  // Remove model with Model Name and Version Number
  def RemoveModel(modelName: String, version: Long): String = {
    RemoveModel(sysNS, modelName, version)
  }

  // Add Model (model def)
  private def AddModel(model: ModelDef): String = {
    var key = model.FullNameWithVer
    try {
      DAOUtils.SaveObject(model, MdMgr.GetMdMgr)
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "AddModel", null, ErrorCodeConstants.Add_Model_Successful + ":" + key)
      apiResult.toString()
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddModel", null, "Error :" + e.toString() + ErrorCodeConstants.Add_Model_Failed + ":" + key)
        apiResult.toString()
      }
    }
  }

  // Add Model (format XML)
  def AddModel(pmmlText: String): String = {
    try {
      var compProxy = new CompilerProxy
  //    compProxy.setLoggerLevel(Level.TRACE)
      var (classStr, modDef) = compProxy.compilePmml(pmmlText,false)

      // Make sure the version of the model is greater than any of previous models with same FullName
      var latestVersion = GetLatestModel(modDef)
      var isValid = true
      if (latestVersion != None) {
        isValid = Utils.IsValidVersion(latestVersion.get, modDef)
      }
      if (isValid) {
        // save the jar file first
        DAOUtils.UploadJarsToDB(modDef)
        val apiResult = AddModel(modDef)
        logger.debug("Model is added..")
        var objectsAdded = new Array[BaseElemDef](0)
        objectsAdded = objectsAdded :+ modDef
        val operations = for (op <- objectsAdded) yield "Add"
        logger.debug("Notify engine via zookeeper")
        Utils.NotifyEngine(objectsAdded, operations)
        apiResult
      } else {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddModel", null, ErrorCodeConstants.Add_Model_Failed_Higher_Version_Required + ":" + modDef.FullNameWithVer)
        apiResult.toString()
      }
    } catch {
      case e: ModelCompilationFailedException => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddModel", null, "Error :" + e.toString() + ErrorCodeConstants.Add_Model_Failed)
        apiResult.toString()
      }
      case e: AlreadyExistsException => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddModel", null, "Error :" + e.toString() + ErrorCodeConstants.Add_Model_Failed)
        apiResult.toString()
      }
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddModel", null, "Error :" + e.toString() + ErrorCodeConstants.Add_Model_Failed)
        apiResult.toString()
      }
    }
  }

  // Add Model (format XML)
  def RecompileModel(mod : ModelDef): String = {
    try {
      var compProxy = new CompilerProxy
   //   compProxy.setLoggerLevel(Level.TRACE)
      val pmmlText = mod.ObjectDefinition
      var (classStr, modDef) = compProxy.compilePmml(pmmlText,true)
      val latestVersion = GetLatestModel(modDef)
      var isValid = true
      if (isValid) {
        RemoveModel(latestVersion.get.nameSpace, latestVersion.get.name, latestVersion.get.ver)
        DAOUtils.UploadJarsToDB(modDef)
        val result = AddModel(modDef)
        var objectsUpdated = new Array[BaseElemDef](0)
        var operations = new Array[String](0)
        objectsUpdated = objectsUpdated :+ latestVersion.get
        operations = operations :+ "Remove"
        objectsUpdated = objectsUpdated :+ modDef
        operations = operations :+ "Add"
        Utils.NotifyEngine(objectsUpdated, operations)
        result
      } else {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddModel", null, ErrorCodeConstants.Add_Model_Failed + ":" + modDef.FullNameWithVer)
        apiResult.toString()
      }
    } catch {
      case e: ModelCompilationFailedException => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddModel", null, "Error in producing scala file or Jar file.." + ErrorCodeConstants.Add_Model_Failed)
        apiResult.toString()
      }
      case e: AlreadyExistsException => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddModel", null, "Error :" + e.toString() + ErrorCodeConstants.Add_Model_Failed)
        apiResult.toString()
      }
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddModel", null, "Error :" + e.toString() + ErrorCodeConstants.Add_Model_Failed)
        apiResult.toString()
      }
    }
  }


  def UpdateModel(pmmlText: String): String = {
    try {
      var compProxy = new CompilerProxy
    //  compProxy.setLoggerLevel(Level.TRACE)
      var (classStr, modDef) = compProxy.compilePmml(pmmlText,false)
      val key = MdMgr.MkFullNameWithVersion(modDef.nameSpace, modDef.name, modDef.ver)
      val latestVersion = GetLatestModel(modDef)
      var isValid = true
      if (latestVersion != None) {
	isValid = Utils.IsValidVersion(latestVersion.get, modDef)
      }
      if (isValid) {
        RemoveModel(latestVersion.get.nameSpace, latestVersion.get.name, latestVersion.get.ver)
        DAOUtils.UploadJarsToDB(modDef)
        val result = AddModel(modDef)
        var objectsUpdated = new Array[BaseElemDef](0)
        var operations = new Array[String](0)
        objectsUpdated = objectsUpdated :+ latestVersion.get
        operations = operations :+ "Remove"
        objectsUpdated = objectsUpdated :+ modDef
        operations = operations :+ "Add"
        Utils.NotifyEngine(objectsUpdated, operations)
        result
      } else {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateModel", null, ErrorCodeConstants.Update_Model_Failed_Invalid_Version + ":" + modDef.FullNameWithVer)
        apiResult.toString()
      }
    } catch {
      case e: ObjectNotFoundException => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateModel", null, "Error :" + e.toString() + ErrorCodeConstants.Update_Model_Failed)
        apiResult.toString()
      }
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "Update Model", null, "Error :" + e.toString() + ErrorCodeConstants.Update_Model_Failed)
        apiResult.toString()
      }
    }
  }

  private def getBaseType(typ: BaseTypeDef): BaseTypeDef = {
    // Just return the "typ" if "typ" is not supported yet
    if (typ.tType == tMap) {
      logger.info("MapTypeDef/ImmutableMapTypeDef is not yet handled")
      return typ
    }
    if (typ.tType == tHashMap) {
      logger.info("HashMapTypeDef is not yet handled")
      return typ
    }
    if (typ.tType == tSet) {
      val typ1 = typ.asInstanceOf[SetTypeDef].keyDef
      return getBaseType(typ1)
    }
    if (typ.tType == tTreeSet) {
      val typ1 = typ.asInstanceOf[TreeSetTypeDef].keyDef
      return getBaseType(typ1)
    }
    if (typ.tType == tSortedSet) {
      val typ1 = typ.asInstanceOf[SortedSetTypeDef].keyDef
      return getBaseType(typ1)
    }
    if (typ.tType == tList) {
      val typ1 = typ.asInstanceOf[ListTypeDef].valDef
      return getBaseType(typ1)
    }
    if (typ.tType == tQueue) {
      val typ1 = typ.asInstanceOf[QueueTypeDef].valDef
      return getBaseType(typ1)
    }
    if (typ.tType == tArray) {
      val typ1 = typ.asInstanceOf[ArrayTypeDef].elemDef
      return getBaseType(typ1)
    }
    if (typ.tType == tArrayBuf) {
      val typ1 = typ.asInstanceOf[ArrayBufTypeDef].elemDef
      return getBaseType(typ1)
    }
    return typ
  }

  def GetDependentModels(msgNameSpace: String, msgName: String, msgVer: Long): Array[ModelDef] = {
    try {
      val msgObj = Array(msgNameSpace, msgName, msgVer).mkString(".").toLowerCase
      val msgObjName = (msgNameSpace + "." + msgName).toLowerCase
      val modDefs = MdMgr.GetMdMgr.Models(true, true)
      var depModels = new Array[ModelDef](0)
      modDefs match {
        case None =>
          logger.debug("No Models found ")
        case Some(ms) =>
          val msa = ms.toArray
          msa.foreach(mod => {
            logger.debug("Checking model " + mod.FullNameWithVer)
            breakable {
              mod.inputVars.foreach(ivar => {
                val baseTyp = getBaseType(ivar.asInstanceOf[AttributeDef].typeDef)
                if (baseTyp.FullName.toLowerCase == msgObjName) {
                  logger.debug("The model " + mod.FullNameWithVer + " is  dependent on the message " + msgObj)
                  depModels = depModels :+ mod
                  break
                }
              })
	      //Output vars don't determine dependant models at this time, comment out the following code
	      // which is causing the Issue 355...
	      /*
              mod.outputVars.foreach(ovar => {
                val baseTyp = getBaseType(ovar.asInstanceOf[AttributeDef].typeDef)
                if (baseTyp.FullName.toLowerCase == msgObjName) {
                  logger.debug("The model " + mod.FullNameWithVer + " is a dependent on the message " + msgObj)
                  depModels = depModels :+ mod
                  break
                }
              })
	      */
            }
          })
      }
      logger.debug("Found " + depModels.length + " dependant models ")
      depModels
    } catch {
      case e: Exception => {
        throw new InternalErrorException("Unable to find dependent models " + e.getMessage())
      }
    }
  }

  // All available models(format JSON or XML) as a String
  def GetAllModelDefs(formatType: String): String = {
    try {
      val modDefs = MdMgr.GetMdMgr.Models(true, true)
      modDefs match {
        case None =>
          None
          logger.debug("No Models found ")
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetAllModelDefs", null, ErrorCodeConstants.Get_All_Models_Failed_Not_Available)
          apiResult.toString()
        case Some(ms) =>
          val msa = ms.toArray
          var apiResult = new ApiResult(ErrorCodeConstants.Success, "GetAllModelDefs",  JsonSerializer.SerializeObjectListToJson("Models", msa), ErrorCodeConstants.Get_All_Models_Successful)
          apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetAllModelDefs", null, "Error :" + e.toString() + ErrorCodeConstants.Get_All_Models_Failed)
        apiResult.toString()
      }
    }
  }

  def GetAllModelsFromCache(active: Boolean): Array[String] = {
    var modelList: Array[String] = new Array[String](0)
    try {
      val modDefs = MdMgr.GetMdMgr.Models(active, true)
      modDefs match {
        case None =>
          None
          logger.debug("No Models found ")
          modelList
        case Some(ms) =>
          val msa = ms.toArray
          val modCount = msa.length
          modelList = new Array[String](modCount)
          for (i <- 0 to modCount - 1) {
            modelList(i) = msa(i).FullNameWithVer
          }
          modelList
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw new UnexpectedMetadataAPIException("Failed to fetch all the models:" + e.toString)
      }
    }
  }

  // Specific models (format JSON or XML) as an array of strings using modelName(without version) as the key
  def GetModelDef(nameSpace: String, objectName: String, formatType: String): String = {
    try {
      val modDefs = MdMgr.GetMdMgr.Models(nameSpace, objectName, true, true)
      modDefs match {
        case None =>
          None
          logger.debug("No Models found ")
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetModelDef", null, ErrorCodeConstants.Get_Model_Failed_Not_Available + ":" + nameSpace+"."+objectName)
          apiResult.toString()
        case Some(ms) =>
          val msa = ms.toArray
          var apiResult = new ApiResult(ErrorCodeConstants.Success, "GetModelDef", JsonSerializer.SerializeObjectListToJson("Models", msa), ErrorCodeConstants.Get_Model_Successful)
          apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetModelDef", null, "Error :" + e.toString() + ErrorCodeConstants.Get_Model_Failed + ":" + nameSpace + "." + objectName)
        apiResult.toString()
      }
    }
  }

  // Specific models (format JSON or XML) as an array of strings using modelName(without version) as the key
  def GetModelDef(objectName: String, formatType: String): String = {
    GetModelDef(sysNS, objectName, formatType)
  }

  // Specific model (format JSON or XML) as a String using modelName(with version) as the key
  def GetModelDefFromCache(nameSpace: String, name: String, formatType: String, version: String): String = {
    try {
      var key = nameSpace + "." + name + "." + version
      val o = MdMgr.GetMdMgr.Model(nameSpace.toLowerCase, name.toLowerCase, version.toInt, true)
      o match {
        case None =>
          None
          logger.debug("model not found => " + key)
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetModelDefFromCache", null, ErrorCodeConstants.Get_Model_From_Cache_Failed_Not_Active + ":" + key)
          apiResult.toString()
        case Some(m) =>
          logger.debug("model found => " + m.asInstanceOf[ModelDef].FullNameWithVer)
          var apiResult = new ApiResult(ErrorCodeConstants.Success, "GetModelDefFromCache", JsonSerializer.SerializeObjectToJson(m), ErrorCodeConstants.Get_Model_From_Cache_Successful + ":" + nameSpace + "." + name + "." + version)
          apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetModelDefFromCache", null, "Error :" + e.toString() + ErrorCodeConstants.Get_Model_From_Cache_Failed + ":" + nameSpace + "." + name + "." + version)
        apiResult.toString()
      }
    }
  }

  // Specific models (format JSON or XML) as an array of strings using modelName(without version) as the key
  def GetModelDef(nameSpace: String, objectName: String, formatType:String, version: String): String = {
    GetModelDefFromCache(nameSpace,objectName,formatType,version)
  }

  // Get the latest model for a given FullName
  def GetLatestModel(modDef: ModelDef): Option[ModelDef] = {
    try {
      var key = modDef.nameSpace + "." + modDef.name + "." + modDef.ver
      val o = MdMgr.GetMdMgr.Models(modDef.nameSpace.toLowerCase,
        modDef.name.toLowerCase,
        false,
        true)
      o match {
        case None =>
          None
          logger.debug("model not in the cache => " + key)
          None
        case Some(m) =>
          val model = GetLatestModelFromModels(m.asInstanceOf[scala.collection.immutable.Set[com.ligadata.olep.metadata.ModelDef]])
          if (model != null) {
            logger.debug("model found => " + model.asInstanceOf[ModelDef].FullNameWithVer)
            Some(model.asInstanceOf[ModelDef])
          } else
            None   
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw new UnexpectedMetadataAPIException(e.getMessage())
      }
    }
  }

  //Get the Higher Version Model from the Set of Models
  private def GetLatestModelFromModels(modelSet: Set[ModelDef]): ModelDef = {
    var model: ModelDef = null
    var verList: List[Long] = List[Long]()
    var modelmap: scala.collection.mutable.Map[Long, ModelDef] = scala.collection.mutable.Map()
    try {
      modelSet.foreach(m => {
        modelmap.put(m.Version, m)
        verList = m.Version :: verList
      })
       model = modelmap.getOrElse(verList.max, null)
    } catch {
      case e: Exception =>
        throw new Exception("Error in traversing Model set " + e.getMessage())
    }
    model
  }

  // Specific message (format JSON or XML) as a String using messageName(with version) as the key
  def GetModelDefFromDB(nameSpace: String, objectName: String, formatType: String, version: String): String = {
     var key = "ModelDef" + "." + nameSpace + '.' + objectName + "." + version
    try {
      var obj = DAOUtils.GetObject(key.toLowerCase, MetadataAPIImpl.modelStore)
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "GetModelDefFromCache", DAOUtils.ValueAsStr(obj.Value), ErrorCodeConstants.Get_Model_From_DB_Successful + ":" + key)
      apiResult.toString()
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetModelDefFromCache", null, "Error :" + e.toString() + ErrorCodeConstants.Get_Model_From_DB_Failed + ":" + key)
        apiResult.toString()
      }
    }
  }


  def LoadModelIntoCache(key: String) {
    try {
      logger.debug("Fetch the object " + key + " from database ")
      val obj = DAOUtils.GetObject(key.toLowerCase, MetadataAPIImpl.modelStore)
      logger.debug("Deserialize the object " + key)
      val model = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
      logger.debug("Get the jar from database ")
      val modDef = model.asInstanceOf[ModelDef]
      DAOUtils.DownloadJarFromDB(modDef)
      logger.debug("Add the object " + key + " to the cache ")
      Utils.AddObjectToCache(modDef, MdMgr.GetMdMgr)
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }
}
