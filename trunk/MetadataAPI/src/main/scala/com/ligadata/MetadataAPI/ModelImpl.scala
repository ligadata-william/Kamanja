package com.ligadata.MetadataAPI

import org.apache.log4j._
import util.control.Breaks._
import com.ligadata.Serialize._
import com.ligadata.Utils._
import com.ligadata.fatafat.metadata.ObjType._
import com.ligadata.fatafat.metadata._
import com.ligadata.fatafat.metadata.MdMgr._

object ModelImpl {
  
  lazy val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)
  lazy val serializer = SerializerManager.GetSerializer("kryo")
  
  // Remove model with Model Name and Version Number
  def DeactivateModel(nameSpace: String, name: String, version: Long): String = {
    var key = nameSpace + "." + name + "." + version
    val dispkey = nameSpace + "." + name + "." + MdMgr.Pad0s2Version(version)
    try {
      val o = MdMgr.GetMdMgr.Model(nameSpace.toLowerCase, name.toLowerCase, version, true)
      o match {
        case None =>
          None
          logger.debug("No active model found => " + dispkey)
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "Deactivate Model", null, ErrorCodeConstants.Deactivate_Model_Failed_Not_Active + ":" + dispkey)
          apiResult.toString()
        case Some(m) =>
          logger.debug("model found => " + m.asInstanceOf[ModelDef].FullName + "." + MdMgr.Pad0s2Version(m.asInstanceOf[ModelDef].Version))
          DaoImpl.DeactivateObject(m.asInstanceOf[ModelDef])
          var objectsUpdated = new Array[BaseElemDef](0)
          objectsUpdated = objectsUpdated :+ m.asInstanceOf[ModelDef]
          val operations = for (op <- objectsUpdated) yield "Deactivate"
          MetadataSynchronizer.NotifyEngine(objectsUpdated, operations)
          var apiResult = new ApiResult(ErrorCodeConstants.Success, "Deactivate Model", null, ErrorCodeConstants.Deactivate_Model_Successful + ":" + dispkey)
          apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "Deactivate Model", null, "Error :" + e.toString() + ErrorCodeConstants.Deactivate_Model_Failed + ":" + dispkey)
        apiResult.toString()
      }
    }
  }
  
  /**
   * 
   */
  def ActivateModel(nameSpace: String, name: String, version: Long): String = {
    var key = nameSpace + "." + name + "." + version
    val dispkey = nameSpace + "." + name + "." + MdMgr.Pad0s2Version(version)
    try {
      val o = MdMgr.GetMdMgr.Model(nameSpace.toLowerCase, name.toLowerCase, version, false)
      o match {
        case None =>
          None
          logger.debug("No active model found => " + dispkey)
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "ActivateModel", null, ErrorCodeConstants.Activate_Model_Failed_Not_Active + ":" + dispkey)
          apiResult.toString()
        case Some(m) =>
          logger.debug("model found => " + m.asInstanceOf[ModelDef].FullName + "." + MdMgr.Pad0s2Version(m.asInstanceOf[ModelDef].Version))
          DaoImpl.ActivateObject(m.asInstanceOf[ModelDef])
          var objectsUpdated = new Array[BaseElemDef](0)
          objectsUpdated = objectsUpdated :+ m.asInstanceOf[ModelDef]
          val operations = for (op <- objectsUpdated) yield "Activate"
          MetadataSynchronizer.NotifyEngine(objectsUpdated, operations)
          var apiResult = new ApiResult(ErrorCodeConstants.Success, "ActivateModel", null, ErrorCodeConstants.Activate_Model_Successful + ":" + dispkey)
          apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "ActivateModel", null, "Error :" + e.toString() + ErrorCodeConstants.Activate_Model_Failed + ":" + dispkey)
        apiResult.toString()
      }
    }
  }
  
  
  // Specific message (format JSON or XML) as a String using messageName(with version) as the key
  def GetModelDefFromDB(nameSpace: String, objectName: String, formatType: String, version: String): String = {
    var key = "ModelDef" + "." + nameSpace + '.' + objectName + "." + version.toLong
    val dispkey = "ModelDef" + "." + nameSpace + '.' + objectName + "." + MdMgr.Pad0s2Version(version.toLong)
    try {
      var obj = DaoImpl.GetObject(key.toLowerCase, MetadataAPIImpl.getModelStore)
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "GetModelDefFromCache", MetadataUtils.ValueAsStr(obj.Value), ErrorCodeConstants.Get_Model_From_DB_Successful + ":" + dispkey)
      apiResult.toString()
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetModelDefFromCache", null, "Error :" + e.toString() + ErrorCodeConstants.Get_Model_From_DB_Failed + ":" + dispkey)
        apiResult.toString()
      }
    }
  }
  
  // Remove model with Model Name and Version Number
  def RemoveModel(nameSpace: String, name: String, version: Long): String = {
    var key = nameSpace + "." + name + "." + version
    val dispkey = nameSpace + "." + name + "." + MdMgr.Pad0s2Version(version)
    try {
      val o = MdMgr.GetMdMgr.Model(nameSpace.toLowerCase, name.toLowerCase, version, true)
      o match {
        case None =>
          None
          logger.debug("model not found => " + dispkey)
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveModel", null, ErrorCodeConstants.Remove_Model_Failed_Not_Found + ":" + dispkey)
          apiResult.toString()
        case Some(m) =>
          logger.debug("model found => " + m.asInstanceOf[ModelDef].FullName + "." + MdMgr.Pad0s2Version(m.asInstanceOf[ModelDef].Version))
          DaoImpl.DeleteObject(m.asInstanceOf[ModelDef])
          var objectsUpdated = new Array[BaseElemDef](0)
          objectsUpdated = objectsUpdated :+ m.asInstanceOf[ModelDef]
          var operations = for (op <- objectsUpdated) yield "Remove"
          MetadataSynchronizer.NotifyEngine(objectsUpdated, operations)
          var apiResult = new ApiResult(ErrorCodeConstants.Success, "RemoveModel", null, ErrorCodeConstants.Remove_Model_Successful + ":" + dispkey)
          apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveModel", null, "Error :" + e.toString() + ErrorCodeConstants.Remove_Model_Failed + ":" + dispkey)
        apiResult.toString()
      }
    }
  }

 // Add Model (model def)
  private def AddModel(model: ModelDef): String = {
    var key = model.FullNameWithVer
    val dispkey = model.FullName + "." + MdMgr.Pad0s2Version(model.Version)
    try {
      DaoImpl.SaveObject(model, MdMgr.GetMdMgr)
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "AddModel", null, ErrorCodeConstants.Add_Model_Successful + ":" + dispkey)
      apiResult.toString()
    } catch {
      case e: Exception => {
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "AddModel", null, ErrorCodeConstants.Add_Model_Successful + ":" + dispkey)
        apiResult.toString()
      }
    }
  }
  
  // Add Model (format XML)
  def AddModel(pmmlText: String): String = {
    try {
      var compProxy = new CompilerProxy
      compProxy.setLoggerLevel(Level.TRACE)
      var (classStr, modDef) = compProxy.compilePmml(pmmlText)

      // Make sure the version of the model is greater than any of previous models with same FullName
      var latestVersion = GetLatestModel(modDef)
      var isValid = true
      if (latestVersion != None) {
        isValid = MetadataUtils.IsValidVersion(latestVersion.get, modDef)
      }
      if (isValid) {
        // save the jar file first
        JarImpl.UploadJarsToDB(modDef)
        val apiResult = AddModel(modDef)
        logger.debug("Model is added..")
        var objectsAdded = new Array[BaseElemDef](0)
        objectsAdded = objectsAdded :+ modDef
        val operations = for (op <- objectsAdded) yield "Add"
        logger.debug("Notify engine via zookeeper")
        MetadataSynchronizer.NotifyEngine(objectsAdded, operations)
        apiResult
      } else {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddModel", null, ErrorCodeConstants.Add_Model_Failed_Higher_Version_Required + ":" +  modDef.FullName + "." + MdMgr.Pad0s2Version(modDef.Version))
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
  
  /**
   * 
   */
  def UpdateModel(pmmlText: String): String = {
    try {
      var compProxy = new CompilerProxy
      compProxy.setLoggerLevel(Level.TRACE)
      var (classStr, modDef) = compProxy.compilePmml(pmmlText)
      val key = MdMgr.MkFullNameWithVersion(modDef.nameSpace, modDef.name, modDef.ver)
      val latestVersion = GetLatestModel(modDef)
      var isValid = true
      if (latestVersion != None) {
          isValid = MetadataUtils.IsValidVersion(latestVersion.get, modDef)
      }
      if (isValid) {
        RemoveModel(latestVersion.get.nameSpace, latestVersion.get.name, latestVersion.get.ver)
        JarImpl.UploadJarsToDB(modDef)
        val result = AddModel(modDef)
        var objectsUpdated = new Array[BaseElemDef](0)
        var operations = new Array[String](0)
        objectsUpdated = objectsUpdated :+ latestVersion.get
        operations = operations :+ "Remove"
        objectsUpdated = objectsUpdated :+ modDef
        operations = operations :+ "Add"
        MetadataSynchronizer.NotifyEngine(objectsUpdated, operations)
        result
      } else {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateModel", null, ErrorCodeConstants.Update_Model_Failed_Invalid_Version + ":" + modDef.FullName + "." + MdMgr.Pad0s2Version(modDef.Version))
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
  
  /**
   * 
   */
  def RecompileModel(mod : ModelDef): String = {
    try {
      var compProxy = new CompilerProxy
      compProxy.setLoggerLevel(Level.TRACE)
      val pmmlText = mod.ObjectDefinition
      var (classStr, modDef) = compProxy.compilePmml(pmmlText,true)
      val latestVersion = GetLatestModel(modDef)
      var isValid = true
      if (isValid) {
        RemoveModel(latestVersion.get.nameSpace, latestVersion.get.name, latestVersion.get.ver)
        JarImpl.UploadJarsToDB(modDef)
        val result = AddModel(modDef)
        var objectsUpdated = new Array[BaseElemDef](0)
        var operations = new Array[String](0)
        objectsUpdated = objectsUpdated :+ latestVersion.get
        operations = operations :+ "Remove"
        objectsUpdated = objectsUpdated :+ modDef
        operations = operations :+ "Add"
        MetadataSynchronizer.NotifyEngine(objectsUpdated, operations)
        result
      } else {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddModel", null, ErrorCodeConstants.Add_Model_Failed + ":" + modDef.FullName + "." + MdMgr.Pad0s2Version(modDef.Version))
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
  
  
  def LoadModelIntoCache(key: String) {
    try {
      logger.debug("Fetch the object " + key + " from database ")
      val obj = DaoImpl.GetObject(key.toLowerCase, MetadataAPIImpl.getModelStore)
      logger.debug("Deserialize the object " + key)
      val model = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
      logger.debug("Get the jar from database ")
      val modDef = model.asInstanceOf[ModelDef]
      JarImpl.DownloadJarFromDB(modDef)
      logger.debug("Add the object " + key + " to the cache ")
      DaoImpl.AddObjectToCache(modDef, MdMgr.GetMdMgr)
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }
  
  def LoadAllModelsIntoCache {
    try {
      val modKeys = MetadataAPIImpl.GetAllKeys("ModelDef")
      if (modKeys.length == 0) {
        logger.debug("No models available in the Database")
        return
      }
      modKeys.foreach(key => {
        val obj = DaoImpl.GetObject(key.toLowerCase, MetadataAPIImpl.getModelStore)
        val modDef = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
        DaoImpl.AddObjectToCache(modDef.asInstanceOf[ModelDef], MdMgr.GetMdMgr)
      })
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }
  
  // Specific model (format JSON or XML) as a String using modelName(with version) as the key
  def GetModelDefFromCache(nameSpace: String, name: String, formatType: String, version: String): String = {
    val dispkey = nameSpace + "." + name + "." + MdMgr.Pad0s2Version(version.toLong)
    try {
      var key = nameSpace + "." + name + "." + version.toLong
      val o = MdMgr.GetMdMgr.Model(nameSpace.toLowerCase, name.toLowerCase, version.toLong, true)
      o match {
        case None =>
          None
          logger.debug("model not found => " + dispkey)
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetModelDefFromCache", null, ErrorCodeConstants.Get_Model_From_Cache_Failed_Not_Active + ":" + dispkey)
          apiResult.toString()
        case Some(m) =>
          logger.debug("model found => " + m.asInstanceOf[ModelDef].FullName + "." + MdMgr.Pad0s2Version(m.asInstanceOf[ModelDef].Version))
          var apiResult = new ApiResult(ErrorCodeConstants.Success, "GetModelDefFromCache", JsonSerializer.SerializeObjectToJson(m), ErrorCodeConstants.Get_Model_From_Cache_Successful + ":" + dispkey)
          apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetModelDefFromCache", null, "Error :" + e.toString() + ErrorCodeConstants.Get_Model_From_Cache_Failed + ":" + dispkey)
        apiResult.toString()
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
            modelList(i) = msa(i).FullName + "." + MdMgr.Pad0s2Version(msa(i).Version)
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
  
  // check whether model already exists in metadata manager. Ideally,
  // we should never add the model into metadata manager more than once
  // and there is no need to use this function in main code flow
  // This is just a utility function being during these initial phases
  def IsModelAlreadyExists(modDef: ModelDef): Boolean = {
    try {
      var key = modDef.nameSpace + "." + modDef.name + "." + modDef.ver
      val dispkey = modDef.nameSpace + "." + modDef.name + "." + MdMgr.Pad0s2Version(modDef.ver)
      val o = MdMgr.GetMdMgr.Model(modDef.nameSpace.toLowerCase,
        modDef.name.toLowerCase,
        modDef.ver,
        false)
      o match {
        case None =>
          None
          logger.debug("model not in the cache => " + dispkey)
          return false;
        case Some(m) =>
          logger.debug("model found => " + m.asInstanceOf[ModelDef].FullName +  "." + MdMgr.Pad0s2Version(m.asInstanceOf[ModelDef].ver))
          return true
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw new UnexpectedMetadataAPIException(e.getMessage())
      }
    }
  }
  
  //Get the Higher Version Model from the Set of Models
  def GetLatestModelFromModels(modelSet: Set[ModelDef]): ModelDef = {
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

  // Get the latest model for a given FullName
  def GetLatestModel(modDef: ModelDef): Option[ModelDef] = {
    try {
      var key = modDef.nameSpace + "." + modDef.name + "." + modDef.ver
      val dispkey = modDef.nameSpace + "." + modDef.name + "." + MdMgr.Pad0s2Version(modDef.ver)
      val o = MdMgr.GetMdMgr.Models(modDef.nameSpace.toLowerCase,
                                    modDef.name.toLowerCase,
                                    false,
                                    true)
      o match {
        case None =>
          None
          logger.debug("model not in the cache => " + dispkey)
          None
        case Some(m) =>
          if (m.size > 0) {
            logger.debug("model found => " + m.head.asInstanceOf[ModelDef].FullName + "." + MdMgr.Pad0s2Version(m.head.asInstanceOf[ModelDef].ver))
            Some(m.head.asInstanceOf[ModelDef])
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
            logger.debug("Checking model " + mod.FullName + "." + MdMgr.Pad0s2Version(mod.Version))
            breakable {
              mod.inputVars.foreach(ivar => {
                val baseTyp = TypeImpl.getBaseType(ivar.asInstanceOf[AttributeDef].typeDef)
                if (baseTyp.FullName.toLowerCase == msgObjName) {
                  logger.debug("The model " + mod.FullName + "." + MdMgr.Pad0s2Version(mod.Version) + " is  dependent on the message " + msgObj)
                  depModels = depModels :+ mod
                  break
                }
              })
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
}