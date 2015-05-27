package com.ligadata.MetadataAPI


import org.apache.log4j._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import com.ligadata.Utils._
import com.ligadata.Serialize._
import com.ligadata.fatafat.metadata.ObjType._
import com.ligadata.fatafat.metadata._
import com.ligadata.fatafat.metadata.MdMgr._


object FunctionImpl {
  lazy val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)
  lazy val serializer = SerializerManager.GetSerializer("kryo") 
 
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
  
  def UpdateFunction(functionDef: FunctionDef): String = {
    val key = functionDef.typeString
    val dispkey = key // This does not have version at this moment
    try {
      if (IsFunctionAlreadyExists(functionDef)) {
        functionDef.ver = functionDef.ver + 1
      }
      AddFunction(functionDef)
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "UpdateFunction", null, ErrorCodeConstants.Update_Function_Successful + ":" + dispkey)
      apiResult.toString()
    } catch {
      case e: AlreadyExistsException => {
        logger.debug("Failed to update the function, key => " + key + ",Error => " + e.getMessage())
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateFunction", null, "Error :" + e.toString() + ErrorCodeConstants.Update_Function_Failed + ":" + dispkey)
        apiResult.toString()
      }
      case e: Exception => {
        logger.debug("Failed to up the type, json => " + key + ",Error => " + e.getMessage())
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateFunction", null, "Error :" + e.toString() + ErrorCodeConstants.Update_Function_Failed + ":" + dispkey)
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
          DaoImpl.SaveObject(func, MdMgr.GetMdMgr)
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
        DaoImpl.SaveObject(func, MdMgr.GetMdMgr)
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
  
  def AddFunction(functionDef: FunctionDef): String = {
    val key = functionDef.FullNameWithVer
    val dispkey = functionDef.FullName + "." + MdMgr.Pad0s2Version(functionDef.Version)
    try {
      val value = JsonSerializer.SerializeObjectToJson(functionDef)

      logger.debug("key => " + key + ",value =>" + value);
      DaoImpl.SaveObject(functionDef, MdMgr.GetMdMgr)
      logger.debug("Added function " + key + " successfully ")
      val apiResult = new ApiResult(ErrorCodeConstants.Success, "AddFunction" , null, ErrorCodeConstants.Add_Function_Successful + ":" + dispkey)
      apiResult.toString()
    } catch {
      case e: Exception => {
        val apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddFunction" , null, ErrorCodeConstants.Add_Function_Failed + ":" + dispkey)
        apiResult.toString()
      }
    }
  }

  def RemoveFunction(functionDef: FunctionDef): String = {
    var key = functionDef.typeString
    val dispkey = functionDef.FullName + "." + MdMgr.Pad0s2Version(functionDef.Version)
    try {
      DaoImpl.DeleteObject(functionDef)
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "RemoveFunction", null, ErrorCodeConstants.Remove_Function_Successfully + ":" + dispkey)
      apiResult.toString()
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveFunction", null, "Error :" + e.toString() + ErrorCodeConstants.Remove_Function_Failed + ":" + dispkey)
        apiResult.toString()
      }
    }
  }

  def RemoveFunction(nameSpace: String, functionName: String, version: Long): String = {
    var key = functionName + ":" + version
    val dispkey = functionName + "." + MdMgr.Pad0s2Version(version)
    try {
      DaoImpl.DeleteObject(key, MetadataAPIImpl.getFunctionStore)
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "RemoveFunction", null, ErrorCodeConstants.Remove_Function_Successfully + ":" + dispkey)
      apiResult.toString()
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveFunction", null, "Error :" + e.toString() + ErrorCodeConstants.Remove_Function_Failed + ":" + dispkey)
        apiResult.toString()
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
  
  def LoadAllFunctionsIntoCache {
    try {
      val functionKeys = MetadataAPIImpl.GetAllKeys("FunctionDef")
      if (functionKeys.length == 0) {
        logger.debug("No functions available in the Database")
        return
      }
      functionKeys.foreach(key => {
        val obj = DaoImpl.GetObject(key.toLowerCase, MetadataAPIImpl.getFunctionStore)
        val function = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
        DaoImpl.AddObjectToCache(function.asInstanceOf[FunctionDef], MdMgr.GetMdMgr)
      })
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

  def LoadFunctionIntoCache(key: String) {
    try {
      val obj = DaoImpl.GetObject(key.toLowerCase, MetadataAPIImpl.getFunctionStore)
      val cont = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
      DaoImpl.AddObjectToCache(cont.asInstanceOf[FunctionDef], MdMgr.GetMdMgr)
    } catch {
      case e: Exception => {
        e.printStackTrace()
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
  
  def IsFunctionAlreadyExists(funcDef: FunctionDef): Boolean = {
    try {
      var key = funcDef.typeString
      val dispkey = key // No version in this string
      val o = MdMgr.GetMdMgr.Function(funcDef.nameSpace,
        funcDef.name,
        funcDef.args.toList.map(a => (a.aType.nameSpace, a.aType.name)),
        funcDef.ver,
        false)
      o match {
        case None =>
          None
          logger.debug("function not in the cache => " + dispkey)
          return false;
        case Some(m) =>
          logger.debug("function found => " + m.asInstanceOf[FunctionDef].typeString)
          return true
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw new UnexpectedMetadataAPIException(e.getMessage())
      }
    }
  }

  def GetFunctionDef(nameSpace: String, objectName: String, formatType: String, version:String): String = {
    GetFunctionDef(nameSpace,objectName,formatType)
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
            functionList(i) = msa(i).FullName + "." + MdMgr.Pad0s2Version(msa(i).Version)
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
  
  def DumpFunctionDef(funcDef: FunctionDef) {
    logger.debug("Name => " + funcDef.Name)
    for (arg <- funcDef.args) {
      logger.debug("arg_name => " + arg.name)
      logger.debug("arg_type => " + arg.Type.tType)
    }
    logger.debug("Json string => " + JsonSerializer.SerializeObjectToJson(funcDef))
  }
}