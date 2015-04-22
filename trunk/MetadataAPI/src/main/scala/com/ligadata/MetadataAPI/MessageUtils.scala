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
object MessageUtils {

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

  def AddMessageDef(msgDef: MessageDef, recompile:Boolean = false): String = {
    try {
      Utils.AddObjectToCache(msgDef, MdMgr.GetMdMgr)
      DAOUtils.UploadJarsToDB(msgDef)
      var objectsAdded = AddMessageTypes(msgDef, MdMgr.GetMdMgr, recompile)
      objectsAdded = objectsAdded :+ msgDef
      DAOUtils.SaveObjectList(objectsAdded, MetadataAPIImpl.metadataStore)
      val operations = for (op <- objectsAdded) yield "Add"
      Utils.NotifyEngine(objectsAdded, operations)
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "AddMessageDef", null,ErrorCodeConstants.Add_Message_Successful + ":" + msgDef.FullNameWithVer)
      apiResult.toString()
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddMessageDef", null, "Error :" + e.toString() + ErrorCodeConstants.Add_Message_Failed + ":" + msgDef.FullNameWithVer)
        apiResult.toString()
      }
    }
  }

  // As per Rich's requirement, Add array/arraybuf/sortedset types for this messageDef
  // along with the messageDef.  
  def AddMessageTypes(msgDef: BaseElemDef, mdMgr: MdMgr, recompile:Boolean = false): Array[BaseElemDef] = {
    logger.debug("The class name => " + msgDef.getClass().getName())
    try {
      var types = new Array[BaseElemDef](0)
      val msgType = Utils.getObjectType(msgDef)
      val depJars = if (msgDef.DependencyJarNames != null)
        (msgDef.DependencyJarNames :+ msgDef.JarName) else Array(msgDef.JarName)
      msgType match {
        case "MessageDef" | "ContainerDef" => {
          // ArrayOf<TypeName>
          var obj: BaseElemDef = mdMgr.MakeArray(msgDef.nameSpace, "arrayof" + msgDef.name, msgDef.nameSpace, msgDef.name, 1, msgDef.ver, recompile)
          obj.dependencyJarNames = depJars
          Utils.AddObjectToCache(obj, mdMgr)
          types = types :+ obj
          // ArrayBufferOf<TypeName>
          obj = mdMgr.MakeArrayBuffer(msgDef.nameSpace, "arraybufferof" + msgDef.name, msgDef.nameSpace, msgDef.name, 1, msgDef.ver, recompile)
          obj.dependencyJarNames = depJars
          Utils.AddObjectToCache(obj, mdMgr)
          types = types :+ obj
          // SortedSetOf<TypeName>
          obj = mdMgr.MakeSortedSet(msgDef.nameSpace, "sortedsetof" + msgDef.name, msgDef.nameSpace, msgDef.name, msgDef.ver, recompile)
          obj.dependencyJarNames = depJars
          Utils.AddObjectToCache(obj, mdMgr)
          types = types :+ obj
          // ImmutableMapOfIntArrayOf<TypeName>
          obj = mdMgr.MakeImmutableMap(msgDef.nameSpace, "immutablemapofintarrayof" + msgDef.name, ("System", "Int"), (msgDef.nameSpace, "arrayof" + msgDef.name), msgDef.ver, recompile)
          obj.dependencyJarNames = depJars
          Utils.AddObjectToCache(obj, mdMgr)
          types = types :+ obj
          // ImmutableMapOfString<TypeName>
          obj = mdMgr.MakeImmutableMap(msgDef.nameSpace, "immutablemapofstringarrayof" + msgDef.name, ("System", "String"), (msgDef.nameSpace, "arrayof" + msgDef.name), msgDef.ver, recompile)
          obj.dependencyJarNames = depJars
          Utils.AddObjectToCache(obj, mdMgr)
          types = types :+ obj
          // ArrayOfArrayOf<TypeName>
          obj = mdMgr.MakeArray(msgDef.nameSpace, "arrayofarrayof" + msgDef.name, msgDef.nameSpace, "arrayof" + msgDef.name, 1, msgDef.ver, recompile)
          obj.dependencyJarNames = depJars
          Utils.AddObjectToCache(obj, mdMgr)
          types = types :+ obj
          // MapOfStringArrayOf<TypeName>
          obj = mdMgr.MakeMap(msgDef.nameSpace, "mapofstringarrayof" + msgDef.name, ("System", "String"), ("System", "arrayof" + msgDef.name), msgDef.ver, recompile)
          obj.dependencyJarNames = depJars
          Utils.AddObjectToCache(obj, mdMgr)
          types = types :+ obj
          // MapOfIntArrayOf<TypeName>
          obj = mdMgr.MakeMap(msgDef.nameSpace, "mapofintarrayof" + msgDef.name, ("System", "Int"), ("System", "arrayof" + msgDef.name), msgDef.ver, recompile)
          obj.dependencyJarNames = depJars
          Utils.AddObjectToCache(obj, mdMgr)
          types = types :+ obj
          // SetOf<TypeName>
          obj = mdMgr.MakeSet(msgDef.nameSpace, "setof" + msgDef.name, msgDef.nameSpace, msgDef.name, msgDef.ver, recompile)
          obj.dependencyJarNames = depJars
          Utils.AddObjectToCache(obj, mdMgr)
          types = types :+ obj
          // TreeSetOf<TypeName>
          obj = mdMgr.MakeTreeSet(msgDef.nameSpace, "treesetof" + msgDef.name, msgDef.nameSpace, msgDef.name, msgDef.ver, recompile)
          obj.dependencyJarNames = depJars
          Utils.AddObjectToCache(obj, mdMgr)
          types = types :+ obj
          types
        }
        case _ => {
          throw new InternalErrorException("Unknown class in AddMessageTypes")
        }
      }
    } catch {
      case e: Exception => {
        throw new Exception(e.getMessage())
      }
    }
  }


  def AddContainerOrMessage(contOrMsgText: String, format: String, recompile:Boolean = false): String = {
    var resultStr:String = ""
    try {
      var compProxy = new CompilerProxy
      compProxy.setLoggerLevel(Level.TRACE)
      val (classStr, cntOrMsgDef) = compProxy.compileMessageDef(contOrMsgText,recompile)
      logger.debug("Message/Container Compiler returned an object of type " + cntOrMsgDef.getClass().getName())
      cntOrMsgDef match {
        case msg: MessageDef => {
	  if( recompile ){
	    // Incase of recompile, Message Compiler is automatically incrementing the previous version
	    // by 1. Before Updating the metadata with the new version, remove the old version
            val latestVersion = GetLatestMessage(msg)
            RemoveMessage(latestVersion.get.nameSpace, latestVersion.get.name, latestVersion.get.ver)
            resultStr = AddMessageDef(msg, recompile)
	  }
	  else{
            resultStr = AddMessageDef(msg, recompile)
	  }
	  if( recompile ){
	    val depModels = ModelUtils.GetDependentModels(msg.NameSpace,msg.Name,msg.ver)
	    if( depModels.length > 0 ){
              depModels.foreach(mod => {
		logger.debug("DependentModel => " + mod.FullNameWithVer)
		resultStr = resultStr + ModelUtils.RecompileModel(mod)
              })
	    }
	  }
	  resultStr
	}
        case cont: ContainerDef => {
	  if( recompile ){
	    // Incase of recompile, Message Compiler is automatically incrementing the previous version
	    // by 1. Before Updating the metadata with the new version, remove the old version
            val latestVersion = ContainerUtils.GetLatestContainer(cont)
            ContainerUtils.RemoveContainer(latestVersion.get.nameSpace, latestVersion.get.name, latestVersion.get.ver)
            resultStr = ContainerUtils.AddContainerDef(cont, recompile)
	  }
	  else{
            resultStr = ContainerUtils.AddContainerDef(cont, recompile)
	  }

	  if( recompile ){
	    val depModels = ModelUtils.GetDependentModels(cont.NameSpace,cont.Name,cont.ver)
	    if( depModels.length > 0 ){
              depModels.foreach(mod => {
		logger.debug("DependentModel => " + mod.FullNameWithVer)
		resultStr = resultStr + ModelUtils.RecompileModel(mod)
              })
	    }
	  }
	  resultStr
        }
      }
    } catch {
      case e: ModelCompilationFailedException => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddContainerOrMessage", null, "Error: " + e.toString + ErrorCodeConstants.Add_Container_Or_Message_Failed + ":" + contOrMsgText)
        apiResult.toString()
      }
      case e: MsgCompilationFailedException => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddContainerOrMessage", null, "Error: " + e.toString + ErrorCodeConstants.Add_Container_Or_Message_Failed + ":" + contOrMsgText)
        apiResult.toString()
      }
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddContainerOrMessage", null, "Error: " + e.toString + ErrorCodeConstants.Add_Container_Or_Message_Failed + ":" + contOrMsgText)
        apiResult.toString()
      }
    }
  }


  def AddMessage(messageText: String, format: String): String = {
    AddContainerOrMessage(messageText, format)
  }

  def AddMessage(messageText: String): String = {
    AddMessage(messageText, "JSON")
  }

  private def RecompileMessage(msgFullName: String): String = {
    var resultStr:String = ""
    try {
      var messageText:String = null

      val latestMsgDef = MdMgr.GetMdMgr.Message(msgFullName,-1,true)
      if( latestMsgDef == None){
	val latestContDef = MdMgr.GetMdMgr.Container(msgFullName,-1,true)
	if( latestContDef == None ){
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RecompileMessage", null, ErrorCodeConstants.Recompile_Message_Failed + ":" + msgFullName + " Error:No message or container named ")
          return apiResult.toString()
	}
	else{
	  messageText = latestContDef.get.objectDefinition
	}
      }
      else{
	messageText = latestMsgDef.get.objectDefinition
      }
      resultStr = AddContainerOrMessage(messageText,"JSON",true)
      resultStr

    } catch {
      case e: MsgCompilationFailedException => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RecompileMessage", null, "Error :" + e.toString() + ErrorCodeConstants.Recompile_Message_Failed + ":" + msgFullName)
        apiResult.toString()
      }
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RecompileMessage", null, "Error :" + e.toString() + ErrorCodeConstants.Recompile_Message_Failed + ":" + msgFullName)
        apiResult.toString()
      }
    }
  }

  def UpdateMessage(messageText: String, format: String): String = {
    var resultStr:String = ""
    try {
      var compProxy = new CompilerProxy
   //  compProxy.setLoggerLevel(Level.TRACE)
      val (classStr, msgDef) = compProxy.compileMessageDef(messageText)
      val key = msgDef.FullNameWithVer
      msgDef match {
        case msg: MessageDef => {
          val latestVersion = GetLatestMessage(msg)
          var isValid = true
          if (latestVersion != None) {
            isValid = Utils.IsValidVersion(latestVersion.get, msg)
          }
          if (isValid) {
            RemoveMessage(latestVersion.get.nameSpace, latestVersion.get.name, latestVersion.get.ver)
            resultStr = AddMessageDef(msg)

	    logger.debug("Check for dependent messages ...")
	    val depMessages = GetDependentMessages.getDependentObjects(msg)
	    if( depMessages.length > 0 ){
	      depMessages.foreach(msg => {
		logger.debug("DependentMessage => " + msg)
		resultStr = resultStr + RecompileMessage(msg)
	      })
	    }
	    val depModels = ModelUtils.GetDependentModels(msg.NameSpace,msg.Name,msg.Version.toInt)
	    if( depModels.length > 0 ){
	      depModels.foreach(mod => {
		logger.debug("DependentModel => " + mod.FullNameWithVer)
		resultStr = resultStr + ModelUtils.RecompileModel(mod)
	      })
	    }
	    resultStr
          } else {
            var apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateMessage", null, ErrorCodeConstants.Update_Message_Failed + ":" + messageText + " Error:Invalid Version")
            apiResult.toString()
          }
        }
        case msg: ContainerDef => {
          val latestVersion = ContainerUtils.GetLatestContainer(msg)
          var isValid = true
          if (latestVersion != None) {
            isValid = Utils.IsValidVersion(latestVersion.get, msg)
          }
          if (isValid) {
            ContainerUtils.RemoveContainer(latestVersion.get.nameSpace, latestVersion.get.name, latestVersion.get.ver)
            resultStr = ContainerUtils.AddContainerDef(msg)


	    val depMessages = GetDependentMessages.getDependentObjects(msg)
	    if( depMessages.length > 0 ){
	      depMessages.foreach(msg => {
		logger.debug("DependentMessage => " + msg)
		resultStr = resultStr + RecompileMessage(msg)
	      })
	    }
	    val depModels = ModelUtils.GetDependentModels(msg.NameSpace,msg.Name,msg.Version.toInt)
	    if( depModels.length > 0 ){
	      depModels.foreach(mod => {
		logger.debug("DependentModel => " + mod.FullNameWithVer)
		resultStr = resultStr + ModelUtils.RecompileModel(mod)
	      })
	    }
	    resultStr
          } else {
            var apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateMessage", null, ErrorCodeConstants.Update_Message_Failed + ":" + messageText + " Error:Invalid Version")
            apiResult.toString()
          }
        }
      }
    } catch {
      case e: MsgCompilationFailedException => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateMessage", null, "Error :" + e.toString() + ErrorCodeConstants.Update_Message_Failed + ":" + messageText)
        apiResult.toString()
      }
      case e: ObjectNotFoundException => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateMessage", null, "Error :" + e.toString() + ErrorCodeConstants.Update_Message_Failed+ ":" + messageText)
        apiResult.toString()
      }
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateMessage", null, "Error :" + e.toString() + ErrorCodeConstants.Update_Message_Failed+ ":" + messageText)
        apiResult.toString()
      }
    }
  }

  def UpdateMessage(messageText: String): String = {
    UpdateMessage(messageText,"JSON")
  }

  // Remove message with Message Name and Version Number
  def RemoveMessage(nameSpace: String, name: String, version: Long, zkNotify: Boolean = true): String = {
    var key = nameSpace + "." + name + "." + version
    try {
      val o = MdMgr.GetMdMgr.Message(nameSpace.toLowerCase, name.toLowerCase, version, true)
      o match {
        case None =>
          None
          logger.debug("Message not found => " + key)
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveMessage", null, ErrorCodeConstants.Remove_Message_Failed_Not_Found + ":" + key)
          apiResult.toString()
        case Some(m) =>
          val msgDef = m.asInstanceOf[MessageDef]
          logger.debug("message found => " + msgDef.FullNameWithVer)
          var objectsToBeRemoved = Utils.GetAdditionalTypesAdded(msgDef, MdMgr.GetMdMgr)
          // Also remove a type with same name as messageDef
          var typeName = name
          var typeDef = TypeUtils.GetType(nameSpace, typeName, version.toString, "JSON")
          if (typeDef != None) {
            objectsToBeRemoved = objectsToBeRemoved :+ typeDef.get
          }
          objectsToBeRemoved.foreach(typ => {
            TypeUtils.RemoveType(typ.nameSpace, typ.name, typ.ver)
          })
          // MessageDef itself
          DAOUtils.DeleteObject(msgDef)
          objectsToBeRemoved = objectsToBeRemoved :+ msgDef
	  if( zkNotify ){
            val operations = for (op <- objectsToBeRemoved) yield "Remove"
            Utils.NotifyEngine(objectsToBeRemoved, operations)
	  }
          var apiResult = new ApiResult(ErrorCodeConstants.Success, "RemoveMessage", null, ErrorCodeConstants.Remove_Message_Successful + ":" + key)
          apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveMessage", null, "Error :" + e.toString() + ErrorCodeConstants.Remove_Message_Failed + ":" + key)
        apiResult.toString()
      }
    }
  }

  // Remove message with Message Name and Version Number
  def RemoveMessage(messageName: String, version: Long): String = {
    RemoveMessage(sysNS, messageName, version)
  }

  // All available messages(format JSON or XML) as a String
  def GetAllMessageDefs(formatType: String): String = {
    try {
      val msgDefs = MdMgr.GetMdMgr.Messages(true, true)
      msgDefs match {
        case None =>
          None
          logger.debug("No Messages found ")
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetAllMessageDefs", null, ErrorCodeConstants.Get_All_Messages_Failed_Not_Available)
          apiResult.toString()
        case Some(ms) =>
          val msa = ms.toArray
          var apiResult = new ApiResult(ErrorCodeConstants.Success, "GetAllMessageDefs", JsonSerializer.SerializeObjectListToJson("Messages", msa), ErrorCodeConstants.Get_All_Messages_Succesful)
          apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetAllMessageDefs", null, "Error :" + e.toString() + ErrorCodeConstants.Get_All_Messages_Failed)
        apiResult.toString()
      }
    }
  }

  def GetAllMessagesFromCache(active: Boolean): Array[String] = {
    var messageList: Array[String] = new Array[String](0)
    try {
      val msgDefs = MdMgr.GetMdMgr.Messages(active, true)
      msgDefs match {
        case None =>
          None
          logger.debug("No Messages found ")
          messageList
        case Some(ms) =>
          val msa = ms.toArray
          val msgCount = msa.length
          messageList = new Array[String](msgCount)
          for (i <- 0 to msgCount - 1) {
            messageList(i) = msa(i).FullNameWithVer
          }
          messageList
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw new UnexpectedMetadataAPIException("Failed to fetch all the messages:" + e.toString)
      }
    }
  }


  // Specific message (format JSON or XML) as a String using messageName(with version) as the key
  def GetMessageDefFromCache(nameSpace: String, name: String, formatType: String, version: String): String = {
    var key = nameSpace + "." + name + "." + version
    try {
      val o = MdMgr.GetMdMgr.Message(nameSpace.toLowerCase, name.toLowerCase, version.toInt, true)
      o match {
        case None =>
          None
          logger.debug("message not found => " + key)
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetMessageDefFromCache", null, ErrorCodeConstants.Get_Message_From_Cache_Failed + ":" + key)
          apiResult.toString()
        case Some(m) =>
          logger.debug("message found => " + m.asInstanceOf[MessageDef].FullNameWithVer)
          var apiResult = new ApiResult(ErrorCodeConstants.Success, "GetMessageDefFromCache", JsonSerializer.SerializeObjectToJson(m), ErrorCodeConstants.Get_Message_From_Cache_Successful)
          apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetMessageDefFromCache", null, "Error :" + e.toString() + ErrorCodeConstants.Get_Message_From_Cache_Failed + ":" + key)
        apiResult.toString()
      }
    }
  }


  // Get the latest message for a given FullName
  private def GetLatestMessage(msgDef: MessageDef): Option[MessageDef] = {
    try {
      var key = msgDef.nameSpace + "." + msgDef.name + "." + msgDef.ver
      val o = MdMgr.GetMdMgr.Messages(msgDef.nameSpace.toLowerCase,
                                      msgDef.name.toLowerCase,
                                      false,
                                      true)
      o match {
        case None =>
          None
          logger.debug("message not in the cache => " + key)
          None
        case Some(m) =>
          logger.debug("message found => " + m.head.asInstanceOf[MessageDef].FullNameWithVer)
          Some(m.head.asInstanceOf[MessageDef])
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw new UnexpectedMetadataAPIException(e.getMessage())
      }
    }
  }

  def LoadMessageIntoCache(key: String) {
    try {
      logger.debug("Fetch the object " + key + " from database ")
      val obj = DAOUtils.GetObject(key.toLowerCase, MetadataAPIImpl.messageStore)
      logger.debug("Deserialize the object " + key)
      val msg = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
      logger.debug("Get the jar from database ")
      val msgDef = msg.asInstanceOf[MessageDef]
      DAOUtils.DownloadJarFromDB(msgDef)
      logger.debug("Add the object " + key + " to the cache ")
      Utils.AddObjectToCache(msgDef, MdMgr.GetMdMgr)
    } catch {
      case e: Exception => {
	logger.debug("Failed to load message into cache " + key + ":" + e.getMessage())
      }
    }
  }

  // Specific messages (format JSON or XML) as a String using messageName(without version) as the key
  def GetMessageDef(objectName: String, formatType: String): String = {
    val nameSpace = MdMgr.sysNS
    GetMessageDefFromCache(nameSpace, objectName, formatType, "-1")
  }
  // Specific message (format JSON or XML) as a String using messageName(with version) as the key
  def GetMessageDef(nameSpace: String, objectName: String, formatType: String, version: String): String = {
    GetMessageDefFromCache(nameSpace, objectName, formatType, version)
  }

  // Specific message (format JSON or XML) as a String using messageName(with version) as the key
  def GetMessageDef(objectName: String, version: String, formatType: String): String = {
    val nameSpace = MdMgr.sysNS
    GetMessageDef(nameSpace, objectName, formatType, version)
  }

}
