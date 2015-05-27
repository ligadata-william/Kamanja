package com.ligadata.MetadataAPI

import org.apache.log4j._

import com.ligadata.Serialize._
import com.ligadata.Utils._
import com.ligadata.fatafat.metadata.ObjType._
import com.ligadata.fatafat.metadata._
import com.ligadata.fatafat.metadata.MdMgr._
import com.ligadata.Serialize._

object MessageImpl {
  lazy val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)  
  lazy val serializer = SerializerManager.GetSerializer("kryo")
  
  // Remove message with Message Name and Version Number
  def RemoveMessageFromCache(zkMessage: ZooKeeperNotification) = {
    try {
      var key = zkMessage.NameSpace + "." + zkMessage.Name + "." + zkMessage.Version
      val dispkey = zkMessage.NameSpace + "." + zkMessage.Name + "." + MdMgr.Pad0s2Version(zkMessage.Version.toLong)
      val o = MdMgr.GetMdMgr.Message(zkMessage.NameSpace, zkMessage.Name, zkMessage.Version.toLong, true)
      o match {
        case None =>
          None
          logger.debug("Message not found, Already Removed? => " + dispkey)
        case Some(m) =>
          val msgDef = m.asInstanceOf[MessageDef]
          logger.debug("message found => " + msgDef.FullName + "." + MdMgr.Pad0s2Version(msgDef.Version))
          val types = TypeImpl.GetAdditionalTypesAdded(msgDef, MdMgr.GetMdMgr)

          var typeName = zkMessage.Name
          MdMgr.GetMdMgr.RemoveType(zkMessage.NameSpace, typeName, zkMessage.Version.toLong)
          typeName = "arrayof" + zkMessage.Name
          MdMgr.GetMdMgr.RemoveType(zkMessage.NameSpace, typeName, zkMessage.Version.toLong)
          typeName = "sortedsetof" + zkMessage.Name
          MdMgr.GetMdMgr.RemoveType(zkMessage.NameSpace, typeName, zkMessage.Version.toLong)
          typeName = "arraybufferof" + zkMessage.Name
          MdMgr.GetMdMgr.RemoveType(zkMessage.NameSpace, typeName, zkMessage.Version.toLong)
          MdMgr.GetMdMgr.RemoveMessage(zkMessage.NameSpace, zkMessage.Name, zkMessage.Version.toLong)
      }
    } catch {
      case e: Exception => {
        logger.error("Failed to delete the Message from cache:" + e.toString)
      }
    }
  }
  
     // Remove message with Message Name and Version Number
  def RemoveMessage(nameSpace: String, name: String, version: Long, zkNotify:Boolean = true): String = {
    var key = nameSpace + "." + name + "." + version
    val dispkey = nameSpace + "." + name + "." + MdMgr.Pad0s2Version(version)
    try {
      val o = MdMgr.GetMdMgr.Message(nameSpace.toLowerCase, name.toLowerCase, version, true)
      o match {
        case None =>
          None
          logger.debug("Message not found => " + key)
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveMessage", null, ErrorCodeConstants.Remove_Message_Failed_Not_Found + ":" + dispkey)
          apiResult.toString()
        case Some(m) =>
          val msgDef = m.asInstanceOf[MessageDef]
          logger.debug("message found => " + msgDef.FullName + "." + MdMgr.Pad0s2Version(msgDef.Version))
          var objectsToBeRemoved = TypeImpl.GetAdditionalTypesAdded(msgDef, MdMgr.GetMdMgr)

          // Also remove a type with same name as messageDef
          var typeName = name
          var typeDef = TypeImpl.GetType(nameSpace, typeName, version.toString, "JSON")
          
          if (typeDef != None) {
            objectsToBeRemoved = objectsToBeRemoved :+ typeDef.get
          }
          
          objectsToBeRemoved.foreach(typ => {           
            MetadataAPIImpl.RemoveType(typ.nameSpace, typ.name, typ.ver)
          })
          
          // MessageDef itself - add it to the list of other objects to be passed to the zookeeper
          // to notify other instnances
          DaoImpl.DeleteObject(msgDef)
          var allObjectsArray = objectsToBeRemoved :+ msgDef
        
        if( zkNotify ){
          val operations = for (op <- allObjectsArray) yield "Remove"
          MetadataSynchronizer.NotifyEngine(allObjectsArray, operations)
        }
          var apiResult = new ApiResult(ErrorCodeConstants.Success, "RemoveMessage", null, ErrorCodeConstants.Remove_Message_Successful + ":" + dispkey)
          apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveMessage", null, "Error :" + e.toString() + ErrorCodeConstants.Remove_Message_Failed + ":" + dispkey)
        apiResult.toString()
      }
    }
  } 
  
  /**
   * 
   */
  def UpdateMessage(messageText: String, format: String): String = {
    var resultStr:String = ""
    try {
      var compProxy = new CompilerProxy
      compProxy.setLoggerLevel(Level.TRACE)
      val (classStr, msgDef) = compProxy.compileMessageDef(messageText)
      val key = msgDef.FullNameWithVer
      msgDef match {
        case msg: MessageDef => {
          val latestVersion = MessageImpl.GetLatestMessage(msg)
          var isValid = true
          if (latestVersion != None) {
            isValid = MetadataUtils.IsValidVersion(latestVersion.get, msg)
          }
          if (isValid) {
            RemoveMessage(latestVersion.get.nameSpace, latestVersion.get.name, latestVersion.get.ver)
            resultStr = MessageImpl.AddMessageDef(msg)

            logger.debug("Check for dependent messages ...")
            val depMessages = GetDependentMessages.getDependentObjects(msg)
            if( depMessages.length > 0 ){
              depMessages.foreach(msg => {
                logger.debug("DependentMessage => " + msg)
                resultStr = resultStr + RecompileMessage(msg)
              })
            }  
            val depModels = ModelImpl.GetDependentModels(msg.NameSpace,msg.Name,msg.Version.toLong)
            if( depModels.length > 0 ){
              depModels.foreach(mod => {
                logger.debug("DependentModel => " + mod.FullNameWithVer)
                resultStr = resultStr + ModelImpl.RecompileModel(mod)
              })
            }
            resultStr
          } else {
            var apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateMessage", null, ErrorCodeConstants.Update_Message_Failed + ":" + messageText + " Error:Invalid Version")
            apiResult.toString()
          }
        }
        case msg: ContainerDef => {
          val latestVersion = ContainerImpl.GetLatestContainer(msg)
          var isValid = true
          if (latestVersion != None) {
            isValid = MetadataUtils.IsValidVersion(latestVersion.get, msg)
          }
          if (isValid) {
            ContainerImpl.RemoveContainer(latestVersion.get.nameSpace, latestVersion.get.name, latestVersion.get.ver)
            resultStr = ContainerImpl.AddContainerDef(msg)
            val depMessages = GetDependentMessages.getDependentObjects(msg)
            if( depMessages.length > 0 ){
              depMessages.foreach(msg => {
                logger.debug("DependentMessage => " + msg)
                resultStr = resultStr + MessageImpl.RecompileMessage(msg)
              })
            }
            val depModels = ModelImpl.GetDependentModels(msg.NameSpace,msg.Name,msg.Version.toLong)
            if( depModels.length > 0 ){
              depModels.foreach(mod => {
                logger.debug("DependentModel => " + mod.FullName + "." + MdMgr.Pad0s2Version(mod.Version))
                resultStr = resultStr + ModelImpl.RecompileModel(mod)
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
  
  
  def RecompileMessage(msgFullName: String): String = {
    var resultStr:String = ""
    try {
      var messageText:String = null

      val latestMsgDef = MdMgr.GetMdMgr.Message(msgFullName,-1,true)
      if( latestMsgDef == None){
        val latestContDef = MdMgr.GetMdMgr.Container(msgFullName,-1,true)
        if( latestContDef == None ){
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RecompileMessage", null, ErrorCodeConstants.Recompile_Message_Failed + ":" + msgFullName + " Error:No message or container named ")
          return apiResult.toString()
        } else {
          messageText = latestContDef.get.objectDefinition
        }
      } else {
        messageText = latestMsgDef.get.objectDefinition
      }
      resultStr = MessageImpl.AddContainerOrMessage(messageText,"JSON",true)
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
  
  
  def AddMessageDef(msgDef: MessageDef, recompile:Boolean = false): String = {
    val dispkey = msgDef.FullName + "." + MdMgr.Pad0s2Version(msgDef.Version)
    try {
      DaoImpl.AddObjectToCache(msgDef, MdMgr.GetMdMgr)
      JarImpl.UploadJarsToDB(msgDef)
      var objectsAdded = AddMessageTypes(msgDef, MdMgr.GetMdMgr, recompile)
      objectsAdded = objectsAdded :+ msgDef
      DaoImpl.SaveObjectList(objectsAdded,  MetadataAPIImpl.getMetadataStore )
      val operations = for (op <- objectsAdded) yield "Add"
      MetadataSynchronizer.NotifyEngine(objectsAdded, operations)
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "AddMessageDef", null,ErrorCodeConstants.Add_Message_Successful + ":" + dispkey)
      apiResult.toString()
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddMessageDef", null, "Error :" + e.toString() + ErrorCodeConstants.Add_Message_Failed + ":" + dispkey)
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
        val msgType = MetadataUtils.getObjectType(msgDef)
        val depJars = if (msgDef.DependencyJarNames != null)
        (msgDef.DependencyJarNames :+ msgDef.JarName) else Array(msgDef.JarName)
        msgType match {
          case "MessageDef" | "ContainerDef" => {
            // ArrayOf<TypeName>
            var obj: BaseElemDef = mdMgr.MakeArray(msgDef.nameSpace, "arrayof" + msgDef.name, msgDef.nameSpace, msgDef.name, 1, msgDef.ver, recompile)
            obj.dependencyJarNames = depJars
            DaoImpl.AddObjectToCache(obj, mdMgr)
            types = types :+ obj
            // ArrayBufferOf<TypeName>
            obj = mdMgr.MakeArrayBuffer(msgDef.nameSpace, "arraybufferof" + msgDef.name, msgDef.nameSpace, msgDef.name, 1, msgDef.ver, recompile)
            obj.dependencyJarNames = depJars
            DaoImpl.AddObjectToCache(obj, mdMgr)
            types = types :+ obj
            // SortedSetOf<TypeName>
            obj = mdMgr.MakeSortedSet(msgDef.nameSpace, "sortedsetof" + msgDef.name, msgDef.nameSpace, msgDef.name, msgDef.ver, recompile)
            obj.dependencyJarNames = depJars
            DaoImpl.AddObjectToCache(obj, mdMgr)
            types = types :+ obj
            // ImmutableMapOfIntArrayOf<TypeName>
            obj = mdMgr.MakeImmutableMap(msgDef.nameSpace, "immutablemapofintarrayof" + msgDef.name, ("System", "Int"), (msgDef.nameSpace, "arrayof" + msgDef.name), msgDef.ver, recompile)
            obj.dependencyJarNames = depJars
            DaoImpl.AddObjectToCache(obj, mdMgr)
            types = types :+ obj
            // ImmutableMapOfString<TypeName>
            obj = mdMgr.MakeImmutableMap(msgDef.nameSpace, "immutablemapofstringarrayof" + msgDef.name, ("System", "String"), (msgDef.nameSpace, "arrayof" + msgDef.name), msgDef.ver, recompile)
            obj.dependencyJarNames = depJars
            DaoImpl.AddObjectToCache(obj, mdMgr)
            types = types :+ obj
            // ArrayOfArrayOf<TypeName>
            obj = mdMgr.MakeArray(msgDef.nameSpace, "arrayofarrayof" + msgDef.name, msgDef.nameSpace, "arrayof" + msgDef.name, 1, msgDef.ver, recompile)
            obj.dependencyJarNames = depJars
            DaoImpl.AddObjectToCache(obj, mdMgr)
            types = types :+ obj
            // MapOfStringArrayOf<TypeName>
            obj = mdMgr.MakeMap(msgDef.nameSpace, "mapofstringarrayof" + msgDef.name, ("System", "String"), ("System", "arrayof" + msgDef.name), msgDef.ver, recompile)
            obj.dependencyJarNames = depJars
            DaoImpl.AddObjectToCache(obj, mdMgr)
            types = types :+ obj
            // MapOfIntArrayOf<TypeName>
            obj = mdMgr.MakeMap(msgDef.nameSpace, "mapofintarrayof" + msgDef.name, ("System", "Int"), ("System", "arrayof" + msgDef.name), msgDef.ver, recompile)
            obj.dependencyJarNames = depJars
            DaoImpl.AddObjectToCache(obj, mdMgr)
            types = types :+ obj
            // SetOf<TypeName>
            obj = mdMgr.MakeSet(msgDef.nameSpace, "setof" + msgDef.name, msgDef.nameSpace, msgDef.name, msgDef.ver, recompile)
            obj.dependencyJarNames = depJars
            DaoImpl.AddObjectToCache(obj, mdMgr)
            types = types :+ obj
            // TreeSetOf<TypeName>
            obj = mdMgr.MakeTreeSet(msgDef.nameSpace, "treesetof" + msgDef.name, msgDef.nameSpace, msgDef.name, msgDef.ver, recompile)
            obj.dependencyJarNames = depJars
            DaoImpl.AddObjectToCache(obj, mdMgr)
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
          if( recompile ) {
            // Incase of recompile, Message Compiler is automatically incrementing the previous version
            // by 1. Before Updating the metadata with the new version, remove the old version
            val latestVersion = MessageImpl.GetLatestMessage(msg)
            RemoveMessage(latestVersion.get.nameSpace, latestVersion.get.name, latestVersion.get.ver)
            resultStr = AddMessageDef(msg, recompile)
          }
          else{
            resultStr = AddMessageDef(msg, recompile)
          }
          
          if( recompile ) {
            val depModels = ModelImpl.GetDependentModels(msg.NameSpace,msg.Name,msg.ver)
            if( depModels.length > 0 ){
              depModels.foreach(mod => {
          logger.debug("DependentModel => " + mod.FullNameWithVer)
          resultStr = resultStr + ModelImpl.RecompileModel(mod)
              })
            }
          }
          resultStr
        }
        case cont: ContainerDef => {
          if( recompile ){
            // Incase of recompile, Message Compiler is automatically incrementing the previous version
            // by 1. Before Updating the metadata with the new version, remove the old version
                  val latestVersion = ContainerImpl.GetLatestContainer(cont)
                  ContainerImpl.RemoveContainer(latestVersion.get.nameSpace, latestVersion.get.name, latestVersion.get.ver)
                  resultStr = ContainerImpl.AddContainerDef(cont, recompile)
          }
          else{
                  resultStr = ContainerImpl.AddContainerDef(cont, recompile)
          }
      
          if( recompile ){
            val depModels = ModelImpl.GetDependentModels(cont.NameSpace,cont.Name,cont.ver)
            if( depModels.length > 0 ){
              depModels.foreach(mod => {
                logger.debug("DependentModel => " + mod.FullNameWithVer)
                resultStr = resultStr + ModelImpl.RecompileModel(mod)
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
  
  // check whether message already exists in metadata manager. Ideally,
  // we should never add the message into metadata manager more than once
  // and there is no need to use this function in main code flow
  // This is just a utility function being during these initial phases
  def IsMessageAlreadyExists(msgDef: MessageDef): Boolean = {
    try {
      var key = msgDef.nameSpace + "." + msgDef.name + "." + msgDef.ver
      val dispkey = msgDef.nameSpace + "." + msgDef.name + "." + MdMgr.Pad0s2Version(msgDef.ver)
      val o = MdMgr.GetMdMgr.Message(msgDef.nameSpace.toLowerCase,
        msgDef.name.toLowerCase,
        msgDef.ver,
        false)
      o match {
        case None =>
          None
          logger.debug("message not in the cache => " + key)
          return false;
        case Some(m) =>
          logger.debug("message found => " + m.asInstanceOf[MessageDef].FullName +  "." + MdMgr.Pad0s2Version(m.asInstanceOf[MessageDef].ver))
          return true
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw new UnexpectedMetadataAPIException(e.getMessage())
      }
    }
  }

  // Get the latest message for a given FullName
  def GetLatestMessage(msgDef: MessageDef): Option[MessageDef] = {
    try {
      var key = msgDef.nameSpace + "." + msgDef.name + "." + msgDef.ver
      val dispkey = msgDef.nameSpace + "." + msgDef.name + "." + MdMgr.Pad0s2Version(msgDef.ver)
      val o = MdMgr.GetMdMgr.Messages(msgDef.nameSpace.toLowerCase,
                                      msgDef.name.toLowerCase,
                                      false,
                                      true)
      o match {
        case None =>
          None
          logger.debug("message not in the cache => " + dispkey)
          None
        case Some(m) =>
          // We can get called from the Add Message path, and M could be empty.
          if (m.size == 0) return None
          logger.debug("message found => " + m.head.asInstanceOf[MessageDef].FullName  + "." + MdMgr.Pad0s2Version( m.head.asInstanceOf[MessageDef].ver))
          Some(m.head.asInstanceOf[MessageDef])
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw new UnexpectedMetadataAPIException(e.getMessage())
      }
    }
  }
  
  // Return Specific messageDef object using messageName(with version) as the key
  @throws(classOf[ObjectNotFoundException])
  def GetMessageDefInstanceFromCache(nameSpace: String, name: String, formatType: String, version: String): MessageDef = {
    var key = nameSpace + "." + name + "." + version.toLong
    val dispkey = nameSpace + "." + name + "." + MdMgr.Pad0s2Version(version.toLong)
    try {
      val o = MdMgr.GetMdMgr.Message(nameSpace.toLowerCase, name.toLowerCase, version.toLong, true)
      o match {
        case None =>
          None
          logger.debug("message not found => " + dispkey)
          throw new ObjectNotFoundException("Failed to Fetch the message:" + dispkey)
        case Some(m) =>
          m.asInstanceOf[MessageDef]
      }
    } catch {
      case e: Exception => {
        throw new ObjectNotFoundException("Failed to Fetch the message:" + dispkey + ":" + e.getMessage())
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
            messageList(i) = msa(i).FullName + "." + MdMgr.Pad0s2Version(msa(i).Version)
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
    val dispkey = nameSpace + "." + name + "." + MdMgr.Pad0s2Version(version.toLong)
    var key = nameSpace + "." + name + "." + version.toLong
    try {
      val o = MdMgr.GetMdMgr.Message(nameSpace.toLowerCase, name.toLowerCase, version.toLong, true)
      o match {
        case None =>
          None
          logger.debug("message not found => " + dispkey)
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetMessageDefFromCache", null, ErrorCodeConstants.Get_Message_From_Cache_Failed + ":" + dispkey)
          apiResult.toString()
        case Some(m) =>
          logger.debug("message found => " + m.asInstanceOf[MessageDef].FullName + "." + MdMgr.Pad0s2Version(m.asInstanceOf[MessageDef].Version))
          var apiResult = new ApiResult(ErrorCodeConstants.Success, "GetMessageDefFromCache", JsonSerializer.SerializeObjectToJson(m), ErrorCodeConstants.Get_Message_From_Cache_Successful)
          apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetMessageDefFromCache", null, "Error :" + e.toString() + ErrorCodeConstants.Get_Message_From_Cache_Failed + ":" + dispkey)
        apiResult.toString()
      }
    }
  }

  def LoadAllMessagesIntoCache {
    try {
      val msgKeys = MetadataAPIImpl.GetAllKeys("MessageDef")
      if (msgKeys.length == 0) {
        logger.debug("No messages available in the Database")
        return
      }
      msgKeys.foreach(key => {
        val obj = DaoImpl.GetObject(key.toLowerCase, MetadataAPIImpl.getMessageStore)
        val msg = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
        DaoImpl.AddObjectToCache(msg.asInstanceOf[MessageDef], MdMgr.GetMdMgr)
      })
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

  def LoadMessageIntoCache(key: String) {
    try {
      logger.debug("Fetch the object " + key + " from database ")
      val obj = DaoImpl.GetObject(key.toLowerCase, MetadataAPIImpl.getMessageStore)
      logger.debug("Deserialize the object " + key)
      val msg = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
      logger.debug("Get the jar from database ")
      val msgDef = msg.asInstanceOf[MessageDef]
      JarImpl.DownloadJarFromDB(msgDef)
      logger.debug("Add the object " + key + " to the cache ")
      DaoImpl.AddObjectToCache(msgDef, MdMgr.GetMdMgr)
    } catch {
      case e: Exception => {
        logger.debug("Failed to load message into cache " + key + ":" + e.getMessage())
      }
    }
  }


}