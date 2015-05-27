package com.ligadata.MetadataAPI

import org.apache.log4j._

import com.ligadata.Utils._
import com.ligadata.fatafat.metadata.ObjType._
import com.ligadata.fatafat.metadata._
import com.ligadata.fatafat.metadata.MdMgr._
import com.ligadata.Serialize._
import org.json4s._

object TypeImpl {
  lazy val serializer = SerializerManager.GetSerializer("kryo")
  
  def AddType(typeText: String, format: String): String = {
    try {
      logger.debug("Parsing type object given as Json String..")
      val typ = JsonSerializer.parseType(typeText, "JSON")
      DaoImpl.SaveObject(typ, MdMgr.GetMdMgr)
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

  def DumpTypeDef(typeDef: ScalarTypeDef) {
    logger.debug("NameSpace => " + typeDef.nameSpace)
    logger.debug("Name => " + typeDef.name)
    logger.debug("Version => " + MdMgr.Pad0s2Version(typeDef.ver))
    logger.debug("Description => " + typeDef.description)
    logger.debug("Implementation class name => " + typeDef.implementationNm)
    logger.debug("Implementation jar name => " + typeDef.jarName)
  }

  def AddType(typeDef: BaseTypeDef): String = {
    val dispkey = typeDef.FullName + "." + MdMgr.Pad0s2Version(typeDef.Version)
    try {
      var key = typeDef.FullNameWithVer
      var value = JsonSerializer.SerializeObjectToJson(typeDef);
      logger.debug("key => " + key + ",value =>" + value);
      DaoImpl.SaveObject(typeDef, MdMgr.GetMdMgr)
       var apiResult = new ApiResult(ErrorCodeConstants.Success, "AddType", null, ErrorCodeConstants.Add_Type_Successful + ":" + dispkey)
      apiResult.toString()
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddType" , null, "Error :" + e.toString() + ErrorCodeConstants.Add_Type_Failed+ ":" + dispkey)
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
            DaoImpl.SaveObject(typ, MdMgr.GetMdMgr)
            logger.debug("Type object name => " + typ.FullName + "." + MdMgr.Pad0s2Version(typ.Version))
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
    val dispkey = typeNameSpace + "." + typeName + "." + MdMgr.Pad0s2Version(version)
    try {
      val typ = MdMgr.GetMdMgr.Type(typeNameSpace, typeName, version, true)
      typ match {
        case None =>
          None
          logger.debug("Type " + dispkey + " is not found in the cache ")
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveType", null, ErrorCodeConstants.Remove_Type_Not_Found + ":" + dispkey)
          apiResult.toString()
        case Some(ts) =>
          DaoImpl.DeleteObject(ts.asInstanceOf[BaseElemDef])
          var apiResult = new ApiResult(ErrorCodeConstants.Success, "RemoveType", null, ErrorCodeConstants.Remove_Type_Successful + ":" + dispkey)
          apiResult.toString()
      }
    } catch {
      case e: ObjectNolongerExistsException => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveType", null, "Error: " + e.toString() + ErrorCodeConstants.Remove_Type_Failed + ":" + dispkey)
        apiResult.toString()
      }
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveType", null, "Error: " + e.toString() + ErrorCodeConstants.Remove_Type_Failed + ":" + dispkey)
        apiResult.toString()
      }
    }
  }
  
  def UpdateType(typeJson: String, format: String): String = {
    implicit val jsonFormats: Formats = DefaultFormats
    val typeDef = JsonSerializer.parseType(typeJson, "JSON")
    val key = typeDef.nameSpace + "." + typeDef.name + "." + typeDef.Version
    val dispkey = typeDef.nameSpace + "." + typeDef.name + "." + MdMgr.Pad0s2Version(typeDef.Version)
    var apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateType" , null, ErrorCodeConstants.Update_Type_Internal_Error + ":" + dispkey)
    try {
      val fName = MdMgr.MkFullName(typeDef.nameSpace, typeDef.name)
      val latestType = MdMgr.GetMdMgr.Types(typeDef.nameSpace, typeDef.name, true, true)
      latestType match {
        case None =>
          None
          logger.debug("No types with the name " + fName + " are found ")
          apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateType", null, ErrorCodeConstants.Update_Type_Failed_Not_Found + ":" + dispkey)
        case Some(ts) =>
          val tsa = ts.toArray
          logger.debug("Found " + tsa.length + " types ")
          if (tsa.length > 1) {
            apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateType", null, ErrorCodeConstants.Update_Type_Failed_More_Than_One_Found + ":" + dispkey)
          } else {
            val latestVersion = tsa(0)
            if (latestVersion.ver > typeDef.ver) {
              RemoveType(latestVersion.nameSpace, latestVersion.name, latestVersion.ver)
              AddType(typeDef)
              apiResult = new ApiResult(ErrorCodeConstants.Success, "UpdateType", null, ErrorCodeConstants.Update_Type_Successful + ":" + key)
            } else {
              apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateType", null, ErrorCodeConstants.Update_Type_Failed_Higher_Version_Required + ":" + dispkey)
            }
          }
      }
      apiResult.toString()
    } catch {
      case e: MappingException => {
        logger.debug("Failed to parse the type, json => " + typeJson + ",Error => " + e.getMessage())
        apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateType", null, "Error :" + e.toString() + ErrorCodeConstants.Update_Type_Failed + ":" + dispkey)
        apiResult.toString()
      }
      case e: AlreadyExistsException => {
        logger.debug("Failed to update the type, json => " + typeJson + ",Error => " + e.getMessage())
        apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateType", null, "Error :" + e.toString() + ErrorCodeConstants.Update_Type_Failed + ":" + dispkey)
        apiResult.toString()
      }
      case e: Exception => {
        logger.debug("Failed to up the type, json => " + typeJson + ",Error => " + e.getMessage())
        apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateType", null, "Error :" + e.toString() + ErrorCodeConstants.Update_Type_Failed + ":" + dispkey)
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
    var key = nameSpace + "." + objectName + "." + version.toLong
    val dispkey = nameSpace + "." + objectName + "." + MdMgr.Pad0s2Version(version.toLong)
    try {
      val typeDefs = MdMgr.GetMdMgr.Types(nameSpace, objectName,false,false)
      typeDefs match {
        case None =>
          None
          logger.debug("No typeDefs found ")
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetTypeDef", null, ErrorCodeConstants.Get_Type_Def_Failed_Not_Available + ":" + dispkey)
          apiResult.toString()
        case Some(ts) =>
          val tsa = ts.toArray
          logger.debug("Found " + tsa.length + " types ")
          var apiResult = new ApiResult(ErrorCodeConstants.Success, "GetTypeDef", JsonSerializer.SerializeObjectListToJson("Types", tsa), ErrorCodeConstants.Get_Type_Def_Successful + ":" + dispkey)
          apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetTypeDef", null, "Error :" + e.toString() + ErrorCodeConstants.Get_Type_Def_Failed + ":" + dispkey)
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
          val tsa = ts.toArray.filter(t => { t.ver == version.toLong })
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
  
 
  def LoadAllTypesIntoCache {
    try {
      val typeKeys = MetadataAPIImpl.GetAllKeys("TypeDef")
      if (typeKeys.length == 0) {
        logger.debug("No types available in the Database")
        return
      }
      typeKeys.foreach(key => {
        val obj = DaoImpl.GetObject(key.toLowerCase, MetadataAPIImpl.getTypeStore)
        val typ = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
        if (typ != null) {
          DaoImpl.AddObjectToCache(typ, MdMgr.GetMdMgr)
        }
      })
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  } 
  
  def LoadTypeIntoCache(key: String) {
    try {
      logger.debug("Fetch the object " + key + " from database ")
      val obj = DaoImpl.GetObject(key.toLowerCase, MetadataAPIImpl.getTypeStore)
      logger.debug("Deserialize the object " + key)
      val typ = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
      if (typ != null) {
        logger.debug("Add the object " + key + " to the cache ")
        DaoImpl.AddObjectToCache(typ, MdMgr.GetMdMgr)
      }
    } catch {
      case e: Exception => {
        logger.warn("Unable to load the object " + key + " into cache ")
      }
    }
  }
  
  def IsTypeAlreadyExists(typeDef: BaseTypeDef): Boolean = {
    try {
      var key = typeDef.nameSpace + "." + typeDef.name + "." + typeDef.ver
      val dispkey = typeDef.nameSpace + "." + typeDef.name + "." + MdMgr.Pad0s2Version(typeDef.ver)
      val o = MdMgr.GetMdMgr.Type(typeDef.nameSpace,
        typeDef.name,
        typeDef.ver,
        false)
      o match {
        case None =>
          None
          logger.debug("Type not in the cache => " + key)
          return false;
        case Some(m) =>
          logger.debug("Type found => " + m.asInstanceOf[BaseTypeDef].FullName + "." + MdMgr.Pad0s2Version(m.asInstanceOf[BaseTypeDef].ver))
          return true
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw new UnexpectedMetadataAPIException(e.getMessage())
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
            typeList(i) = msa(i).FullName + "." + MdMgr.Pad0s2Version(msa(i).Version)
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

  
  def GetAdditionalTypesAdded(msgDef: BaseElemDef, mdMgr: MdMgr): Array[BaseElemDef] = {
    var types = new Array[BaseElemDef](0)
    logger.debug("The class name => " + msgDef.getClass().getName())
    try {
      val msgType = MetadataUtils.getObjectType(msgDef)
      msgType match {
        case "MessageDef" | "ContainerDef" => {
          // ArrayOf<TypeName>
          var typeName = "arrayof" + msgDef.name
          var typeDef = GetType(msgDef.nameSpace, typeName, msgDef.ver.toString, "JSON")
          if (typeDef != None) {
            types = types :+ typeDef.get
          }
          // ArrayBufferOf<TypeName>
          typeName = "arraybufferof" + msgDef.name
          typeDef = GetType(msgDef.nameSpace, typeName, msgDef.ver.toString, "JSON")
          if (typeDef != None) {
            types = types :+ typeDef.get
          }
          // SortedSetOf<TypeName>
          typeName = "sortedsetof" + msgDef.name
          typeDef = GetType(msgDef.nameSpace, typeName, msgDef.ver.toString, "JSON")
          if (typeDef != None) {
            types = types :+ typeDef.get
          }
          // ImmutableMapOfIntArrayOf<TypeName>
          typeName = "immutablemapofintarrayof" + msgDef.name
          typeDef = GetType(msgDef.nameSpace, typeName, msgDef.ver.toString, "JSON")
          if (typeDef != None) {
            types = types :+ typeDef.get
          }
          // ImmutableMapOfString<TypeName>
          typeName = "immutablemapofstringarrayof" + msgDef.name
          typeDef = GetType(msgDef.nameSpace, typeName, msgDef.ver.toString, "JSON")
          if (typeDef != None) {
            types = types :+ typeDef.get
          }
          // ArrayOfArrayOf<TypeName>
          typeName = "arrayofarrayof" + msgDef.name
          typeDef = GetType(msgDef.nameSpace, typeName, msgDef.ver.toString, "JSON")
          if (typeDef != None) {
            types = types :+ typeDef.get
          }
          // MapOfStringArrayOf<TypeName>
          typeName = "mapofstringarrayof" + msgDef.name
          typeDef = GetType(msgDef.nameSpace, typeName, msgDef.ver.toString, "JSON")
          if (typeDef != None) {
            types = types :+ typeDef.get
          }
          // MapOfIntArrayOf<TypeName>
          typeName = "mapofintarrayof" + msgDef.name
          typeDef = GetType(msgDef.nameSpace, typeName, msgDef.ver.toString, "JSON")
          if (typeDef != None) {
            types = types :+ typeDef.get
          }
          // SetOf<TypeName>
          typeName = "setof" + msgDef.name
          typeDef = GetType(msgDef.nameSpace, typeName, msgDef.ver.toString, "JSON")
          if (typeDef != None) {
            types = types :+ typeDef.get
          }
          // TreeSetOf<TypeName>
          typeName = "treesetof" + msgDef.name
          typeDef = GetType(msgDef.nameSpace, typeName, msgDef.ver.toString, "JSON")
          if (typeDef != None) {
            types = types :+ typeDef.get
          }
          logger.debug("Type objects to be removed = " + types.length)
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
  
  def getBaseType(typ: BaseTypeDef): BaseTypeDef = {
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

}