package com.ligadata.MetadataAPI

import java.util.Properties
import java.io._
import java.text.SimpleDateFormat
import java.util.Calendar
import java.text.ParseException
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
object Utils {

  lazy val sysNS = "System" // system name space
  lazy val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)
  lazy val serializer = SerializerManager.GetSerializer("kryo")

  private[this] val lock = new Object
  var zkc: CuratorFramework = null

  private def GetMetadataAPIConfig: Properties = {
    MetadataAPIImpl.metadataAPIConfig
  }

  def SetLoggerLevel(level: Level) {
    logger.setLevel(level);
  }

  def CloseZKSession: Unit = lock.synchronized {
    if (zkc != null) {
      logger.debug("Closing zookeeper session ..")
      try {
        zkc.close()
        logger.info("Closed zookeeper session ..")
        zkc = null
      } catch {
        case e: Exception => {
          logger.error("Unexpected Error while closing zookeeper session: " + e.getMessage())
        }
      }
    }
    else{
      logger.info("no zookeeper connection yet")
    }
  }

  /**
   * InitZooKeeper - Establish a connection to zookeeper
   */
  private def InitZooKeeper: Unit = {
    logger.debug("Connect to zookeeper..")
    if (zkc != null) {
      // Zookeeper is already connected
      return
    }

    val zkcConnectString = GetMetadataAPIConfig.getProperty("ZOOKEEPER_CONNECT_STRING")
    val znodePath = GetMetadataAPIConfig.getProperty("ZNODE_PATH") + "/metadataupdate"
    logger.debug("Connect To ZooKeeper using " + zkcConnectString)
    try {
      CreateClient.CreateNodeIfNotExists(zkcConnectString, znodePath)
      zkc = CreateClient.createSimple(zkcConnectString)
    } catch {
      case e: Exception => {
        throw new InternalErrorException("Failed to start a zookeeper session with(" + zkcConnectString + "): " + e.getMessage())
      }
    }
  }

  def getObjectType(obj: BaseElemDef): String = {
    val className = obj.getClass().getName();
    className.split("\\.").last
  }


  private def ZooKeeperMessage(objList: Array[BaseElemDef], operations: Array[String]): Array[Byte] = {
    try {
      val notification = JsonSerializer.zkSerializeObjectListToJson("Notifications", objList, operations)
      notification.getBytes
    } catch {
      case e: Exception => {
        throw new InternalErrorException("Failed to generate a zookeeper message from the objList " + e.getMessage())
      }
    }
  }

  def NotifyEngine(objList: Array[BaseElemDef], operations: Array[String]) {
    try {
      val notifyEngine = GetMetadataAPIConfig.getProperty("NOTIFY_ENGINE")
      if (notifyEngine != "YES") {
        logger.warn("Not Notifying the engine about this operation because The property NOTIFY_ENGINE is not set to YES")
        DAOUtils.PutTranId(objList(0).tranId)
        return
      }
      val data = ZooKeeperMessage(objList, operations)
      InitZooKeeper
      val znodePath = GetMetadataAPIConfig.getProperty("ZNODE_PATH") + "/metadataupdate"
      logger.debug("Set the data on the zookeeper node " + znodePath)
      zkc.setData().forPath(znodePath, data)
      DAOUtils.PutTranId(objList(0).tranId)
    } catch {
      case e: Exception => {
        throw new InternalErrorException("Failed to notify a zookeeper message from the objectList " + e.getMessage())
      }
    }
  }

  /**
   * loadJar - load the specified jar into the classLoader
   */
  def loadJar (classLoader : MetadataLoader, implJarName: String): Unit = {
    // Add the Jarfile to the class loader
    val jarPaths = GetMetadataAPIConfig.getProperty("JAR_PATHS").split(",").toSet
    val jarName = JarPathsUtils.GetValidJarFile(jarPaths, implJarName)
    val fl = new File(jarName)
    if (fl.exists) {
      try {
        classLoader.loader.addURL(fl.toURI().toURL())
        logger.debug("Jar " + implJarName.trim + " added to class path.")
        classLoader.loadedJars += fl.getPath()     
      } catch {
          case e: Exception => {
            logger.error("Failed to add "+implJarName + " due to internal exception " + e.printStackTrace)
            return
          }
        }
    } else {
      logger.error("Unable to locate Jar '"+implJarName+"'")
      return 
    }   
  }
  
  /**
   * getSSLCertificatePath
   */
  def getSSLCertificatePath: String = {
    val certPath = GetMetadataAPIConfig.getProperty("SSL_CERTIFICATE")  
    if (certPath != null) return certPath
    ""
  }

  def GetJarAsArrayOfBytes(jarName: String): Array[Byte] = {
    try {
      val iFile = new File(jarName)
      if (!iFile.exists) {
        throw new FileNotFoundException("Jar file (" + jarName + ") is not found: ")
      }

      val bis = new BufferedInputStream(new FileInputStream(iFile));
      val baos = new ByteArrayOutputStream();
      var readBuf = new Array[Byte](1024) // buffer size

      // read until a single byte is available
      while (bis.available() > 0) {
        val c = bis.read();
        baos.write(c)
      }
      bis.close();
      baos.toByteArray()
    } catch {
      case e: IOException => {
        e.printStackTrace()
        throw new FileNotFoundException("Failed to Convert the Jar (" + jarName + ") to array of bytes: " + e.getMessage())
      }
      case e: Exception => {
        e.printStackTrace()
        throw new InternalErrorException("Failed to Convert the Jar (" + jarName + ") to array of bytes: " + e.getMessage())
      }
    }
  }

  def PutArrayOfBytesToJar(ba: Array[Byte], jarName: String) = {
    logger.debug("Downloading the jar contents into the file " + jarName)
    try {
      val iFile = new File(jarName)
      val bos = new BufferedOutputStream(new FileOutputStream(iFile));
      bos.write(ba)
      bos.close();
    } catch {
      case e: Exception => {
        logger.error("Failed to dump array of bytes to the Jar file (" + jarName + "):  " + e.getMessage())
      }
    }
  }

  private def GetValidJarFile(jarPaths: collection.immutable.Set[String], jarName: String): String = {
    if (jarPaths == null) return jarName // Returning base jarName if no jarpaths found
    jarPaths.foreach(jPath => {
      val fl = new File(jPath + "/" + jarName)
      if (fl.exists) {
        return fl.getPath
      }
    })
    return jarName // Returning base jarName if not found in jar paths
  }


  def IsDownloadNeeded(jar: String, obj: BaseElemDef): Boolean = {
    try {
      if (jar == null) {
        logger.debug("The object " + obj.FullNameWithVer + " has no jar associated with it. Nothing to download..")
        false
      }
      val jarPaths = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_PATHS").split(",").toSet
      val jarName = JarPathsUtils.GetValidJarFile(jarPaths, jar)
      val f = new File(jarName)
      if (f.exists()) {
        val key = jar
        val mObj = DAOUtils.GetObject(key, MetadataAPIImpl.jarStore)
        val ba = mObj.Value.toArray[Byte]
        val fs = f.length()
        if (fs != ba.length) {
          logger.debug("A jar file already exists, but it's size (" + fs + ") doesn't match with the size of the Jar (" +
            jar + "," + ba.length + ") of the object(" + obj.FullNameWithVer + ")")
          true
        } else {
          logger.debug("A jar file already exists, and it's size (" + fs + ")  matches with the size of the existing Jar (" +
            jar + "," + ba.length + ") of the object(" + obj.FullNameWithVer + "), no need to download.")
          false
        }
      } else {
        logger.debug("The jar " + jarName + " is not available, download from database. ")
        true
      }
    } catch {
      case e: Exception => {
        throw new InternalErrorException("Failed to verify whether a download is required for the jar " + jar + " of the object(" + obj.FullNameWithVer + "): " + e.getMessage())
      }
    }
  }

  def GetDependantJars(obj: BaseElemDef): Array[String] = {
    try {
      var allJars = new Array[String](0)
      allJars = allJars :+ obj.JarName
      if (obj.DependencyJarNames != null) {
        obj.DependencyJarNames.foreach(j => {
          if (j.endsWith(".jar")) {
            allJars = allJars :+ j
          }
        })
      }
      allJars
    } catch {
      case e: Exception => {
        throw new InternalErrorException("Failed to get dependant jars for the given object (" + obj.FullNameWithVer + "): " + e.getMessage())
      }
    }
  }


  def UpdateObjectInCache(obj: BaseElemDef, operation: String, mdMgr: MdMgr): BaseElemDef = {
    var updatedObject: BaseElemDef = null
    try {
      obj match {
        case o: FunctionDef => {
          updatedObject = mdMgr.ModifyFunction(o.nameSpace, o.name, o.ver, operation)
        }
        case o: ModelDef => {
          updatedObject = mdMgr.ModifyModel(o.nameSpace, o.name, o.ver, operation)
        }
        case o: MessageDef => {
          updatedObject = mdMgr.ModifyMessage(o.nameSpace, o.name, o.ver, operation)
        }
        case o: ContainerDef => {
          updatedObject = mdMgr.ModifyContainer(o.nameSpace, o.name, o.ver, operation)
        }
        case o: AttributeDef => {
          updatedObject = mdMgr.ModifyAttribute(o.nameSpace, o.name, o.ver, operation)
        }
        case o: ScalarTypeDef => {
          updatedObject = mdMgr.ModifyType(o.nameSpace, o.name, o.ver, operation)
        }
        case o: ArrayTypeDef => {
          updatedObject = mdMgr.ModifyType(o.nameSpace, o.name, o.ver, operation)
        }
        case o: ArrayBufTypeDef => {
          updatedObject = mdMgr.ModifyType(o.nameSpace, o.name, o.ver, operation)
        }
        case o: ListTypeDef => {
          updatedObject = mdMgr.ModifyType(o.nameSpace, o.name, o.ver, operation)
        }
        case o: QueueTypeDef => {
          updatedObject = mdMgr.ModifyType(o.nameSpace, o.name, o.ver, operation)
        }
        case o: SetTypeDef => {
          updatedObject = mdMgr.ModifyType(o.nameSpace, o.name, o.ver, operation)
        }
        case o: TreeSetTypeDef => {
          updatedObject = mdMgr.ModifyType(o.nameSpace, o.name, o.ver, operation)
        }
        case o: SortedSetTypeDef => {
          updatedObject = mdMgr.ModifyType(o.nameSpace, o.name, o.ver, operation)
        }
        case o: MapTypeDef => {
          updatedObject = mdMgr.ModifyType(o.nameSpace, o.name, o.ver, operation)
        }
        case o: ImmutableMapTypeDef => {
          updatedObject = mdMgr.ModifyType(o.nameSpace, o.name, o.ver, operation)
        }
        case o: HashMapTypeDef => {
          updatedObject = mdMgr.ModifyType(o.nameSpace, o.name, o.ver, operation)
        }
        case o: TupleTypeDef => {
          updatedObject = mdMgr.ModifyType(o.nameSpace, o.name, o.ver, operation)
        }
        case o: ContainerTypeDef => {
          updatedObject = mdMgr.ModifyType(o.nameSpace, o.name, o.ver, operation)
        }
        case _ => {
          throw new InternalErrorException("UpdateObjectInCache is not implemented for objects of type " + obj.getClass.getName)
        }
      }
      updatedObject
    } catch {
      case e: ObjectNolongerExistsException => {
        throw new ObjectNolongerExistsException("The object " + obj.FullNameWithVer + " nolonger exists in metadata : It may have been removed already")
      }
      case e: Exception => {
        throw new Exception("Unexpected error in UpdateObjectInCache: " + e.getMessage())
      }
    }
  }

  def AddObjectToCache(o: Object, mdMgr: MdMgr) {
    // If the object's Delete flag is set, this is a noop.
    val obj = o.asInstanceOf[BaseElemDef]
    if (obj.IsDeleted)
      return
    try {
      val key = obj.FullNameWithVer.toLowerCase
      obj match {
        case o: ModelDef => {
          logger.debug("Adding the model to the cache: name of the object =>  " + key)
          mdMgr.AddModelDef(o)
        }
        case o: MessageDef => {
          logger.debug("Adding the message to the cache: name of the object =>  " + key)
          mdMgr.AddMsg(o)
        }
        case o: ContainerDef => {
          logger.debug("Adding the container to the cache: name of the object =>  " + key)
          mdMgr.AddContainer(o)
        }
        case o: FunctionDef => {
          val funcKey = o.typeString.toLowerCase
          logger.debug("Adding the function to the cache: name of the object =>  " + funcKey)
          mdMgr.AddFunc(o)
        }
        case o: AttributeDef => {
          logger.debug("Adding the attribute to the cache: name of the object =>  " + key)
          mdMgr.AddAttribute(o)
        }
        case o: ScalarTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + key)
          mdMgr.AddScalar(o)
        }
        case o: ArrayTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + key)
          mdMgr.AddArray(o)
        }
        case o: ArrayBufTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + key)
          mdMgr.AddArrayBuffer(o)
        }
        case o: ListTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + key)
          mdMgr.AddList(o)
        }
        case o: QueueTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + key)
          mdMgr.AddQueue(o)
        }
        case o: SetTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + key)
          mdMgr.AddSet(o)
        }
        case o: TreeSetTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + key)
          mdMgr.AddTreeSet(o)
        }
        case o: SortedSetTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + key)
          mdMgr.AddSortedSet(o)
        }
        case o: MapTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + key)
          mdMgr.AddMap(o)
        }
        case o: ImmutableMapTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + key)
          mdMgr.AddImmutableMap(o)
        }
        case o: HashMapTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + key)
          mdMgr.AddHashMap(o)
        }
        case o: TupleTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + key)
          mdMgr.AddTupleType(o)
        }
        case o: ContainerTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + key)
          mdMgr.AddContainerType(o)
        }
        case _ => {
          logger.error("SaveObject is not implemented for objects of type " + obj.getClass.getName)
        }
      }
    } catch {
      case e: AlreadyExistsException => {
        logger.warn("Failed to Save the object(" + obj.FullNameWithVer + "): " + e.getMessage())
      }
      case e: Exception => {
        logger.warn("Failed to Cache the object(" + obj.FullNameWithVer + "): " + e.getMessage())
      }
    }
  }


  @throws(classOf[Json4sParsingException])
  @throws(classOf[ApiResultParsingException])
  def getApiResult(apiResultJson: String): String = {
     // parse using Json4s
    try {
      implicit val jsonFormats: Formats = DefaultFormats
      val json = parse(apiResultJson)
      //logger.debug("Parsed the json : " + apiResultJson)
      val apiResultInfo = json.extract[APIResultJsonProxy]
    /*  if( apiResultInfo.APIResults.statusCode != ErrorCodeConstants.Success )
      {
        (apiResultInfo.APIResults.statusCode, apiResultInfo.APIResults.functionName + apiResultInfo.APIResults.resultData + apiResultInfo.APIResults.description)
      }
      else
      {
        (apiResultInfo.APIResults.statusCode, "Status Description :" + apiResultInfo.APIResults.functionName + " ResultData :" + apiResultInfo.APIResults.resultData + " Error Description :" + apiResultInfo.APIResults.description)
      }*/
      (apiResultInfo.APIResults.statusCode + apiResultInfo.APIResults.functionName + apiResultInfo.APIResults.resultData + apiResultInfo.APIResults.description)
    } catch {
      case e: MappingException => {
        e.printStackTrace()
        throw Json4sParsingException(e.getMessage())
      }
      case e: Exception => {
        e.printStackTrace()
        throw new ApiResultParsingException(e.getMessage())
      }
    }
  }


  // Upload Jars into system. Dependency jars may need to upload first. Once we upload the jar, if we retry to upload it will throw an exception.
  def UploadJar(jarPath: String): String = {
    try {
      val iFile = new File(jarPath)
      if (!iFile.exists) {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "UploadJar", null, ErrorCodeConstants.File_Not_Found + ":" + jarPath)
        apiResult.toString()
      } else {
        val jarName = iFile.getName()
        val jarObject = MdMgr.GetMdMgr.MakeJarDef(MetadataAPIImpl.sysNS, jarName, "100")
        var objectsAdded = new Array[BaseElemDef](0)
        objectsAdded = objectsAdded :+ jarObject
        DAOUtils.UploadJarToDB(jarPath)
        val operations = for (op <- objectsAdded) yield "Add"
        NotifyEngine(objectsAdded, operations)
        var apiResult = new ApiResult(ErrorCodeConstants.Success, "UploadJar", null, ErrorCodeConstants.Upload_Jar_Successful + ":" + jarPath)
        apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "UploadJar", null, "Error :" + e.toString() + ErrorCodeConstants.Upload_Jar_Failed + ":" + jarPath)
        apiResult.toString()
      }
    }
  }


  def GetAdditionalTypesAdded(msgDef: BaseElemDef, mdMgr: MdMgr): Array[BaseElemDef] = {
    var types = new Array[BaseElemDef](0)
    logger.debug("The class name => " + msgDef.getClass().getName())
    try {
      val msgType = getObjectType(msgDef)
      msgType match {
        case "MessageDef" | "ContainerDef" => {
          // ArrayOf<TypeName>
          var typeName = "arrayof" + msgDef.name
          var typeDef = TypeUtils.GetType(msgDef.nameSpace, typeName, msgDef.ver.toString, "JSON")
          if (typeDef != None) {
            types = types :+ typeDef.get
          }
          // ArrayBufferOf<TypeName>
          typeName = "arraybufferof" + msgDef.name
          typeDef = TypeUtils.GetType(msgDef.nameSpace, typeName, msgDef.ver.toString, "JSON")
          if (typeDef != None) {
            types = types :+ typeDef.get
          }
          // SortedSetOf<TypeName>
          typeName = "sortedsetof" + msgDef.name
          typeDef = TypeUtils.GetType(msgDef.nameSpace, typeName, msgDef.ver.toString, "JSON")
          if (typeDef != None) {
            types = types :+ typeDef.get
          }
          // ImmutableMapOfIntArrayOf<TypeName>
          typeName = "immutablemapofintarrayof" + msgDef.name
          typeDef = TypeUtils.GetType(msgDef.nameSpace, typeName, msgDef.ver.toString, "JSON")
          if (typeDef != None) {
            types = types :+ typeDef.get
          }
          // ImmutableMapOfString<TypeName>
          typeName = "immutablemapofstringarrayof" + msgDef.name
          typeDef = TypeUtils.GetType(msgDef.nameSpace, typeName, msgDef.ver.toString, "JSON")
          if (typeDef != None) {
            types = types :+ typeDef.get
          }
          // ArrayOfArrayOf<TypeName>
          typeName = "arrayofarrayof" + msgDef.name
          typeDef = TypeUtils.GetType(msgDef.nameSpace, typeName, msgDef.ver.toString, "JSON")
          if (typeDef != None) {
            types = types :+ typeDef.get
          }
          // MapOfStringArrayOf<TypeName>
          typeName = "mapofstringarrayof" + msgDef.name
          typeDef = TypeUtils.GetType(msgDef.nameSpace, typeName, msgDef.ver.toString, "JSON")
          if (typeDef != None) {
            types = types :+ typeDef.get
          }
          // MapOfIntArrayOf<TypeName>
          typeName = "mapofintarrayof" + msgDef.name
          typeDef = TypeUtils.GetType(msgDef.nameSpace, typeName, msgDef.ver.toString, "JSON")
          if (typeDef != None) {
            types = types :+ typeDef.get
          }
          // SetOf<TypeName>
          typeName = "setof" + msgDef.name
          typeDef = TypeUtils.GetType(msgDef.nameSpace, typeName, msgDef.ver.toString, "JSON")
          if (typeDef != None) {
            types = types :+ typeDef.get
          }
          // TreeSetOf<TypeName>
          typeName = "treesetof" + msgDef.name
          typeDef = TypeUtils.GetType(msgDef.nameSpace, typeName, msgDef.ver.toString, "JSON")
          if (typeDef != None) {
            types = types :+ typeDef.get
          }
          logger.debug("Type objects to be removed = " + types.length)
          types
        }
        case _ => {
          throw new InternalErrorException("Unknown class in GetAdditionalTypesAdded")
        }
      }
    } catch {
      case e: Exception => {
        throw new Exception(e.getMessage())
      }
    }
  }


  def IsValidVersion(oldObj: BaseElemDef, newObj: BaseElemDef): Boolean = {
    if (newObj.ver > oldObj.ver) {
      return true
    } else {
      return false
    }
  }

  def GetAllKeys(objectType: String): Array[String] = {
    try {
      var keys = scala.collection.mutable.Set[String]()
      MetadataAPIImpl.metadataStore.getAllKeys({ (key: Key) => {
          val strKey = DAOUtils.KeyAsStr(key)
          val i = strKey.indexOf(".")
          val objType = strKey.substring(0, i)
          val objectName = strKey.substring(i + 1)
          objectType match {
            case "TypeDef" => {
              if (TypeUtils.IsTypeObject(objType)) {
                keys.add(objectName)
              }
            }
            case "FunctionDef" => {
              if (objType == "functiondef") {
                keys.add(objectName)
              }
            }
            case "MessageDef" => {
              if (objType == "messagedef") {
                keys.add(objectName)
              }
            }
            case "ContainerDef" => {
              if (objType == "containerdef") {
                keys.add(objectName)
              }
            }
            case "Concept" => {
              if (objType == "attributedef") {
                keys.add(objectName)
              }
            }
            case "ModelDef" => {
              if (objType == "modeldef") {
                keys.add(objectName)
              }
            }
            case _ => {
              logger.error("Unknown object type " + objectType + " in GetAllKeys function")
              throw InternalErrorException("Unknown object type " + objectType + " in GetAllKeys function")
            }
          }
        }
      })
      keys.toArray
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw InternalErrorException("Failed to get keys from persistent store")
      }
    }
  }


  def UpdateMdMgr(zkTransaction: ZooKeeperTransaction) = {
    var key: String = null
  //  SetLoggerLevel(Level.TRACE)
    try {
      zkTransaction.Notifications.foreach(zkMessage => {
        key = (zkMessage.ObjectType + "." + zkMessage.NameSpace + "." + zkMessage.Name + "." + zkMessage.Version).toLowerCase
	logger.debug("Processing ZooKeeperNotification, the object => " +  key + ",objectType => " + zkMessage.ObjectType + ",Operation => " + zkMessage.Operation)
        zkMessage.ObjectType match {
          case "ModelDef" => {
            zkMessage.Operation match {
              case "Add" => {
                ModelUtils.LoadModelIntoCache(key)
              }
              case "Remove" | "Activate" | "Deactivate" => {
                try {
                  MdMgr.GetMdMgr.ModifyModel(zkMessage.NameSpace, zkMessage.Name, zkMessage.Version.toInt, zkMessage.Operation)
                } catch {
                  case e: ObjectNolongerExistsException => {
                    logger.error("The object " + key + " nolonger exists in metadata : It may have been removed already")
                  }
                }
              }
              case _ => {
                logger.error("Unknown Operation " + zkMessage.Operation + " in zookeeper notification, notification is not processed ..")
              }
            }
          }
          case "MessageDef" => {
            zkMessage.Operation match {
              case "Add" => {
                MessageUtils.LoadMessageIntoCache(key)
              }
              case "Remove" => {
                try {
                  MessageUtils.RemoveMessage(zkMessage.NameSpace, zkMessage.Name, zkMessage.Version.toInt,false)
                } catch {
                  case e: ObjectNolongerExistsException => {
                    logger.error("The object " + key + " nolonger exists in metadata : It may have been removed already")
                  }
                }
              }
              case "Activate" | "Deactivate" => {
                try {
                  MdMgr.GetMdMgr.ModifyMessage(zkMessage.NameSpace, zkMessage.Name, zkMessage.Version.toInt, zkMessage.Operation)
                } catch {
                  case e: ObjectNolongerExistsException => {
                    logger.error("The object " + key + " nolonger exists in metadata : It may have been removed already")
                  }
                }
              }
              case _ => {
                logger.error("Unknown Operation " + zkMessage.Operation + " in zookeeper notification, notification is not processed ..")
              }
            }
          }
          case "ContainerDef" => {
            zkMessage.Operation match {
              case "Add" => {
                ContainerUtils.LoadContainerIntoCache(key)
              }
              case "Remove" => {
                try {
                  ContainerUtils.RemoveContainer(zkMessage.NameSpace, zkMessage.Name, zkMessage.Version.toInt,false)
                } catch {
                  case e: ObjectNolongerExistsException => {
                    logger.error("The object " + key + " nolonger exists in metadata : It may have been removed already")
                  }
                }
              }
              case "Activate" | "Deactivate" => {
                try {
                  MdMgr.GetMdMgr.ModifyContainer(zkMessage.NameSpace, zkMessage.Name, zkMessage.Version.toInt, zkMessage.Operation)
                } catch {
                  case e: ObjectNolongerExistsException => {
                    logger.error("The object " + key + " nolonger exists in metadata : It may have been removed already")
                  }
                }
              }
              case _ => {
                logger.error("Unknown Operation " + zkMessage.Operation + " in zookeeper notification, notification is not processed ..")
              }
            }
          }
          case "FunctionDef" => {
            zkMessage.Operation match {
              case "Add" => {
                FunctionUtils.LoadFunctionIntoCache(key)
              }
              case "Remove" | "Activate" | "Deactivate" => {
                try {
                  MdMgr.GetMdMgr.ModifyFunction(zkMessage.NameSpace, zkMessage.Name, zkMessage.Version.toInt, zkMessage.Operation)
                } catch {
                  case e: ObjectNolongerExistsException => {
                    logger.error("The object " + key + " nolonger exists in metadata : It may have been removed already")
                  }
                }
              }
              case _ => {
                logger.error("Unknown Operation " + zkMessage.Operation + " in zookeeper notification, notification is not processed ..")
              }
            }
          }
          case "AttributeDef" => {
            zkMessage.Operation match {
              case "Add" => {
                ConceptUtils.LoadAttributeIntoCache(key)
              }
              case "Remove" | "Activate" | "Deactivate" => {
                try {
                  MdMgr.GetMdMgr.ModifyAttribute(zkMessage.NameSpace, zkMessage.Name, zkMessage.Version.toInt, zkMessage.Operation)
                } catch {
                  case e: ObjectNolongerExistsException => {
                    logger.error("The object " + key + " nolonger exists in metadata : It may have been removed already")
                  }
                }
              }
              case _ => {
                logger.error("Unknown Operation " + zkMessage.Operation + " in zookeeper notification, notification is not processed ..")
              }
            }
          }
          case "JarDef" => {
            zkMessage.Operation match {
              case "Add" => {
                DAOUtils.DownloadJarFromDB(MdMgr.GetMdMgr.MakeJarDef(zkMessage.NameSpace, zkMessage.Name, zkMessage.Version))
              }
              case _ => {
                logger.error("Unknown Operation " + zkMessage.Operation + " in zookeeper notification, notification is not processed ..")
              }
            }
          }
          case "ScalarTypeDef" | "ArrayTypeDef" | "ArrayBufTypeDef" | "ListTypeDef" | "SetTypeDef" | "TreeSetTypeDef" | "QueueTypeDef" | "MapTypeDef" | "ImmutableMapTypeDef" | "HashMapTypeDef" | "TupleTypeDef" | "StructTypeDef" | "SortedSetTypeDef" => {
            zkMessage.Operation match {
              case "Add" => {
                TypeUtils.LoadTypeIntoCache(key)
              }
              case "Remove" | "Activate" | "Deactivate" => {
                try {
                  logger.debug("Remove the type " + key + " from cache ")
                  MdMgr.GetMdMgr.ModifyType(zkMessage.NameSpace, zkMessage.Name, zkMessage.Version.toInt, zkMessage.Operation)
                } catch {
                  case e: ObjectNolongerExistsException => {
                    logger.error("The object " + key + " nolonger exists in metadata : It may have been removed already")
                  }
                }
              }
              case _ => {
                logger.error("Unknown Operation " + zkMessage.Operation + " in zookeeper notification, notification is not processed ..")
              }
            }
          }
          case _ => {
            logger.error("Unknown objectType " + zkMessage.ObjectType + " in zookeeper notification, notification is not processed ..")
          }
        }
      })
    } catch {
      case e: AlreadyExistsException => {
        logger.warn("Failed to load the object(" + key + ") into cache: " + e.getMessage())
      }
      case e: Exception => {
        logger.warn("Failed to load the object(" + key + ") into cache: " + e.getMessage())
      }
    }
  }

  /**
   * getCurrentTime - Return string representation of the current Date/Time
   */
  def getCurrentTime: String = {
    new Date().getTime().toString()
  }

  /**
   * parseDateStr
   */
  def parseDateStr(dateStr: String) : Date = {
    try{
      val format = new java.text.SimpleDateFormat("yyyyMMddHHmmss")
      val d = format.parse(dateStr)
      d
    }catch {
      case e:ParseException => {
	val format = new java.text.SimpleDateFormat("yyyyMMdd")
	val d = format.parse(dateStr)
	d
      }
    }
  }

  def getLeaderHost(leaderNode: String) : String = {
    val nodes = MdMgr.GetMdMgr.Nodes.values.toArray
    if ( nodes.length == 0 ){
      logger.trace("No Nodes found ")
      var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetLeaderHost", leaderNode, ErrorCodeConstants.Get_Leader_Host_Failed_Not_Available)
      apiResult.toString()
    }
    else{
      val nhosts = nodes.filter(n => n.nodeId == leaderNode)
      if ( nhosts.length == 0 ){
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetLeaderHost", leaderNode, ErrorCodeConstants.Get_Leader_Host_Failed_Not_Available)
        apiResult.toString()
      }
      else{
	val nhost = nhosts(0)
	logger.trace("node addr => " + nhost.NodeAddr)
	nhost.NodeAddr
      }
    }
  }

  def dumpMetadataAPIConfig {
    val e = GetMetadataAPIConfig.propertyNames()
    while (e.hasMoreElements()) {
      val key = e.nextElement().asInstanceOf[String]
      val value = GetMetadataAPIConfig.getProperty(key)
      logger.debug("Key : " + key + ", Value : " + value)
    }
  }
  
}
