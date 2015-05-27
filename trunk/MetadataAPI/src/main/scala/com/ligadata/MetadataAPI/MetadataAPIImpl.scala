package com.ligadata.MetadataAPI

import java.util.Properties
import java.io._
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date
import java.text.ParseException
import scala.Enumeration
import scala.io._

import scala.collection.mutable._
import scala.reflect.runtime.{ universe => ru }
import java.net.URL
import java.net.URLClassLoader

import com.ligadata.fatafat.metadata.ObjType._
import com.ligadata.fatafat.metadata._
import com.ligadata.fatafat.metadata.MdMgr._

import com.ligadata.fatafat.metadataload.MetadataLoad

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

import com.ligadata.Serialize._
import com.ligadata.Utils._
import util.control.Breaks._

import java.util.Date

case class JsonException(message: String) extends Exception(message)

case class TypeDef(MetadataType: String, NameSpace: String, Name: String, TypeTypeName: String, TypeNameSpace: String, TypeName: String, PhysicalName: String, var Version: String, JarName: String, DependencyJars: List[String], Implementation: String, Fixed: Option[Boolean], NumberOfDimensions: Option[Int], KeyTypeNameSpace: Option[String], KeyTypeName: Option[String], ValueTypeNameSpace: Option[String], ValueTypeName: Option[String], TupleDefinitions: Option[List[TypeDef]])
case class TypeDefList(Types: List[TypeDef])

case class Argument(ArgName: String, ArgTypeNameSpace: String, ArgTypeName: String)
case class Function(NameSpace: String, Name: String, PhysicalName: String, ReturnTypeNameSpace: String, ReturnTypeName: String, Arguments: List[Argument], Version: String, JarName: String, DependantJars: List[String])
case class FunctionList(Functions: List[Function])

//case class Concept(NameSpace: String,Name: String, TypeNameSpace: String, TypeName: String,Version: String,Description: String, Author: String, ActiveDate: String)
case class Concept(NameSpace: String, Name: String, TypeNameSpace: String, TypeName: String, Version: String)
case class ConceptList(Concepts: List[Concept])

// case class Attr(NameSpace: String, Name: String, Version: Long, Type: TypeDef)

// case class MessageStruct(NameSpace: String, Name: String, FullName: String, Version: Long, JarName: String, PhysicalName: String, DependencyJars: List[String], Attributes: List[Attr])
case class MessageDefinition(Message: MessageStruct)
case class ContainerDefinition(Container: MessageStruct)

case class ModelInfo(NameSpace: String, Name: String, Version: String, ModelType: String, JarName: String, PhysicalName: String, DependencyJars: List[String], InputAttributes: List[Attr], OutputAttributes: List[Attr])
case class ModelDefinition(Model: ModelInfo)

case class ParameterMap(RootDir: String, GitRootDir: String, MetadataStoreType: String, MetadataSchemaName: Option[String], MetadataLocation: String, JarTargetDir: String, ScalaHome: String, JavaHome: String, ManifestPath: String, ClassPath: String, NotifyEngine: String, ZnodePath: String, ZooKeeperConnectString: String, MODEL_FILES_DIR: Option[String], TYPE_FILES_DIR: Option[String], FUNCTION_FILES_DIR: Option[String], CONCEPT_FILES_DIR: Option[String], MESSAGE_FILES_DIR: Option[String], CONTAINER_FILES_DIR: Option[String], COMPILER_WORK_DIR: Option[String], MODEL_EXEC_FLAG: Option[String])

case class ZooKeeperInfo(ZooKeeperNodeBasePath: String, ZooKeeperConnectString: String, ZooKeeperSessionTimeoutMs: Option[String], ZooKeeperConnectionTimeoutMs: Option[String])

case class MetadataAPIConfig(APIConfigParameters: ParameterMap)

case class APIResultInfo(statusCode: Int, functionName: String, resultData: String, description: String)
case class APIResultJsonProxy(APIResults: APIResultInfo)

case class UnsupportedObjectException(e: String) extends Exception(e)
case class Json4sParsingException(e: String) extends Exception(e)
case class FunctionListParsingException(e: String) extends Exception(e)
case class FunctionParsingException(e: String) extends Exception(e)
case class TypeDefListParsingException(e: String) extends Exception(e)
case class TypeParsingException(e: String) extends Exception(e)
case class TypeDefProcessingException(e: String) extends Exception(e)
case class ConceptListParsingException(e: String) extends Exception(e)
case class ConceptParsingException(e: String) extends Exception(e)
case class MessageDefParsingException(e: String) extends Exception(e)
case class ContainerDefParsingException(e: String) extends Exception(e)
case class ModelDefParsingException(e: String) extends Exception(e)
case class ApiResultParsingException(e: String) extends Exception(e)
case class UnexpectedMetadataAPIException(e: String) extends Exception(e)
case class ObjectNotFoundException(e: String) extends Exception(e)
case class CreateStoreFailedException(e: String) extends Exception(e)
case class UpdateStoreFailedException(e: String) extends Exception(e)

case class LoadAPIConfigException(e: String) extends Exception(e)
case class MissingPropertyException(e: String) extends Exception(e)
case class InvalidPropertyException(e: String) extends Exception(e)
case class KryoSerializationException(e: String) extends Exception(e)
case class InternalErrorException(e: String) extends Exception(e)
case class TranIdNotFoundException(e: String) extends Exception(e)

/**
 *  MetadataClassLoader - contains the classes that need to be dynamically resolved the the
 *  reflective calls
 */
class MetadataClassLoader(urls: Array[URL], parent: ClassLoader) extends URLClassLoader(urls, parent) {
  override def addURL(url: URL) {
    super.addURL(url)
  }
}

/**
 * MetadataLoaderInfo
 */
class MetadataLoader {
  // class loader
  val loader: MetadataClassLoader = new MetadataClassLoader(ClassLoader.getSystemClassLoader().asInstanceOf[URLClassLoader].getURLs(), getClass().getClassLoader())
  // Loaded jars
  val loadedJars: TreeSet[String] = new TreeSet[String]
}

// The implementation class
object MetadataAPIImpl extends MetadataAPI {

  lazy val sysNS = "System" // system name space
  lazy val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)
  lazy val serializer = SerializerManager.GetSerializer("kryo")
  lazy val metadataAPIConfig = new Properties()
  val configFile = System.getenv("HOME") + "/MetadataAPIConfig.json"
  var propertiesAlreadyLoaded = false
  var isInitilized: Boolean = false
  
  // For future debugging  purposes, we want to know which properties were not set - so create a set
  // of values that can be set via our config files
  var pList: Set[String] = Set("ZK_SESSION_TIMEOUT_MS","ZK_CONNECTION_TIMEOUT_MS","DATABASE_SCHEMA","DATABASE","DATABASE_LOCATION","DATABASE_HOST","API_LEADER_SELECTION_ZK_NODE",
                               "JAR_PATHS","JAR_TARGET_DIR","ROOT_DIR","GIT_ROOT","SCALA_HOME","JAVA_HOME","MANIFEST_PATH","CLASSPATH","NOTIFY_ENGINE","SERVICE_HOST",
                               "ZNODE_PATH","ZOOKEEPER_CONNECT_STRING","COMPILER_WORK_DIR","SERVICE_PORT","MODEL_FILES_DIR","TYPE_FILES_DIR","FUNCTION_FILES_DIR",
                               "CONCEPT_FILES_DIR","MESSAGE_FILES_DIR","CONTAINER_FILES_DIR","CONFIG_FILES_DIR","MODEL_EXEC_LOG","NODE_ID","SSL_CERTIFICATE","DO_AUTH","SECURITY_IMPL_CLASS",
                               "SECURITY_IMPL_JAR", "AUDIT_IMPL_CLASS","AUDIT_IMPL_JAR", "DO_AUDIT")
  var isCassandra = false
  private[this] val lock = new Object
  var startup = false

  private var tableStoreMap: Map[String, DataStore] = Map()
  def setTableStoreMap(map:  Map[String, DataStore]): Unit = {tableStoreMap = map}
  def getTableStoreMap: Map[String, DataStore]  = {tableStoreMap}
  
  def GetMetadataAPIConfig: Properties = {
    metadataAPIConfig
  }

  def SetLoggerLevel(level: Level) {
    logger.setLevel(level);
  }


  def getLeaderHost(leaderNode: String) : String = {
    val nodes = MdMgr.GetMdMgr.Nodes.values.toArray
    if ( nodes.length == 0 ){
      logger.trace("No Nodes found ")
      var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetLeaderHost", null, ErrorCodeConstants.Get_Leader_Host_Failed_Not_Available +" :" + leaderNode)
      apiResult.toString()
    }
    else{
      val nhosts = nodes.filter(n => n.nodeId == leaderNode)
      if ( nhosts.length == 0 ){
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetLeaderHost", null, ErrorCodeConstants.Get_Leader_Host_Failed_Not_Available +" :" + leaderNode)
        apiResult.toString()
      }
      else{
  val nhost = nhosts(0)
  logger.trace("node addr => " + nhost.NodeAddr)
  nhost.NodeAddr
      }
    }
  }

  private var metadataStore: DataStore = _
  private var transStore: DataStore = _
  private var modelStore: DataStore = _
  private var messageStore: DataStore = _
  private var containerStore: DataStore = _
  private var functionStore: DataStore = _
  private var conceptStore: DataStore = _
  private var typeStore: DataStore = _
  private var otherStore: DataStore = _
  private var jarStore: DataStore = _
  private var configStore: DataStore = _
  
  def getMetadataStore: DataStore = {metadataStore}
  def getMessageStore: DataStore = {messageStore}
  def getTransStore: DataStore = {transStore}
  def getModelStore: DataStore = {modelStore}
  def getTypeStore: DataStore = {typeStore}
  def getContainerStore: DataStore = {containerStore}
  def getConfigStore: DataStore = {configStore}
  def getConceptStore: DataStore = {conceptStore}
  def getJarStore: DataStore = {jarStore}
  def getFunctionStore: DataStore = {functionStore}
  def getOtherStore: DataStore = {otherStore}
      
  
  def setMetadataStore(ds: DataStore): Unit = {metadataStore = ds}
  def setMessageStore(ds: DataStore): Unit = {messageStore = ds}
  def setTransStore(ds: DataStore): Unit = {transStore = ds}
  def setModelStore(ds: DataStore): Unit = {modelStore = ds}
  def setTypeStore(ds: DataStore): Unit = {typeStore = ds}
  def setContainerStore(ds: DataStore): Unit = {containerStore = ds}
  def setConfigStore(ds: DataStore): Unit = {configStore = ds}
  def setConceptStore(ds: DataStore): Unit = {conceptStore = ds}
  def setJarStore(ds: DataStore): Unit = {jarStore = ds}
  def setFunctionStore(ds: DataStore): Unit = {functionStore = ds}
  def setOtherStore(ds: DataStore): Unit = {otherStore = ds}

  def oStore = otherStore
  
  @throws(classOf[CreateStoreFailedException])
  def GetDataStoreHandle(storeType: String, storeName: String, tableName: String): DataStore = {
    MetadataUtils.GetDataStoreHandle(storeType, storeName, tableName)
  }

  @throws(classOf[CreateStoreFailedException])
  def OpenDbStore(storeType: String) {
    MetadataUtils.OpenDbStore(storeType)
  }

  def CloseDbStore: Unit = lock.synchronized {
    MetadataUtils.CloseDbStore
  }
  
  /**
   * shutdown - call this method to release various resources held by 
   */
  def shutdown: Unit = {
    CloseDbStore
    MetadataSynchronizer.shutdownZkListener
    SecAuditImpl.shutdownAuditAdapter
  }

  /**
   * getApiResult - Create an API result object in a string form. from a given string
   * @params String
   * @return String
   */
  def getApiResult(apiResultJson: String): String = {
    MetadataUtils.getApiResult(apiResultJson) 
  }
  
  //*******************************************************************************
  //************************ Entry points for Jar Files related methods ***********
  //******************************************************************************* 
  
  def UploadJar(jarPath: String): String = {
    JarImpl.UploadJar(jarPath)
  }
  
  def UploadJarToDB(jarName:String,byteArray: Array[Byte]): String = {
    JarImpl.UploadJarToDB(jarName, byteArray)
  }

  
  //*******************************************************************************
  //************************ Entry points for FUNCTION related methods ************
  //******************************************************************************* 
  def GetAllFunctionsFromCache(active: Boolean): Array[String] = {
    FunctionImpl.GetAllFunctionsFromCache(active)
  }
  
  // Answer count and dump of all available functions(format JSON or XML) as a String
  def GetAllFunctionDefs(formatType: String): (Int,String) = {
    FunctionImpl.GetAllFunctionDefs(formatType)
  }

  def GetFunctionDef(nameSpace: String, objectName: String, formatType: String): String = {
    FunctionImpl.GetFunctionDef(nameSpace, objectName, formatType)
  }
  
  def GetFunctionDef(nameSpace: String, objectName: String, formatType: String, version:String): String = {
    FunctionImpl.GetFunctionDef(nameSpace, objectName, formatType)
  }
 
  // Specific messages (format JSON or XML) as a String using messageName(without version) as the key
  def GetFunctionDef(objectName: String, formatType: String): String = {
    GetFunctionDef(MdMgr.sysNS, objectName, formatType)
  } 
  
  def UpdateFunctions(functionsText: String, format: String): String = {
    FunctionImpl.UpdateFunctions(functionsText, format)
  }
  
  def AddFunctions(functionsText: String, format: String): String = {
    FunctionImpl.AddFunctions(functionsText, format)
  } 
 
  def RemoveFunction(nameSpace: String, functionName: String, version: Long): String = {
    FunctionImpl.RemoveFunction(nameSpace, functionName, version)
  }
 
  //*******************************************************************************
  //************************ Entry points for TYPES related methods ***************
  //*******************************************************************************
  
  // Get types for a given name
  def GetType(objectName: String, formatType: String): String = {
    TypeImpl.GetType(objectName, formatType)
  }

  def GetTypeDef(nameSpace: String, objectName: String, formatType: String,version: String): String = {
    TypeImpl.GetTypeDef(nameSpace, objectName, formatType, version)
  }
  
  
  def GetAllTypesFromCache(active: Boolean): Array[String] = {
    TypeImpl.GetAllTypesFromCache(active)
  }
  
  // All available types(format JSON or XML) as a String
  def GetAllTypes(formatType: String): String = {
    TypeImpl.GetAllTypes(formatType)
  }
  
  def UpdateType(typeJson: String, format: String): String = {
    TypeImpl.UpdateType(typeJson, format)
  }
  
  def AddTypes(typesText: String, format: String): String = {
    TypeImpl.AddTypes(typesText, format)
  }
   
  def AddType(typeText: String, format: String): String = {
    TypeImpl.AddType(typeText, format)
  }
  
  // Remove type for given TypeName and Version
  def RemoveType(typeNameSpace: String, typeName: String, version: Long): String = {
    TypeImpl.RemoveType(typeNameSpace, typeName, version)
  }

  //*******************************************************************************
  //************************ Entry points for MESSAGE related methods *************
  //*******************************************************************************  
  def AddMessage(messageText: String, format: String): String = {
    MessageImpl.AddContainerOrMessage(messageText, format, false)
  }

  def AddMessage(messageText: String): String = {
    AddMessage(messageText,"JSON")
  }
  
  // Remove message with Message Name and Version Number
  def RemoveMessage(messageName: String, version: Long): String = {
    RemoveMessage(sysNS, messageName, version)
  }

   // Remove message with Message Name and Version Number
  def RemoveMessage(nameSpace: String, name: String, version: Long, zkNotify:Boolean = true): String = {
    MessageImpl.RemoveMessage(nameSpace, name, version, zkNotify)
  } 

  def UpdateMessage(messageText: String, format: String): String = {
    MessageImpl.UpdateMessage(messageText, format)
  }

  // Specific messages (format JSON or XML) as a String using messageName(without version) as the key
  def GetMessageDef(objectName: String, formatType: String): String = {
    MessageImpl.GetMessageDefFromCache(MdMgr.sysNS, objectName, formatType, "-1")
  }
  
  // Specific message (format JSON or XML) as a String using messageName(with version) as the key
  def GetMessageDef(nameSpace: String, objectName: String, formatType: String, version: String): String = {
    MessageImpl.GetMessageDefFromCache(nameSpace, objectName, formatType, version)
  }
  
  // All available messages(format JSON or XML) as a String
  def GetAllMessageDefs(formatType: String): String = {
    MessageImpl.GetAllMessageDefs(formatType)
  }

  def GetAllMessagesFromCache(active: Boolean): Array[String] = {
     MessageImpl.GetAllMessagesFromCache(active)
  }
  
  // Specific message (format JSON or XML) as a String using messageName(with version) as the key
  def GetMessageDef(objectName: String, version: String, formatType: String): String = {
    GetMessageDef(MdMgr.sysNS, objectName, formatType, version)
  }
  
  
  //*******************************************************************************
  //************************ Entry points for Container related methods ***********
  //******************************************************************************* 
  
  // Specific containers (format JSON or XML) as a String using containerName(without version) as the key
  def GetContainerDef(objectName: String, formatType: String): String = {
     ContainerImpl.GetContainerDefFromCache(MdMgr.sysNS, objectName, formatType, "-1")
  }
  
  // Specific container (format JSON or XML) as a String using containerName(with version) as the key
  def GetContainerDef(nameSpace: String, objectName: String, formatType: String, version: String): String = {
     ContainerImpl.GetContainerDefFromCache(nameSpace, objectName, formatType, version)
  }

  // Specific container (format JSON or XML) as a String using containerName(with version) as the key
  def GetContainerDef(objectName: String, version: String, formatType: String): String = {
    GetContainerDef(MdMgr.sysNS, objectName, formatType, version)
  }
  
  // Remove container with Container Name and Version Number
  def RemoveContainer(containerName: String, version: Long): String = {
    RemoveContainer(sysNS, containerName, version)
  }
  
  // All available containers(format JSON or XML) as a String
  def GetAllContainerDefs(formatType: String): String = {
    ContainerImpl.GetAllContainerDefs(formatType)
  }
  
  def AddContainer(containerText: String, format: String): String = {
     MessageImpl.AddContainerOrMessage(containerText, format, false)
  }

  def UpdateContainer(messageText: String, format: String): String = {
    UpdateMessage(messageText,format)
  }
  
  // Remove container with Container Name and Version Number
  def RemoveContainer(nameSpace: String, name: String, version: Long, zkNotify:Boolean = true): String = {
    ContainerImpl.RemoveContainer(nameSpace, name, version, zkNotify)
  }
  
  def GetAllContainersFromCache(active: Boolean): Array[String] = {
    ContainerImpl.GetAllContainersFromCache(active)
  }
 
  
  //*******************************************************************************
  //************************ Entry points for MODEL related methods ***************
  //*******************************************************************************
  
  // Remove model with Model Name and Version Number
  def DeactivateModel(nameSpace: String, name: String, version: Long): String = {
    ModelImpl.DeactivateModel(nameSpace, name, version)  
  }

  def ActivateModel(nameSpace: String, name: String, version: Long): String = {
    ModelImpl.ActivateModel(nameSpace, name, version)
  }  

  // Remove model with Model Name and Version Number
  def RemoveModel(nameSpace: String, name: String, version: Long): String = {
    ModelImpl.RemoveModel(nameSpace, name, version)
  }

  // Remove model with Model Name and Version Number
  def RemoveModel(modelName: String, version: Long): String = {
    ModelImpl.RemoveModel(sysNS, modelName, version)
  }

  // Add Model (format XML)
  def AddModel(pmmlText: String): String = {
    ModelImpl.AddModel(pmmlText)
  }
  
  /**
   * 
   */
  def UpdateModel(pmmlText: String): String = {
    ModelImpl.UpdateModel(pmmlText)
  }
  
  // All available models(format JSON or XML) as a String
  def GetAllModelDefs(formatType: String): String = {
    ModelImpl.GetAllModelDefs(formatType)
  }
  
  def GetAllModelsFromCache(active: Boolean): Array[String] = {
    ModelImpl.GetAllModelsFromCache(active)
  }
  
  // Specific models (format JSON or XML) as an array of strings using modelName(without version) as the key
  def GetModelDef(nameSpace: String, objectName: String, formatType: String): String = {
    ModelImpl.GetModelDef(nameSpace, objectName, formatType)
  }

  // Specific models (format JSON or XML) as an array of strings using modelName(without version) as the key
  def GetModelDef(objectName: String, formatType: String): String = {
    GetModelDef(sysNS, objectName, formatType)
  }

  // Specific models (format JSON or XML) as an array of strings using modelName(without version) as the key
  def GetModelDef(nameSpace: String, objectName: String, formatType:String, version: String): String = {
    ModelImpl.GetModelDefFromCache(nameSpace, objectName, formatType, version)
  }
    
  //*******************************************************************************
  //************************ Entry points for CONFIG related methods **************
  //*******************************************************************************

  
  def RemoveConfig(cfgStr: String): String = {
    ConfigImpl.RemoveConfig(cfgStr)
  }
  
  // All available config objects(format JSON) as a String
  def GetAllCfgObjects(formatType: String): String = {
    ConfigImpl.GetAllCfgObjects(formatType)
  }
   
  def UploadConfig(cfgStr: String): String = {
    ConfigImpl.UploadConfig(cfgStr)
  } 
  
  
  // All available adapters(format JSON) as a String
  def GetAllAdapters(formatType: String): String = {
    ConfigImpl.GetAllAdapters(formatType)
  }

  // All available clusters(format JSON) as a String
  def GetAllClusters(formatType: String): String = {
    ConfigImpl.GetAllClusters(formatType)
  }
 
  // All available nodes(format JSON) as a String
  def GetAllNodes(formatType: String): String = {
    ConfigImpl.GetAllNodes(formatType)
  }

  // All available clusterCfgs(format JSON) as a String
  def GetAllClusterCfgs(formatType: String): String = {
    ConfigImpl.GetAllClusterCfgs(formatType)
  }
  
  //*******************************************************************************
  //************************ Entry points for GENERIC OBJECT related methods ******
  //*******************************************************************************

  def LoadAllObjectsIntoCache {
    DaoImpl.LoadAllObjectsIntoCache
  }
 
  //*******************************************************************************
  //************************ Entry points for CONCEPTS/ATTRIBUTES related methods *
  //******************************************************************************* 
  private def LoadAllConceptsIntoCache {
    ConceptImpl.LoadAllConceptsIntoCache
  }
  
  def GetAllConceptsFromCache(active: Boolean): Array[String] = {
    ConceptImpl.GetAllConceptsFromCache(active)
  }
  
  // A list of concept(s) as a string using name 
  def GetConcept(objectName: String, formatType: String): String = {
    ConceptImpl.GetConcept(objectName, formatType)
  }
 
  // All available concepts as a String
  def GetAllConcepts(formatType: String): String = {
    ConceptImpl.GetAllConcepts(formatType)
  }
  
  // A single concept as a string using name and version as the key
  def GetConcept(objectName: String, version: String, formatType: String): String = {
    ConceptImpl.GetConcept(MdMgr.sysNS, objectName, version, formatType)
  }

  // A single concept as a string using name and version as the key
  def GetConceptDef(nameSpace:String, objectName: String, formatType: String,version: String): String = {
    ConceptImpl.GetConcept(nameSpace, objectName, version, formatType)
  }
  
  
  // All available derived concepts(format JSON or XML) as a String
  def GetAllDerivedConcepts(formatType: String): String = {
    ConceptImpl.GetAllDerivedConcepts(formatType)
  }
  
  // A derived concept(format JSON or XML) as a string using name(without version) as the key
  def GetDerivedConcept(objectName: String, formatType: String): String = {
    ConceptImpl.GetDerivedConcept(objectName, formatType)
  }

  // A derived concept(format JSON or XML) as a string using name and version as the key
  def GetDerivedConcept(objectName: String, version: String, formatType: String): String = {
    ConceptImpl.GetDerivedConcept(objectName, version, formatType)
  } 
  
  def AddConcepts(conceptsText: String, format: String): String = {
    ConceptImpl.AddConcepts(conceptsText, format)
  }

  def UpdateConcepts(conceptsText: String, format: String): String = {
    ConceptImpl.UpdateConcepts(conceptsText, format)
  }

  // RemoveConcepts take all concepts names to be removed as an Array
  def RemoveConcepts(concepts: Array[String]): String = {
    ConceptImpl.RemoveConcepts(concepts)
  }

  
  def RemoveConcept(nameSpace:String, name:String, version:Long): String = {
    ConceptImpl.RemoveConcept(nameSpace, name, version)
  }

  //*******************************************************************************
  //************************ Entry points for KEYS operations related methods     *
  //*******************************************************************************  
  def GetAllKeys(objectType: String): Array[String] = {
    try {
      var keys = scala.collection.mutable.Set[String]()
      typeStore.getAllKeys({ (key: Key) =>
        {
          val strKey = MetadataUtils.KeyAsStr(key)
          val i = strKey.indexOf(".")
          val objType = strKey.substring(0, i)
          val typeName = strKey.substring(i + 1)
          objectType match {
            case "TypeDef" => {
              if (TypeImpl.IsTypeObject(objType)) {
                keys.add(typeName)
              }
            }
            case "FunctionDef" => {
              if (objType == "functiondef") {
                keys.add(typeName)
              }
            }
            case "MessageDef" => {
              if (objType == "messagedef") {
                keys.add(typeName)
              }
            }
            case "ContainerDef" => {
              if (objType == "containerdef") {
                keys.add(typeName)
              }
            }
            case "Concept" => {
              if (objType == "attributedef") {
                keys.add(typeName)
              }
            }
            case "ModelDef" => {
              if (objType == "modeldef") {
                keys.add(typeName)
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
      case e: ObjectNolongerExistsException => {
        e.printStackTrace()
        throw InternalErrorException("Failed to get keys from persistent store")
      }
    }
  }


  def InitMdMgrFromBootStrap(configFile: String) {
    MdMgr.GetMdMgr.truncate
    val mdLoader = new MetadataLoad(MdMgr.mdMgr, "", "", "", "")
    mdLoader.initialize
    if (configFile.endsWith(".json")) {
      MetadataUtils.readMetadataAPIConfigFromJsonFile(configFile)
    } else {
      MetadataUtils.readMetadataAPIConfigFromPropertiesFile(configFile)
    }

    MetadataSynchronizer.initZkListener
    MetadataAPIImpl.OpenDbStore(GetMetadataAPIConfig.getProperty("DATABASE"))
    MetadataAPIImpl.LoadAllObjectsIntoCache
    SecAuditImpl.InitSecImpl
    isInitilized = true
    logger.info("Metadata synching is now available.")
    
  }

  /**
   *  InitMdMgr - 
   */
  def InitMdMgr(mgr: MdMgr, database: String, databaseHost: String, databaseSchema: String, databaseLocation: String) {

    SetLoggerLevel(Level.TRACE)
    val mdLoader = new MetadataLoad(mgr, "", "", "", "")
    mdLoader.initialize

    metadataAPIConfig.setProperty("DATABASE", database)
    metadataAPIConfig.setProperty("DATABASE_HOST", databaseHost)
    metadataAPIConfig.setProperty("DATABASE_SCHEMA", databaseSchema)
    metadataAPIConfig.setProperty("DATABASE_LOCATION", databaseLocation)

    MetadataAPIImpl.OpenDbStore(GetMetadataAPIConfig.getProperty("DATABASE"))
    MetadataAPIImpl.LoadAllObjectsIntoCache
  }
}
