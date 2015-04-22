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

case class JsonException(message: String) extends Exception(message)

case class TypeDef(MetadataType: String, NameSpace: String, Name: String, TypeTypeName: String, TypeNameSpace: String, TypeName: String, PhysicalName: String, var Version: String, JarName: String, DependencyJars: List[String], Implementation: String, Fixed: Option[Boolean], NumberOfDimensions: Option[Int], KeyTypeNameSpace: Option[String], KeyTypeName: Option[String], ValueTypeNameSpace: Option[String], ValueTypeName: Option[String], TupleDefinitions: Option[List[TypeDef]])
case class TypeDefList(Types: List[TypeDef])

case class Argument(ArgName: String, ArgTypeNameSpace: String, ArgTypeName: String)
case class Function(NameSpace: String, Name: String, PhysicalName: String, ReturnTypeNameSpace: String, ReturnTypeName: String, Arguments: List[Argument], Version: String, JarName: String, DependantJars: List[String])
case class FunctionList(Functions: List[Function])

//case class Concept(NameSpace: String,Name: String, TypeNameSpace: String, TypeName: String,Version: String,Description: String, Author: String, ActiveDate: String)
case class Concept(NameSpace: String, Name: String, TypeNameSpace: String, TypeName: String, Version: String)
case class ConceptList(Concepts: List[Concept])

// case class Attr(NameSpace: String, Name: String, Version: Int, Type: TypeDef)

// case class MessageStruct(NameSpace: String, Name: String, FullName: String, Version: Int, JarName: String, PhysicalName: String, DependencyJars: List[String], Attributes: List[Attr])
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
  // Get a mirror for reflection
  //val mirror: scala.reflect.runtime.universe.Mirror = ru.runtimeMirror(loader)
}

// The implementation class
object MetadataAPIImpl extends MetadataAPI {

  lazy val sysNS = "System" // system name space
  lazy val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)
  lazy val serializer = SerializerManager.GetSerializer("kryo")
  lazy val metadataAPIConfig = new Properties()
  var zkc: CuratorFramework = null
  var authObj: SecurityAdapter = null
  var auditObj: AuditAdapter = null
  val configFile = System.getenv("HOME") + "/MetadataAPIConfig.json"
  var propertiesAlreadyLoaded = false
  var isInitilized: Boolean = false
  private var zkListener: ZooKeeperListener = _
  
  // For future debugging  purposes, we want to know which properties were not set - so create a set
  // of values that can be set via our config files
  var pList: Set[String] = Set("ZK_SESSION_TIMEOUT_MS","ZK_CONNECTION_TIMEOUT_MS","DATABASE_SCHEMA","DATABASE","DATABASE_LOCATION","DATABASE_HOST","API_LEADER_SELECTION_ZK_NODE",
                               "JAR_PATHS","JAR_TARGET_DIR","ROOT_DIR","GIT_ROOT","SCALA_HOME","JAVA_HOME","MANIFEST_PATH","CLASSPATH","NOTIFY_ENGINE","SERVICE_HOST",
                               "ZNODE_PATH","ZOOKEEPER_CONNECT_STRING","COMPILER_WORK_DIR","SERVICE_PORT","MODEL_FILES_DIR","TYPE_FILES_DIR","FUNCTION_FILES_DIR",
                               "CONCEPT_FILES_DIR","MESSAGE_FILES_DIR","CONTAINER_FILES_DIR","CONFIG_FILES_DIR","MODEL_EXEC_LOG","NODE_ID","SSL_CERTIFICATE","DO_AUTH","SECURITY_IMPL_CLASS",
                               "SECURITY_IMPL_JAR", "AUDIT_INTERVAL")
  var isCassandra = false
  private[this] val lock = new Object
  var startup = false

  var tableStoreMap: Map[String, DataStore] = Map()

  
  /**
   * InitSecImpl  - 1. Create the Security Adapter class.  The class name and jar name containing 
   *                the implementation of that class are specified in the CONFIG FILE.
   *                2. Create the Audit Adapter class, The class name and jar name containing 
   *                the implementation of that class are specified in the CONFIG FILE.
   */
  def InitSecImpl: Unit = {
    logger.debug("Establishing connection to domain security server..")
    val classLoader: MetadataLoader = new MetadataLoader
    // If already have one, use that!
    if (authObj == null) {
      createAuthObj(classLoader)
    } 
    if (auditObj == null) {
      createAuditObj(classLoader)
    }
  }
  
  /**
   * private method to instantiate an authObj
   */
  private def createAuthObj(classLoader : MetadataLoader): Unit = {
    // Load the location and name of the implementing class from the 
    val implJarName = if (metadataAPIConfig.getProperty("SECURITY_IMPL_JAR") == null) "" else metadataAPIConfig.getProperty("SECURITY_IMPL_JAR").trim
    val implClassName = metadataAPIConfig.getProperty("SECURITY_IMPL_CLASS").trim 
    logger.debug("Using "+implClassName+", from the "+implJarName+" jar file")
    if (implClassName == null) {
      logger.error("Security Adapter Class is not specified")
      return
    }

    // Add the Jarfile to the class loader
    Utils.loadJar(classLoader,implJarName)
    
    // All is good, create the new class 
    var className = Class.forName(implClassName, true, classLoader.loader).asInstanceOf[Class[com.ligadata.olep.metadata.SecurityAdapter]]
    authObj = className.newInstance
    logger.debug("Created class "+ className.getName)    
  }
  
   /**
   * private method to instantiate an authObj
   */
  private def createAuditObj (classLoader : MetadataLoader): Unit = {
    // Load the location and name of the implementing class froms the 
    val implJarName = if (metadataAPIConfig.getProperty("AUDIT_IMPL_JAR") == null) "" else metadataAPIConfig.getProperty("AUDIT_IMPL_JAR").trim
    val implClassName = metadataAPIConfig.getProperty("AUDIT_IMPL_CLASS").trim 
    logger.debug("Using "+implClassName+", from the "+implJarName+" jar file")
    if (implClassName == null) {
      logger.error("Audit Adapter Class is not specified")
      return
    }
    
    // Add the Jarfile to the class loader
    Utils.loadJar(classLoader,implJarName)
    
    // All is good, create the new class 
    var className = Class.forName(implClassName, true, classLoader.loader).asInstanceOf[Class[com.ligadata.olep.metadata.AuditAdapter]]
    auditObj = className.newInstance
    auditObj.init
    logger.debug("Created class "+ className.getName)    
  }
 
  /**
   * checkAuth
   */
  def checkAuth (usrid:Option[String], password:Option[String], role: Option[String], privilige: String): Boolean = {  

    var authParms: java.util.Properties = new Properties
    // Do we want to run AUTH?
    if (!GetMetadataAPIConfig.getProperty("DO_AUTH").equalsIgnoreCase("YES")) return true

    // check if the Auth object exists
    if (authObj == null) return false
   
    // Run the Auth - if userId is supplied, defer to that.
    if ((usrid == None) && (role == None)) return false
    
    if (usrid != None) authParms.setProperty("userid",usrid.get.asInstanceOf[String])
    if (password != None) authParms.setProperty("password", password.get.asInstanceOf[String]) 
    if (role != None) authParms.setProperty("role", role.get.asInstanceOf[String]) 
    authParms.setProperty("privilige",privilige)  

    return authObj.performAuth(authParms) 
  }
  
  /**
   * getPrivilegeName 
   */
  def getPrivilegeName(op: String, objName: String): String = {
    // check if the Auth object exists
    logger.debug("op => " + op)
    logger.debug("objName => " + objName)
    if (authObj == null) return ""
    return authObj.getPrivilegeName (op,objName)
  }
  

  def GetMetadataAPIConfig: Properties = {
    metadataAPIConfig
  }

  def SetLoggerLevel(level: Level) {
    logger.setLevel(level);
  }

  var metadataStore: DataStore = _
  var transStore: DataStore = _
  var modelStore: DataStore = _
  var messageStore: DataStore = _
  var containerStore: DataStore = _
  var functionStore: DataStore = _
  var conceptStore: DataStore = _
  var typeStore: DataStore = _
  var otherStore: DataStore = _
  var jarStore: DataStore = _
  var configStore: DataStore = _

  def oStore = otherStore

  @throws(classOf[CreateStoreFailedException])
  def OpenDbStore(storeType: String) {
    try {
      logger.debug("Opening datastore")
      metadataStore = DAOUtils.GetDataStoreHandle(storeType, "metadata_store", "metadata_objects")
      configStore = DAOUtils.GetDataStoreHandle(storeType, "config_store", "config_objects")
      jarStore = DAOUtils.GetDataStoreHandle(storeType, "metadata_jars", "jar_store")
      transStore = DAOUtils.GetDataStoreHandle(storeType, "metadata_trans", "transaction_id")
      modelStore = metadataStore
      messageStore = metadataStore
      containerStore = metadataStore
      functionStore = metadataStore
      conceptStore = metadataStore
      typeStore = metadataStore
      otherStore = metadataStore
      tableStoreMap = Map("models" -> modelStore,
        "messages" -> messageStore,
        "containers" -> containerStore,
        "functions" -> functionStore,
        "concepts" -> conceptStore,
        "types" -> typeStore,
        "others" -> otherStore,
	"jar_store" -> jarStore,
	"config_objects" -> configStore,
        "transaction_id" -> transStore)
    } catch {
      case e: CreateStoreFailedException => {
        e.printStackTrace()
        throw new CreateStoreFailedException(e.getMessage())
      }
      case e: Exception => {
        e.printStackTrace()
        throw new CreateStoreFailedException(e.getMessage())
      }
    }
  }

  def CloseDbStore: Unit = lock.synchronized {
    try {
      logger.debug("Closing datastore")
      if (metadataStore != null) {
        metadataStore.Shutdown()
        metadataStore = null
        logger.debug("metdatastore closed")
      }
      if (transStore != null) {
        transStore.Shutdown()
        transStore = null
        logger.debug("transStore closed")
      }
      if (jarStore != null) {
        jarStore.Shutdown()
        jarStore = null
        logger.debug("jarStore closed")
      }
      if (configStore != null) {
        configStore.Shutdown()
        configStore = null
        logger.debug("configStore closed")
      }
    } catch {
      case e: Exception => {
        throw e;
      }
    }
  }

  def AddType(typeText: String, format: String): String = {
    TypeUtils.AddType(typeText,format)
  }

  def AddTypes(typesText: String, format: String): String = {
    TypeUtils.AddTypes(typesText,format)
  }

  // Remove type for given TypeName and Version
  def RemoveType(typeNameSpace: String, typeName: String, version: Long): String = {
    TypeUtils.RemoveType(typeNameSpace,typeName,version)
  }

  def UpdateType(typeText: String, format: String): String = {
    TypeUtils.UpdateType(typeText,format)
  }


  // Upload Jars into system. Dependency jars may need to upload first. 
  // Once we upload the jar, if we retry to upload it will throw an exception.
  def UploadJarToDB(jarPath: String, byteArray: Array[Byte]): String = {
    DAOUtils.UploadJarToDB(jarPath,byteArray)
  }

  def RemoveConcept(nameSpace:String, name:String, version: Long): String = {
    ConceptUtils.RemoveConcept(nameSpace,name,version)
  }

  def RemoveConcept(key:String): String = {
    ConceptUtils.RemoveConcept(key)
  }

  // RemoveConcepts take all concepts names to be removed as an Array
  def RemoveConcepts(concepts: Array[String]): String = {
    ConceptUtils.RemoveConcepts(concepts)
  }

  def RemoveFunction(nameSpace: String, functionName: String, version: Long): String = {
    FunctionUtils.RemoveFunction(nameSpace,functionName,version)
  }    

  def AddFunctions(functionsText: String, format: String): String = {
    FunctionUtils.AddFunctions(functionsText,format)
  }

  def AddFunction(functionText: String, format: String): String = {
    FunctionUtils.AddFunction(functionText,format)
  }

  def UpdateFunctions(functionsText: String, format: String): String = {
    FunctionUtils.UpdateFunctions(functionsText,format)
  }

  def AddDerivedConcept(conceptsText: String, format: String): String = {
    ConceptUtils.AddDerivedConcept(conceptsText,format)
  }

  def AddConcepts(conceptsText: String, format: String): String = {
    ConceptUtils.AddConcepts(conceptsText,format)
  }

  def UpdateConcepts(conceptsText: String, format: String): String = {
    ConceptUtils.UpdateConcepts(conceptsText,format)
  }

  def AddMessage(messageText: String, format: String): String = {
    MessageUtils.AddMessage(messageText,format)
  }

  def AddMessage(messageText: String): String = {
    MessageUtils.AddMessage(messageText,"JSON")
  }

  def AddContainer(containerText: String, format: String): String = {
    ContainerUtils.AddContainer(containerText,format)
  }

  def AddContainer(containerText: String): String = {
    AddContainer(containerText,"JSON")
  }

  def UpdateMessage(messageText: String, format: String): String = {
    MessageUtils.UpdateMessage(messageText,format)
  }

  def UpdateMessage(messageText: String): String = {
    UpdateMessage(messageText,"JSON")
  }

  def UpdateContainer(messageText: String, format: String): String = {
    ContainerUtils.UpdateContainer(messageText,format)
  }

  def UpdateContainer(messageText: String): String = {
    UpdateMessage(messageText,"JSON")
  }

  // Remove message with Message Name and Version Number
  def RemoveMessage(messageName: String, version: Long): String = {
    MessageUtils.RemoveMessage(sysNS, messageName, version)
  }

  // Remove message with Message NameSpace, Message Name and Version Number
  def RemoveMessage(nameSpace:String, messageName: String, version: Long): String = {
    MessageUtils.RemoveMessage(nameSpace, messageName, version)
  }

  // Remove container with Container Name and Version Number
  def RemoveContainer(containerName: String, version: Long): String = {
    ContainerUtils.RemoveContainer(sysNS, containerName, version)
  }

  // Remove container with Container NameSpace, Container Name and Version Number
  def RemoveContainer(nameSpace:String, containerName: String, version: Long): String = {
    ContainerUtils.RemoveContainer(nameSpace, containerName, version)
  }

  // Add Model (format XML)
  def AddModel(pmmlText: String): String = {
    ModelUtils.AddModel(pmmlText)
  }

  def UpdateModel(pmmlText: String): String = {
    ModelUtils.UpdateModel(pmmlText)
  }

  // Remove model with Model Name and Version Number
  def RemoveModel(modelName: String, version: Long): String = {
    ModelUtils.RemoveModel(sysNS, modelName, version)
  }

  // Remove model with Model Name and Version Number
  def RemoveModel(nameSpace: String,modelName: String, version: Long): String = {
    ModelUtils.RemoveModel(nameSpace, modelName, version)
  }

  // Activate model with Model Name and Version Number
  def ActivateModel(nameSpace: String,modelName: String, version: Long): String = {
    ModelUtils.ActivateModel(nameSpace, modelName, version)
  }

  // Deactivate model with Model Name and Version Number
  def DeactivateModel(nameSpace: String,modelName: String, version: Long): String = {
    ModelUtils.DeactivateModel(nameSpace, modelName, version)
  }

  // All available models(format JSON or XML) as a String
  def GetAllModelDefs(formatType: String): String = {
    ModelUtils.GetAllModelDefs(formatType);
  }

  def GetAllModelsFromCache(active: Boolean): Array[String] = {
    ModelUtils.GetAllModelsFromCache(active);
  }

  // Specific models (format JSON or XML) as an array of strings using modelName(without version) as the key
  def GetModelDef(objectName: String, formatType: String): String = {
    ModelUtils.GetModelDef(sysNS, objectName, formatType)
  }

  // Specific model (format JSON or XML) as a String using modelName(with version) as the key
  def GetModelDef(objectName: String, version: String, formatType: String): String = {
    ModelUtils.GetModelDef(sysNS,objectName,formatType,version)
  }

  // Specific model (format JSON or XML) as a String using modelName(with version) as the key
  def GetModelDefFromCache(nameSpace:String, objectName: String, formatType: String, version: String): String = {
    ModelUtils.GetModelDefFromCache(nameSpace,objectName,formatType,version)
  }

  // Specific model (format JSON or XML) as a String using modelName(with version) as the key
  def GetModelDefFromDB(nameSpace:String, objectName: String, formatType: String, version: String): String = {
    ModelUtils.GetModelDefFromDB(nameSpace,objectName,formatType,version)
  }

  // Specific model (format JSON or XML) as a String using modelName(with version) as the key
  def GetModelDef(nameSpace:String, objectName: String, formatType: String, version: String): String = {
    ModelUtils.GetModelDef(nameSpace,objectName,formatType,version)
  }

  // All available messages(format JSON or XML) as a String
  def GetAllMessageDefs(formatType: String): String = {
    MessageUtils.GetAllMessageDefs(formatType)
  }

  def GetAllMessagesFromCache(active: Boolean): Array[String] = {
    MessageUtils.GetAllMessagesFromCache(active)
  }

  def GetMessageDefFromCache(nameSpace:String, objectName: String, formatType: String, version: String): String = {
    MessageUtils.GetMessageDefFromCache(nameSpace,objectName,formatType,version)
  }

  // All available containers(format JSON or XML) as a String
  def GetAllContainerDefs(formatType: String): String = {
    ContainerUtils.GetAllContainerDefs(formatType)
  }

  def GetAllContainersFromCache(active: Boolean): Array[String] = {
    ContainerUtils.GetAllContainersFromCache(active);
  }

  def GetContainerDefFromCache(nameSpace:String, objectName: String, formatType: String, version: String): String = {
    ContainerUtils.GetContainerDefFromCache(nameSpace,objectName,formatType,version)
  }

  def GetAllKeys(objectType: String): Array[String] = {
    Utils.GetAllKeys(objectType)
  }

  def UpdateMdMgr(zkTransaction: ZooKeeperTransaction) = {
    Utils.UpdateMdMgr(zkTransaction)
  }

  // Specific messages (format JSON or XML) as a String using messageName(without version) as the key
  def GetMessageDef(objectName: String, formatType: String): String = {
    val nameSpace = MdMgr.sysNS
    MessageUtils.GetMessageDefFromCache(nameSpace, objectName, formatType, "-1")
  }
  // Specific message (format JSON or XML) as a String using messageName(with version) as the key
  def GetMessageDef(nameSpace: String, objectName: String, formatType: String, version: String): String = {
    MessageUtils.GetMessageDefFromCache(nameSpace, objectName, formatType, version)
  }

  // Specific message (format JSON or XML) as a String using messageName(with version) as the key
  def GetMessageDef(objectName: String, version: String, formatType: String): String = {
    val nameSpace = MdMgr.sysNS
    MessageUtils.GetMessageDef(nameSpace, objectName, formatType, version)
  }

  // Specific containers (format JSON or XML) as a String using containerName(without version) as the key
  def GetContainerDef(objectName: String, formatType: String): String = {
    val nameSpace = MdMgr.sysNS
    ContainerUtils.GetContainerDefFromCache(nameSpace, objectName, formatType, "-1")
  }
  // Specific container (format JSON or XML) as a String using containerName(with version) as the key
  def GetContainerDef(nameSpace: String, objectName: String, formatType: String, version: String): String = {
    ContainerUtils.GetContainerDefFromCache(nameSpace, objectName, formatType, version)
  }

  // Specific container (format JSON or XML) as a String using containerName(with version) as the key
  def GetContainerDef(objectName: String, version: String, formatType: String): String = {
    val nameSpace = MdMgr.sysNS
    ContainerUtils.GetContainerDef(nameSpace, objectName, formatType, version)
  }

  // Answer count and dump of all available functions(format JSON or XML) as a String
  def GetAllFunctionDefs(formatType: String): (Int,String) = {
    FunctionUtils.GetAllFunctionDefs(formatType)
  }

  def GetAllFunctionsFromCache(active: Boolean): Array[String] = {
    FunctionUtils.GetAllFunctionsFromCache(active)
  }

  def GetFunctionDef(nameSpace: String, objectName: String, formatType: String): String = {
    FunctionUtils.GetFunctionDef(nameSpace,objectName,formatType)
  }

  def GetFunctionDef(nameSpace: String, objectName: String, formatType: String, version:String): String = {
    FunctionUtils.GetFunctionDef(nameSpace,objectName,formatType)
  }

  // Specific messages (format JSON or XML) as a String using messageName(without version) as the key
  def GetFunctionDef(objectName: String, formatType: String): String = {
    val nameSpace = MdMgr.sysNS
    FunctionUtils.GetFunctionDef(nameSpace, objectName, formatType)
  }

  // All available concepts as a String
  def GetAllConcepts(formatType: String): String = {
    ConceptUtils.GetAllConcepts(formatType)
  }

  def GetAllConceptsFromCache(active: Boolean): Array[String] = {
    ConceptUtils.GetAllConceptsFromCache(active)
  }

  // A single concept as a string using name and version as the key
  def GetConcept(nameSpace:String, objectName: String, version: String, formatType: String): String = {
    ConceptUtils.GetConcept(nameSpace,objectName,version,formatType)
  }

  // A single concept as a string using name and version as the key
  def GetConcept(objectName: String, version: String, formatType: String): String = {
    ConceptUtils.GetConcept(MdMgr.sysNS,objectName,version,formatType)
  }

  // A single concept as a string using name and version as the key
  def GetConceptDef(nameSpace:String, objectName: String, formatType: String,version: String): String = {
    ConceptUtils.GetConcept(nameSpace,objectName,version,formatType)
  }

  // A list of concept(s) as a string using name 
  def GetConcept(objectName: String, formatType: String): String = {
    ConceptUtils.GetConcept(objectName,formatType)
  }

  // All available derived concepts(format JSON or XML) as a String
  def GetAllDerivedConcepts(formatType: String): String = {
    ConceptUtils.GetAllDerivedConcepts(formatType)
  }

  // A derived concept(format JSON or XML) as a string using name(without version) as the key
  def GetDerivedConcept(objectName: String, formatType: String): String = {
    ConceptUtils.GetDerivedConcept(objectName,formatType)
  }

  // A derived concept(format JSON or XML) as a string using name and version as the key
  def GetDerivedConcept(objectName: String, version: String, formatType: String): String = {
    ConceptUtils.GetDerivedConcept(objectName,version,formatType)
  }

  // All available types(format JSON or XML) as a String
  def GetAllTypes(formatType: String): String = {
    TypeUtils.GetAllTypes(formatType)
  }

  def GetAllTypesFromCache(active: Boolean): Array[String] = {
    TypeUtils.GetAllTypesFromCache(active)
  }

  // All available types(format JSON or XML) as a String
  def GetAllTypesByObjType(formatType: String, objType: String): String = {
    TypeUtils.GetAllTypesByObjType(formatType,objType)
  }

  // Get types for a given name
  def GetType(objectName: String, formatType: String): String = {
    TypeUtils.GetType(objectName,formatType)
  }

  def GetTypeDef(nameSpace: String, objectName: String, formatType: String,version: String): String = {
    TypeUtils.GetTypeDef(nameSpace,objectName,formatType,version)
  }

  def GetType(nameSpace: String, objectName: String, version: String, formatType: String): Option[BaseTypeDef] = {
    TypeUtils.GetType(nameSpace,objectName,version,formatType)
  }

  def AddNode(nodeId:String,nodePort:Int,nodeIpAddr:String,
	      jarPaths:List[String],scala_home:String,
	      java_home:String, classpath: String,
	      clusterId:String,power:Int,
	      roles:Int,description:String): String = {
    ClusterCfgUtils.AddNode(nodeId,nodePort,nodeIpAddr,jarPaths,scala_home,
				       java_home,classpath,clusterId,power,roles,description)
  }

  def UpdateNode(nodeId:String,nodePort:Int,nodeIpAddr:String,
		 jarPaths:List[String],scala_home:String,
		 java_home:String, classpath: String,
		 clusterId:String,power:Int,
		 roles:Int,description:String): String = {
    ClusterCfgUtils.AddNode(nodeId,nodePort,nodeIpAddr,jarPaths,scala_home,
				       java_home,classpath,clusterId,power,roles,description)
  }

  def RemoveNode(nodeId:String) : String = {
    ClusterCfgUtils.RemoveNode(nodeId)
  }


  def AddAdapter(name:String,typeString:String,dataFormat: String,className: String, 
		 jarName: String, dependencyJars: List[String], 
		 adapterSpecificCfg: String,inputAdapterToVerify: String): String = {
    ClusterCfgUtils.AddAdapter(name,typeString,dataFormat,className,jarName,
					  dependencyJars,adapterSpecificCfg,inputAdapterToVerify)
  }

  def UpdateAdapter(name:String,typeString:String,dataFormat: String,className: String, 
		    jarName: String, dependencyJars: List[String], 
		    adapterSpecificCfg: String,inputAdapterToVerify: String): String = {
    ClusterCfgUtils.AddAdapter(name,typeString,dataFormat,className,jarName,
					  dependencyJars,adapterSpecificCfg,inputAdapterToVerify)
  }

  def RemoveAdapter(name:String) : String = {
    ClusterCfgUtils.RemoveAdapter(name)
  }


  def AddCluster(clusterId:String,description:String,privileges:String) : String = {
    ClusterCfgUtils.AddCluster(clusterId,description,privileges)    
  }

  def UpdateCluster(clusterId:String,description:String,privileges:String): String = {
    ClusterCfgUtils.AddCluster(clusterId,description,privileges)
  }

  def RemoveCluster(clusterId:String) : String = {
    ClusterCfgUtils.RemoveCluster(clusterId)
  }


  def AddClusterCfg(clusterCfgId:String,cfgMap:scala.collection.mutable.HashMap[String,String],
		    modifiedTime:Date,createdTime:Date) : String = {
    ClusterCfgUtils.AddClusterCfg(clusterCfgId,cfgMap, modifiedTime,createdTime)
  }

  def UpdateClusterCfg(clusterCfgId:String,cfgMap:scala.collection.mutable.HashMap[String,String],
		       modifiedTime:Date,createdTime:Date): String = {
    ClusterCfgUtils.UpdateClusterCfg(clusterCfgId,cfgMap,modifiedTime,createdTime)
  }

  def RemoveClusterCfg(clusterCfgId:String) : String = {
    ClusterCfgUtils.RemoveClusterCfg(clusterCfgId)
  }

  def RemoveConfig(cfgStr: String): String = {
    ClusterCfgUtils.RemoveConfig(cfgStr)
  }

  def UploadConfig(cfgStr: String): String = {
    ClusterCfgUtils.UploadConfig(cfgStr)
  }

  // All available nodes(format JSON) as a String
  def GetAllNodes(formatType: String): String = {
    ClusterCfgUtils.GetAllNodes(formatType)
  }
    
  // All available adapters(format JSON) as a String
  def GetAllAdapters(formatType: String): String = {
    ClusterCfgUtils.GetAllAdapters(formatType)
  }

  // All available clusters(format JSON) as a String
  def GetAllClusters(formatType: String): String = {
    ClusterCfgUtils.GetAllClusters(formatType)
  }

  // All available clusterCfgs(format JSON) as a String
  def GetAllClusterCfgs(formatType: String): String = {
    ClusterCfgUtils.GetAllClusterCfgs(formatType)
  }

  // All available config objects(format JSON) as a String
  def GetAllCfgObjects(formatType: String): String = {
    ClusterCfgUtils.GetAllCfgObjects(formatType)
  }

  def getApiResult(apiResultJson: String): String = {
    Utils.getApiResult(apiResultJson)
  }

  def UploadJar(jarName: String) = {
    Utils.UploadJar(jarName)
  }

  def NotifyEngine(objList: Array[BaseElemDef], operations: Array[String]) = {
    Utils.NotifyEngine(objList,operations)
  }

  def UpdateObject(key: String, value: Array[Byte], store: DataStore) = {
    DAOUtils.UpdateObject(key,value,store)
  }

  def GetObject(key: String, store: DataStore) : IStorage = {
    DAOUtils.GetObject(key,store)
  }

  def CloseZKSession {
    Utils.CloseZKSession
  }

  def GetDependentModels(msgNameSpace: String, msgName: String, msgVer: Long): Array[ModelDef] = {
    ModelUtils.GetDependentModels(msgNameSpace,msgName,msgVer)
  }

  def getLeaderHost(leaderNode: String) : String = {
    Utils.getLeaderHost(leaderNode)
  }

  def getSSLCertificatePath: String = {
    Utils.getSSLCertificatePath
  }

  /**
   * logAuditRec - Record an Audit event using the audit adapter.
   */
  def logAuditRec(userOrRole:Option[String], userPrivilege:Option[String], action:String, objectAccessed:String, success:String, transactionId:String, notes:String) = {
    logger.info("AUDIT SHIT GOING ON")
    if( auditObj != null ){
      logger.info("AUDIT SHIT GOING ON 2")
      val aRec = new AuditRecord
      
      // If no userName is provided here, that means that somehow we are not running with Security but with Audit ON.
      var userName = "undefined"
      if( userOrRole != None ){
        userName = userOrRole.get
      }
      
      // If no priv is provided here, that means that somehow we are not running with Security but with Audit ON.
      var priv = "undefined"
      if( userPrivilege != None ){
        priv = userPrivilege.get
      }

      aRec.userOrRole = userName
      aRec.userPrivilege = priv
      aRec.actionTime = Utils.getCurrentTime
      aRec.action = action
      aRec.objectAccessed = objectAccessed
      aRec.success = success
      aRec.transactionId = transactionId
      aRec.notes  = notes
      try{
        auditObj.addAuditRecord(aRec)
      } catch {
        case e: Exception => {
          throw new UpdateStoreFailedException("Failed to save audit record" + aRec.toString + ":" + e.getMessage())
	      }
      }
    }
  }

  
   /**
   * getAuditRec - Get an audit record from the audit adapter.
   */
  def getAuditRec(startTime: Date, endTime: Date, userOrRole:String, action:String, objectAccessed:String) : String = {
    var apiResultStr = ""
    if( auditObj == null ){
      apiResultStr = "no audit records found "
      return apiResultStr
    }
    try{
      val recs = auditObj.getAuditRecord(startTime,endTime,userOrRole,action,objectAccessed)
      if( recs.length > 0 ){
        apiResultStr = JsonSerializer.SerializeAuditRecordsToJson(recs)
      }
      else{
        apiResultStr = "no audit records found "
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "Failed to fetch all the audit objects:", null, "Error :" + e.toString)
        apiResultStr = apiResult.toString()
      }
    }
    logger.debug(apiResultStr)
    apiResultStr
  }
  
  def getAuditRec(filterParameters: Array[String]) : String = {
    var apiResultStr = ""
    if( auditObj == null ){
      apiResultStr = "no audit records found "
      return apiResultStr
    }
    try{
      var audit_interval = 10
      var ai = metadataAPIConfig.getProperty("AUDIT_INTERVAL")
      if( ai != null ){
	audit_interval = ai.toInt
      }
      var startTime:Date = new Date((new Date).getTime() - audit_interval * 60000)
      var endTime:Date = new Date()
      var userOrRole:String = null
      var action:String = null
      var objectAccessed:String = null

      if (filterParameters != null ){
	val paramCnt = filterParameters.size
	paramCnt match {
	  case 1 => {
	    startTime = Utils.parseDateStr(filterParameters(0))
	  }
	  case 2 => {
	    startTime = Utils.parseDateStr(filterParameters(0))
	    endTime = Utils.parseDateStr(filterParameters(1))
	  }
	  case 3 => {
	    startTime = Utils.parseDateStr(filterParameters(0))
	    endTime = Utils.parseDateStr(filterParameters(1))
	    userOrRole = filterParameters(2)
	  }
	  case 4 => {
	    startTime = Utils.parseDateStr(filterParameters(0))
	    endTime = Utils.parseDateStr(filterParameters(1))
	    userOrRole = filterParameters(2)
	    action = filterParameters(3)
	  }
	  case 5 => {
	    startTime = Utils.parseDateStr(filterParameters(0))
	    endTime = Utils.parseDateStr(filterParameters(1))
	    userOrRole = filterParameters(2)
	    action = filterParameters(3)
	    objectAccessed = filterParameters(4)
	  }
	}
      }
      else{
	logger.debug("filterParameters is null")
      }
      apiResultStr = getAuditRec(startTime,endTime,userOrRole,action,objectAccessed)
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "Failed to fetch all the audit objects:", null, "Error :" + e.toString )
        apiResultStr = apiResult.toString()
      }
    }
    logger.debug(apiResultStr)
    apiResultStr
  }
    
  /**
   * setPropertyFromConfigFile - convert a specific KEY:VALUE pair in the config file into the
   * KEY:VALUE pair in the  Properties object
   */
  private def setPropertyFromConfigFile(key:String, value:String) {  
    var finalKey = key
    var finalValue = value
    
    // JAR_PATHs need to be trimmed 
    if (key.equalsIgnoreCase("JarPaths") || key.equalsIgnoreCase("JAR_PATHS")) {
      val jp = value
      val j_paths = jp.split(",").map(s => s.trim).filter(s => s.size > 0)
      finalValue = j_paths.mkString(",")
      finalKey = "JAR_PATHS"
    } 
    
    // Special case 1. for config.  if JAR_PATHS is never set, then it should default to JAR_TARGET_DIR..
    // so we set the JAR_PATH if it was never set.. no worries, if JAR_PATH comes later, it willsimply
    // overwrite the value.
    if (key.equalsIgnoreCase("JAR_TARGET_DIR") && (metadataAPIConfig.getProperty("JAR_PATHS")==null)) {
      metadataAPIConfig.setProperty("JAR_PATHS", finalValue)
      logger.debug("JAR_PATHS = " + value)
      pList = pList - "JAR_PATHS"
    }
    
    // Special case 2.. MetadataLocation must set 2 properties in the config object.. 1. prop set by DATABASE_HOST,
    // 2. prop set by DATABASE_LOCATION.  MetadataLocation will overwrite those values, but not the other way around.
    if (key.equalsIgnoreCase("MetadataLocation")) {
      metadataAPIConfig.setProperty("DATABASE_LOCATION", finalValue)
      metadataAPIConfig.setProperty("DATABASE_HOST", finalValue)
      logger.debug("DATABASE_LOCATION  = " + finalValue)
       pList = pList - "DATABASE_LOCATION"
      logger.debug("DATABASE_HOST  = " + finalValue)
       pList = pList - "DATABASE_HOST"
      return
    }
    
    // Special case 2a.. DATABASE_HOST should not override METADATA_LOCATION
    if (key.equalsIgnoreCase("DATABASE_HOST") && (metadataAPIConfig.getProperty(key.toUpperCase)!=null)) {
      return
    }
    // Special case 2b.. DATABASE_LOCATION should not override METADATA_LOCATION
    if (key.equalsIgnoreCase("DATABASE_LOCATION") && (metadataAPIConfig.getProperty(key.toUpperCase)!=null)) {
      return
    }
    
    // Special case 3: SCHEMA_NAME can come it under several keys, but we store it as DATABASE SCHEMA
    if (key.equalsIgnoreCase("MetadataSchemaName")) {
      finalKey = "DATABASE_SCHEMA"
    } 
    
    // Special case 4: DATABASE can come under DATABASE or MetaDataStoreType
    if (key.equalsIgnoreCase("DATABASE") || key.equalsIgnoreCase("MetadataStoreType")) {
      finalKey = "DATABASE"
    }
    
    // Special case 5: NodeId or Node_ID is possible
    if (key.equalsIgnoreCase("NODE_ID") || key.equalsIgnoreCase("NODEID")) {
      finalKey = "NODE_ID"
    }
  
    // Store the Key/Value pair
    metadataAPIConfig.setProperty(finalKey.toUpperCase, finalValue)
    logger.debug(finalKey.toUpperCase + " = " + value)
    pList = pList - finalKey.toUpperCase
  }



  def RefreshApiConfigForGivenNode(nodeId: String): Boolean = {
    val nd = mdMgr.Nodes.getOrElse(nodeId, null)
    if (nd == null) {
      logger.error("Node %s not found in metadata".format(nodeId))
      return false
    }

    metadataAPIConfig.setProperty("SERVICE_HOST", nd.nodeIpAddr)
    logger.trace("NodeIpAddr Based on node(%s) => %s".format(nodeId,nd.nodeIpAddr))
    metadataAPIConfig.setProperty("SERVICE_PORT", nd.nodePort.toString)
    logger.trace("NodeIpAddr Based on node(%s) => %d".format(nodeId,nd.nodePort))

    val clusterId = nd.ClusterId

    val cluster = mdMgr.ClusterCfgs.getOrElse(nd.ClusterId, null)
    if (cluster == null) {
      logger.error("Cluster not found for Node %s  & ClusterId : %s".format(nodeId, nd.ClusterId))
      return false
    }

    val zooKeeperInfo = cluster.cfgMap.getOrElse("ZooKeeperInfo", null)
    if (zooKeeperInfo == null) {
      logger.error("ZooKeeperInfo not found for Node %s  & ClusterId : %s".format(nodeId, nd.ClusterId))
      return false
    }

    val jarPaths = if (nd.JarPaths == null) Set[String]() else nd.JarPaths.map(str => str.replace("\"", "").trim).filter(str => str.size > 0).toSet
    if (jarPaths.size == 0) {
      logger.error("Not found valid JarPaths.")
      return false
    }
    else{
      metadataAPIConfig.setProperty("JAR_PATHS", jarPaths.mkString(","))
      logger.debug("JarPaths Based on node(%s) => %s".format(nodeId,jarPaths))
      val jarDir = compact(render(jarPaths(0))).replace("\"", "").trim
      metadataAPIConfig.setProperty("JAR_TARGET_DIR", jarDir)
      logger.debug("Jar_target_dir Based on node(%s) => %s".format(nodeId,jarDir))
    }

    implicit val jsonFormats: Formats = DefaultFormats
    val zKInfo = parse(zooKeeperInfo).extract[ZooKeeperInfo]

    val zkConnectString = zKInfo.ZooKeeperConnectString.replace("\"", "").trim
    metadataAPIConfig.setProperty("ZOOKEEPER_CONNECT_STRING", zkConnectString)
    logger.debug("ZOOKEEPER_CONNECT_STRING(based on nodeId) => " + zkConnectString)


    val zkNodeBasePath = zKInfo.ZooKeeperNodeBasePath.replace("\"", "").trim
    metadataAPIConfig.setProperty("ZNODE_PATH", zkNodeBasePath)
    logger.debug("ZNODE_PATH(based on nodeid) => " + zkNodeBasePath)

    val zkSessionTimeoutMs1 = if (zKInfo.ZooKeeperSessionTimeoutMs == None || zKInfo.ZooKeeperSessionTimeoutMs == null) 0 else zKInfo.ZooKeeperSessionTimeoutMs.get.toString.toInt
    // Taking minimum values in case if needed
    val zkSessionTimeoutMs = if (zkSessionTimeoutMs1 <= 0) 1000 else zkSessionTimeoutMs1
    metadataAPIConfig.setProperty("ZK_SESSION_TIMEOUT_MS", zkSessionTimeoutMs.toString)
    logger.debug("ZK_SESSION_TIMEOUT_MS(based on nodeId) => " + zkSessionTimeoutMs)

    val zkConnectionTimeoutMs1 = if (zKInfo.ZooKeeperConnectionTimeoutMs == None || zKInfo.ZooKeeperConnectionTimeoutMs == null) 0 else zKInfo.ZooKeeperConnectionTimeoutMs.get.toString.toInt
    // Taking minimum values in case if needed
    val zkConnectionTimeoutMs = if (zkConnectionTimeoutMs1 <= 0) 30000 else zkConnectionTimeoutMs1
    metadataAPIConfig.setProperty("ZK_CONNECTION_TIMEOUT_MS", zkConnectionTimeoutMs.toString)
    logger.debug("ZK_CONNECTION_TIMEOUT_MS(based on nodeId) => " + zkConnectionTimeoutMs)
    true
  }

  def LoadAllObjectsIntoCache {
    try {
      val configAvailable = ClusterCfgUtils.LoadAllConfigObjectsIntoCache
      if( configAvailable ){
	RefreshApiConfigForGivenNode(metadataAPIConfig.getProperty("NODE_ID"))
      }
      else{
	logger.debug("Assuming bootstrap... No config objects in persistent store")
      }
      startup = true
      var objectsChanged = new Array[BaseElemDef](0)
      var operations = new Array[String](0)
      val maxTranId = DAOUtils.GetTranId
      logger.debug("Max Transaction Id => " + maxTranId)
      var keys = scala.collection.mutable.Set[com.ligadata.keyvaluestore.Key]()
      metadataStore.getAllKeys({ (key: Key) => keys.add(key) })
      val keyArray = keys.toArray
      if (keyArray.length == 0) {
        logger.debug("No objects available in the Database")
        return
      }
      keyArray.foreach(key => {
        val obj = DAOUtils.GetObject(key, metadataStore)
        val mObj = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte]).asInstanceOf[BaseElemDef]
        if (mObj != null) {
          if (mObj.tranId <= maxTranId) {
            Utils.AddObjectToCache(mObj, MdMgr.GetMdMgr)
            DAOUtils.DownloadJarFromDB(mObj)
          } else {
            logger.debug("The transaction id of the object => " + mObj.tranId)
            Utils.AddObjectToCache(mObj, MdMgr.GetMdMgr)
            DAOUtils.DownloadJarFromDB(mObj)
            logger.error("Transaction is incomplete with the object " + DAOUtils.KeyAsStr(key) + ",we may not have notified engine, attempt to do it now...")
            objectsChanged = objectsChanged :+ mObj
            if (mObj.IsActive) {
              operations = for (op <- objectsChanged) yield "Add"
            } else {
              operations = for (op <- objectsChanged) yield "Remove"
            }
          }
        } else {
          throw InternalErrorException("serializer.Deserialize returned a null object")
        }
      })
      if (objectsChanged.length > 0) {
        Utils.NotifyEngine(objectsChanged, operations)
      }
      startup = false
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

  @throws(classOf[MissingPropertyException])
  @throws(classOf[InvalidPropertyException])
  def readMetadataAPIConfigFromPropertiesFile(configFile: String): Unit = {
    try {
      if (propertiesAlreadyLoaded) {
        logger.debug("Configuratin properties already loaded, skipping the load configuration step")
        return ;
      }

      val (prop, failStr) = com.ligadata.Utils.Utils.loadConfiguration(configFile.toString, true)
      if (failStr != null && failStr.size > 0) {
        logger.error(failStr)
        return
      }
      if (prop == null) {
        logger.error("Failed to load configuration")
        return
      }
      
      // some zookeper vals can be safely defaulted to.
      setPropertyFromConfigFile("NODE_ID","Undefined")
      setPropertyFromConfigFile("API_LEADER_SELECTION_ZK_NODE","/ligadata")
      setPropertyFromConfigFile("ZK_SESSION_TIMEOUT_MS","3000")
      setPropertyFromConfigFile("ZK_CONNECTION_TIMEOUT_MS","3000")

      // Loop through and set the rest of the values.
      val eProps1 = prop.propertyNames();
      while (eProps1.hasMoreElements()) {
        val key = eProps1.nextElement().asInstanceOf[String]
        val value = prop.getProperty(key);
        setPropertyFromConfigFile(key,value)
      }
      pList.map(v => logger.error(v+" remains unset"))
      propertiesAlreadyLoaded = true;

    } catch {
      case e: Exception =>
        logger.error("Failed to load configuration: " + e.getMessage)
        sys.exit(1)
    }
  }

  def InitMdMgr(configFile: String) {
    MdMgr.GetMdMgr.truncate
    val mdLoader = new MetadataLoad(MdMgr.mdMgr, "", "", "", "")
    mdLoader.initialize
    readMetadataAPIConfigFromPropertiesFile(configFile)
    OpenDbStore(GetMetadataAPIConfig.getProperty("DATABASE"))
    LoadAllObjectsIntoCache
    CloseDbStore
    InitSecImpl
    initZkListener
  }

  def InitMdMgrFromBootStrap(configFile: String) {
    MdMgr.GetMdMgr.truncate
    val mdLoader = new MetadataLoad(MdMgr.mdMgr, "", "", "", "")
    mdLoader.initialize
    readMetadataAPIConfigFromPropertiesFile(configFile)
    initZkListener
    OpenDbStore(GetMetadataAPIConfig.getProperty("DATABASE"))
    LoadAllObjectsIntoCache
    InitSecImpl
    isInitilized = true
    logger.info("Metadata synching is now available.")
  }

  /**
   * Create a listener to monitor Meatadata Cache
   */
  private def initZkListener: Unit = {
     // Set up a zk listener for metadata invalidation   metadataAPIConfig.getProperty("AUDIT_IMPL_CLASS").trim
    var znodePath = metadataAPIConfig.getProperty("ZNODE_PATH")
    if (znodePath != null) znodePath = znodePath.trim + "/metadataupdate" else return
    var zkConnectString = metadataAPIConfig.getProperty("ZOOKEEPER_CONNECT_STRING")
    if (zkConnectString != null) zkConnectString = zkConnectString.trim else return

    if (zkConnectString != null && zkConnectString.isEmpty() == false && znodePath != null && znodePath.isEmpty() == false) {
      try {
        CreateClient.CreateNodeIfNotExists(zkConnectString, znodePath)
        zkListener = new ZooKeeperListener
        zkListener.CreateListener(zkConnectString, znodePath, UpdateMetadata, 3000, 3000)
      } catch {
        case e: Exception => {
          logger.error("Failed to initialize ZooKeeper Connection. Reason:%s Message:%s".format(e.getCause, e.getMessage))
          throw e
        }
      }
    }   
  }
  
  /**
   * shutdownZkListener - should be called by application using MetadataAPIImpl directly to disable synching of Metadata cache.
   */
  def shutdownZkListener: Unit = {
    try {
      CloseZKSession
      if (zkListener != null) {
        zkListener.Shutdown
      }
    } catch {
        case e: Exception => {
          logger.error("Error trying to shutdown zookeeper listener.  ")
          throw e
        }
      }
  }
  
 /**
  * UpdateMetadata - This is a callback function for the Zookeeper Listener.  It will get called when we detect Metadata being updated from
  *                  a different metadataImpl service.
  */
  def UpdateMetadata(receivedJsonStr: String): Unit = {
    logger.debug("Process ZooKeeper notification " + receivedJsonStr)

    if (receivedJsonStr == null || receivedJsonStr.size == 0 || !isInitilized) {
      // nothing to do
      logger.info("Metadata synching is not available.")
      return
    }

    val zkTransaction = JsonSerializer.parseZkTransaction(receivedJsonStr, "JSON")
    UpdateMdMgr(zkTransaction)        
  }

  def InitMdMgr(mgr: MdMgr, database: String, databaseHost: String, databaseSchema: String, databaseLocation: String) {

    SetLoggerLevel(Level.TRACE)
    val mdLoader = new MetadataLoad(mgr, "", "", "", "")
    mdLoader.initialize

    metadataAPIConfig.setProperty("DATABASE", database)
    metadataAPIConfig.setProperty("DATABASE_HOST", databaseHost)
    metadataAPIConfig.setProperty("DATABASE_SCHEMA", databaseSchema)
    metadataAPIConfig.setProperty("DATABASE_LOCATION", databaseLocation)

    OpenDbStore(GetMetadataAPIConfig.getProperty("DATABASE"))
    LoadAllObjectsIntoCache
    InitSecImpl
  }
}
