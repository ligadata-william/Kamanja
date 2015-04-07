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
//import scala.reflect.runtime.universe._
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

case class Attr(NameSpace: String, Name: String, Version: Int, Type: TypeDef)

case class MessageStruct(NameSpace: String, Name: String, FullName: String, Version: Int, JarName: String, PhysicalName: String, DependencyJars: List[String], Attributes: List[Attr])
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

  private var tableStoreMap: Map[String, DataStore] = Map()

  def CloseZKSession: Unit = lock.synchronized {
    if (zkc != null) {
      logger.debug("Closing zookeeper session ..")
      try {
        zkc.close()
        logger.debug("Closed zookeeper session ..")
        zkc = null
      } catch {
        case e: Exception => {
          logger.error("Unexpected Error while closing zookeeper session: " + e.getMessage())
        }
      }
    }
  }
  
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
    loadJar(classLoader,implJarName)
    
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
    loadJar(classLoader,implJarName)
    
    // All is good, create the new class 
    var className = Class.forName(implClassName, true, classLoader.loader).asInstanceOf[Class[com.ligadata.olep.metadata.AuditAdapter]]
    auditObj = className.newInstance
    auditObj.init
    logger.debug("Created class "+ className.getName)    
  }
 
  /**
   * loadJar - load the specified jar into the classLoader
   */
  private def loadJar (classLoader : MetadataLoader, implJarName: String): Unit = {
    // Add the Jarfile to the class loader
    val jarPaths = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_PATHS").split(",").toSet
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
   * checkAuth
   */
  def checkAuth (usrid:Option[String], password:Option[String], role: Option[String], privilige: String): Boolean = {  

    var authParms: java.util.Properties = new Properties
    // Do we want to run AUTH?
    if (!metadataAPIConfig.getProperty("DO_AUTH").equalsIgnoreCase("YES")) return true

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
  
  /**
   * getSSLCertificatePath
   */
  def getSSLCertificatePath: String = {
    val certPath = metadataAPIConfig.getProperty("SSL_CERTIFICATE")  
    if (certPath != null) return certPath
    ""
  }

  /**
   * InitZooKeeper - Establish a connection to zookeeper
   */
  def InitZooKeeper: Unit = {
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

  def GetMetadataAPIConfig: Properties = {
    metadataAPIConfig
  }

  def SetLoggerLevel(level: Level) {
    logger.setLevel(level);
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

  def oStore = otherStore

  def KeyAsStr(k: com.ligadata.keyvaluestore.Key): String = {
    val k1 = k.toArray[Byte]
    new String(k1)
  }

  def ValueAsStr(v: com.ligadata.keyvaluestore.Value): String = {
    val v1 = v.toArray[Byte]
    new String(v1)
  }

  def GetObject(key: com.ligadata.keyvaluestore.Key, store: DataStore): IStorage = {
    try {
      object o extends IStorage {
        var key = new com.ligadata.keyvaluestore.Key;
        var value = new com.ligadata.keyvaluestore.Value

        def Key = key
        def Value = value
        def Construct(k: com.ligadata.keyvaluestore.Key, v: com.ligadata.keyvaluestore.Value) =
          {
            key = k;
            value = v;
          }
      }

      var k = key
      logger.debug("Found the object in the store, key => " + KeyAsStr(k))
      store.get(k, o)
      o
    } catch {
      case e: KeyNotFoundException => {
        logger.debug("The object " + KeyAsStr(key) + " is not found in the database. Error => " + e.getMessage())
        throw new ObjectNotFoundException(e.getMessage())
      }
      case e: Exception => {
        e.printStackTrace()
        logger.debug("The object " + KeyAsStr(key) + " is not found in the database, Generic Exception: Error => " + e.getMessage())
        throw new ObjectNotFoundException(e.getMessage())
      }
    }
  }

  def GetObject(key: String, store: DataStore): IStorage = {
    try {
      var k = new com.ligadata.keyvaluestore.Key
      for (c <- key) {
        k += c.toByte
      }
      GetObject(k, store)
    }
  }

  def SaveObject(key: String, value: Array[Byte], store: DataStore) {
    val t = store.beginTx
    object obj extends IStorage {
      var k = new com.ligadata.keyvaluestore.Key
      var v = new com.ligadata.keyvaluestore.Value
      for (c <- key) {
        k += c.toByte
      }
      for (c <- value) {
        v += c
      }
      def Key = k
      def Value = v
      def Construct(Key: com.ligadata.keyvaluestore.Key, Value: com.ligadata.keyvaluestore.Value) = {}
    }
    try {
      store.put(obj)
      store.commitTx(t)
    } catch {
      case e: Exception => {
        logger.debug("Failed to insert/update object for : " + key)
        store.endTx(t)
        throw new UpdateStoreFailedException("Failed to insert/update object for : " + key)
      }
    }
  }

  def SaveObjectList(keyList: Array[String], valueList: Array[Array[Byte]], store: DataStore) {
    var i = 0
    val t = store.beginTx
    var storeObjects = new Array[IStorage](keyList.length)
    keyList.foreach(key => {
      var value = valueList(i)
      object obj extends IStorage {
        var k = new com.ligadata.keyvaluestore.Key
        var v = new com.ligadata.keyvaluestore.Value
        for (c <- key) {
          k += c.toByte
        }
        for (c <- value) {
          v += c
        }
        def Key = k
        def Value = v
        def Construct(Key: com.ligadata.keyvaluestore.Key, Value: com.ligadata.keyvaluestore.Value) = {}
      }
      storeObjects(i) = obj
      i = i + 1
    })
    try {
      store.putBatch(storeObjects)
      store.commitTx(t)
    } catch {
      case e: Exception => {
        logger.debug("Failed to insert/update object for : " + keyList.mkString(","))
        store.endTx(t)
        throw new UpdateStoreFailedException("Failed to insert/update object for : " + keyList.mkString(","))
      }
    }
  }


  def RemoveObjectList(keyList: Array[String], store: DataStore) {
    var i = 0
    val t = store.beginTx
    val KeyList = new Array[Key](keyList.length)
    keyList.foreach(key => {
      var k = new com.ligadata.keyvaluestore.Key
      for (c <- key) {
        k += c.toByte
      }
      KeyList(i) = k
      i = i + 1
    })
    try {
      store.delBatch(KeyList)
      store.commitTx(t)
    } catch {
      case e: Exception => {
        logger.debug("Failed to delete object batch for : " + keyList.mkString(","))
        store.endTx(t)
        throw new UpdateStoreFailedException("Failed to delete object batch for : " + keyList.mkString(","))
      }
    }
  }

  // If tables are different, an internal utility function
  def getTable(obj: BaseElemDef): String = {
    obj match {
      case o: ModelDef => {
        "models"
      }
      case o: MessageDef => {
        "messages"
      }
      case o: ContainerDef => {
        "containers"
      }
      case o: FunctionDef => {
        "functions"
      }
      case o: AttributeDef => {
        "concepts"
      }
      case o: BaseTypeDef => {
        "types"
      }
      case _ => {
        logger.error("getTable is not implemented for objects of type " + obj.getClass.getName)
        throw new InternalErrorException("getTable is not implemented for objects of type " + obj.getClass.getName)
      }
    }
  }

  def getObjectType(obj: BaseElemDef): String = {
    val className = obj.getClass().getName();
    className.split("\\.").last
  }

  // 
  // The following batch function is useful when we store data in single table
  // If we use Storage component library, note that table itself is associated with a single
  // database connection( which itself can be mean different things depending on the type
  // of datastore, such as cassandra, hbase, etc..)
  // 
  def SaveObjectList(objList: Array[BaseElemDef], store: DataStore) {
    logger.debug("Save " + objList.length + " objects in a single transaction ")
    val tranId = GetNewTranId
    var keyList = new Array[String](objList.length)
    var valueList = new Array[Array[Byte]](objList.length)
    try {
      var i = 0;
      objList.foreach(obj => {
        obj.tranId = tranId
        val key = (getObjectType(obj) + "." + obj.FullNameWithVer).toLowerCase
        var value = serializer.SerializeObjectToByteArray(obj)
        keyList(i) = key
        valueList(i) = value
        i = i + 1
      })
      SaveObjectList(keyList, valueList, store)
    } catch {
      case e: Exception => {
        logger.debug("Failed to insert/update object for : " + keyList.mkString(","))
        throw new UpdateStoreFailedException("Failed to insert/update object for : " + keyList.mkString(","))
      }
    }
  }

  // The following batch function is useful when we store data in multiple tables
  // If we use Storage component library, note that each table is associated with a different
  // database connection( which itself can be mean different things depending on the type
  // of datastore, such as cassandra, hbase, etc..)
  def SaveObjectList(objList: Array[BaseElemDef]) {
    logger.debug("Save " + objList.length + " objects in a single transaction ")
    val tranId = GetNewTranId
    var keyList = new Array[String](objList.length)
    var valueList = new Array[Array[Byte]](objList.length)
    var tableList = new Array[String](objList.length)
    try {
      var i = 0;
      objList.foreach(obj => {
        obj.tranId = tranId
        val key = (getObjectType(obj) + "." + obj.FullNameWithVer).toLowerCase
        var value = serializer.SerializeObjectToByteArray(obj)
        keyList(i) = key
        valueList(i) = value
        tableList(i) = getTable(obj)
        i = i + 1
      })
      i = 0
      var storeObjects = new Array[IStorage](keyList.length)
      keyList.foreach(key => {
        var value = valueList(i)
        object obj extends IStorage {
          var k = new com.ligadata.keyvaluestore.Key
          var v = new com.ligadata.keyvaluestore.Value
          for (c <- key) {
            k += c.toByte
          }
          for (c <- value) {
            v += c
          }
          def Key = k
          def Value = v
          def Construct(Key: com.ligadata.keyvaluestore.Key, Value: com.ligadata.keyvaluestore.Value) = {}
        }
        storeObjects(i) = obj
        i = i + 1
      })
      val storeType = GetMetadataAPIConfig.getProperty("DATABASE")
      storeType match {
        case "cassandra" => {
          val store = tableStoreMap(tableList(0))
          val t = store.beginTx
          KeyValueBatchTransaction.putCassandraBatch(tableList, store, storeObjects)
          store.commitTx(t)
        }
        case _ => {
          var i = 0
          tableList.foreach(table => {
            val store = tableStoreMap(table)
            val t = store.beginTx
            store.put(storeObjects(i))
            store.commitTx(t)
            i = i + 1
          })
        }
      }
    } catch {
      case e: Exception => {
        logger.debug("Failed to insert/update object for : " + keyList.mkString(","))
        throw new UpdateStoreFailedException("Failed to insert/update object for : " + keyList.mkString(","))
      }
    }
  }

  def SaveObject(key: String, value: String, store: DataStore) {
    var ba = serializer.SerializeObjectToByteArray(value)
    SaveObject(key, ba, store)
  }

  def UpdateObject(key: String, value: Array[Byte], store: DataStore) {
    SaveObject(key, value, store)
  }

  def ZooKeeperMessage(objList: Array[BaseElemDef], operations: Array[String]): Array[Byte] = {
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
        PutTranId(objList(0).tranId)
        return
      }
      val data = ZooKeeperMessage(objList, operations)
      InitZooKeeper
      val znodePath = GetMetadataAPIConfig.getProperty("ZNODE_PATH") + "/metadataupdate"
      logger.debug("Set the data on the zookeeper node " + znodePath)
      zkc.setData().forPath(znodePath, data)
      PutTranId(objList(0).tranId)
    } catch {
      case e: Exception => {
        throw new InternalErrorException("Failed to notify a zookeeper message from the objectList " + e.getMessage())
      }
    }
  }

  def GetNewTranId: Long = {
    try {
      val key = "transaction_id"
      val obj = GetObject(key, transStore)
      val idStr = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte]).asInstanceOf[String]
      idStr.toLong + 1
    } catch {
      case e: ObjectNotFoundException => {
        // first time
        1
      }
      case e: Exception => {
        throw new TranIdNotFoundException("Unable to retrieve the transaction id " + e.toString)
      }
    }
  }

  def GetTranId: Long = {
    try {
      val key = "transaction_id"
      val obj = GetObject(key, transStore)
      val idStr = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte]).asInstanceOf[String]
      idStr.toLong
    } catch {
      case e: ObjectNotFoundException => {
        // first time
        0
      }
      case e: Exception => {
        throw new TranIdNotFoundException("Unable to retrieve the transaction id " + e.toString)
      }
    }
  }

  def PutTranId(tId: Long) = {
    try {
      val key = "transaction_id"
      val value = tId.toString
      SaveObject(key, value, transStore)
    } catch {
      case e: Exception => {
        throw new UpdateStoreFailedException("Unable to Save the transaction id " + tId + ":" + e.getMessage())
      }
    }
  }

  def SaveObject(obj: BaseElemDef, mdMgr: MdMgr) {
    try {
      val key = (getObjectType(obj) + "." + obj.FullNameWithVer).toLowerCase
      obj.tranId = GetNewTranId
      //val value = JsonSerializer.SerializeObjectToJson(obj)
      logger.debug("Serialize the object: name of the object => " + key)
      var value = serializer.SerializeObjectToByteArray(obj)
      obj match {
        case o: ModelDef => {
          logger.debug("Adding the model to the cache: name of the object =>  " + key)
          SaveObject(key, value, modelStore)
          mdMgr.AddModelDef(o)
        }
        case o: MessageDef => {
          logger.debug("Adding the message to the cache: name of the object =>  " + key)
          SaveObject(key, value, messageStore)
          mdMgr.AddMsg(o)
        }
        case o: ContainerDef => {
          logger.debug("Adding the container to the cache: name of the object =>  " + key)
          SaveObject(key, value, containerStore)
          mdMgr.AddContainer(o)
        }
        case o: FunctionDef => {
          val funcKey = (obj.getClass().getName().split("\\.").last + "." + o.typeString).toLowerCase
          logger.debug("Adding the function to the cache: name of the object =>  " + funcKey)
          SaveObject(funcKey, value, functionStore)
          mdMgr.AddFunc(o)
        }
        case o: AttributeDef => {
          logger.debug("Adding the attribute to the cache: name of the object =>  " + key)
          SaveObject(key, value, conceptStore)
          mdMgr.AddAttribute(o)
        }
        case o: ScalarTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + key)
          SaveObject(key, value, typeStore)
          mdMgr.AddScalar(o)
        }
        case o: ArrayTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + key)
          SaveObject(key, value, typeStore)
          mdMgr.AddArray(o)
        }
        case o: ArrayBufTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + key)
          SaveObject(key, value, typeStore)
          mdMgr.AddArrayBuffer(o)
        }
        case o: ListTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + key)
          SaveObject(key, value, typeStore)
          mdMgr.AddList(o)
        }
        case o: QueueTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + key)
          SaveObject(key, value, typeStore)
          mdMgr.AddQueue(o)
        }
        case o: SetTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + key)
          SaveObject(key, value, typeStore)
          mdMgr.AddSet(o)
        }
        case o: TreeSetTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + key)
          SaveObject(key, value, typeStore)
          mdMgr.AddTreeSet(o)
        }
        case o: SortedSetTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + key)
          SaveObject(key, value, typeStore)
          mdMgr.AddSortedSet(o)
        }
        case o: MapTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + key)
          SaveObject(key, value, typeStore)
          mdMgr.AddMap(o)
        }
        case o: ImmutableMapTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + key)
          SaveObject(key, value, typeStore)
          mdMgr.AddImmutableMap(o)
        }
        case o: HashMapTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + key)
          SaveObject(key, value, typeStore)
          mdMgr.AddHashMap(o)
        }
        case o: TupleTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + key)
          SaveObject(key, value, typeStore)
          mdMgr.AddTupleType(o)
        }
        case o: ContainerTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + key)
          SaveObject(key, value, typeStore)
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
        logger.warn("Failed to Save the object(" + obj.FullNameWithVer + "): " + e.getMessage())
      }
    }
  }

  def UpdateObjectInDB(obj: BaseElemDef) {
    try {
      val key = (getObjectType(obj) + "." + obj.FullNameWithVer).toLowerCase

      logger.debug("Serialize the object: name of the object => " + key)
      var value = serializer.SerializeObjectToByteArray(obj)
      obj match {
        case o: ModelDef => {
          logger.debug("Updating the model in the DB: name of the object =>  " + key)
          UpdateObject(key, value, modelStore)
        }
        case o: MessageDef => {
          logger.debug("Updating the message in the DB: name of the object =>  " + key)
          UpdateObject(key, value, messageStore)
        }
        case o: ContainerDef => {
          logger.debug("Updating the container in the DB: name of the object =>  " + key)
          UpdateObject(key, value, containerStore)
        }
        case o: FunctionDef => {
          val funcKey = (obj.getClass().getName().split("\\.").last + "." + o.typeString).toLowerCase
          logger.debug("Updating the function in the DB: name of the object =>  " + funcKey)
          UpdateObject(funcKey, value, functionStore)
        }
        case o: AttributeDef => {
          logger.debug("Updating the attribute in the DB: name of the object =>  " + key)
          UpdateObject(key, value, conceptStore)
        }
        case o: ScalarTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + key)
          UpdateObject(key, value, typeStore)
        }
        case o: ArrayTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + key)
          UpdateObject(key, value, typeStore)
        }
        case o: ArrayBufTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + key)
          UpdateObject(key, value, typeStore)
        }
        case o: ListTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + key)
          UpdateObject(key, value, typeStore)
        }
        case o: QueueTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + key)
          UpdateObject(key, value, typeStore)
        }
        case o: SetTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + key)
          UpdateObject(key, value, typeStore)
        }
        case o: TreeSetTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + key)
          UpdateObject(key, value, typeStore)
        }
        case o: SortedSetTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + key)
          UpdateObject(key, value, typeStore)
        }
        case o: MapTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + key)
          UpdateObject(key, value, typeStore)
        }
        case o: ImmutableMapTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + key)
          UpdateObject(key, value, typeStore)
        }
        case o: HashMapTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + key)
          UpdateObject(key, value, typeStore)
        }
        case o: TupleTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + key)
          UpdateObject(key, value, typeStore)
        }
        case o: ContainerTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + key)
          UpdateObject(key, value, typeStore)
        }
        case _ => {
          logger.error("UpdateObject is not implemented for objects of type " + obj.getClass.getName)
        }
      }
    } catch {
      case e: AlreadyExistsException => {
        logger.error("Failed to Update the object(" + obj.FullNameWithVer + "): " + e.getMessage())
      }
      case e: Exception => {
        logger.error("Failed to Update the object(" + obj.FullNameWithVer + "): " + e.getMessage())
      }
    }
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

  def UploadJarsToDB(obj: BaseElemDef) {
    try {
      var keyList = new Array[String](0)
      var valueList = new Array[Array[Byte]](0)

      val jarPaths = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_PATHS").split(",").toSet

      logger.debug("jarpaths => " + jarPaths)

      keyList = keyList :+ obj.jarName
      var jarName = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_TARGET_DIR") + "/" + obj.jarName
      var value = GetJarAsArrayOfBytes(jarName)
      logger.debug("Update the jarfile (size => " + value.length + ") of the object: " + obj.jarName)
      valueList = valueList :+ value

      if (obj.DependencyJarNames != null) {
        obj.DependencyJarNames.foreach(j => {
          // do not upload if it already exist, minor optimization
          var loadObject = false
          if (j.endsWith(".jar")) {
            jarName = JarPathsUtils.GetValidJarFile(jarPaths, j)            
            value = GetJarAsArrayOfBytes(jarName)
            var mObj: IStorage = null
            try {
              mObj = GetObject(j, jarStore)
            } catch {
              case e: ObjectNotFoundException => {
                loadObject = true
              }
            }

            if (loadObject == false) {
              val ba = mObj.Value.toArray[Byte]
              val fs = ba.length
              if (fs != value.length) {
                logger.debug("A jar file already exists, but it's size (" + fs + ") doesn't match with the size of the Jar (" +
                  jarName + "," + value.length + ") of the object(" + obj.FullNameWithVer + ")")
                loadObject = true
              }
            }

            if (loadObject) {
              keyList = keyList :+ j
              logger.debug("Update the jarfile (size => " + value.length + ") of the object: " + j)
              valueList = valueList :+ value
            } else {
              logger.debug("The jarfile " + j + " already exists in DB.")
            }
          }
        })
      }
      if (keyList.length > 0) {
        SaveObjectList(keyList, valueList, jarStore)
      }
    } catch {
      case e: Exception => {
        throw new InternalErrorException("Failed to Update the Jar of the object(" + obj.FullNameWithVer + "): " + e.getMessage())
      }
    }
  }

  def UploadJarToDB(jarName: String) {
    try {
      val f = new File(jarName)
      if (f.exists()) {
        var key = f.getName()
        var value = GetJarAsArrayOfBytes(jarName)
        logger.debug("Update the jarfile (size => " + value.length + ") of the object: " + jarName)
        SaveObject(key, value, jarStore)
        var apiResult = new ApiResult(ErrorCodeConstants.Success, "UploadJarToDB", null, ErrorCodeConstants.Upload_Jar_Successful + ":" + jarName)
        apiResult.toString()

      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "UploadJarToDB", null, "Error : " + e.toString() + ErrorCodeConstants.Upload_Jar_Failed + ":" + jarName)
        apiResult.toString()
      }
    }
  }


  /**
   *  UploadJarToDB - Interface to the SERIVCES
   */
  def UploadJarToDB(jarName:String,byteArray: Array[Byte]): String = {
    try {
        var key = jarName
        var value = byteArray
        logger.debug("Update the jarfile (size => " + value.length + ") of the object: " + jarName)
        SaveObject(key, value, jarStore)
        var apiResult = new ApiResult(ErrorCodeConstants.Success, "UploadJarToDB", null, ErrorCodeConstants.Upload_Jar_Successful + ":" + jarName)
        apiResult.toString()
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "UploadJarToDB", null, "Error : " + e.toString() + ErrorCodeConstants.Upload_Jar_Failed + ":" + jarName)
        apiResult.toString()
      }
    }
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
        val mObj = GetObject(key, jarStore)
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

  def DownloadJarFromDB(obj: BaseElemDef) {
    try {
      //val key:String = (getObjectType(obj) + "." + obj.FullNameWithVer).toLowerCase
      if (obj.jarName == null) {
        logger.debug("The object " + obj.FullNameWithVer + " has no jar associated with it. Nothing to download..")
        return
      }
      var allJars = GetDependantJars(obj)
      logger.debug("Found " + allJars.length + " dependant jars ")
      if (allJars.length > 0) {
        val jarPaths = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_PATHS").split(",").toSet
        jarPaths.foreach(jardir => {
          val dir = new File(jardir)
          if (!dir.exists()) {
            // attempt to create the missing directory
            dir.mkdir();
          }
        })
        
        val dirPath = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_TARGET_DIR")
        val dir = new File(dirPath)
        if (!dir.exists()) {
          // attempt to create the missing directory
          dir.mkdir();
        }
        
        allJars.foreach(jar => {
          // download only if it doesn't already exists
          val b = IsDownloadNeeded(jar, obj)
          if (b == true) {
            val key = jar
            val mObj = GetObject(key, jarStore)
            val ba = mObj.Value.toArray[Byte]
            val jarName = dirPath + "/" + jar
            PutArrayOfBytesToJar(ba, jarName)
          }
        })
      }
    } catch {
      case e: Exception => {
        logger.error("Failed to download the Jar of the object(" + obj.FullNameWithVer + "): " + e.getMessage())
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

  def ModifyObject(obj: BaseElemDef, operation: String) {
    try {
      val o1 = UpdateObjectInCache(obj, operation, MdMgr.GetMdMgr)
      UpdateObjectInDB(o1)
    } catch {
      case e: ObjectNolongerExistsException => {
        logger.error("The object " + obj.FullNameWithVer + " nolonger exists in metadata : It may have been removed already")
      }
      case e: Exception => {
        throw new Exception("Unexpected error in ModifyObject: " + e.getMessage())
      }
    }
  }

  def DeleteObject(key: String, store: DataStore) {
    var k = new com.ligadata.keyvaluestore.Key
    for (c <- key) {
      k += c.toByte
    }
    val t = store.beginTx
    store.del(k)
    store.commitTx(t)
  }

  def DeleteObject(obj: BaseElemDef) {
    try {
      ModifyObject(obj, "Remove")
    } catch {
      case e: ObjectNolongerExistsException => {
        logger.error("The object " + obj.FullNameWithVer + " nolonger exists in metadata : It may have been removed already")
      }
      case e: Exception => {
        throw new Exception("Unexpected error in DeleteObject: " + e.getMessage())
      }
    }
  }

  def ActivateObject(obj: BaseElemDef) {
    try {
      ModifyObject(obj, "Activate")
    } catch {
      case e: ObjectNolongerExistsException => {
        logger.error("The object " + obj.FullNameWithVer + " nolonger exists in metadata : It may have been removed already")
      }
      case e: Exception => {
        throw new Exception("Unexpected error in ActivateObject: " + e.getMessage())
      }
    }
  }

  def DeactivateObject(obj: BaseElemDef) {
    try {
      ModifyObject(obj, "Deactivate")
    } catch {
      case e: ObjectNolongerExistsException => {
        logger.error("The object " + obj.FullNameWithVer + " nolonger exists in metadata : It may have been removed already")
      }
      case e: Exception => {
        throw new Exception("Unexpected error in DeactivateObject: " + e.getMessage())
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

  def GetDataStoreProperties(storeType: String, storeName: String, tableName: String): PropertyMap = {
    var connectinfo = new PropertyMap
    try {
      connectinfo += ("connectiontype" -> storeType)
      connectinfo += ("table" -> tableName)
      storeType match {
        case "hbase" => {
          var databaseHost = GetMetadataAPIConfig.getProperty("DATABASE_HOST")
          connectinfo += ("hostlist" -> databaseHost)
          connectinfo += ("schema" -> storeName)
        }
        case "hashmap" => {
          var databaseLocation = GetMetadataAPIConfig.getProperty("DATABASE_LOCATION")
          connectinfo += ("path" -> databaseLocation)
          connectinfo += ("schema" -> storeName)
          connectinfo += ("inmemory" -> "false")
          connectinfo += ("withtransaction" -> "true")
        }
        case "treemap" => {
          var databaseLocation = GetMetadataAPIConfig.getProperty("DATABASE_LOCATION")
          connectinfo += ("path" -> databaseLocation)
          connectinfo += ("schema" -> storeName)
          connectinfo += ("inmemory" -> "false")
          connectinfo += ("withtransaction" -> "true")
        }
        case "cassandra" => {
          var databaseHost = GetMetadataAPIConfig.getProperty("DATABASE_HOST")
          var databaseSchema = GetMetadataAPIConfig.getProperty("DATABASE_SCHEMA")
          connectinfo += ("hostlist" -> databaseHost)
          connectinfo += ("schema" -> databaseSchema)
          connectinfo += ("ConsistencyLevelRead" -> "ONE")
        }
        case _ => {
          throw new CreateStoreFailedException("The database type " + storeType + " is not supported yet ")
        }
      }
      connectinfo
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw new CreateStoreFailedException(e.getMessage())
      }
    }
  }


  @throws(classOf[CreateStoreFailedException])
  def GetDataStoreHandle(storeType: String, storeName: String, tableName: String): DataStore = {
    try {
      var connectinfo = GetDataStoreProperties(storeType,storeName,tableName)
      KeyValueManager.Get(connectinfo)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw new CreateStoreFailedException(e.getMessage())
      }
    }
  }


 /* @throws(classOf[CreateStoreFailedException])
  def GetAuditStoreHandle(storeType: String, storeName: String, tableName: String): AuditStore = {
    var as:AuditStore = null
    try {
      var connectinfo = GetDataStoreProperties(storeType,storeName,tableName)
      AuditStoreManager.Get(connectinfo)
    } catch {
      case e: Exception => {
        logger.warn("Unable to create database store connection for auditing: " + e.getMessage())
        as
      }
    }
  }*/

  @throws(classOf[CreateStoreFailedException])
  def OpenDbStore(storeType: String) {
    try {
      logger.debug("Opening datastore")
      metadataStore = GetDataStoreHandle(storeType, "metadata_store", "metadata_objects")
      configStore = GetDataStoreHandle(storeType, "config_store", "config_objects")
      jarStore = GetDataStoreHandle(storeType, "metadata_jars", "jar_store")
      transStore = GetDataStoreHandle(storeType, "metadata_trans", "transaction_id")
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
     // if (auditStore != null) {
     //   auditStore.Shutdown()
    //    auditStore = null
    //    logger.debug("auditStore closed")
    //  }
    } catch {
      case e: Exception => {
        throw e;
      }
    }
  }

  def AddType(typeText: String, format: String): String = {
    try {
      logger.debug("Parsing type object given as Json String..")
      val typ = JsonSerializer.parseType(typeText, "JSON")
      SaveObject(typ, MdMgr.GetMdMgr)
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

  def DumpTypeDef(typeDef: ScalarTypeDef) {
    logger.debug("NameSpace => " + typeDef.nameSpace)
    logger.debug("Name => " + typeDef.name)
    logger.debug("Version => " + typeDef.ver)
    logger.debug("Description => " + typeDef.description)
    logger.debug("Implementation class name => " + typeDef.implementationNm)
    logger.debug("Implementation jar name => " + typeDef.jarName)
  }

  def AddType(typeDef: BaseTypeDef): String = {
    try {
      var key = typeDef.FullNameWithVer
      var value = JsonSerializer.SerializeObjectToJson(typeDef);
      logger.debug("key => " + key + ",value =>" + value);
      SaveObject(typeDef, MdMgr.GetMdMgr)
       var apiResult = new ApiResult(ErrorCodeConstants.Success, "AddType", null, ErrorCodeConstants.Add_Type_Successful + ":" + key)
      apiResult.toString()
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddType" , null, "Error :" + e.toString() + ErrorCodeConstants.Add_Type_Failed+ ":" + typeDef.FullNameWithVer)
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
            SaveObject(typ, MdMgr.GetMdMgr)
            logger.debug("Type object name => " + typ.FullNameWithVer)
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
  def RemoveType(typeNameSpace: String, typeName: String, version: Int): String = {
    val key = typeNameSpace + "." + typeName + "." + version
    try {
      val typ = MdMgr.GetMdMgr.Type(typeNameSpace, typeName, version, true)
      typ match {
        case None =>
          None
          logger.debug("Type " + key + " is not found in the cache ")
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveType", null, ErrorCodeConstants.Remove_Type_Not_Found + ":" + key)
          apiResult.toString()
        case Some(ts) =>
          DeleteObject(ts.asInstanceOf[BaseElemDef])
         var apiResult = new ApiResult(ErrorCodeConstants.Success, "RemoveType", null, ErrorCodeConstants.Remove_Type_Successful + ":" + key)
          apiResult.toString()
      }
    } catch {
      case e: ObjectNolongerExistsException => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveType", null, "Error: " + e.toString() + ErrorCodeConstants.Remove_Type_Failed + ":" + key)
        apiResult.toString()
      }
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveType", null, "Error: " + e.toString() + ErrorCodeConstants.Remove_Type_Failed + ":" + key)
        apiResult.toString()
      }
    }
  }

  def UpdateType(typeJson: String, format: String): String = {
    implicit val jsonFormats: Formats = DefaultFormats
    val typeDef = JsonSerializer.parseType(typeJson, "JSON")
    val key = typeDef.nameSpace + "." + typeDef.name + "." + typeDef.ver
    var apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateType" , null, ErrorCodeConstants.Update_Type_Internal_Error + ":" + key)
    try {
      val fName = MdMgr.MkFullName(typeDef.nameSpace, typeDef.name)
      val latestType = MdMgr.GetMdMgr.Types(typeDef.nameSpace, typeDef.name, true, true)
      latestType match {
        case None =>
          None
          logger.debug("No types with the name " + fName + " are found ")
          apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateType", null, ErrorCodeConstants.Update_Type_Failed_Not_Found + ":" + key)
        case Some(ts) =>
          val tsa = ts.toArray
          logger.debug("Found " + tsa.length + " types ")
          if (tsa.length > 1) {
            apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateType", null, ErrorCodeConstants.Update_Type_Failed_More_Than_One_Found + ":" + key)
          } else {
            val latestVersion = tsa(0)
            if (latestVersion.ver > typeDef.ver) {
              RemoveType(latestVersion.nameSpace, latestVersion.name, latestVersion.ver)
              AddType(typeDef)
              apiResult = new ApiResult(ErrorCodeConstants.Success, "UpdateType", null, ErrorCodeConstants.Update_Type_Successful + ":" + key)
            } else {
              apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateType", null, ErrorCodeConstants.Update_Type_Failed_Higher_Version_Required + ":" + key)
            }
          }
      }
      apiResult.toString()
    }catch {
      case e: MappingException => {
        logger.debug("Failed to parse the type, json => " + typeJson + ",Error => " + e.getMessage())
        apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateType", null, "Error :" + e.toString() + ErrorCodeConstants.Update_Type_Failed + ":" + key)
        apiResult.toString()
      }
      case e: AlreadyExistsException => {
        logger.debug("Failed to update the type, json => " + typeJson + ",Error => " + e.getMessage())
        apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateType", null, "Error :" + e.toString() + ErrorCodeConstants.Update_Type_Failed + ":" + key)
        apiResult.toString()
      }
      case e: Exception => {
        logger.debug("Failed to up the type, json => " + typeJson + ",Error => " + e.getMessage())
        apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateType", null, "Error :" + e.toString() + ErrorCodeConstants.Update_Type_Failed + ":" + key)
        apiResult.toString()
      }
    }
  }

  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
        UploadJarToDB(jarPath)
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

  def DumpAttributeDef(attrDef: AttributeDef) {
    logger.debug("NameSpace => " + attrDef.nameSpace)
    logger.debug("Name => " + attrDef.name)
    logger.debug("Type => " + attrDef.typeString)
  }

  def AddConcept(attributeDef: BaseAttributeDef): String = {
    val key = attributeDef.FullNameWithVer
    try {
      SaveObject(attributeDef, MdMgr.GetMdMgr)
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "AddConcept", null, ErrorCodeConstants.Add_Concept_Successful + ":" + key)
      apiResult.toString()
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddConcept", null, "Error :" + e.toString() + ErrorCodeConstants.Add_Concept_Failed + ":" + key)
        apiResult.toString()
      }
    }
  }

  def RemoveConcept(concept: AttributeDef): String = {
    var key = concept.nameSpace + ":" + concept.name
    try {
      DeleteObject(concept)
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "RemoveConcept", null, ErrorCodeConstants.Remove_Concept_Successful + ":" + key)
      apiResult.toString()
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveConcept", null, "Error :" + e.toString() + ErrorCodeConstants.Remove_Concept_Failed + ":" + key)
        apiResult.toString()
      }
    }
  }

  def RemoveConcept(key: String): String = {
    try {
      val c = MdMgr.GetMdMgr.Attributes(key, false, false)
      c match {
        case None =>
          None
          logger.debug("No concepts found ")
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveConcept", null, ErrorCodeConstants.Remove_Concept_Failed_Not_Available + ":" + key)
          apiResult.toString()
        case Some(cs) =>
          val conceptArray = cs.toArray
          conceptArray.foreach(concept => { DeleteObject(concept) })
          var apiResult = new ApiResult(ErrorCodeConstants.Success, "RemoveConcept", null, ErrorCodeConstants.Remove_Concept_Successful + ":" + key)//JsonSerializer.SerializeObjectListToJson(conceptArray))
          apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveConcept", null, "Error :" + e.toString() + ErrorCodeConstants.Remove_Concept_Failed + ":" + key)
        apiResult.toString()
      }
    }
  }

  def RemoveConcept(nameSpace:String, name:String, version:Int): String = {
    try {
      val c = MdMgr.GetMdMgr.Attribute(nameSpace,name,version, true)
      c match {
        case None =>
          None
          logger.debug("No concepts found ")
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveConcept", null, ErrorCodeConstants.Remove_Concept_Failed_Not_Available + ":" + nameSpace + "." + name + "." + version)
          apiResult.toString()
        case Some(cs) =>
          val concept = cs.asInstanceOf[AttributeDef]
          DeleteObject(concept)
          var apiResult = new ApiResult(ErrorCodeConstants.Success, "RemoveConcept", null, ErrorCodeConstants.Remove_Concept_Successful + ":" + nameSpace + "." + name + "." + version)//JsonSerializer.SerializeObjectListToJson(concept))
          apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveConcept", null, "Error :" + e.toString() + ErrorCodeConstants.Remove_Concept_Failed+ ":" + nameSpace + "." + name + "." + version)
        apiResult.toString()
      }
    }
  }

  def AddFunction(functionDef: FunctionDef): String = {
     val key = functionDef.FullNameWithVer
    try {
      val value = JsonSerializer.SerializeObjectToJson(functionDef)

      logger.debug("key => " + key + ",value =>" + value);
      SaveObject(functionDef, MdMgr.GetMdMgr)
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
      DeleteObject(functionDef)
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
      DeleteObject(key, functionStore)
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
          SaveObject(func, MdMgr.GetMdMgr)
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
        SaveObject(func, MdMgr.GetMdMgr)
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

  def AddDerivedConcept(conceptsText: String, format: String): String = {
    try {
      if (format != "JSON") {
        var apiResult = new ApiResult(ErrorCodeConstants.Not_Implemented_Yet, "AddDerivedConcept", null, ErrorCodeConstants.Not_Implemented_Yet_Msg + ":" + conceptsText + ".Format not JSON.")
        apiResult.toString()
      } else {
        var concept = JsonSerializer.parseDerivedConcept(conceptsText, format)
        var apiResult = new ApiResult(ErrorCodeConstants.Success, "AddDerivedConcept", null, ErrorCodeConstants.Add_Concept_Successful + ":" + conceptsText)
        apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants. Failure, "AddDerivedConcept", null, "Error :" + e.toString() + ErrorCodeConstants.Add_Concept_Failed + ":" + conceptsText)
        apiResult.toString()
      }
    }
  }

  def AddConcepts(conceptsText: String, format: String): String = {
    try {
      if (format != "JSON") {
        var apiResult = new ApiResult(ErrorCodeConstants.Not_Implemented_Yet, "AddConcepts", null, ErrorCodeConstants.Not_Implemented_Yet_Msg + ":" + conceptsText + ".Format not JSON.")
        apiResult.toString()
      } else {
        var conceptList = JsonSerializer.parseConceptList(conceptsText, format)
        conceptList.foreach(concept => {
          //logger.debug("Save concept object " + JsonSerializer.SerializeObjectToJson(concept))
          SaveObject(concept, MdMgr.GetMdMgr)
        })
        var apiResult = new ApiResult(ErrorCodeConstants.Success, "AddConcepts", null, ErrorCodeConstants.Add_Concept_Successful + ":" + conceptsText)
        apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddConcepts", null, "Error :" + e.toString() + ErrorCodeConstants.Add_Concept_Failed + ":" + conceptsText)
        apiResult.toString()
      }
    }
  }

  def UpdateConcept(concept: BaseAttributeDef): String = {
    val key = concept.FullNameWithVer
    try {
      if (IsConceptAlreadyExists(concept)) {
        concept.ver = concept.ver + 1
      }
      AddConcept(concept)
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "UpdateConcept", null, ErrorCodeConstants.Update_Concept_Successful + ":" + key)
      apiResult.toString()
    } catch {
      case e: AlreadyExistsException => {
        logger.debug("Failed to update the concept, key => " + key + ",Error => " + e.getMessage())
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateConcept", null, "Error :" + e.toString() + ErrorCodeConstants.Update_Concept_Failed + ":" + key)
        apiResult.toString()
      }
      case e: Exception => {
        logger.debug("Failed to update the concept, key => " + key + ",Error => " + e.getMessage())
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateConcept", null, "Error :" + e.toString() + ErrorCodeConstants.Update_Concept_Failed + ":" + key)
        apiResult.toString()
      }
    }
  }

  def UpdateConcepts(conceptsText: String, format: String): String = {
    try {
      if (format != "JSON") {
        var apiResult = new ApiResult(ErrorCodeConstants.Not_Implemented_Yet, "UpdateConcepts", null, ErrorCodeConstants.Not_Implemented_Yet_Msg + ":" + conceptsText + ".Format not JSON.")
        apiResult.toString()
      } else {
        var conceptList = JsonSerializer.parseConceptList(conceptsText, "JSON")
        conceptList.foreach(concept => {
          UpdateConcept(concept)
        })
        var apiResult = new ApiResult(ErrorCodeConstants.Success, "UpdateConcepts", null, ErrorCodeConstants.Update_Concept_Successful + ":" + conceptsText)
        apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateConcepts", null, "Error :" + e.toString() + ErrorCodeConstants.Update_Concept_Failed + ":" + conceptsText)
        apiResult.toString()
      }
    }
  }

  // RemoveConcepts take all concepts names to be removed as an Array
  def RemoveConcepts(concepts: Array[String]): String = {
    val json = ("ConceptList" -> concepts.toList)
    val jsonStr = pretty(render(json))
    try {
      concepts.foreach(c => { RemoveConcept(c) })
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "RemoveConcepts", null, ErrorCodeConstants.Remove_Concept_Successful + ":" + jsonStr)
      apiResult.toString()
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveConcepts", null, "Error :" + e.toString() + ErrorCodeConstants.Remove_Concept_Failed + ":" + jsonStr)
        apiResult.toString()
      }
    }
  }

  def AddContainerDef(contDef: ContainerDef): String = {
    var key = contDef.FullNameWithVer
    try {
      AddObjectToCache(contDef, MdMgr.GetMdMgr)
      UploadJarsToDB(contDef)
      var objectsAdded = AddMessageTypes(contDef, MdMgr.GetMdMgr)
      objectsAdded = objectsAdded :+ contDef
      SaveObjectList(objectsAdded, metadataStore)
      val operations = for (op <- objectsAdded) yield "Add"
      NotifyEngine(objectsAdded, operations)
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "AddContainerDef", null, ErrorCodeConstants.Add_Container_Successful + ":" + key)
      apiResult.toString()
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddContainerDef", null, "Error :" + e.toString() + ErrorCodeConstants.Add_Container_Failed + ":" + key)
        apiResult.toString()
      }
    }
  }

  def AddMessageDef(msgDef: MessageDef): String = {
    try {
      AddObjectToCache(msgDef, MdMgr.GetMdMgr)
      UploadJarsToDB(msgDef)
      var objectsAdded = AddMessageTypes(msgDef, MdMgr.GetMdMgr)
      objectsAdded = objectsAdded :+ msgDef
      SaveObjectList(objectsAdded, metadataStore)
      val operations = for (op <- objectsAdded) yield "Add"
      NotifyEngine(objectsAdded, operations)
      //check again
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "AddMessageDef", null,ErrorCodeConstants.Add_Message_Successful + ":" + msgDef.FullNameWithVer)
      apiResult.toString()
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddMessageDef", null, "Error :" + e.toString() + ErrorCodeConstants.Add_Message_Failed + ":" + msgDef.FullNameWithVer)
        apiResult.toString()
      }
    }
  }

/*
  def UpdateMessageDef(msgDef: MessageDef): String = {
    try {
      UpdateObjectInCache(msgDef,"Activate", MdMgr.GetMdMgr)
      UploadJarsToDB(msgDef)
      var objectsAdded = new Array[BaseElemDef](0)
      objectsAdded = objectsAdded :+ msgDef
      SaveObjectList(objectsAdded, metadataStore)
      val operations = for (op <- objectsAdded) yield "Add"
      NotifyEngine(objectsAdded, operations)
      var apiResult = new ApiResult(0, "Added the message successfully:", msgDef.FullNameWithVer)
      apiResult.toString()
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(-1, "Failed to add the Message:", e.toString)
        apiResult.toString()
      }
    }
  }

  def UpdateContainerDef(contDef: ContainerDef): String = {
    try {
      UpdateObjectInCache(contDef,"Activate", MdMgr.GetMdMgr)
      UploadJarsToDB(contDef)
      var objectsAdded = new Array[BaseElemDef](0)
      objectsAdded = objectsAdded :+ contDef
      SaveObjectList(objectsAdded, metadataStore)
      val operations = for (op <- objectsAdded) yield "Add"
      NotifyEngine(objectsAdded, operations)
      var apiResult = new ApiResult(0, "Added the container successfully:", contDef.FullNameWithVer)
      apiResult.toString()
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(-1, "Failed to add the Container:", e.toString)
        apiResult.toString()
      }
    }
  }
*/
  // As per Model Compiler requirement, Add array/arraybuf/sortedset types for this messageDef
  // along with the messageDef.  
  def AddMessageTypes(msgDef: BaseElemDef, mdMgr: MdMgr): Array[BaseElemDef] = {
    logger.debug("The class name => " + msgDef.getClass().getName())
    try {
      var types = new Array[BaseElemDef](0)
      val msgType = getObjectType(msgDef)
      val depJars = if (msgDef.DependencyJarNames != null)
        (msgDef.DependencyJarNames :+ msgDef.JarName) else Array(msgDef.JarName)
      msgType match {
        case "MessageDef" | "ContainerDef" => {
          // ArrayOf<TypeName>
          var obj: BaseElemDef = mdMgr.MakeArray(msgDef.nameSpace, "arrayof" + msgDef.name, msgDef.nameSpace, msgDef.name, 1, msgDef.ver)
          obj.dependencyJarNames = depJars
          AddObjectToCache(obj, mdMgr)
          types = types :+ obj
          // ArrayBufferOf<TypeName>
          obj = mdMgr.MakeArrayBuffer(msgDef.nameSpace, "arraybufferof" + msgDef.name, msgDef.nameSpace, msgDef.name, 1, msgDef.ver)
          obj.dependencyJarNames = depJars
          AddObjectToCache(obj, mdMgr)
          types = types :+ obj
          // SortedSetOf<TypeName>
          obj = mdMgr.MakeSortedSet(msgDef.nameSpace, "sortedsetof" + msgDef.name, msgDef.nameSpace, msgDef.name, msgDef.ver)
          obj.dependencyJarNames = depJars
          AddObjectToCache(obj, mdMgr)
          types = types :+ obj
          // ImmutableMapOfIntArrayOf<TypeName>
          obj = mdMgr.MakeImmutableMap(msgDef.nameSpace, "immutablemapofintarrayof" + msgDef.name, ("System", "Int"), (msgDef.nameSpace, "arrayof" + msgDef.name), msgDef.ver)
          obj.dependencyJarNames = depJars
          AddObjectToCache(obj, mdMgr)
          types = types :+ obj
          // ImmutableMapOfString<TypeName>
          obj = mdMgr.MakeImmutableMap(msgDef.nameSpace, "immutablemapofstringarrayof" + msgDef.name, ("System", "String"), (msgDef.nameSpace, "arrayof" + msgDef.name), msgDef.ver)
          obj.dependencyJarNames = depJars
          AddObjectToCache(obj, mdMgr)
          types = types :+ obj
          // ArrayOfArrayOf<TypeName>
          obj = mdMgr.MakeArray(msgDef.nameSpace, "arrayofarrayof" + msgDef.name, msgDef.nameSpace, "arrayof" + msgDef.name, 1, msgDef.ver)
          obj.dependencyJarNames = depJars
          AddObjectToCache(obj, mdMgr)
          types = types :+ obj
          // MapOfStringArrayOf<TypeName>
          obj = mdMgr.MakeMap(msgDef.nameSpace, "mapofstringarrayof" + msgDef.name, ("System", "String"), ("System", "arrayof" + msgDef.name), msgDef.ver)
          obj.dependencyJarNames = depJars
          AddObjectToCache(obj, mdMgr)
          types = types :+ obj
          // MapOfIntArrayOf<TypeName>
          obj = mdMgr.MakeMap(msgDef.nameSpace, "mapofintarrayof" + msgDef.name, ("System", "Int"), ("System", "arrayof" + msgDef.name), msgDef.ver)
          obj.dependencyJarNames = depJars
          AddObjectToCache(obj, mdMgr)
          types = types :+ obj
          // SetOf<TypeName>
          obj = mdMgr.MakeSet(msgDef.nameSpace, "setof" + msgDef.name, msgDef.nameSpace, msgDef.name, msgDef.ver)
          obj.dependencyJarNames = depJars
          AddObjectToCache(obj, mdMgr)
          types = types :+ obj
          // TreeSetOf<TypeName>
          obj = mdMgr.MakeTreeSet(msgDef.nameSpace, "treesetof" + msgDef.name, msgDef.nameSpace, msgDef.name, msgDef.ver)
          obj.dependencyJarNames = depJars
          AddObjectToCache(obj, mdMgr)
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

  private def AddContainerOrMessage(contOrMsgText: String, format: String, recompile:Boolean = false): String = {
    var resultStr:String = ""
    try {
      var compProxy = new CompilerProxy
     // compProxy.setLoggerLevel(Level.TRACE)
      val (classStr, cntOrMsgDef) = compProxy.compileMessageDef(contOrMsgText,recompile)
      logger.debug("Message/Container Compiler returned an object of type " + cntOrMsgDef.getClass().getName())
      cntOrMsgDef match {
        case msg: MessageDef => {
	  if( recompile ){
            RemoveMessage(msg.nameSpace, msg.name, msg.ver)
            resultStr = AddMessageDef(msg)
	    //resultStr = UpdateMessageDef(msg)
	  }
	  else{
            resultStr = AddMessageDef(msg)
	  }
	  if( recompile ){
	    val depModels = GetDependentModels(msg.NameSpace,msg.Name,msg.ver)
	    if( depModels.length > 0 ){
	      depModels.foreach(mod => {
		logger.debug("DependentModel => " + mod.FullNameWithVer)
		resultStr = resultStr + RecompileModel(mod)
	      })
	    }
	  }
	  resultStr
	}
        case cont: ContainerDef => {
	  if( recompile ){
            RemoveContainer(cont.nameSpace, cont.name, cont.ver)
            resultStr = AddContainerDef(cont)
            //resultStr = UpdateContainerDef(cont)
	  }
	  else{
            resultStr = AddContainerDef(cont)
	  }

	  if( recompile ){
	    val depModels = GetDependentModels(cont.NameSpace,cont.Name,cont.ver)
	    if( depModels.length > 0 ){
	      depModels.foreach(mod => {
		logger.debug("DependentModel => " + mod.FullNameWithVer)
		resultStr = resultStr + RecompileModel(mod)
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

  def AddContainer(containerText: String, format: String): String = {
    AddContainerOrMessage(containerText, format)
  }

  def AddContainer(containerText: String): String = {
    AddContainer(containerText, "JSON")
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
            isValid = IsValidVersion(latestVersion.get, msg)
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
	    val depModels = GetDependentModels(msg.NameSpace,msg.Name,msg.Version.toInt)
	    if( depModels.length > 0 ){
	      depModels.foreach(mod => {
		logger.debug("DependentModel => " + mod.FullNameWithVer)
		resultStr = resultStr + RecompileModel(mod)
	      })
	    }
	    resultStr
          } else {
            var apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateMessage", null, ErrorCodeConstants.Update_Message_Failed + ":" + messageText + " Error:Invalid Version")
            apiResult.toString()
          }
        }
        case msg: ContainerDef => {
          val latestVersion = GetLatestContainer(msg)
          var isValid = true
          if (latestVersion != None) {
            isValid = IsValidVersion(latestVersion.get, msg)
          }
          if (isValid) {
            RemoveContainer(latestVersion.get.nameSpace, latestVersion.get.name, latestVersion.get.ver)
            resultStr = AddContainerDef(msg)


	    val depMessages = GetDependentMessages.getDependentObjects(msg)
	    if( depMessages.length > 0 ){
	      depMessages.foreach(msg => {
		logger.debug("DependentMessage => " + msg)
		resultStr = resultStr + RecompileMessage(msg)
	      })
	    }
	    val depModels = MetadataAPIImpl.GetDependentModels(msg.NameSpace,msg.Name,msg.Version.toInt)
	    if( depModels.length > 0 ){
	      depModels.foreach(mod => {
		logger.debug("DependentModel => " + mod.FullNameWithVer)
		resultStr = resultStr + RecompileModel(mod)
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


  def UpdateContainer(messageText: String, format: String): String = {
    UpdateMessage(messageText,format)
  }

  def UpdateContainer(messageText: String): String = {
    UpdateMessage(messageText,"JSON")
  }

  def UpdateMessage(messageText: String): String = {
    UpdateMessage(messageText,"JSON")
  }

  // Remove container with Container Name and Version Number
  def RemoveContainer(nameSpace: String, name: String, version: Int,zkNotify:Boolean = true): String = {
    var key = nameSpace + "." + name + "." + version
    try {
      val o = MdMgr.GetMdMgr.Container(nameSpace.toLowerCase, name.toLowerCase, version, true)
      o match {
        case None =>
          None
          logger.debug("container not found => " + key)
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveContainer", null, ErrorCodeConstants.Remove_Container_Failed_Not_Found + ":" + key)
          apiResult.toString()
        case Some(m) =>
          logger.debug("container found => " + m.asInstanceOf[ContainerDef].FullNameWithVer)
          val contDef = m.asInstanceOf[ContainerDef]
          var objectsToBeRemoved = GetAdditionalTypesAdded(contDef, MdMgr.GetMdMgr)
          // Also remove a type with same name as messageDef
          var typeName = name
          var typeDef = GetType(nameSpace, typeName, version.toString, "JSON")
          if (typeDef != None) {
            objectsToBeRemoved = objectsToBeRemoved :+ typeDef.get
          }
          objectsToBeRemoved.foreach(typ => {
            RemoveType(typ.nameSpace, typ.name, typ.ver)
          })
          // ContainerDef itself
          DeleteObject(contDef)
          objectsToBeRemoved = objectsToBeRemoved :+ contDef

	  if( zkNotify ){
            val operations = for (op <- objectsToBeRemoved) yield "Remove"
            NotifyEngine(objectsToBeRemoved, operations)
	  }
          var apiResult = new ApiResult(ErrorCodeConstants.Success, "RemoveContainer", null, ErrorCodeConstants.Remove_Container_Successful + ":" + key)
          apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveContainer", null, "Error :" + e.toString() + ErrorCodeConstants.Remove_Container_Failed + ":" + key)
        apiResult.toString()
      }
    }
  }

  // Remove message with Message Name and Version Number
  def RemoveMessage(nameSpace: String, name: String, version: Int, zkNotify: Boolean = true): String = {
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
          var objectsToBeRemoved = GetAdditionalTypesAdded(msgDef, MdMgr.GetMdMgr)
          // Also remove a type with same name as messageDef
          var typeName = name
          var typeDef = GetType(nameSpace, typeName, version.toString, "JSON")
          if (typeDef != None) {
            objectsToBeRemoved = objectsToBeRemoved :+ typeDef.get
          }
          objectsToBeRemoved.foreach(typ => {
            RemoveType(typ.nameSpace, typ.name, typ.ver)
          })
          // MessageDef itself
          DeleteObject(msgDef)
          objectsToBeRemoved = objectsToBeRemoved :+ msgDef
	  if( zkNotify ){
            val operations = for (op <- objectsToBeRemoved) yield "Remove"
            NotifyEngine(objectsToBeRemoved, operations)
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



  def GetAdditionalTypesAdded(msgDef: BaseElemDef, mdMgr: MdMgr): Array[BaseElemDef] = {
    var types = new Array[BaseElemDef](0)
    logger.debug("The class name => " + msgDef.getClass().getName())
    try {
      val msgType = getObjectType(msgDef)
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
          throw new InternalErrorException("Unknown class in GetAdditionalTypesAdded")
        }
      }
    } catch {
      case e: Exception => {
        throw new Exception(e.getMessage())
      }
    }
  }

  // Remove message with Message Name and Version Number
  def RemoveMessage(messageName: String, version: Int): String = {
    RemoveMessage(sysNS, messageName, version)
  }


  // Remove container with Container Name and Version Number
  def RemoveContainer(containerName: String, version: Int): String = {
    RemoveContainer(sysNS, containerName, version)
  }

  // Remove model with Model Name and Version Number
  def DeactivateModel(nameSpace: String, name: String, version: Int): String = {
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
          DeactivateObject(m.asInstanceOf[ModelDef])
          var objectsUpdated = new Array[BaseElemDef](0)
          objectsUpdated = objectsUpdated :+ m.asInstanceOf[ModelDef]
          val operations = for (op <- objectsUpdated) yield "Deactivate"
          NotifyEngine(objectsUpdated, operations)
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

  def ActivateModel(nameSpace: String, name: String, version: Int): String = {
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
          ActivateObject(m.asInstanceOf[ModelDef])
          var objectsUpdated = new Array[BaseElemDef](0)
          objectsUpdated = objectsUpdated :+ m.asInstanceOf[ModelDef]
          val operations = for (op <- objectsUpdated) yield "Activate"
          NotifyEngine(objectsUpdated, operations)
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
  def RemoveModel(nameSpace: String, name: String, version: Int): String = {
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
          DeleteObject(m.asInstanceOf[ModelDef])
          var objectsUpdated = new Array[BaseElemDef](0)
          objectsUpdated = objectsUpdated :+ m.asInstanceOf[ModelDef]
          var operations = for (op <- objectsUpdated) yield "Remove"
          NotifyEngine(objectsUpdated, operations)
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
  def RemoveModel(modelName: String, version: Int): String = {
    RemoveModel(sysNS, modelName, version)
  }

  // Add Model (model def)
  def AddModel(model: ModelDef): String = {
    var key = model.FullNameWithVer
    try {
      SaveObject(model, MdMgr.GetMdMgr)
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
        isValid = IsValidVersion(latestVersion.get, modDef)
      }
      if (isValid) {
        // save the jar file first
        UploadJarsToDB(modDef)
        val apiResult = AddModel(modDef)
        logger.debug("Model is added..")
        var objectsAdded = new Array[BaseElemDef](0)
        objectsAdded = objectsAdded :+ modDef
        val operations = for (op <- objectsAdded) yield "Add"
        logger.debug("Notify engine via zookeeper")
        NotifyEngine(objectsAdded, operations)
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
        UploadJarsToDB(modDef)
        val result = AddModel(modDef)
        var objectsUpdated = new Array[BaseElemDef](0)
        var operations = new Array[String](0)
        objectsUpdated = objectsUpdated :+ latestVersion.get
        operations = operations :+ "Remove"
        objectsUpdated = objectsUpdated :+ modDef
        operations = operations :+ "Add"
        NotifyEngine(objectsUpdated, operations)
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
	        isValid = IsValidVersion(latestVersion.get, modDef)
      }
      if (isValid) {
        RemoveModel(latestVersion.get.nameSpace, latestVersion.get.name, latestVersion.get.ver)
        UploadJarsToDB(modDef)
        val result = AddModel(modDef)
        var objectsUpdated = new Array[BaseElemDef](0)
        var operations = new Array[String](0)
        objectsUpdated = objectsUpdated :+ latestVersion.get
        operations = operations :+ "Remove"
        objectsUpdated = objectsUpdated :+ modDef
        operations = operations :+ "Add"
        NotifyEngine(objectsUpdated, operations)
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


  def GetDependentModels(msgNameSpace: String, msgName: String, msgVer: Int): Array[ModelDef] = {
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
		//logger.debug("comparing with " + ivar.asInstanceOf[AttributeDef].typeDef.FullNameWithVer.toLowerCase)
                if (ivar.asInstanceOf[AttributeDef].typeDef.FullName.toLowerCase == msgObjName) {
		  logger.debug("The model " + mod.FullNameWithVer + " is  dependent on the message " + msgObj)
                  depModels = depModels :+ mod
                  break
                }
              })
              mod.outputVars.foreach(ivar => {
		//logger.debug("comparing with " + ivar.asInstanceOf[AttributeDef].typeDef.FullNameWithVer.toLowerCase)
                if (ivar.asInstanceOf[AttributeDef].typeDef.FullName.toLowerCase == msgObjName) {
		  logger.debug("The model " + mod.FullNameWithVer + " is a dependent on the message " + msgObj)
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

  // All available containers(format JSON or XML) as a String
  def GetAllContainerDefs(formatType: String): String = {
    try {
      val msgDefs = MdMgr.GetMdMgr.Containers(true, true)
      msgDefs match {
        case None =>
          None
          logger.debug("No Containers found ")
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetAllContainerDefs", null, ErrorCodeConstants.Get_All_Containers_Failed_Not_Available)
          apiResult.toString()
        case Some(ms) =>
          val msa = ms.toArray
          var apiResult = new ApiResult(ErrorCodeConstants.Success, "GetAllContainerDefs", JsonSerializer.SerializeObjectListToJson("Containers", msa), ErrorCodeConstants.Get_All_Containers_Successful)
          apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetAllContainerDefs", null, "Error :" + e.toString() + ErrorCodeConstants.Get_All_Containers_Failed)
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

  def GetAllContainersFromCache(active: Boolean): Array[String] = {
    var containerList: Array[String] = new Array[String](0)
    try {
      val contDefs = MdMgr.GetMdMgr.Containers(active, true)
      contDefs match {
        case None =>
          None
          logger.debug("No Containers found ")
          containerList
        case Some(ms) =>
          val msa = ms.toArray
          val contCount = msa.length
          containerList = new Array[String](contCount)
          for (i <- 0 to contCount - 1) {
            containerList(i) = msa(i).FullNameWithVer
          }
          containerList
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw new UnexpectedMetadataAPIException("Failed to fetch all the containers:" + e.toString)
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


  def GetAllConceptsFromCache(active: Boolean): Array[String] = {
    var conceptList: Array[String] = new Array[String](0)
    try {
      val contDefs = MdMgr.GetMdMgr.Attributes(active, true)
      contDefs match {
        case None =>
          None
          logger.debug("No Concepts found ")
          conceptList
        case Some(ms) =>
          val msa = ms.toArray
          val contCount = msa.length
          conceptList = new Array[String](contCount)
          for (i <- 0 to contCount - 1) {
            conceptList(i) = msa(i).FullNameWithVer
          }
          conceptList
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw new UnexpectedMetadataAPIException("Failed to fetch all the concepts:" + e.toString)
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
            typeList(i) = msa(i).FullNameWithVer
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

  // Specific container (format JSON or XML) as a String using containerName(with version) as the key
  def GetContainerDefFromCache(nameSpace: String, name: String, formatType: String, version: String): String = {
    var key = nameSpace + "." + name + "." + version
    try {
      val o = MdMgr.GetMdMgr.Container(nameSpace.toLowerCase, name.toLowerCase, version.toInt, true)
      o match {
        case None =>
          None
          logger.debug("container not found => " + key)
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetContainerDefFromCache", null, ErrorCodeConstants.Get_Container_From_Cache_Failed + ":" + key)
          apiResult.toString()
        case Some(m) =>
          logger.debug("container found => " + m.asInstanceOf[ContainerDef].FullNameWithVer)
          var apiResult = new ApiResult(ErrorCodeConstants.Success, "GetContainerDefFromCache", JsonSerializer.SerializeObjectToJson(m), ErrorCodeConstants.Get_Container_From_Cache_Successful)
          apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetContainerDefFromCache", null, "Error :" + e.toString() + ErrorCodeConstants.Get_Container_From_Cache_Failed + ":" + key)
        apiResult.toString()
      }
    }
  }

  // Return Specific messageDef object using messageName(with version) as the key
  @throws(classOf[ObjectNotFoundException])
  def GetMessageDefInstanceFromCache(nameSpace: String, name: String, formatType: String, version: String): MessageDef = {
    var key = nameSpace + "." + name + "." + version
    try {
      val o = MdMgr.GetMdMgr.Message(nameSpace.toLowerCase, name.toLowerCase, version.toInt, true)
      o match {
        case None =>
          None
          logger.debug("message not found => " + key)
          throw new ObjectNotFoundException("Failed to Fetch the message:" + key)
        case Some(m) =>
          m.asInstanceOf[MessageDef]
      }
    } catch {
      case e: Exception => {
        throw new ObjectNotFoundException("Failed to Fetch the message:" + key + ":" + e.getMessage())
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
      val o = MdMgr.GetMdMgr.Model(modDef.nameSpace.toLowerCase,
        modDef.name.toLowerCase,
        modDef.ver,
        false)
      o match {
        case None =>
          None
          logger.debug("model not in the cache => " + key)
          return false;
        case Some(m) =>
          logger.debug("model found => " + m.asInstanceOf[ModelDef].FullNameWithVer)
          return true
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw new UnexpectedMetadataAPIException(e.getMessage())
      }
    }
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
          val model = GetLatestModelFromModels(m.asInstanceOf[scala.collection.mutable.Set[com.ligadata.olep.metadata.ModelDef]])
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
  def GetLatestModelFromModels(modelSet: Set[ModelDef]): ModelDef = {
    var model: ModelDef = null
    var verList: List[Int] = List[Int]()
    var modelmap: scala.collection.mutable.Map[Int, ModelDef] = scala.collection.mutable.Map()
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

  // Get the latest message for a given FullName
  def GetLatestMessage(msgDef: MessageDef): Option[MessageDef] = {
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

  // Get the latest container for a given FullName
  def GetLatestContainer(contDef: ContainerDef): Option[ContainerDef] = {
    try {
      var key = contDef.nameSpace + "." + contDef.name + "." + contDef.ver
      val o = MdMgr.GetMdMgr.Containers(contDef.nameSpace.toLowerCase,
        contDef.name.toLowerCase,
        false,
        true)
      o match {
        case None =>
          None
          logger.debug("container not in the cache => " + key)
          None
        case Some(m) =>
          logger.debug("container found => " + m.head.asInstanceOf[ContainerDef].FullNameWithVer)
          Some(m.head.asInstanceOf[ContainerDef])
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw new UnexpectedMetadataAPIException(e.getMessage())
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

  // check whether message already exists in metadata manager. Ideally,
  // we should never add the message into metadata manager more than once
  // and there is no need to use this function in main code flow
  // This is just a utility function being during these initial phases
  def IsMessageAlreadyExists(msgDef: MessageDef): Boolean = {
    try {
      var key = msgDef.nameSpace + "." + msgDef.name + "." + msgDef.ver
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
          logger.debug("message found => " + m.asInstanceOf[MessageDef].FullNameWithVer)
          return true
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw new UnexpectedMetadataAPIException(e.getMessage())
      }
    }
  }

  def IsContainerAlreadyExists(contDef: ContainerDef): Boolean = {
    try {
      var key = contDef.nameSpace + "." + contDef.name + "." + contDef.ver
      val o = MdMgr.GetMdMgr.Container(contDef.nameSpace.toLowerCase,
        contDef.name.toLowerCase,
        contDef.ver,
        false)
      o match {
        case None =>
          None
          logger.debug("container not in the cache => " + key)
          return false;
        case Some(m) =>
          logger.debug("container found => " + m.asInstanceOf[ContainerDef].FullNameWithVer)
          return true
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw new UnexpectedMetadataAPIException(e.getMessage())
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

  def IsConceptAlreadyExists(attrDef: BaseAttributeDef): Boolean = {
    try {
      var key = attrDef.nameSpace + "." + attrDef.name + "." + attrDef.ver
      val o = MdMgr.GetMdMgr.Attribute(attrDef.nameSpace,
        attrDef.name,
        attrDef.ver,
        false)
      o match {
        case None =>
          None
          logger.debug("concept not in the cache => " + key)
          return false;
        case Some(m) =>
          logger.debug("concept found => " + m.asInstanceOf[AttributeDef].FullNameWithVer)
          return true
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw new UnexpectedMetadataAPIException(e.getMessage())
      }
    }
  }

  def IsTypeAlreadyExists(typeDef: BaseTypeDef): Boolean = {
    try {
      var key = typeDef.nameSpace + "." + typeDef.name + "." + typeDef.ver
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
          logger.debug("Type found => " + m.asInstanceOf[BaseTypeDef].FullNameWithVer)
          return true
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw new UnexpectedMetadataAPIException(e.getMessage())
      }
    }
  }

  // Specific message (format JSON or XML) as a String using messageName(with version) as the key
  def GetModelDefFromDB(nameSpace: String, objectName: String, formatType: String, version: String): String = {
     var key = "ModelDef" + "." + nameSpace + '.' + objectName + "." + version
    try {
      var obj = GetObject(key.toLowerCase, modelStore)
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "GetModelDefFromCache", ValueAsStr(obj.Value), ErrorCodeConstants.Get_Model_From_DB_Successful + ":" + key)
      apiResult.toString()
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetModelDefFromCache", null, "Error :" + e.toString() + ErrorCodeConstants.Get_Model_From_DB_Failed + ":" + key)
        apiResult.toString()
      }
    }
  }

  private def IsTypeObject(typeName: String): Boolean = {
    typeName match {
      case "scalartypedef" | "arraytypedef" | "arraybuftypedef" | "listtypedef" | "settypedef" | "treesettypedef" | "queuetypedef" | "maptypedef" | "immutablemaptypedef" | "hashmaptypedef" | "tupletypedef" | "structtypedef" | "sortedsettypedef" => {
        return true
      }
      case _ => {
        return false
      }
    }
  }

  def GetAllKeys(objectType: String): Array[String] = {
    try {
      var keys = scala.collection.mutable.Set[String]()
      typeStore.getAllKeys({ (key: Key) =>
        {
          val strKey = KeyAsStr(key)
          val i = strKey.indexOf(".")
          val objType = strKey.substring(0, i)
          val typeName = strKey.substring(i + 1)
          objectType match {
            case "TypeDef" => {
              if (IsTypeObject(objType)) {
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
      case e: Exception => {
        e.printStackTrace()
        throw InternalErrorException("Failed to get keys from persistent store")
      }
    }
  }


  def LoadAllConfigObjectsIntoCache: Boolean = {
    try {
      var keys = scala.collection.mutable.Set[com.ligadata.keyvaluestore.Key]()
      configStore.getAllKeys({ (key: Key) => keys.add(key) })
      val keyArray = keys.toArray
      if (keyArray.length == 0) {
        logger.debug("No config objects available in the Database")
        return false
      }
      keyArray.foreach(key => {
	//logger.debug("key => " + KeyAsStr(key))
        val obj = GetObject(key, configStore)
        val strKey = KeyAsStr(key)
        val i = strKey.indexOf(".")
        val objType = strKey.substring(0, i)
        val typeName = strKey.substring(i + 1)
	objType match {
	  case "nodeinfo" => {
            val ni = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte]).asInstanceOf[NodeInfo]
	    MdMgr.GetMdMgr.AddNode(ni)
	  }
	  case "adapterinfo" => {
            val ai = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte]).asInstanceOf[AdapterInfo]
	    MdMgr.GetMdMgr.AddAdapter(ai)
	  }
	  case "clusterinfo" => {
            val ci = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte]).asInstanceOf[ClusterInfo]
	    MdMgr.GetMdMgr.AddCluster(ci)
	  }
	  case "clustercfginfo" => {
            val ci = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte]).asInstanceOf[ClusterCfgInfo]
	    MdMgr.GetMdMgr.AddClusterCfg(ci)
	  }
	  case _ => {
            throw InternalErrorException("LoadAllConfigObjectsIntoCache: Unknown objectType " + objType)
	  }
	}
      })
      return true
    } catch {
      case e: Exception => {
        e.printStackTrace()
	return false
      }
    }
  }

  def LoadAllObjectsIntoCache {
    try {
      val configAvailable = LoadAllConfigObjectsIntoCache
      if( configAvailable ){
	RefreshApiConfigForGivenNode(metadataAPIConfig.getProperty("NODE_ID"))
      }
      else{
	logger.debug("Assuming bootstrap... No config objects in persistent store")
      }
      startup = true
      var objectsChanged = new Array[BaseElemDef](0)
      var operations = new Array[String](0)
      val maxTranId = GetTranId
      logger.debug("Max Transaction Id => " + maxTranId)
      var keys = scala.collection.mutable.Set[com.ligadata.keyvaluestore.Key]()
      metadataStore.getAllKeys({ (key: Key) => keys.add(key) })
      val keyArray = keys.toArray
      if (keyArray.length == 0) {
        logger.debug("No objects available in the Database")
        return
      }
      keyArray.foreach(key => {
        val obj = GetObject(key, metadataStore)
        val mObj = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte]).asInstanceOf[BaseElemDef]
        if (mObj != null) {
          if (mObj.tranId <= maxTranId) {
            AddObjectToCache(mObj, MdMgr.GetMdMgr)
            DownloadJarFromDB(mObj)
          } else {
            logger.debug("The transaction id of the object => " + mObj.tranId)
            AddObjectToCache(mObj, MdMgr.GetMdMgr)
            DownloadJarFromDB(mObj)
            logger.error("Transaction is incomplete with the object " + KeyAsStr(key) + ",we may not have notified engine, attempt to do it now...")
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
        NotifyEngine(objectsChanged, operations)
      }
      startup = false
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

  def LoadAllTypesIntoCache {
    try {
      val typeKeys = GetAllKeys("TypeDef")
      if (typeKeys.length == 0) {
        logger.debug("No types available in the Database")
        return
      }
      typeKeys.foreach(key => {
        val obj = GetObject(key.toLowerCase, typeStore)
        val typ = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
        if (typ != null) {
          AddObjectToCache(typ, MdMgr.GetMdMgr)
        }
      })
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

  def LoadAllConceptsIntoCache {
    try {
      val conceptKeys = GetAllKeys("Concept")
      if (conceptKeys.length == 0) {
        logger.debug("No concepts available in the Database")
        return
      }
      conceptKeys.foreach(key => {
        val obj = GetObject(key.toLowerCase, conceptStore)
        val concept = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
        AddObjectToCache(concept.asInstanceOf[AttributeDef], MdMgr.GetMdMgr)
      })
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

  def LoadAllFunctionsIntoCache {
    try {
      val functionKeys = GetAllKeys("FunctionDef")
      if (functionKeys.length == 0) {
        logger.debug("No functions available in the Database")
        return
      }
      functionKeys.foreach(key => {
        val obj = GetObject(key.toLowerCase, functionStore)
        val function = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
        AddObjectToCache(function.asInstanceOf[FunctionDef], MdMgr.GetMdMgr)
      })
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

  def LoadAllMessagesIntoCache {
    try {
      val msgKeys = GetAllKeys("MessageDef")
      if (msgKeys.length == 0) {
        logger.debug("No messages available in the Database")
        return
      }
      msgKeys.foreach(key => {
        val obj = GetObject(key.toLowerCase, messageStore)
        val msg = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
        AddObjectToCache(msg.asInstanceOf[MessageDef], MdMgr.GetMdMgr)
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
      val obj = GetObject(key.toLowerCase, messageStore)
      logger.debug("Deserialize the object " + key)
      val msg = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
      logger.debug("Get the jar from database ")
      val msgDef = msg.asInstanceOf[MessageDef]
      DownloadJarFromDB(msgDef)
      logger.debug("Add the object " + key + " to the cache ")
      AddObjectToCache(msgDef, MdMgr.GetMdMgr)
    } catch {
      case e: Exception => {
	logger.debug("Failed to load message into cache " + key + ":" + e.getMessage())
      }
    }
  }

  def LoadTypeIntoCache(key: String) {
    try {
      logger.debug("Fetch the object " + key + " from database ")
      val obj = GetObject(key.toLowerCase, typeStore)
      logger.debug("Deserialize the object " + key)
      val typ = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
      if (typ != null) {
        logger.debug("Add the object " + key + " to the cache ")
        AddObjectToCache(typ, MdMgr.GetMdMgr)
      }
    } catch {
      case e: Exception => {
        logger.warn("Unable to load the object " + key + " into cache ")
      }
    }
  }

  def LoadModelIntoCache(key: String) {
    try {
      logger.debug("Fetch the object " + key + " from database ")
      val obj = GetObject(key.toLowerCase, modelStore)
      logger.debug("Deserialize the object " + key)
      val model = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
      logger.debug("Get the jar from database ")
      val modDef = model.asInstanceOf[ModelDef]
      DownloadJarFromDB(modDef)
      logger.debug("Add the object " + key + " to the cache ")
      AddObjectToCache(modDef, MdMgr.GetMdMgr)
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

  def LoadContainerIntoCache(key: String) {
    try {
      val obj = GetObject(key.toLowerCase, containerStore)
      val cont = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
      logger.debug("Get the jar from database ")
      val contDef = cont.asInstanceOf[ContainerDef]
      DownloadJarFromDB(contDef)
      AddObjectToCache(contDef, MdMgr.GetMdMgr)
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

  def LoadFunctionIntoCache(key: String) {
    try {
      val obj = GetObject(key.toLowerCase, functionStore)
      val cont = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
      AddObjectToCache(cont.asInstanceOf[FunctionDef], MdMgr.GetMdMgr)
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

  def LoadAttributeIntoCache(key: String) {
    try {
      val obj = GetObject(key.toLowerCase, conceptStore)
      val cont = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
      AddObjectToCache(cont.asInstanceOf[AttributeDef], MdMgr.GetMdMgr)
    } catch {
      case e: Exception => {
        e.printStackTrace()
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
                LoadModelIntoCache(key)
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
                LoadMessageIntoCache(key)
              }
              case "Remove" => {
                try {
                  RemoveMessage(zkMessage.NameSpace, zkMessage.Name, zkMessage.Version.toInt,false)
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
                LoadContainerIntoCache(key)
              }
              case "Remove" => {
                try {
                  RemoveContainer(zkMessage.NameSpace, zkMessage.Name, zkMessage.Version.toInt,false)
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
                LoadFunctionIntoCache(key)
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
                LoadAttributeIntoCache(key)
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
                DownloadJarFromDB(MdMgr.GetMdMgr.MakeJarDef(zkMessage.NameSpace, zkMessage.Name, zkMessage.Version))
              }
              case _ => {
                logger.error("Unknown Operation " + zkMessage.Operation + " in zookeeper notification, notification is not processed ..")
              }
            }
          }
          case "ScalarTypeDef" | "ArrayTypeDef" | "ArrayBufTypeDef" | "ListTypeDef" | "SetTypeDef" | "TreeSetTypeDef" | "QueueTypeDef" | "MapTypeDef" | "ImmutableMapTypeDef" | "HashMapTypeDef" | "TupleTypeDef" | "StructTypeDef" | "SortedSetTypeDef" => {
            zkMessage.Operation match {
              case "Add" => {
                LoadTypeIntoCache(key)
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

  def LoadAllContainersIntoCache {
    try {
      val contKeys = GetAllKeys("ContainerDef")
      if (contKeys.length == 0) {
        logger.debug("No containers available in the Database")
        return
      }
      contKeys.foreach(key => {
        val obj = GetObject(key.toLowerCase, containerStore)
        val contDef = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
        AddObjectToCache(contDef.asInstanceOf[ContainerDef], MdMgr.GetMdMgr)
      })
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

  def LoadAllModelsIntoCache {
    try {
      val modKeys = GetAllKeys("ModelDef")
      if (modKeys.length == 0) {
        logger.debug("No models available in the Database")
        return
      }
      modKeys.foreach(key => {
        val obj = GetObject(key.toLowerCase, modelStore)
        val modDef = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte])
        AddObjectToCache(modDef.asInstanceOf[ModelDef], MdMgr.GetMdMgr)
      })
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

  def LoadObjectsIntoCache {
    LoadAllModelsIntoCache
    LoadAllMessagesIntoCache
    LoadAllContainersIntoCache
    LoadAllFunctionsIntoCache
    LoadAllConceptsIntoCache
    LoadAllTypesIntoCache
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

  // Specific containers (format JSON or XML) as a String using containerName(without version) as the key
  def GetContainerDef(objectName: String, formatType: String): String = {
    val nameSpace = MdMgr.sysNS
    GetContainerDefFromCache(nameSpace, objectName, formatType, "-1")
  }
  // Specific container (format JSON or XML) as a String using containerName(with version) as the key
  def GetContainerDef(nameSpace: String, objectName: String, formatType: String, version: String): String = {
    GetContainerDefFromCache(nameSpace, objectName, formatType, version)
  }

  // Specific container (format JSON or XML) as a String using containerName(with version) as the key
  def GetContainerDef(objectName: String, version: String, formatType: String): String = {
    val nameSpace = MdMgr.sysNS
    GetContainerDef(nameSpace, objectName, formatType, version)
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

  // All available concepts as a String
  def GetAllConcepts(formatType: String): String = {
    try {
      val concepts = MdMgr.GetMdMgr.Attributes(true, true)
      concepts match {
        case None =>
          None
          logger.debug("No concepts found ")
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetAllConcepts", null, ErrorCodeConstants.Get_All_Concepts_Failed_Not_Available)
          apiResult.toString()
        case Some(cs) =>
          val csa = cs.toArray
          var apiResult = new ApiResult(ErrorCodeConstants.Success, "GetAllConcepts", JsonSerializer.SerializeObjectListToJson("Concepts", csa), ErrorCodeConstants.Get_All_Concepts_Successful)
          apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetAllConcepts", null, "Error :" + e.toString() + ErrorCodeConstants.Get_All_Concepts_Failed)
        apiResult.toString()
      }
    }
  }

  // A single concept as a string using name and version as the key
  def GetConcept(nameSpace:String, objectName: String, version: String, formatType: String): String = {
    var key = nameSpace + "." + objectName + "." + version
    try {
      val concept = MdMgr.GetMdMgr.Attribute(nameSpace, objectName, version.toInt, false)
      concept match {
        case None =>
          None
          logger.debug("No concepts found ")
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetConcept", null, ErrorCodeConstants.Get_Concept_Failed_Not_Available)
          apiResult.toString()
        case Some(cs) =>
          var apiResult = new ApiResult(ErrorCodeConstants.Success, "GetConcept", JsonSerializer.SerializeObjectToJson(cs), ErrorCodeConstants.Get_Concept_Successful + ":" + key)
          apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetConcept", null, "Error :" + e.toString() + ErrorCodeConstants.Get_Concept_Failed + ":" + key)
        apiResult.toString()
      }
    }
  }

  // A single concept as a string using name and version as the key
  def GetConcept(objectName: String, version: String, formatType: String): String = {
    GetConcept(MdMgr.sysNS,objectName,version,formatType)
  }

  // A single concept as a string using name and version as the key
  def GetConceptDef(nameSpace:String, objectName: String, formatType: String,version: String): String = {
    GetConcept(nameSpace,objectName,version,formatType)
  }

  // A list of concept(s) as a string using name 
  def GetConcept(objectName: String, formatType: String): String = {
    try {
      val concepts = MdMgr.GetMdMgr.Attributes(MdMgr.sysNS, objectName, false, false)
      concepts match {
        case None =>
          None
          logger.debug("No concepts found ")
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetConcept", null, ErrorCodeConstants.Get_Concept_Failed_Not_Available + ":" + objectName)
          apiResult.toString()
        case Some(cs) =>
          val csa = cs.toArray
          var apiResult = new ApiResult(ErrorCodeConstants.Success, "GetConcept", JsonSerializer.SerializeObjectListToJson("Concepts", csa), ErrorCodeConstants.Get_Concept_Successful + ":" + objectName)
          apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetConcept", null, "Error :" + e.toString() + ErrorCodeConstants.Get_Concept_Failed + ":" + objectName)
        apiResult.toString()
      }
    }
  }

  // All available derived concepts(format JSON or XML) as a String
  def GetAllDerivedConcepts(formatType: String): String = {
    try {
      val concepts = MdMgr.GetMdMgr.Attributes(true, true)
      concepts match {
        case None =>
          None
          logger.debug("No concepts found ")
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetAllDerivedConcepts", null, ErrorCodeConstants.Get_All_Derived_Concepts_Failed_Not_Available)
          apiResult.toString()
        case Some(cs) =>
          val csa = cs.toArray.filter(t => { t.getClass.getName.contains("DerivedAttributeDef") })
          if (csa.length > 0) {
            var apiResult = new ApiResult(ErrorCodeConstants.Success, "GetAllDerivedConcepts", JsonSerializer.SerializeObjectListToJson("Concepts", csa), ErrorCodeConstants.Get_All_Derived_Concepts_Successful)
            apiResult.toString()
          } else {
            var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetAllDerivedConcepts", null, ErrorCodeConstants.Get_All_Derived_Concepts_Failed_Not_Available)
            apiResult.toString()
          }
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetAllDerivedConcepts", null, "Error :" + e.toString() + ErrorCodeConstants.Get_All_Derived_Concepts_Failed)
        apiResult.toString()
      }
    }
  }
  // A derived concept(format JSON or XML) as a string using name(without version) as the key
  def GetDerivedConcept(objectName: String, formatType: String): String = {
    try {
      val concepts = MdMgr.GetMdMgr.Attributes(MdMgr.sysNS, objectName, false, false)
      concepts match {
        case None =>
          None
          logger.debug("No concepts found ")
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetDerivedConcept", null, ErrorCodeConstants.Get_Derived_Concept_Failed_Not_Available + ":" + objectName)
          apiResult.toString()
        case Some(cs) =>
          val csa = cs.toArray.filter(t => { t.getClass.getName.contains("DerivedAttributeDef") })
          if (csa.length > 0) {
            var apiResult = new ApiResult(ErrorCodeConstants.Success, "GetDerivedConcept", JsonSerializer.SerializeObjectListToJson("Concepts", csa), ErrorCodeConstants.Get_Derived_Concept_Successful + ":" + objectName)
            apiResult.toString()
          } else {
            var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetDerivedConcept", null, ErrorCodeConstants.Get_Derived_Concept_Failed_Not_Available + ":" + objectName)
            apiResult.toString()
          }
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetDerivedConcept", null, "Error :" + e.toString() + ErrorCodeConstants.Get_Derived_Concept_Failed + ":" + objectName)
        apiResult.toString()
      }
    }
  }
  // A derived concept(format JSON or XML) as a string using name and version as the key
  def GetDerivedConcept(objectName: String, version: String, formatType: String): String = {
    var key = objectName + "." + version
    try {
      val concept = MdMgr.GetMdMgr.Attribute(MdMgr.sysNS, objectName, version.toInt, false)
      concept match {
        case None =>
          None
          logger.debug("No concepts found ")
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetDerivedConcept", null, ErrorCodeConstants.Get_Derived_Concept_Failed_Not_Available + ":" + key)
          apiResult.toString()
        case Some(cs) =>
          if (cs.isInstanceOf[DerivedAttributeDef]) {
            var apiResult = new ApiResult(ErrorCodeConstants.Success, "GetDerivedConcept", JsonSerializer.SerializeObjectToJson(cs), ErrorCodeConstants.Get_Derived_Concept_Successful + ":" + key)
            apiResult.toString()
          } else {
            logger.debug("No Derived concepts found ")
            var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetDerivedConcept", null, ErrorCodeConstants.Get_Derived_Concept_Failed_Not_Available + ":" + key)
            apiResult.toString()
          }
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetDerivedConcept", null, "Error :" + e.toString() + ErrorCodeConstants.Get_Derived_Concept_Failed + ":" + key)
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
    var key = nameSpace + "." + objectName + "." + version
    try {
      val typeDefs = MdMgr.GetMdMgr.Types(nameSpace, objectName,false,false)
      typeDefs match {
        case None =>
          None
          logger.debug("No typeDefs found ")
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetTypeDef", null, ErrorCodeConstants.Get_Type_Def_Failed_Not_Available + ":" + key)
          apiResult.toString()
        case Some(ts) =>
          val tsa = ts.toArray
          logger.debug("Found " + tsa.length + " types ")
          var apiResult = new ApiResult(ErrorCodeConstants.Success, "GetTypeDef", JsonSerializer.SerializeObjectListToJson("Types", tsa), ErrorCodeConstants.Get_Type_Def_Successful + ":" + key)
          apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetTypeDef", null, "Error :" + e.toString() + ErrorCodeConstants.Get_Type_Def_Failed + ":" + key)
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
          val tsa = ts.toArray.filter(t => { t.ver == version.toInt })
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

  def AddNode(nodeId:String,nodePort:Int,nodeIpAddr:String,
	      jarPaths:List[String],scala_home:String,
	      java_home:String, classpath: String,
	      clusterId:String,power:Int,
	      roles:Int,description:String): String = {
    try{
      // save in memory
      val ni = MdMgr.GetMdMgr.MakeNode(nodeId,nodePort,nodeIpAddr,jarPaths,scala_home,
				       java_home,classpath,clusterId,power,roles,description)
      MdMgr.GetMdMgr.AddNode(ni)
      // save in database
      val key = "NodeInfo." + nodeId
      val value = serializer.SerializeObjectToByteArray(ni)
      SaveObject(key.toLowerCase,value,configStore)
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "AddNode", null, ErrorCodeConstants.Add_Node_Successful + ":" + nodeId)
      apiResult.toString()
    } catch{
      case e:Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddNode", null, "Error :" + e.toString() + ErrorCodeConstants.Add_Node_Failed + ":" + nodeId)
        apiResult.toString()
      }
    }	
  }

  def UpdateNode(nodeId:String,nodePort:Int,nodeIpAddr:String,
		 jarPaths:List[String],scala_home:String,
		 java_home:String, classpath: String,
		 clusterId:String,power:Int,
		 roles:Int,description:String): String = {
    AddNode(nodeId,nodePort,nodeIpAddr,jarPaths,scala_home,
	    java_home,classpath,
	    clusterId,power,roles,description)
  }

  def RemoveNode(nodeId:String) : String = {
    try{
      MdMgr.GetMdMgr.RemoveNode(nodeId)
      val key = "NodeInfo." + nodeId
      DeleteObject(key.toLowerCase,configStore)
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "RemoveNode", null, ErrorCodeConstants.Remove_Node_Successful + ":" + nodeId)
      apiResult.toString()
    } catch{
      case e:Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveNode", null, "Error :" + e.toString() + ErrorCodeConstants.Remove_Node_Failed + ":" + nodeId)
        apiResult.toString()
      }
    }	
  }


  def AddAdapter(name:String,typeString:String,dataFormat: String,className: String, 
		 jarName: String, dependencyJars: List[String], 
		 adapterSpecificCfg: String,inputAdapterToVerify: String): String = {
    try{
      // save in memory
      val ai = MdMgr.GetMdMgr.MakeAdapter(name,typeString,dataFormat,className,jarName,
					  dependencyJars,adapterSpecificCfg,inputAdapterToVerify)
      MdMgr.GetMdMgr.AddAdapter(ai)
      // save in database
      val key = "AdapterInfo." + name
      val value = serializer.SerializeObjectToByteArray(ai)
      SaveObject(key.toLowerCase,value,configStore)
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "AddAdapter", null, ErrorCodeConstants.Add_Adapter_Successful + ":" + name)
      apiResult.toString()
    } catch{
      case e:Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddAdapter", null, "Error :" + e.toString() + ErrorCodeConstants.Add_Adapter_Failed + ":" + name)
        apiResult.toString()
      }
    }	
  }

  def UpdateAdapter(name:String,typeString:String,dataFormat: String,className: String, 
		    jarName: String, dependencyJars: List[String], 
		    adapterSpecificCfg: String,inputAdapterToVerify: String): String = {
    AddAdapter(name,typeString,dataFormat,className,jarName,dependencyJars,adapterSpecificCfg,inputAdapterToVerify)
  }

  def RemoveAdapter(name:String) : String = {
    try{
      MdMgr.GetMdMgr.RemoveAdapter(name)
      val key = "AdapterInfo." + name
      DeleteObject(key.toLowerCase,configStore)
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "RemoveAdapter", null, ErrorCodeConstants.Remove_Adapter_Successful + ":" + name)
      apiResult.toString()
    } catch{
      case e:Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveAdapter", null, "Error :" + e.toString() + ErrorCodeConstants.Remove_Adapter_Failed + ":" + name)
        apiResult.toString()
      }
    }	
  }


  def AddCluster(clusterId:String,description:String,privileges:String) : String = {
    try{
      // save in memory
      val ci = MdMgr.GetMdMgr.MakeCluster(clusterId,description,privileges)
      MdMgr.GetMdMgr.AddCluster(ci)
      // save in database
      val key = "ClusterInfo." + clusterId
      val value = serializer.SerializeObjectToByteArray(ci)
      SaveObject(key.toLowerCase,value,configStore)
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "AddCluster", null, ErrorCodeConstants.Add_Cluster_Successful + ":" + clusterId)
      apiResult.toString()
    } catch{
      case e:Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddCluster", null, "Error :" + e.toString() + ErrorCodeConstants.Add_Cluster_Failed + ":" + clusterId)
        apiResult.toString()
      }
    }	
  }

  def UpdateCluster(clusterId:String,description:String,privileges:String): String = {
    AddCluster(clusterId,description,privileges)
  }

  def RemoveCluster(clusterId:String) : String = {
    try{
      MdMgr.GetMdMgr.RemoveCluster(clusterId)
      val key = "ClusterInfo." + clusterId
      DeleteObject(key.toLowerCase,configStore)
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "RemoveCluster", null, ErrorCodeConstants.Remove_Cluster_Successful + ":" + clusterId)
      apiResult.toString()
    } catch{
      case e:Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveCluster", null, "Error :" + e.toString() + ErrorCodeConstants.Remove_Cluster_Failed + ":" + clusterId)
        apiResult.toString()
      }
    }	
  }


  def AddClusterCfg(clusterCfgId:String,cfgMap:scala.collection.mutable.HashMap[String,String],
		    modifiedTime:Date,createdTime:Date) : String = {
    try{
      // save in memory
      val ci = MdMgr.GetMdMgr.MakeClusterCfg(clusterCfgId,cfgMap, modifiedTime,createdTime)
      MdMgr.GetMdMgr.AddClusterCfg(ci)
      // save in database
      val key = "ClusterCfgInfo." + clusterCfgId
      val value = serializer.SerializeObjectToByteArray(ci)
      SaveObject(key.toLowerCase,value,configStore)
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "AddClusterCfg", null, ErrorCodeConstants.Add_Cluster_Config_Successful + ":" + clusterCfgId)
      apiResult.toString()
    } catch{
      case e:Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddClusterCfg", null, "Error :" + e.toString() + ErrorCodeConstants.Add_Cluster_Config_Failed + ":" + clusterCfgId)
        apiResult.toString()
      }
    }	
  }


  def UpdateClusterCfg(clusterCfgId:String,cfgMap:scala.collection.mutable.HashMap[String,String],
		       modifiedTime:Date,createdTime:Date): String = {
    AddClusterCfg(clusterCfgId,cfgMap,modifiedTime,createdTime)
  }

  def RemoveClusterCfg(clusterCfgId:String) : String = {
    try{
      MdMgr.GetMdMgr.RemoveClusterCfg(clusterCfgId)
      val key = "ClusterCfgInfo." + clusterCfgId
      DeleteObject(key.toLowerCase,configStore)
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "RemoveCLusterCfg", null, ErrorCodeConstants.Remove_Cluster_Config_Successful + ":" + clusterCfgId)
      apiResult.toString()
    } catch{
      case e:Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveCLusterCfg", null, "Error :" + e.toString() + ErrorCodeConstants.Remove_Cluster_Config_Failed + ":" + clusterCfgId)
        apiResult.toString()
      }
    }	
  }


  def RemoveConfig(cfgStr: String): String = {
    var keyList = new Array[String](0)
    try {
      // extract config objects
      val cfg = JsonSerializer.parseEngineConfig(cfgStr)
      // process clusterInfo object if it exists
      val clusters = cfg.Clusters
      if ( clusters != None ){
	val ciList = clusters.get
	ciList.foreach(c1 => {
	  MdMgr.GetMdMgr.RemoveCluster(c1.ClusterId.toLowerCase)
	  var key = "ClusterInfo." + c1.ClusterId
	  keyList   = keyList :+ key.toLowerCase
	  MdMgr.GetMdMgr.RemoveClusterCfg(c1.ClusterId.toLowerCase)
	  key = "ClusterCfgInfo." + c1.ClusterId
	  keyList   = keyList :+ key.toLowerCase
	  val nodes = c1.Nodes
	  nodes.foreach(n => {
	    MdMgr.GetMdMgr.RemoveNode(n.NodeId.toLowerCase)
	    key = "NodeInfo." + n.NodeId
	    keyList   = keyList :+ key.toLowerCase
	  })
	})
      }
      val aiList = cfg.Adapters
      if ( aiList != None ){
	val adapters = aiList.get
	adapters.foreach(a => {
	  MdMgr.GetMdMgr.RemoveAdapter(a.Name.toLowerCase)
	  val key = "AdapterInfo." + a.Name
	  keyList   = keyList :+ key.toLowerCase
	})
      }
      RemoveObjectList(keyList,configStore)
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "RemoveConfig", null, ErrorCodeConstants.Remove_Config_Successful + ":" + cfgStr)
      apiResult.toString()
    }catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveConfig", null, "Error :" + e.toString() + ErrorCodeConstants.Remove_Config_Failed + ":" + cfgStr)
        apiResult.toString()
      }
    }
  }

  def UploadConfig(cfgStr: String): String = {
    var keyList = new Array[String](0)
    var valueList = new Array[Array[Byte]](0)
    try {
      // extract config objects
      val cfg = JsonSerializer.parseEngineConfig(cfgStr)
      // process clusterInfo object if it exists
      val clusters = cfg.Clusters

      if ( clusters == None ){
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "UploadConfig", null, ErrorCodeConstants.Upload_Config_Failed + ":" + cfgStr)
        apiResult.toString()
      }
      else{
	val ciList = clusters.get
	logger.debug("Found " + ciList.length + " cluster objects ")
	ciList.foreach(c1 => {
	  logger.debug("Processing the cluster => " + c1.ClusterId)
	  // save in memory
	  var ci = MdMgr.GetMdMgr.MakeCluster(c1.ClusterId,null,null)
	  MdMgr.GetMdMgr.AddCluster(ci)
	  var key = "ClusterInfo." + ci.clusterId
	  var value = serializer.SerializeObjectToByteArray(ci)
	  keyList   = keyList :+ key.toLowerCase
	  valueList = valueList :+ value

	  // gather config name-value pairs
	  val cfgMap = new scala.collection.mutable.HashMap[String,String] 
	  cfgMap("DataStore") = c1.Config.DataStore
	  cfgMap("StatusInfo") = c1.Config.StatusInfo
	  cfgMap("ZooKeeperInfo") = c1.Config.ZooKeeperInfo
	  cfgMap("EnvironmentContext") = c1.Config.EnvironmentContext

	  // save in memory
	  val cic = MdMgr.GetMdMgr.MakeClusterCfg(c1.ClusterId,cfgMap,null,null)
	  MdMgr.GetMdMgr.AddClusterCfg(cic)
	  key = "ClusterCfgInfo." + cic.clusterId
	  value = serializer.SerializeObjectToByteArray(cic)
	  keyList   = keyList :+ key.toLowerCase
	  valueList = valueList :+ value

	  val nodes = c1.Nodes
	  nodes.foreach(n => {
	    val ni = MdMgr.GetMdMgr.MakeNode(n.NodeId,n.NodePort,n.NodeIpAddr,n.JarPaths,
					     n.Scala_home,n.Java_home,n.Classpath,c1.ClusterId,0,0,null)
	    MdMgr.GetMdMgr.AddNode(ni)
	    val key = "NodeInfo." + ni.nodeId
	    val value = serializer.SerializeObjectToByteArray(ni)
	    keyList   = keyList :+ key.toLowerCase
	    valueList = valueList :+ value
	  })
	})
	val aiList = cfg.Adapters
	if ( aiList != None ){
	  val adapters = aiList.get
	  adapters.foreach(a => {
	    var depJars:List[String] = null
	    if(a.DependencyJars != None ){
	      depJars = a.DependencyJars.get
	    }
	    var ascfg:String = null
	    if(a.AdapterSpecificCfg != None ){
	      ascfg = a.AdapterSpecificCfg.get
	    }
	    var inputAdapterToVerify: String = null
	    if(a.InputAdapterToVerify != None ){
	      inputAdapterToVerify = a.InputAdapterToVerify.get
	    }
	    var dataFormat: String = null
	    if(a.DataFormat != None ){
	      dataFormat = a.DataFormat.get
	    }
	    // save in memory
	    val ai = MdMgr.GetMdMgr.MakeAdapter(a.Name,a.TypeString,dataFormat,a.ClassName,a.JarName,depJars,ascfg, inputAdapterToVerify)
	    MdMgr.GetMdMgr.AddAdapter(ai)
	    val key = "AdapterInfo." + ai.name
	    val value = serializer.SerializeObjectToByteArray(ai)
	    keyList   = keyList :+ key.toLowerCase
	    valueList = valueList :+ value
	  })
	}
	else{
	  logger.debug("Found no adapater objects in the config file")
	}
	SaveObjectList(keyList,valueList,configStore)
	var apiResult = new ApiResult(ErrorCodeConstants.Success, "UploadConfig", null, ErrorCodeConstants.Upload_Config_Successful + ":" + cfgStr)
	apiResult.toString()
      }
    }catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "UploadConfig", null, "Error :" + e.toString() + ErrorCodeConstants.Upload_Config_Failed + ":" + cfgStr)
        apiResult.toString()
      }
    }
  }

  // All available nodes(format JSON) as a String
  def GetAllNodes(formatType: String): String = {
    try {
      val nodes = MdMgr.GetMdMgr.Nodes.values.toArray
      if ( nodes.length == 0 ){
          logger.debug("No Nodes found ")
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetAllNodes", null, ErrorCodeConstants.Get_All_Nodes_Failed_Not_Available)
          apiResult.toString()
      }
      else{
        var apiResult = new ApiResult(ErrorCodeConstants.Success, "GetAllNodes", JsonSerializer.SerializeCfgObjectListToJson("Nodes", nodes), ErrorCodeConstants.Get_All_Nodes_Successful)
        apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetAllNodes", null, "Error :" + e.toString() + ErrorCodeConstants.Get_All_Nodes_Failed)
        apiResult.toString()
      }
    }
  }


  // All available adapters(format JSON) as a String
  def GetAllAdapters(formatType: String): String = {
    try {
      val adapters = MdMgr.GetMdMgr.Adapters.values.toArray
      if ( adapters.length == 0 ){
          logger.debug("No Adapters found ")
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetAllAdapters", null, ErrorCodeConstants.Get_All_Adapters_Failed_Not_Available)
          apiResult.toString()
      }
      else{
        var apiResult = new ApiResult(ErrorCodeConstants.Success, "GetAllAdapters", JsonSerializer.SerializeCfgObjectListToJson("Adapters", adapters), ErrorCodeConstants.Get_All_Adapters_Successful)
        apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetAllAdapters", null, "Error :" + e.toString() + ErrorCodeConstants.Get_All_Adapters_Failed)
        apiResult.toString()
      }
    }
  }

  // All available clusters(format JSON) as a String
  def GetAllClusters(formatType: String): String = {
    try {
      val clusters = MdMgr.GetMdMgr.Clusters.values.toArray
      if ( clusters.length == 0 ){
          logger.debug("No Clusters found ")
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetAllClusters", null, ErrorCodeConstants.Get_All_Clusters_Failed_Not_Available)
          apiResult.toString()
      }
      else{
        var apiResult = new ApiResult(ErrorCodeConstants.Success, "GetAllClusters", JsonSerializer.SerializeCfgObjectListToJson("Clusters", clusters), ErrorCodeConstants.Get_All_Clusters_Successful)
        apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetAllClusters", null, "Error :" + e.toString() + ErrorCodeConstants.Get_All_Clusters_Failed)
        apiResult.toString()
      }
    }
  }

  // All available clusterCfgs(format JSON) as a String
  def GetAllClusterCfgs(formatType: String): String = {
    try {
      val clusterCfgs = MdMgr.GetMdMgr.ClusterCfgs.values.toArray
      if ( clusterCfgs.length == 0 ){
          logger.debug("No ClusterCfgs found ")
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetAllClusterCfgs", null, ErrorCodeConstants.Get_All_Cluster_Configs_Failed_Not_Available)
          apiResult.toString()
      }
      else{
        var apiResult = new ApiResult(ErrorCodeConstants.Success, "GetAllClusterCfgs", JsonSerializer.SerializeCfgObjectListToJson("ClusterCfgs", clusterCfgs), ErrorCodeConstants.Get_All_Cluster_Configs_Successful)
        apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetAllClusterCfgs", null, "Error :" + e.toString() + ErrorCodeConstants.Get_All_Cluster_Configs_Failed)
        apiResult.toString()
      }
    }
  }


  // All available config objects(format JSON) as a String
  def GetAllCfgObjects(formatType: String): String = {
    var cfgObjList = new Array[Object](0)
    var jsonStr:String = ""
    var jsonStr1:String = ""
    try {
      val clusters = MdMgr.GetMdMgr.Clusters.values.toArray
      if ( clusters.length != 0 ){
	cfgObjList = cfgObjList :+ clusters
	jsonStr1 = JsonSerializer.SerializeCfgObjectListToJson("Clusters", clusters)
	jsonStr1 = jsonStr1.substring(1)
	jsonStr1 = JsonSerializer.replaceLast(jsonStr1,"}",",")
        jsonStr = jsonStr + jsonStr1
      }
      val clusterCfgs = MdMgr.GetMdMgr.ClusterCfgs.values.toArray
      if ( clusterCfgs.length != 0 ){
        cfgObjList = cfgObjList :+ clusterCfgs
	jsonStr1 = JsonSerializer.SerializeCfgObjectListToJson("ClusterCfgs", clusterCfgs)
	jsonStr1 = jsonStr1.substring(1)
	jsonStr1 = JsonSerializer.replaceLast(jsonStr1,"}",",")
        jsonStr = jsonStr + jsonStr1
      }
      val nodes = MdMgr.GetMdMgr.Nodes.values.toArray
      if ( nodes.length != 0 ){
        cfgObjList = cfgObjList :+ nodes
	jsonStr1 = JsonSerializer.SerializeCfgObjectListToJson("Nodes", nodes)
	jsonStr1 = jsonStr1.substring(1)
	jsonStr1 = JsonSerializer.replaceLast(jsonStr1,"}",",")
        jsonStr = jsonStr + jsonStr1
      }
      val adapters = MdMgr.GetMdMgr.Adapters.values.toArray
      if ( adapters.length != 0 ){
	cfgObjList = cfgObjList :+ adapters
	jsonStr1 = JsonSerializer.SerializeCfgObjectListToJson("Adapters", adapters)
	jsonStr1 = jsonStr1.substring(1)
	jsonStr1 = JsonSerializer.replaceLast(jsonStr1,"}",",")
        jsonStr = jsonStr + jsonStr1
      }

      jsonStr = "{" + JsonSerializer.replaceLast(jsonStr,",","") + "}"

      if ( cfgObjList.length == 0 ){
          logger.debug("No Config Objects found ")
          var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetAllCfgObjects", null, ErrorCodeConstants.Get_All_Configs_Failed_Not_Available)
          apiResult.toString()
      }
      else{
        var apiResult = new ApiResult(ErrorCodeConstants.Success, "GetAllCfgObjects", null, ErrorCodeConstants.Get_All_Configs_Successful)
	//JsonSerializer.SerializeCfgObjectListToJson(cfgObjList))
        apiResult.toString()
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetAllCfgObjects", null, "Error :" + e.toString() + ErrorCodeConstants.Get_All_Configs_Failed)
        apiResult.toString()
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
      aRec.actionTime = getCurrentTime
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
      val recs = auditObj.get(startTime,endTime,userOrRole,action,objectAccessed)
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
	    startTime = parseDateStr(filterParameters(0))
	  }
	  case 2 => {
	    startTime = parseDateStr(filterParameters(0))
	    endTime = parseDateStr(filterParameters(1))
	  }
	  case 3 => {
	    startTime = parseDateStr(filterParameters(0))
	    endTime = parseDateStr(filterParameters(1))
	    userOrRole = filterParameters(2)
	  }
	  case 4 => {
	    startTime = parseDateStr(filterParameters(0))
	    endTime = parseDateStr(filterParameters(1))
	    userOrRole = filterParameters(2)
	    action = filterParameters(3)
	  }
	  case 5 => {
	    startTime = parseDateStr(filterParameters(0))
	    endTime = parseDateStr(filterParameters(1))
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
    
  def dumpMetadataAPIConfig {
    val e = metadataAPIConfig.propertyNames()
    while (e.hasMoreElements()) {
      val key = e.nextElement().asInstanceOf[String]
      val value = metadataAPIConfig.getProperty(key)
      logger.debug("Key : " + key + ", Value : " + value)
    }
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

  @throws(classOf[MissingPropertyException])
  @throws(classOf[InvalidPropertyException])
  def readMetadataAPIConfigFromPropertiesFile(configFile: String): Unit = {
    try {
      if (propertiesAlreadyLoaded) {
        logger.debug("Configuratin properties already loaded, skipping the load configuration step")
        return ;
      }

      val (prop, failStr) = Utils.loadConfiguration(configFile.toString, true)
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

  @throws(classOf[MissingPropertyException])
  @throws(classOf[LoadAPIConfigException])
  def readMetadataAPIConfigFromJsonFile(cfgFile: String): Unit = {
    try {
      if (propertiesAlreadyLoaded) {
        return ;
      }
      var configFile = "MetadataAPIConfig.json"
      if (cfgFile != null) {
        configFile = cfgFile
      }

      val configJson = Source.fromFile(configFile).mkString
      implicit val jsonFormats: Formats = DefaultFormats
      val json = parse(configJson)

      logger.debug("Parsed the json : " + configJson)
      val configMap = json.extract[MetadataAPIConfig]

      var rootDir = configMap.APIConfigParameters.RootDir
      if (rootDir == null) {
        rootDir = System.getenv("HOME")
      }
      logger.debug("RootDir => " + rootDir)

      var gitRootDir = configMap.APIConfigParameters.GitRootDir
      if (gitRootDir == null) {
        gitRootDir = rootDir + "git_hub"
      }
      logger.debug("GitRootDir => " + gitRootDir)

      var database = configMap.APIConfigParameters.MetadataStoreType
      if (database == null) {
        database = "hashmap"
      }
      logger.debug("Database => " + database)

      var databaseLocation = "/tmp"
      var databaseHost = configMap.APIConfigParameters.MetadataLocation
      if (databaseHost == null) {
        databaseHost = "localhost"
      } else {
        databaseLocation = databaseHost
      }
      logger.debug("DatabaseHost => " + databaseHost + ", DatabaseLocation(applicable to treemap or hashmap databases only) => " + databaseLocation)

      var databaseSchema = "metadata"
      val databaseSchemaOpt = configMap.APIConfigParameters.MetadataSchemaName
      if (databaseSchemaOpt != None) {
        databaseSchema = databaseSchemaOpt.get
      }
      logger.debug("DatabaseSchema(applicable to cassandra only) => " + databaseSchema)

      var jarTargetDir = configMap.APIConfigParameters.JarTargetDir
      if (jarTargetDir == null) {
        throw new MissingPropertyException("The property JarTargetDir must be defined in the config file " + configFile)
      }
      logger.debug("JarTargetDir => " + jarTargetDir)

      var jarPaths = jarTargetDir // configMap.APIConfigParameters.JarPaths
      if (jarPaths == null) {
        throw new MissingPropertyException("The property JarPaths must be defined in the config file " + configFile)
      }
      logger.debug("JarPaths => " + jarPaths)
      
      
      var scalaHome = configMap.APIConfigParameters.ScalaHome
      if (scalaHome == null) {
        throw new MissingPropertyException("The property ScalaHome must be defined in the config file " + configFile)
      }
      logger.debug("ScalaHome => " + scalaHome)

      var javaHome = configMap.APIConfigParameters.JavaHome
      if (javaHome == null) {
        throw new MissingPropertyException("The property JavaHome must be defined in the config file " + configFile)
      }
      logger.debug("JavaHome => " + javaHome)

      var manifestPath = configMap.APIConfigParameters.ManifestPath
      if (manifestPath == null) {
        throw new MissingPropertyException("The property ManifestPath must be defined in the config file " + configFile)
      }
      logger.debug("ManifestPath => " + manifestPath)

      var classPath = configMap.APIConfigParameters.ClassPath
      if (classPath == null) {
        throw new MissingPropertyException("The property ClassPath must be defined in the config file " + configFile)
      }
      logger.debug("ClassPath => " + classPath)

      var notifyEngine = configMap.APIConfigParameters.NotifyEngine
      if (notifyEngine == null) {
        throw new MissingPropertyException("The property NotifyEngine must be defined in the config file " + configFile)
      }
      logger.debug("NotifyEngine => " + notifyEngine)

      var znodePath = configMap.APIConfigParameters.ZnodePath
      if (znodePath == null) {
        throw new MissingPropertyException("The property ZnodePath must be defined in the config file " + configFile)
      }
      logger.debug("ZNodePath => " + znodePath)

      var zooKeeperConnectString = configMap.APIConfigParameters.ZooKeeperConnectString
      if (zooKeeperConnectString == null) {
        throw new MissingPropertyException("The property ZooKeeperConnectString must be defined in the config file " + configFile)
      }
      logger.debug("ZooKeeperConnectString => " + zooKeeperConnectString)

      var MODEL_FILES_DIR = ""
      val MODEL_FILES_DIR1 = configMap.APIConfigParameters.MODEL_FILES_DIR
      if (MODEL_FILES_DIR1 == None) {
        MODEL_FILES_DIR = gitRootDir + "/RTD/trunk/MetadataAPI/src/test/SampleTestFiles/Models"
      } else
        MODEL_FILES_DIR = MODEL_FILES_DIR1.get
      logger.debug("MODEL_FILES_DIR => " + MODEL_FILES_DIR)

      var TYPE_FILES_DIR = ""
      val TYPE_FILES_DIR1 = configMap.APIConfigParameters.TYPE_FILES_DIR
      if (TYPE_FILES_DIR1 == None) {
        TYPE_FILES_DIR = gitRootDir + "/RTD/trunk/MetadataAPI/src/test/SampleTestFiles/Types"
      } else
        TYPE_FILES_DIR = TYPE_FILES_DIR1.get
      logger.debug("TYPE_FILES_DIR => " + TYPE_FILES_DIR)

      var FUNCTION_FILES_DIR = ""
      val FUNCTION_FILES_DIR1 = configMap.APIConfigParameters.FUNCTION_FILES_DIR
      if (FUNCTION_FILES_DIR1 == None) {
        FUNCTION_FILES_DIR = gitRootDir + "/RTD/trunk/MetadataAPI/src/test/SampleTestFiles/Functions"
      } else
        FUNCTION_FILES_DIR = FUNCTION_FILES_DIR1.get
      logger.debug("FUNCTION_FILES_DIR => " + FUNCTION_FILES_DIR)

      var CONCEPT_FILES_DIR = ""
      val CONCEPT_FILES_DIR1 = configMap.APIConfigParameters.CONCEPT_FILES_DIR
      if (CONCEPT_FILES_DIR1 == None) {
        CONCEPT_FILES_DIR = gitRootDir + "/RTD/trunk/MetadataAPI/src/test/SampleTestFiles/Concepts"
      } else
        CONCEPT_FILES_DIR = CONCEPT_FILES_DIR1.get
      logger.debug("CONCEPT_FILES_DIR => " + CONCEPT_FILES_DIR)

      var MESSAGE_FILES_DIR = ""
      val MESSAGE_FILES_DIR1 = configMap.APIConfigParameters.MESSAGE_FILES_DIR
      if (MESSAGE_FILES_DIR1 == None) {
        MESSAGE_FILES_DIR = gitRootDir + "/RTD/trunk/MetadataAPI/src/test/SampleTestFiles/Messages"
      } else
        MESSAGE_FILES_DIR = MESSAGE_FILES_DIR1.get
      logger.debug("MESSAGE_FILES_DIR => " + MESSAGE_FILES_DIR)

      var CONTAINER_FILES_DIR = ""
      val CONTAINER_FILES_DIR1 = configMap.APIConfigParameters.CONTAINER_FILES_DIR
      if (CONTAINER_FILES_DIR1 == None) {
        CONTAINER_FILES_DIR = gitRootDir + "/RTD/trunk/MetadataAPI/src/test/SampleTestFiles/Containers"
      } else
        CONTAINER_FILES_DIR = CONTAINER_FILES_DIR1.get

      logger.debug("CONTAINER_FILES_DIR => " + CONTAINER_FILES_DIR)

      var COMPILER_WORK_DIR = ""
      val COMPILER_WORK_DIR1 = configMap.APIConfigParameters.COMPILER_WORK_DIR
      if (COMPILER_WORK_DIR1 == None) {
        COMPILER_WORK_DIR = "/tmp"
      } else
        COMPILER_WORK_DIR = COMPILER_WORK_DIR1.get

      logger.debug("COMPILER_WORK_DIR => " + COMPILER_WORK_DIR)

      var MODEL_EXEC_FLAG = ""
      val MODEL_EXEC_FLAG1 = configMap.APIConfigParameters.MODEL_EXEC_FLAG
      if (MODEL_EXEC_FLAG1 == None) {
        MODEL_EXEC_FLAG = "false"
      } else
        MODEL_EXEC_FLAG = MODEL_EXEC_FLAG1.get

      logger.debug("MODEL_EXEC_FLAG => " + MODEL_EXEC_FLAG)

      val CONFIG_FILES_DIR = gitRootDir + "/RTD/trunk/SampleApplication/Medical/Configs"
      logger.debug("CONFIG_FILES_DIR => " + CONFIG_FILES_DIR)

      metadataAPIConfig.setProperty("ROOT_DIR", rootDir)
      metadataAPIConfig.setProperty("GIT_ROOT", gitRootDir)
      metadataAPIConfig.setProperty("DATABASE", database)
      metadataAPIConfig.setProperty("DATABASE_HOST", databaseHost)
      metadataAPIConfig.setProperty("DATABASE_SCHEMA", databaseSchema)
      metadataAPIConfig.setProperty("DATABASE_LOCATION", databaseLocation)
      metadataAPIConfig.setProperty("JAR_TARGET_DIR", jarTargetDir)
      val jp = if (jarPaths != null) jarPaths else jarTargetDir
      val j_paths = jp.split(",").map(s => s.trim).filter(s => s.size > 0)
      metadataAPIConfig.setProperty("JAR_PATHS", j_paths.mkString(","))
      metadataAPIConfig.setProperty("SCALA_HOME", scalaHome)
      metadataAPIConfig.setProperty("JAVA_HOME", javaHome)
      metadataAPIConfig.setProperty("MANIFEST_PATH", manifestPath)
      metadataAPIConfig.setProperty("CLASSPATH", classPath)
      metadataAPIConfig.setProperty("NOTIFY_ENGINE", notifyEngine)
      metadataAPIConfig.setProperty("ZNODE_PATH", znodePath)
      metadataAPIConfig.setProperty("ZOOKEEPER_CONNECT_STRING", zooKeeperConnectString)
      metadataAPIConfig.setProperty("MODEL_FILES_DIR", MODEL_FILES_DIR)
      metadataAPIConfig.setProperty("TYPE_FILES_DIR", TYPE_FILES_DIR)
      metadataAPIConfig.setProperty("FUNCTION_FILES_DIR", FUNCTION_FILES_DIR)
      metadataAPIConfig.setProperty("CONCEPT_FILES_DIR", CONCEPT_FILES_DIR)
      metadataAPIConfig.setProperty("MESSAGE_FILES_DIR", MESSAGE_FILES_DIR)
      metadataAPIConfig.setProperty("CONTAINER_FILES_DIR", CONTAINER_FILES_DIR)
      metadataAPIConfig.setProperty("COMPILER_WORK_DIR", COMPILER_WORK_DIR)
      metadataAPIConfig.setProperty("MODEL_EXEC_LOG", MODEL_EXEC_FLAG)
      metadataAPIConfig.setProperty("CONFIG_FILES_DIR", CONFIG_FILES_DIR)

      propertiesAlreadyLoaded = true;

    } catch {
      case e: MappingException => {
        throw Json4sParsingException(e.getMessage())
      }
      case e: Exception => {
        throw LoadAPIConfigException("Failed to load configuration: " + e.getMessage())
      }
    }
  }

  def InitMdMgr(configFile: String) {
    MdMgr.GetMdMgr.truncate
    val mdLoader = new MetadataLoad(MdMgr.mdMgr, "", "", "", "")
    mdLoader.initialize
    if (configFile.endsWith(".json")) {
      MetadataAPIImpl.readMetadataAPIConfigFromJsonFile(configFile)
    } else {
      MetadataAPIImpl.readMetadataAPIConfigFromPropertiesFile(configFile)
    }
    MetadataAPIImpl.OpenDbStore(GetMetadataAPIConfig.getProperty("DATABASE"))
    MetadataAPIImpl.LoadAllObjectsIntoCache
    MetadataAPIImpl.CloseDbStore
    MetadataAPIImpl.InitSecImpl
  }

  def InitMdMgrFromBootStrap(configFile: String) {
    MdMgr.GetMdMgr.truncate
    val mdLoader = new MetadataLoad(MdMgr.mdMgr, "", "", "", "")
    mdLoader.initialize
    if (configFile.endsWith(".json")) {
      MetadataAPIImpl.readMetadataAPIConfigFromJsonFile(configFile)
    } else {
      MetadataAPIImpl.readMetadataAPIConfigFromPropertiesFile(configFile)
    }

    MetadataAPIImpl.OpenDbStore(GetMetadataAPIConfig.getProperty("DATABASE"))
    MetadataAPIImpl.LoadAllObjectsIntoCache
    MetadataAPIImpl.InitSecImpl
  }

  def InitMdMgr(mgr: MdMgr, database: String, databaseHost: String, databaseSchema: String, databaseLocation: String) {

  //  SetLoggerLevel(Level.TRACE)
    val mdLoader = new MetadataLoad(mgr, "", "", "", "")
    mdLoader.initialize

    metadataAPIConfig.setProperty("DATABASE", database)
    metadataAPIConfig.setProperty("DATABASE_HOST", databaseHost)
    metadataAPIConfig.setProperty("DATABASE_SCHEMA", databaseSchema)
    metadataAPIConfig.setProperty("DATABASE_LOCATION", databaseLocation)

    MetadataAPIImpl.OpenDbStore(GetMetadataAPIConfig.getProperty("DATABASE"))
    MetadataAPIImpl.LoadAllObjectsIntoCache
    MetadataAPIImpl.InitSecImpl
  }
}

