package com.ligadata.MetadataAPI

import org.apache.log4j._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import scala.io._
import java.util.Date
import java.text.SimpleDateFormat
import java.text.ParseException

import com.ligadata.Utils._
import com.ligadata.Serialize._
import com.ligadata.fatafat.metadata.ObjType._
import com.ligadata.fatafat.metadata._
import com.ligadata.fatafat.metadata.MdMgr._
import com.ligadata.keyvaluestore._
import com.ligadata.keyvaluestore.mapdb._
import com.ligadata.keyvaluestore.cassandra._


object MetadataUtils {
  
  lazy val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName) 
  lazy val serializer = SerializerManager.GetSerializer("kryo")
  private[this] val lock = new Object
  
  def IsValidVersion(oldObj: BaseElemDef, newObj: BaseElemDef): Boolean = {
    if (newObj.ver > oldObj.ver) {
      return true
    } else {
      return false
    }
  }
  
  def KeyAsStr(k: com.ligadata.keyvaluestore.Key): String = {
    val k1 = k.toArray[Byte]
    new String(k1)
  }

  def ValueAsStr(v: com.ligadata.keyvaluestore.Value): String = {
    val v1 = v.toArray[Byte]
    new String(v1)
  }
    
  
  def getObjectType(obj: BaseElemDef): String = {
    val className = obj.getClass().getName();
    className.split("\\.").last
  }
  
  def GetNewTranId: Long = {
    try {
      val key = "transaction_id"
      val obj = DaoImpl.GetObject(key, MetadataAPIImpl.getTransStore)
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
      val obj = DaoImpl.GetObject(key, MetadataAPIImpl.getTransStore)
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
      DaoImpl.SaveObject(key, value, MetadataAPIImpl.getTransStore)
    } catch {
      case e: Exception => {
        throw new UpdateStoreFailedException("Unable to Save the transaction id " + tId + ":" + e.getMessage())
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
  
  
  def dumpMetadataAPIConfig {
    val e = MetadataAPIImpl.GetMetadataAPIConfig.propertyNames()
    while (e.hasMoreElements()) {
      val key = e.nextElement().asInstanceOf[String]
      val value = MetadataAPIImpl.GetMetadataAPIConfig.getProperty(key)
      logger.debug("Key : " + key + ", Value : " + value)
    }
  }
  
  /**
   * 
   */
  def RefreshApiConfigForGivenNode(nodeId: String): Boolean = {
    val nd = mdMgr.Nodes.getOrElse(nodeId, null)
    if (nd == null) {
      logger.error("Node %s not found in metadata".format(nodeId))
      return false
    }

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
      MetadataAPIImpl.GetMetadataAPIConfig.setProperty("JAR_PATHS", jarPaths.mkString(","))
      logger.debug("JarPaths Based on node(%s) => %s".format(nodeId,jarPaths))
      val jarDir = compact(render(jarPaths(0))).replace("\"", "").trim
      MetadataAPIImpl.GetMetadataAPIConfig.setProperty("JAR_TARGET_DIR", jarDir)
      logger.debug("Jar_target_dir Based on node(%s) => %s".format(nodeId,jarDir))
    }

    implicit val jsonFormats: Formats = DefaultFormats
    val zKInfo = parse(zooKeeperInfo).extract[ZooKeeperInfo]

    val zkConnectString = zKInfo.ZooKeeperConnectString.replace("\"", "").trim
    MetadataAPIImpl.GetMetadataAPIConfig.setProperty("ZOOKEEPER_CONNECT_STRING", zkConnectString)
    logger.debug("ZOOKEEPER_CONNECT_STRING(based on nodeId) => " + zkConnectString)


    val zkNodeBasePath = zKInfo.ZooKeeperNodeBasePath.replace("\"", "").trim
    MetadataAPIImpl.GetMetadataAPIConfig.setProperty("ZNODE_PATH", zkNodeBasePath)
    logger.debug("ZNODE_PATH(based on nodeid) => " + zkNodeBasePath)

    val zkSessionTimeoutMs1 = if (zKInfo.ZooKeeperSessionTimeoutMs == None || zKInfo.ZooKeeperSessionTimeoutMs == null) 0 else zKInfo.ZooKeeperSessionTimeoutMs.get.toString.toInt
    // Taking minimum values in case if needed
    val zkSessionTimeoutMs = if (zkSessionTimeoutMs1 <= 0) 1000 else zkSessionTimeoutMs1
    MetadataAPIImpl.GetMetadataAPIConfig.setProperty("ZK_SESSION_TIMEOUT_MS", zkSessionTimeoutMs.toString)
    logger.debug("ZK_SESSION_TIMEOUT_MS(based on nodeId) => " + zkSessionTimeoutMs)

    val zkConnectionTimeoutMs1 = if (zKInfo.ZooKeeperConnectionTimeoutMs == None || zKInfo.ZooKeeperConnectionTimeoutMs == null) 0 else zKInfo.ZooKeeperConnectionTimeoutMs.get.toString.toInt
    // Taking minimum values in case if needed
    val zkConnectionTimeoutMs = if (zkConnectionTimeoutMs1 <= 0) 30000 else zkConnectionTimeoutMs1
    MetadataAPIImpl.GetMetadataAPIConfig.setProperty("ZK_CONNECTION_TIMEOUT_MS", zkConnectionTimeoutMs.toString)
    logger.debug("ZK_CONNECTION_TIMEOUT_MS(based on nodeId) => " + zkConnectionTimeoutMs)
    true
  }
  
  @throws(classOf[CreateStoreFailedException])
  def OpenDbStore(storeType: String) {
    try {
      logger.debug("Opening datastore")
      MetadataAPIImpl.setMetadataStore(GetDataStoreHandle(storeType, "metadata_store", "metadata_objects"))
      MetadataAPIImpl.setConfigStore(GetDataStoreHandle(storeType, "config_store", "config_objects"))
      MetadataAPIImpl.setJarStore(GetDataStoreHandle(storeType, "metadata_jars", "jar_store"))
      MetadataAPIImpl.setTransStore(GetDataStoreHandle(storeType, "metadata_trans", "transaction_id"))
      MetadataAPIImpl.setModelStore(MetadataAPIImpl.getMetadataStore)
      MetadataAPIImpl.setMessageStore(MetadataAPIImpl.getMetadataStore)
      MetadataAPIImpl.setContainerStore(MetadataAPIImpl.getMetadataStore)
      MetadataAPIImpl.setFunctionStore (MetadataAPIImpl.getMetadataStore)
      MetadataAPIImpl.setConceptStore(MetadataAPIImpl.getMetadataStore)
      MetadataAPIImpl.setTypeStore(MetadataAPIImpl.getMetadataStore)
      MetadataAPIImpl.setOtherStore(MetadataAPIImpl.getMetadataStore)
      MetadataAPIImpl.setTableStoreMap(Map("models" ->  MetadataAPIImpl.getModelStore,
                                           "messages" -> MetadataAPIImpl.getMessageStore,
                                           "containers" -> MetadataAPIImpl.getContainerStore,
                                           "functions" -> MetadataAPIImpl.getFunctionStore,
                                           "concepts" -> MetadataAPIImpl.getConceptStore,
                                           "types" -> MetadataAPIImpl.getTypeStore,
                                           "others" -> MetadataAPIImpl.getOtherStore,
                                           "jar_store" -> MetadataAPIImpl.getJarStore,
                                           "config_objects" -> MetadataAPIImpl.getConfigStore,
                                           "transaction_id" -> MetadataAPIImpl.getTransStore))
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
  
    @throws(classOf[CreateStoreFailedException])
  def GetDataStoreHandle(storeType: String, storeName: String, tableName: String): DataStore = {
    try {
      var connectinfo = new PropertyMap
      connectinfo += ("connectiontype" -> storeType)
      connectinfo += ("table" -> tableName)
      storeType match {
        case "hbase" => {
          var databaseHost = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("DATABASE_HOST")
          connectinfo += ("hostlist" -> databaseHost)
          connectinfo += ("schema" -> storeName)
        }
        case "hashmap" => {
          var databaseLocation = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("DATABASE_LOCATION")
          connectinfo += ("path" -> databaseLocation)
          connectinfo += ("schema" -> storeName)
          connectinfo += ("inmemory" -> "false")
          connectinfo += ("withtransaction" -> "true")
        }
        case "treemap" => {
          var databaseLocation = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("DATABASE_LOCATION")
          connectinfo += ("path" -> databaseLocation)
          connectinfo += ("schema" -> storeName)
          connectinfo += ("inmemory" -> "false")
          connectinfo += ("withtransaction" -> "true")
        }
        case "cassandra" => {
          var databaseHost = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("DATABASE_HOST")
          var databaseSchema = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("DATABASE_SCHEMA")
          connectinfo += ("hostlist" -> databaseHost)
          connectinfo += ("schema" -> databaseSchema)
          connectinfo += ("ConsistencyLevelRead" -> "ONE")
        }
        case _ => {
          throw new CreateStoreFailedException("The database type " + storeType + " is not supported yet ")
        }
      }
      KeyValueManager.Get(connectinfo)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw new CreateStoreFailedException(e.getMessage())
      }
    }
  }
    
  def CloseDbStore: Unit = lock.synchronized {
    try {
      logger.debug("Closing datastore")
      if (MetadataAPIImpl.getMetadataStore != null) {
        MetadataAPIImpl.getMetadataStore.Shutdown() 
        MetadataAPIImpl.setMetadataStore(null)
        logger.debug("metdatastore closed")
      }
      if (MetadataAPIImpl.getTransStore != null) {
        MetadataAPIImpl.getTransStore.Shutdown() 
        MetadataAPIImpl.setTransStore(null)
        logger.debug("transStore closed")
      }
      if (MetadataAPIImpl.getJarStore != null) {
        MetadataAPIImpl.getJarStore.Shutdown() 
        MetadataAPIImpl.setJarStore(null)
        logger.debug("jarStore closed")
      }
      if (MetadataAPIImpl.getConfigStore != null) {
        MetadataAPIImpl.getConfigStore.Shutdown() 
        MetadataAPIImpl.setConfigStore(null)
        logger.debug("configStore closed")
      }
    } catch {
      case e: Exception => {
        throw e;
      }
    }
  }
  

  @throws(classOf[MissingPropertyException])
  @throws(classOf[InvalidPropertyException])
  def readMetadataAPIConfigFromPropertiesFile(configFile: String): Unit = {
    try {
      if (MetadataAPIImpl.propertiesAlreadyLoaded) {
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
      MetadataAPIImpl.pList.map(v => logger.warn(v+" remains unset"))
      MetadataAPIImpl.propertiesAlreadyLoaded = true;

    } catch {
      case e: Exception =>
        logger.error("Failed to load configuration: " + e.getMessage)
        sys.exit(1)
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
    if (key.equalsIgnoreCase("JAR_TARGET_DIR") && (MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_PATHS")==null)) {
      MetadataAPIImpl.GetMetadataAPIConfig.setProperty("JAR_PATHS", finalValue)
      logger.debug("JAR_PATHS = " + finalValue)
      MetadataAPIImpl.pList = MetadataAPIImpl.pList - "JAR_PATHS"
    }
    
    // Special case 2.. MetadataLocation must set 2 properties in the config object.. 1. prop set by DATABASE_HOST,
    // 2. prop set by DATABASE_LOCATION.  MetadataLocation will overwrite those values, but not the other way around.
    if (key.equalsIgnoreCase("MetadataLocation")) {
      MetadataAPIImpl.GetMetadataAPIConfig.setProperty("DATABASE_LOCATION", finalValue)
      MetadataAPIImpl.GetMetadataAPIConfig.setProperty("DATABASE_HOST", finalValue)
      logger.debug("DATABASE_LOCATION  = " + finalValue)
      MetadataAPIImpl.pList = MetadataAPIImpl.pList - "DATABASE_LOCATION"
      logger.debug("DATABASE_HOST  = " + finalValue)
      MetadataAPIImpl.pList = MetadataAPIImpl.pList - "DATABASE_HOST"
      return
    }
    
    // Special case 2a.. DATABASE_HOST should not override METADATA_LOCATION
    if (key.equalsIgnoreCase("DATABASE_HOST") && (MetadataAPIImpl.GetMetadataAPIConfig.getProperty(key.toUpperCase)!=null)) {
      return
    }
    // Special case 2b.. DATABASE_LOCATION should not override METADATA_LOCATION
    if (key.equalsIgnoreCase("DATABASE_LOCATION") && (MetadataAPIImpl.GetMetadataAPIConfig.getProperty(key.toUpperCase)!=null)) {
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
    MetadataAPIImpl.GetMetadataAPIConfig.setProperty(finalKey.toUpperCase, finalValue)
    logger.debug(finalKey.toUpperCase + " = " + finalValue)
    MetadataAPIImpl.pList = MetadataAPIImpl.pList - finalKey.toUpperCase
  }


  @throws(classOf[MissingPropertyException])
  @throws(classOf[LoadAPIConfigException])
  def readMetadataAPIConfigFromJsonFile(cfgFile: String): Unit = {
    try {
      if (MetadataAPIImpl.propertiesAlreadyLoaded) {
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
        MODEL_FILES_DIR = gitRootDir + "/Fatafat/trunk/MetadataAPI/src/test/SampleTestFiles/Models"
      } else
        MODEL_FILES_DIR = MODEL_FILES_DIR1.get
      logger.debug("MODEL_FILES_DIR => " + MODEL_FILES_DIR)

      var TYPE_FILES_DIR = ""
      val TYPE_FILES_DIR1 = configMap.APIConfigParameters.TYPE_FILES_DIR
      if (TYPE_FILES_DIR1 == None) {
        TYPE_FILES_DIR = gitRootDir + "/Fatafat/trunk/MetadataAPI/src/test/SampleTestFiles/Types"
      } else
        TYPE_FILES_DIR = TYPE_FILES_DIR1.get
      logger.debug("TYPE_FILES_DIR => " + TYPE_FILES_DIR)

      var FUNCTION_FILES_DIR = ""
      val FUNCTION_FILES_DIR1 = configMap.APIConfigParameters.FUNCTION_FILES_DIR
      if (FUNCTION_FILES_DIR1 == None) {
        FUNCTION_FILES_DIR = gitRootDir + "/Fatafat/trunk/MetadataAPI/src/test/SampleTestFiles/Functions"
      } else
        FUNCTION_FILES_DIR = FUNCTION_FILES_DIR1.get
      logger.debug("FUNCTION_FILES_DIR => " + FUNCTION_FILES_DIR)

      var CONCEPT_FILES_DIR = ""
      val CONCEPT_FILES_DIR1 = configMap.APIConfigParameters.CONCEPT_FILES_DIR
      if (CONCEPT_FILES_DIR1 == None) {
        CONCEPT_FILES_DIR = gitRootDir + "/Fatafat/trunk/MetadataAPI/src/test/SampleTestFiles/Concepts"
      } else
        CONCEPT_FILES_DIR = CONCEPT_FILES_DIR1.get
      logger.debug("CONCEPT_FILES_DIR => " + CONCEPT_FILES_DIR)

      var MESSAGE_FILES_DIR = ""
      val MESSAGE_FILES_DIR1 = configMap.APIConfigParameters.MESSAGE_FILES_DIR
      if (MESSAGE_FILES_DIR1 == None) {
        MESSAGE_FILES_DIR = gitRootDir + "/Fatafat/trunk/MetadataAPI/src/test/SampleTestFiles/Messages"
      } else
        MESSAGE_FILES_DIR = MESSAGE_FILES_DIR1.get
      logger.debug("MESSAGE_FILES_DIR => " + MESSAGE_FILES_DIR)

      var CONTAINER_FILES_DIR = ""
      val CONTAINER_FILES_DIR1 = configMap.APIConfigParameters.CONTAINER_FILES_DIR
      if (CONTAINER_FILES_DIR1 == None) {
        CONTAINER_FILES_DIR = gitRootDir + "/Fatafat/trunk/MetadataAPI/src/test/SampleTestFiles/Containers"
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

      val CONFIG_FILES_DIR = gitRootDir + "/Fatafat/trunk/SampleApplication/Medical/Configs"
      logger.debug("CONFIG_FILES_DIR => " + CONFIG_FILES_DIR)

      MetadataAPIImpl.GetMetadataAPIConfig.setProperty("ROOT_DIR", rootDir)
      MetadataAPIImpl.GetMetadataAPIConfig.setProperty("GIT_ROOT", gitRootDir)
      MetadataAPIImpl.GetMetadataAPIConfig.setProperty("DATABASE", database)
      MetadataAPIImpl.GetMetadataAPIConfig.setProperty("DATABASE_HOST", databaseHost)
      MetadataAPIImpl.GetMetadataAPIConfig.setProperty("DATABASE_SCHEMA", databaseSchema)
      MetadataAPIImpl.GetMetadataAPIConfig.setProperty("DATABASE_LOCATION", databaseLocation)
      MetadataAPIImpl.GetMetadataAPIConfig.setProperty("JAR_TARGET_DIR", jarTargetDir)
      val jp = if (jarPaths != null) jarPaths else jarTargetDir
      val j_paths = jp.split(",").map(s => s.trim).filter(s => s.size > 0)
      MetadataAPIImpl.GetMetadataAPIConfig.setProperty("JAR_PATHS", j_paths.mkString(","))
      MetadataAPIImpl.GetMetadataAPIConfig.setProperty("SCALA_HOME", scalaHome)
      MetadataAPIImpl.GetMetadataAPIConfig.setProperty("JAVA_HOME", javaHome)
      MetadataAPIImpl.GetMetadataAPIConfig.setProperty("MANIFEST_PATH", manifestPath)
      MetadataAPIImpl.GetMetadataAPIConfig.setProperty("CLASSPATH", classPath)
      MetadataAPIImpl.GetMetadataAPIConfig.setProperty("NOTIFY_ENGINE", notifyEngine)
      MetadataAPIImpl.GetMetadataAPIConfig.setProperty("ZNODE_PATH", znodePath)
      MetadataAPIImpl.GetMetadataAPIConfig.setProperty("ZOOKEEPER_CONNECT_STRING", zooKeeperConnectString)
      MetadataAPIImpl.GetMetadataAPIConfig.setProperty("MODEL_FILES_DIR", MODEL_FILES_DIR)
      MetadataAPIImpl.GetMetadataAPIConfig.setProperty("TYPE_FILES_DIR", TYPE_FILES_DIR)
      MetadataAPIImpl.GetMetadataAPIConfig.setProperty("FUNCTION_FILES_DIR", FUNCTION_FILES_DIR)
      MetadataAPIImpl.GetMetadataAPIConfig.setProperty("CONCEPT_FILES_DIR", CONCEPT_FILES_DIR)
      MetadataAPIImpl.GetMetadataAPIConfig.setProperty("MESSAGE_FILES_DIR", MESSAGE_FILES_DIR)
      MetadataAPIImpl.GetMetadataAPIConfig.setProperty("CONTAINER_FILES_DIR", CONTAINER_FILES_DIR)
      MetadataAPIImpl.GetMetadataAPIConfig.setProperty("COMPILER_WORK_DIR", COMPILER_WORK_DIR)
      MetadataAPIImpl.GetMetadataAPIConfig.setProperty("MODEL_EXEC_LOG", MODEL_EXEC_FLAG)
      MetadataAPIImpl.GetMetadataAPIConfig.setProperty("CONFIG_FILES_DIR", CONFIG_FILES_DIR)

      MetadataAPIImpl.propertiesAlreadyLoaded = true;

    } catch {
      case e: MappingException => {
        throw Json4sParsingException(e.getMessage())
      }
      case e: Exception => {
        throw LoadAPIConfigException("Failed to load configuration: " + e.getMessage())
      }
    }
  }
    
   /**
   * getSSLCertificatePath
   */
  def getSSLCertificatePath: String = {
    val certPath = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("SSL_CERTIFICATE")
    if (certPath != null) return certPath
    ""
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


  
}