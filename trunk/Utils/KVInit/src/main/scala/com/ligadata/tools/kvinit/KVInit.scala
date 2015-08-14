package com.ligadata.tools.kvinit

import scala.collection.mutable._
import scala.io.Source
import scala.util.control.Breaks._
import java.io.BufferedWriter
import java.io.FileWriter
import sys.process._
import java.io.PrintWriter
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import java.io.ByteArrayInputStream
import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import org.apache.log4j.Logger
import com.ligadata.keyvaluestore._
import com.ligadata.keyvaluestore.mapdb._
import com.ligadata.KamanjaBase._
import com.ligadata.kamanja.metadataload.MetadataLoad
import com.ligadata.Utils.Utils
import java.util.Properties
import com.ligadata.MetadataAPI.MetadataAPIImpl
import com.ligadata.kamanja.metadata.MdMgr._
import com.ligadata.kamanja.metadata._
import java.net.URL
import java.net.URLClassLoader
import scala.reflect.runtime.{ universe => ru }
import com.ligadata.Serialize._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import com.ligadata.KamanjaData.KamanjaData
import java.io.FileInputStream
import java.util.zip.GZIPInputStream
import java.io.BufferedReader
import java.io.InputStreamReader
//import scala.util.control.Breaks._
import scala.collection.mutable.ArrayBuffer
import com.ligadata.ZooKeeper._
import org.apache.curator.framework._
import com.ligadata.Serialize.{ JDataStore, JZKInfo, JEnvCtxtJsonStr }

trait LogTrait {
  val loggerName = this.getClass.getName()
  val logger = Logger.getLogger(loggerName)
}

object KVInit extends App with LogTrait {

  def usage: String = {
    """ 
Usage: scala com.ligadata.kvinit.KVInit 
    --config <config file while has jarpaths, metadata store information & data store information>
    --kvname <full package qualified name of a Container or Message> 
    --csvpath <input to load> 
    --keyfieldname  <name of one of the fields in the first line of the csvpath file>
    --dump <if any{Y | y | yes | Yes} just dump an existing store>

Nothing fancy here.  Mapdb kv store is created from arguments... style is hash map. Support
for other styles of input (e.g., JSON, XML) are not supported.  
      
The name of the kvstore will be the classname(without it path).

It is expected that the first row of the csv file will be the column names.  One of the names
must be specified as the key field name.  Failure to find this name causes termination and
no kv store creation.
      
Sample uses:
      java -jar /tmp/KamanjaInstall/KVInit-1.0 --kvname System.TestContainer --config /tmp/KamanjaInstall/EngineConfig.cfg --csvpath /tmp/KamanjaInstall/sampledata/TestContainer.csv --keyfieldname Id

"""
  }

  override def main(args: Array[String]) {

    logger.debug("KVInit.main begins")

    if (args.length == 0) logger.error(usage)
    val arglist = args.toList
    type OptionMap = Map[Symbol, String]
    logger.debug(arglist)
    def nextOption(map: OptionMap, list: List[String]): OptionMap = {
      list match {
        case Nil => map
        case "--config" :: value :: tail =>
          nextOption(map ++ Map('config -> value), tail)
        case "--kvname" :: value :: tail =>
          nextOption(map ++ Map('kvname -> value), tail)
        case "--csvpath" :: value :: tail =>
          nextOption(map ++ Map('csvpath -> value), tail)
        case "--keyfieldname" :: value :: tail =>
          nextOption(map ++ Map('keyfieldname -> value), tail)
        case "--dump" :: value :: tail =>
          nextOption(map ++ Map('dump -> value), tail)
        case "--delimiter" :: value :: tail =>
          nextOption(map ++ Map('delimiter -> value), tail)
        case "--keyseparator" :: value :: tail =>
          nextOption(map ++ Map('keyseparator -> value), tail)
        case "--ignoreerrors" :: value :: tail =>
          nextOption(map ++ Map('ignoreerrors -> value), tail)
        case option :: tail =>
          logger.error("Unknown option " + option)
          sys.exit(1)
      }
    }

    val options = nextOption(Map(), arglist)

    var cfgfile = if (options.contains('config)) options.apply('config) else null
    var kvname = if (options.contains('kvname)) options.apply('kvname) else null
    var csvpath = if (options.contains('csvpath)) options.apply('csvpath) else null
    val tmpkeyfieldnames = (if (options.contains('keyfieldname)) options.apply('keyfieldname) else "")
    val dump = if (options.contains('dump)) options.apply('dump) else null
    val delimiterString = if (options.contains('delimiter)) options.apply('delimiter) else null
    var keyseparator = if (options.contains('keyseparator)) options.apply('keyseparator) else ","
    var ignoreerrors = (if (options.contains('ignoreerrors)) options.apply('ignoreerrors) else "0").trim
    if (keyseparator.size == 0) keyseparator = ","
    if (ignoreerrors.size == 0) ignoreerrors = "0"

    val keyfieldnames = tmpkeyfieldnames.split(",").map(_.trim).filter(_.length() > 0)

    var valid: Boolean = (cfgfile != null && csvpath != null && keyfieldnames.size > 0 && kvname != null)

    if (valid) {
      cfgfile = cfgfile.trim
      csvpath = csvpath.trim
      kvname = kvname.trim
      valid = (cfgfile.size != 0 && csvpath.size != 0 && keyfieldnames.size > 0 && kvname.size != 0)
    }

    if (valid) {
      val (loadConfigs, failStr) = Utils.loadConfiguration(cfgfile.toString, true)
      if (failStr != null && failStr.size > 0) {
        logger.error(failStr)
        return
      }
      if (loadConfigs == null) {
        logger.error("Failed to load configurations from configuration file")
        return
      }

      KvInitConfiguration.configFile = cfgfile.toString
      val kvmaker: KVInit = new KVInit(loadConfigs, kvname.toLowerCase, csvpath, keyfieldnames, delimiterString, keyseparator, ignoreerrors)
      if (kvmaker.isOk) {
        if (dump != null && dump.toLowerCase().startsWith("y")) {
          val dstore: DataStore = kvmaker.GetDataStoreHandle(kvmaker.dataStoreType, kvmaker.dataSchemaName, "AllData", kvmaker.dataLocation, kvmaker.adapterSpecificConfig)
          kvmaker.dump(dstore)
          dstore.Shutdown()
        } else {
          val dstore: DataStore = kvmaker.buildContainerOrMessage
          //kvmaker.dump(dstore)
          dstore.Shutdown()
        }
      }
      MetadataAPIImpl.CloseDbStore

    } else {
      logger.error("Illegal and/or missing arguments")
      logger.error(usage)
    }
  }
}

class KvInitClassLoader(urls: Array[URL], parent: ClassLoader) extends URLClassLoader(urls, parent) {
  override def addURL(url: URL) {
    super.addURL(url)
  }
}

class KvInitLoaderInfo {
  // class loader
  val loader: KvInitClassLoader = new KvInitClassLoader(ClassLoader.getSystemClassLoader().asInstanceOf[URLClassLoader].getURLs(), getClass().getClassLoader())

  // Loaded jars
  val loadedJars: TreeSet[String] = new TreeSet[String];

  // Get a mirror for reflection
  val mirror = ru.runtimeMirror(loader)
}

object KvInitConfiguration {
  var nodeId: Int = _
  var configFile: String = _
  var jarPaths: collection.immutable.Set[String] = _
  def GetValidJarFile(jarPaths: collection.immutable.Set[String], jarName: String): String = {
    if (jarPaths == null) return jarName // Returning base jarName if no jarpaths found
    jarPaths.foreach(jPath => {
      val fl = new File(jPath + "/" + jarName)
      if (fl.exists) {
        return fl.getPath
      }
    })
    return jarName // Returning base jarName if not found in jar paths
  }
}

class KVInit(val loadConfigs: Properties, val kvname: String, val csvpath: String, val keyfieldnames: Array[String], delimiterString: String, keyseparator: String, ignoreerrors: String) extends LogTrait {

  val dataDelim = if (delimiterString != null && delimiterString.size > 0) delimiterString else ","
  var ignoreErrsCount = if (ignoreerrors != null && ignoreerrors.size > 0) ignoreerrors.toInt else 0
  if (ignoreErrsCount < 0) ignoreErrsCount = 0
  //  val csvdata: List[String] = Source.fromFile(csvpath).mkString.split("\n", -1).toList
  val csvdata: List[String] = fileData(csvpath)
  val header: Array[String] = csvdata.head.split(dataDelim, -1).map(_.trim.toLowerCase)
  var isOk: Boolean = true
  var keyPositions = new Array[Int](keyfieldnames.size)

  keyfieldnames.foreach(keynm => {
    isOk = isOk && header.contains(keynm.toLowerCase())
  })

  val kvInitLoader = new KvInitLoaderInfo

  val metadataStoreType = loadConfigs.getProperty("MetadataStoreType".toLowerCase, "").replace("\"", "").trim
  if (metadataStoreType.size == 0) {
    logger.error("Not found valid MetadataStoreType.")
    isOk = false
  }

  val metadataSchemaName = loadConfigs.getProperty("MetadataSchemaName".toLowerCase, "").replace("\"", "").trim
  if (metadataSchemaName.size == 0) {
    logger.error("Not found valid MetadataSchemaName.")
    isOk = false
  }

  val metadataLocation = loadConfigs.getProperty("MetadataLocation".toLowerCase, "").replace("\"", "").trim
  if (metadataLocation.size == 0) {
    logger.error("Not found valid MetadataLocation.")
    isOk = false
  }

  /*
  val metadataPrincipal = loadConfigs.getProperty("MetadataPrincipal".toLowerCase, "").replace("\"", "").trim
  val metadataKeytab = loadConfigs.getProperty("MetadataKeytab".toLowerCase, "").replace("\"", "").trim
*/

  KvInitConfiguration.nodeId = loadConfigs.getProperty("nodeId".toLowerCase, "0").replace("\"", "").trim.toInt
  if (KvInitConfiguration.nodeId <= 0) {
    logger.error("Not found valid nodeId. It should be greater than 0")
    isOk = false
  }

  var nodeInfo: NodeInfo = _

  if (isOk) {
    /*
    if (metadataStoreType.compareToIgnoreCase("cassandra") == 0 || metadataStoreType.compareToIgnoreCase("hbase") == 0)
      MetadataAPIImpl.InitMdMgr(mdMgr, metadataStoreType, metadataLocation, metadataSchemaName, "")
    else if ((metadataStoreType.compareToIgnoreCase("treemap") == 0) || (metadataStoreType.compareToIgnoreCase("hashmap") == 0))
      MetadataAPIImpl.InitMdMgr(mdMgr, metadataStoreType, "", metadataSchemaName, metadataLocation)
*/
    MetadataAPIImpl.InitMdMgrFromBootStrap(KvInitConfiguration.configFile, false)

    nodeInfo = mdMgr.Nodes.getOrElse(KvInitConfiguration.nodeId.toString, null)
    if (nodeInfo == null) {
      logger.error("Node %d not found in metadata".format(KvInitConfiguration.nodeId))
      isOk = false
    }
  }

  if (isOk) {
    KvInitConfiguration.jarPaths = if (nodeInfo.JarPaths == null) Array[String]().toSet else nodeInfo.JarPaths.map(str => str.replace("\"", "").trim).filter(str => str.size > 0).toSet
    if (KvInitConfiguration.jarPaths.size == 0) {
      logger.error("Not found valid JarPaths.")
      isOk = false
    }
  }

  val cluster = if (isOk) mdMgr.ClusterCfgs.getOrElse(nodeInfo.ClusterId, null) else null
  if (isOk && cluster == null) {
    logger.error("Cluster not found for Node %d  & ClusterId : %s".format(KvInitConfiguration.nodeId, nodeInfo.ClusterId))
    isOk = false
  }

  val dataStore = if (isOk) cluster.cfgMap.getOrElse("DataStore", null) else null
  if (isOk && dataStore == null) {
    logger.error("DataStore not found for Node %d  & ClusterId : %s".format(KvInitConfiguration.nodeId, nodeInfo.ClusterId))
    isOk = false
  }

  val zooKeeperInfo = if (isOk) cluster.cfgMap.getOrElse("ZooKeeperInfo", null) else null
  if (isOk && dataStore == null) {
    logger.error("ZooKeeperInfo not found for Node %d  & ClusterId : %s".format(KvInitConfiguration.nodeId, nodeInfo.ClusterId))
    isOk = false
  }

  var dataStoreType: String = null
  var dataSchemaName: String = null
  var dataLocation: String = null
  var adapterSpecificConfig: String = null
  var zkConnectString: String = null
  var zkNodeBasePath: String = null
  var zkSessionTimeoutMs: Int = 0
  var zkConnectionTimeoutMs: Int = 0

  if (isOk) {
    implicit val jsonFormats: Formats = DefaultFormats
    val dataStoreInfo = parse(dataStore).extract[JDataStore]
    val zKInfo = parse(zooKeeperInfo).extract[JZKInfo]

    if (isOk) {
      dataStoreType = dataStoreInfo.StoreType.replace("\"", "").trim
      if (dataStoreType.size == 0) {
        logger.error("Not found valid DataStoreType.")
        isOk = false
      }
    }

    if (isOk) {
      dataSchemaName = dataStoreInfo.SchemaName.replace("\"", "").trim
      if (dataSchemaName.size == 0) {
        logger.error("Not found valid DataSchemaName.")
        isOk = false
      }
    }

    if (isOk) {
      dataLocation = dataStoreInfo.Location.replace("\"", "").trim
      if (dataLocation.size == 0) {
        logger.error("Not found valid DataLocation.")
        isOk = false
      }
    }

    adapterSpecificConfig = if (dataStoreInfo.AdapterSpecificConfig == None || dataStoreInfo.AdapterSpecificConfig == null) "" else dataStoreInfo.AdapterSpecificConfig.get.replace("\"", "").trim

    if (isOk) {
      zkConnectString = zKInfo.ZooKeeperConnectString.replace("\"", "").trim
      zkNodeBasePath = zKInfo.ZooKeeperNodeBasePath.replace("\"", "").trim
      zkSessionTimeoutMs = if (zKInfo.ZooKeeperSessionTimeoutMs == None || zKInfo.ZooKeeperSessionTimeoutMs == null) 0 else zKInfo.ZooKeeperSessionTimeoutMs.get.toString.toInt
      zkConnectionTimeoutMs = if (zKInfo.ZooKeeperConnectionTimeoutMs == None || zKInfo.ZooKeeperConnectionTimeoutMs == null) 0 else zKInfo.ZooKeeperConnectionTimeoutMs.get.toString.toInt

      // Taking minimum values in case if needed
      if (zkSessionTimeoutMs <= 0)
        zkSessionTimeoutMs = 30000
      if (zkConnectionTimeoutMs <= 0)
        zkConnectionTimeoutMs = 30000
    }

    if (zkConnectString.size == 0) {
      logger.warn("Not found valid Zookeeper connection string.")
    }

    if (zkConnectString.size > 0 && zkNodeBasePath.size == 0) {
      logger.warn("Not found valid Zookeeper ZNode Base Path.")
    }
  }

  var kvNameCorrType: BaseTypeDef = _
  var kvTableName: String = _
  var messageObj: BaseMsgObj = _
  var containerObj: BaseContainerObj = _
  var objFullName: String = _
  // var kryoSer: com.ligadata.Serialize.Serializer = null

  if (isOk) {
    kvNameCorrType = mdMgr.ActiveType(kvname.toLowerCase)
    if (kvNameCorrType == null || kvNameCorrType == None) {
      logger.error("Not found valid type for " + kvname.toLowerCase)
      isOk = false
    } else {
      objFullName = kvNameCorrType.FullName.toLowerCase
      kvTableName = objFullName.replace('.', '_')
    }
  }

  var isMsg = false
  var isContainer = false

  if (isOk) {
    isOk = LoadJarIfNeeded(kvNameCorrType, kvInitLoader.loadedJars, kvInitLoader.loader)
  }

  if (isOk) {
    var clsName = kvNameCorrType.PhysicalName.trim
    if (clsName.size > 0 && clsName.charAt(clsName.size - 1) != '$') // if no $ at the end we are taking $
      clsName = clsName + "$"

    if (isMsg == false) {
      // Checking for Message
      try {
        // If required we need to enable this test
        // Convert class name into a class
        var curClz = Class.forName(clsName, true, kvInitLoader.loader)

        while (curClz != null && isContainer == false) {
          isContainer = isDerivedFrom(curClz, "com.ligadata.KamanjaBase.BaseContainerObj")
          if (isContainer == false)
            curClz = curClz.getSuperclass()
        }
      } catch {
        case e: Exception => {
          logger.error("Failed to get classname:%s as message".format(clsName))
          e.printStackTrace
        }
      }
    }

    if (isContainer == false) {
      // Checking for container
      try {
        // If required we need to enable this test
        // Convert class name into a class
        var curClz = Class.forName(clsName, true, kvInitLoader.loader)

        while (curClz != null && isMsg == false) {
          isMsg = isDerivedFrom(curClz, "com.ligadata.KamanjaBase.BaseMsgObj")
          if (isMsg == false)
            curClz = curClz.getSuperclass()
        }
      } catch {
        case e: Exception => {
          logger.error("Failed to get classname:%s as container".format(clsName))
          e.printStackTrace
        }
      }
    }

    if (isMsg || isContainer) {
      try {
        val module = kvInitLoader.mirror.staticModule(clsName)
        val obj = kvInitLoader.mirror.reflectModule(module)
        val objinst = obj.instance
        if (objinst.isInstanceOf[BaseMsgObj]) {
          messageObj = objinst.asInstanceOf[BaseMsgObj]
          logger.debug("Created Message Object")
        } else if (objinst.isInstanceOf[BaseContainerObj]) {
          containerObj = objinst.asInstanceOf[BaseContainerObj]
          logger.debug("Created Container Object")
        } else {
          logger.error("Failed to instantiate message or conatiner object :" + clsName)
          isOk = false
        }
      } catch {
        case e: Exception => {
          logger.error("Failed to instantiate message or conatiner object:" + clsName + ". Reason:" + e.getCause + ". Message:" + e.getMessage())
          isOk = false
        }
      }
    } else {
      logger.error("Failed to instantiate message or conatiner object :" + clsName)
      isOk = false
    }
    /*
    if (isOk) {
      kryoSer = SerializerManager.GetSerializer("kryo")
      if (kryoSer != null)
        kryoSer.SetClassLoader(kvInitLoader.loader)
    }
*/
  }

  private def LoadJarIfNeeded(elem: BaseElem, loadedJars: TreeSet[String], loader: KvInitClassLoader): Boolean = {
    if (KvInitConfiguration.jarPaths == null) return false

    var retVal: Boolean = true
    var allJars: Array[String] = null

    val jarname = if (elem.JarName == null) "" else elem.JarName.trim

    if (elem.DependencyJarNames != null && elem.DependencyJarNames.size > 0 && jarname.size > 0) {
      allJars = elem.DependencyJarNames :+ jarname
    } else if (elem.DependencyJarNames != null && elem.DependencyJarNames.size > 0) {
      allJars = elem.DependencyJarNames
    } else if (jarname.size > 0) {
      allJars = Array(jarname)
    } else {
      return retVal
    }

    val jars = allJars.map(j => KvInitConfiguration.GetValidJarFile(KvInitConfiguration.jarPaths, j))

    // Loading all jars
    for (j <- jars) {
      logger.debug("Processing Jar " + j.trim)
      val fl = new File(j.trim)
      if (fl.exists) {
        try {
          if (loadedJars(fl.getPath())) {
            logger.debug("Jar " + j.trim + " already loaded to class path.")
          } else {
            loader.addURL(fl.toURI().toURL())
            logger.debug("Jar " + j.trim + " added to class path.")
            loadedJars += fl.getPath()
          }
        } catch {
          case e: Exception => {
            logger.error("Jar " + j.trim + " failed added to class path. Message: " + e.getMessage)
            return false
          }
        }
      } else {
        logger.error("Jar " + j.trim + " not found")
        return false
      }
    }

    true
  }

  def GetDataStoreHandle(storeType: String, storeName: String, tableName: String, dataLocation: String, adapterSpecificConfig: String): DataStore = {
    try {
      var connectinfo = new PropertyMap
      connectinfo += ("connectiontype" -> storeType)
      connectinfo += ("table" -> tableName)
      if (adapterSpecificConfig != null)
        connectinfo += ("adapterspecificconfig" -> adapterSpecificConfig)
      storeType match {
        case "hashmap" => {
          connectinfo += ("path" -> dataLocation)
          connectinfo += ("schema" -> tableName) // Using tableName instead of storeName here to save into different tables
          connectinfo += ("inmemory" -> "false")
          connectinfo += ("withtransaction" -> "false")
        }
        case "treemap" => {
          connectinfo += ("path" -> dataLocation)
          connectinfo += ("schema" -> tableName) // Using tableName instead of storeName here to save into different tables
          connectinfo += ("inmemory" -> "false")
          connectinfo += ("withtransaction" -> "false")
        }
        case "cassandra" => {
          connectinfo += ("hostlist" -> dataLocation)
          connectinfo += ("schema" -> storeName)
          connectinfo += ("ConsistencyLevelRead" -> "ONE")
        }
        case "hbase" => {
          connectinfo += ("hostlist" -> dataLocation)
          connectinfo += ("schema" -> storeName)
        }
        case _ => {
          throw new Exception("The database type " + storeType + " is not supported yet ")
        }
      }
      logger.debug("Getting DB Connection: " + connectinfo.mkString(","))
      KeyValueManager.Get(connectinfo)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw new Exception(e.getMessage())
      }
    }
  }

  def locateKeyPos: Unit = {
    /** locate the key position */
    for (i <- 0 until keyfieldnames.size)
      keyPositions(i) = -1 // By default -1
    var i: Int = 0
    keyfieldnames.foreach(keynm => {
      var keyPos = 0
      val kvKey = keynm.trim.toLowerCase()
      breakable {
        header.foreach(datum => {
          if (datum.trim.toLowerCase() == kvKey) {
            keyPositions(i) = keyPos
            break
          }
          keyPos += 1
        })
      }
      i += 1
    })
  }

  def isDerivedFrom(clz: Class[_], clsName: String): Boolean = {
    var isIt: Boolean = false

    val interfecs = clz.getInterfaces()
    logger.debug("Interfaces => " + interfecs.length + ",isDerivedFrom: Class=>" + clsName)

    breakable {
      for (intf <- interfecs) {
        logger.debug("Interface:" + intf.getName())
        if (intf.getName().equals(clsName)) {
          isIt = true
          break
        }
      }
    }

    isIt
  }

  def buildContainerOrMessage: DataStore = {
    if (!isOk) return null

    val kvstore: DataStore = GetDataStoreHandle(dataStoreType, dataSchemaName, "AllData", dataLocation, adapterSpecificConfig)
    // kvstore.TruncateStore

    locateKeyPos
    /** locate key idx */

    var processedRows: Int = 0
    var errsCnt: Int = 0

    val csvdataRecs: List[String] = csvdata.tail
    val savedKeys = new ArrayBuffer[List[String]](csvdataRecs.size)

    csvdataRecs.foreach(tuples => {
      if (tuples.size > 0) {
        /** if we can make one ... we add the data to the store. This will crash if the data is bad */
        val inputData = new DelimitedData(tuples, dataDelim)
        inputData.tokens = inputData.dataInput.split(inputData.dataDelim, -1)
        inputData.curPos = 0

        var messageOrContainer: MessageContainerBase = null

        if (isMsg) {
          messageOrContainer = messageObj.CreateNewMessage
        } else if (isContainer) {
          messageOrContainer = containerObj.CreateNewContainer
        } else { // This should not happen

        }

        if (messageOrContainer != null) {
          try {
            messageOrContainer.populate(inputData)
          } catch {
            case e: Exception => {
              logger.error("Failed to populate message/container.")
              e.printStackTrace
              errsCnt += 1
            }
          }
          try {
            val datarec = new KamanjaData
            // var keyData = messageOrContainer.PartitionKeyData
            val keyData = keyPositions.map(kp => inputData.tokens(kp)) // We should take messageOrContainer.PartitionKeyData instead of this. That way we have proper value convertion before giving keys to us.
            datarec.SetKey(keyData)
            datarec.SetTypeName(objFullName) // objFullName should be messageOrContainer.FullName.toString
            datarec.AddMessageContainerBase(messageOrContainer, true, true)
            SaveObject(datarec.SerializeKey, datarec.SerializeData, kvstore, "manual")
            savedKeys += keyData.toList
            processedRows += 1
          } catch {
            case e: Exception => {
              logger.error("Failed to serialize/write data.")
              e.printStackTrace
              errsCnt += 1
            }
          }
        }
      }
      if (errsCnt > ignoreErrsCount) {
        val errStr = "Populate/Serialize errors (%d) exceed the given count(%d)." format (errsCnt, ignoreErrsCount)
        logger.error(errStr)
        throw new Exception(errStr)
      }
    })

    logger.info("Inserted %d values in KVName %s".format(processedRows, kvname))

    if (zkConnectString != null && zkNodeBasePath != null && zkConnectString.size > 0 && zkNodeBasePath.size > 0) {
      logger.info("Notifying Engines after updating is done through Zookeeper.")
      var zkcForSetData: CuratorFramework = null
      try {
        val dataChangeZkNodePath = zkNodeBasePath + "/datachange"
        CreateClient.CreateNodeIfNotExists(zkConnectString, dataChangeZkNodePath) // Creating 
        zkcForSetData = CreateClient.createSimple(zkConnectString, zkSessionTimeoutMs, zkConnectionTimeoutMs)
        val changedContainersData = Map[String, List[List[String]]]()
        changedContainersData(kvname) = savedKeys.toList
        val datachangedata = ("txnid" -> "0") ~
          ("changeddatakeys" -> changedContainersData.map(kv =>
            ("C" -> kv._1) ~
              ("K" -> kv._2)))
        val sendJson = compact(render(datachangedata))
        zkcForSetData.setData().forPath(dataChangeZkNodePath, sendJson.getBytes("UTF8"))
      } catch {
        case e: Exception => logger.error("Failed to send update notification to engine.")
      } finally {
        if (zkcForSetData != null)
          zkcForSetData.close
      }
    } else {
      logger.error("Failed to send update notification to engine.")
    }

    kvstore
  }

  private def getSerializeInfo(tupleBytes: Value): String = {
    if (tupleBytes.size < _serInfoBufBytes) return ""
    val serInfoBytes = new Array[Byte](_serInfoBufBytes)
    tupleBytes.copyToArray(serInfoBytes, 0, _serInfoBufBytes)
    return (new String(serInfoBytes)).trim
  }

  private def getValueInfo(tupleBytes: Value): Array[Byte] = {
    if (tupleBytes.size < _serInfoBufBytes) return null
    val valInfoBytes = new Array[Byte](tupleBytes.size - _serInfoBufBytes)
    Array.copy(tupleBytes.toArray, _serInfoBufBytes, valInfoBytes, 0, tupleBytes.size - _serInfoBufBytes)
    valInfoBytes
  }

  def printTuples(tupleBytes: Value) {
    // Get first _serInfoBufBytes bytes
    if (tupleBytes.size < _serInfoBufBytes) {
      val errMsg = s"Invalid input. This has only ${tupleBytes.size} bytes data. But we are expecting serializer buffer bytes as of size ${_serInfoBufBytes}"
      logger.error(errMsg)
      throw new Exception(errMsg)
    }

    val serInfo = getSerializeInfo(tupleBytes)

    serInfo.toLowerCase match {
      case "kryo" => {
        // BUGBUG:: Yet to handle
      }
      case "manual" => {
        // BUGBUG:: Yet to handle
      }
      case _ => {
        throw new Exception("Found un-handled Serializer Info: " + serInfo)
      }
    }

    logger.debug(s"\n$kvname")
  }

  def dump(datastore: DataStore): Unit = {
    logger.debug(s"\nDump of data store $kvname")

    locateKeyPos
    /** locate key idx */

    val printOne = (tupleBytes: Value) => { printTuples(tupleBytes) }
    val csvdataRecs: List[String] = csvdata.tail
    csvdataRecs.foreach(tuples => {
      /** if we can make one ... we add the data to the store. This will crash if the data is bad */

      val data: Array[String] = tuples.split(dataDelim, -1).map(_.trim)
      val partkey = keyPositions.map(kp => data(kp)).toList
      datastore.get(makeKey(KamanjaData.PrepareKey(objFullName, partkey, 0, 0)), printOne)
    })
  }

  private def makeKey(key: String): com.ligadata.keyvaluestore.Key = {
    var k = new com.ligadata.keyvaluestore.Key
    k ++= key.getBytes("UTF8")
    k
  }

  private[this] var _serInfoBufBytes = 32

  private def makeValue(value: String, serializerInfo: String): com.ligadata.keyvaluestore.Value = {
    var v = new com.ligadata.keyvaluestore.Value
    v ++= serializerInfo.getBytes("UTF8")

    // Making sure we write first _serInfoBufBytes bytes as serializerInfo. Pad it if it is less than _serInfoBufBytes bytes
    if (v.size < _serInfoBufBytes) {
      val spacebyte = ' '.toByte
      for (c <- v.size to _serInfoBufBytes)
        v += spacebyte
    }

    // Trim if it is more than _serInfoBufBytes bytes
    if (v.size > _serInfoBufBytes) {
      v.reduceToSize(_serInfoBufBytes)
    }

    // Saving Value
    v ++= value.getBytes("UTF8")

    v
  }

  private def makeValue(value: Array[Byte], serializerInfo: String): com.ligadata.keyvaluestore.Value = {
    var v = new com.ligadata.keyvaluestore.Value
    v ++= serializerInfo.getBytes("UTF8")

    // Making sure we write first _serInfoBufBytes bytes as serializerInfo. Pad it if it is less than _serInfoBufBytes bytes
    if (v.size < _serInfoBufBytes) {
      val spacebyte = ' '.toByte
      for (c <- v.size to _serInfoBufBytes)
        v += spacebyte
    }

    // Trim if it is more than _serInfoBufBytes bytes
    if (v.size > _serInfoBufBytes) {
      v.reduceToSize(_serInfoBufBytes)
    }

    // Saving Value
    v ++= value

    v
  }

  private def SaveObject(key: String, value: Array[Byte], store: DataStore, serializerInfo: String) {
    object i extends IStorage {
      var k = makeKey(key)
      var v = makeValue(value, serializerInfo)

      def Key = k
      def Value = v
      def Construct(Key: com.ligadata.keyvaluestore.Key, Value: com.ligadata.keyvaluestore.Value) = {}
    }
    store.put(i)
  }

  private def fileData(inputeventfile: String): List[String] = {
    var br: BufferedReader = null
    try {
      if (isCompressed(inputeventfile)) {
        br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(inputeventfile))))
      } else {
        br = new BufferedReader(new InputStreamReader(new FileInputStream(inputeventfile)))
      }
    } catch {
      case e: Exception => {
        logger.error("Failed to open Input File %s. Message:%s".format(inputeventfile, e.getMessage))
        throw e
      }
    }
    var fileContentsArray = ArrayBuffer[String]()
    var line: String = ""
    while ({ line = br.readLine(); line != null }) {
      fileContentsArray += line
    }
    br.close();

    return fileContentsArray.toList
  }

  private def isCompressed(inputfile: String): Boolean = {
    var is: FileInputStream = null
    try {
      is = new FileInputStream(inputfile)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        return false
    }

    val maxlen = 2
    val buffer = new Array[Byte](maxlen)
    val readlen = is.read(buffer, 0, maxlen)

    is.close() // Close before we really check and return the data

    if (readlen < 2)
      return false;

    val b0: Int = buffer(0)
    val b1: Int = buffer(1)

    val head = (b0 & 0xff) | ((b1 << 8) & 0xff00)

    return (head == GZIPInputStream.GZIP_MAGIC);
  }

}


