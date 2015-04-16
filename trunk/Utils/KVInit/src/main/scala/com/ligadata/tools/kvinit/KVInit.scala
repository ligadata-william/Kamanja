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
import com.ligadata.OnLEPBase._
import com.ligadata.olep.metadataload.MetadataLoad
import com.ligadata.Utils.Utils
import java.util.Properties
import com.ligadata.MetadataAPI.MetadataAPIImpl
import com.ligadata.olep.metadata.MdMgr._
import com.ligadata.olep.metadata._
import java.net.URL
import java.net.URLClassLoader
import scala.reflect.runtime.{ universe => ru }
import com.ligadata.Serialize._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

case class DataStoreInfo(StoreType: String, SchemaName: String, Location: String)

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
      java -jar /tmp/OnLEPInstall/KVInit-1.0 --kvname System.TestContainer --config /tmp/OnLEPInstall/EngineConfig.cfg --csvpath /tmp/OnLEPInstall/sampledata/TestContainer.csv --keyfieldname Id

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
        case option :: tail =>
          logger.error("Unknown option " + option)
          sys.exit(1)
      }
    }

    val options = nextOption(Map(), arglist)

    var cfgfile = if (options.contains('config)) options.apply('config) else null
    var kvname = if (options.contains('kvname)) options.apply('kvname) else null
    var csvpath = if (options.contains('csvpath)) options.apply('csvpath) else null
    var keyfieldname = if (options.contains('keyfieldname)) options.apply('keyfieldname) else null
    val dump = if (options.contains('dump)) options.apply('dump) else null

    var valid: Boolean = (cfgfile != null && csvpath != null && keyfieldname != null && kvname != null)

    if (valid) {
      cfgfile = cfgfile.trim
      csvpath = csvpath.trim
      keyfieldname = keyfieldname.trim
      kvname = kvname.trim
      valid = (cfgfile.size != 0 && csvpath.size != 0 && keyfieldname.size != 0 && kvname.size != 0)
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
      val kvmaker: KVInit = new KVInit(loadConfigs, kvname.toLowerCase, csvpath, keyfieldname)
      if (kvmaker.isOk) {
        if (dump != null && dump.toLowerCase().startsWith("y")) {
          val dstore: DataStore = kvmaker.GetDataStoreHandle(kvmaker.dataStoreType, kvmaker.dataSchemaName, "AllData", kvmaker.dataLocation)
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

class KVInit(val loadConfigs: Properties, val kvname: String, val csvpath: String, val keyfieldname: String) extends LogTrait {

  val csvdata: List[String] = Source.fromFile(csvpath).mkString.split("\n", -1).toList
  val header: Array[String] = csvdata.head.split(",", -1).map(_.trim.toLowerCase)
  var isOk: Boolean = header.contains(keyfieldname.toLowerCase())
  var keyPos: Int = 0

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
    MetadataAPIImpl.InitMdMgrFromBootStrap(KvInitConfiguration.configFile)

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

  var dataStoreType: String = null
  var dataSchemaName: String = null
  var dataLocation: String = null

  if (isOk) {
    implicit val jsonFormats: Formats = DefaultFormats
    val dataStoreInfo = parse(dataStore).extract[DataStoreInfo]

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
          isContainer = isDerivedFrom(curClz, "com.ligadata.OnLEPBase.BaseContainerObj")
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
          isMsg = isDerivedFrom(curClz, "com.ligadata.OnLEPBase.BaseMsgObj")
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

  def GetDataStoreHandle(storeType: String, storeName: String, tableName: String, dataLocation: String): DataStore = {
    try {
      var connectinfo = new PropertyMap
      connectinfo += ("connectiontype" -> storeType)
      connectinfo += ("table" -> tableName)
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
    keyPos = 0
    breakable {
      val kvKey: String = keyfieldname.trim.toLowerCase()
      header.foreach(datum => {
        if (datum.trim.toLowerCase() == kvKey) {
          break
        }
        keyPos += 1
      })
    }
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

    val kvstore: DataStore = GetDataStoreHandle(dataStoreType, dataSchemaName, "AllData", dataLocation)
    // kvstore.TruncateStore

    locateKeyPos
    /** locate key idx */

    var processedRows: Int = 0

    val csvdataRecs: List[String] = csvdata.tail
    csvdataRecs.foreach(tuples => {
      if (tuples.size > 0) {
        /** if we can make one ... we add the data to the store. This will crash if the data is bad */
        val inputData = new DelimitedData(tuples, ",")
        inputData.tokens = inputData.dataInput.split(inputData.dataDelim, -1)
        inputData.curPos = 0

        var messageOrContainer: MessageContainerBase = null

        if (isMsg) {
          messageOrContainer = messageObj.CreateNewMessage
        } else if (isContainer) {
          messageOrContainer = containerObj.CreateNewContainer
        } else { // This should not happen

        }

        val key: String = inputData.tokens(keyPos)
        if (messageOrContainer != null) {
          messageOrContainer.populate(inputData)
          try {
            val v = SerializeDeserialize.Serialize(messageOrContainer)
            SaveObject(objFullName, key, v, kvstore, "manual")
            processedRows += 1
          } catch {
            case e: Exception => {
              logger.error("Failed to serialize/write data.")
              e.printStackTrace
            }
          }
        }
      }
    })

    logger.debug("Inserted %d values in KVName %s".format(processedRows, kvname))

    kvstore
  }

  def printTuples(tupleBytes: Value) {
    val buffer: StringBuilder = new StringBuilder
    val tuplesAsString: String = tupleBytes.toString
    tupleBytes.foreach(c => buffer.append(c.toChar))
    val tuples: String = buffer.toString
    //logger.debug(tuples)

    val inputData = new DelimitedData(tuples, ",")
    inputData.tokens = inputData.dataInput.split(inputData.dataDelim, -1)
    inputData.curPos = 0
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

      val data: Array[String] = tuples.split(",", -1).map(_.trim)
      val key: String = data(keyPos)

      datastore.get(makeKey(objFullName, key), printOne)
    })
  }

  private def makeKey(objectname: String, key: String): com.ligadata.keyvaluestore.Key = {
    var k = new com.ligadata.keyvaluestore.Key
    val json =
      ("Obj" -> objectname) ~
        ("Key" -> key.toLowerCase)
    val compjson = compact(render(json))
    k ++= compjson.getBytes("UTF8")

    k
  }

  private def makeValue(value: String, serializerInfo: String): com.ligadata.keyvaluestore.Value = {
    var v = new com.ligadata.keyvaluestore.Value
    v ++= serializerInfo.getBytes("UTF8")

    // Making sure we write first 32 bytes as serializerInfo. Pad it if it is less than 32 bytes
    if (v.size < 32) {
      val spacebyte = ' '.toByte
      for (c <- v.size to 32)
        v += spacebyte
    }

    // Trim if it is more than 32 bytes
    if (v.size > 32) {
      v.reduceToSize(32)
    }

    // Saving Value
    v ++= value.getBytes("UTF8")

    v
  }

  private def makeValue(value: Array[Byte], serializerInfo: String): com.ligadata.keyvaluestore.Value = {
    var v = new com.ligadata.keyvaluestore.Value
    v ++= serializerInfo.getBytes("UTF8")

    // Making sure we write first 32 bytes as serializerInfo. Pad it if it is less than 32 bytes
    if (v.size < 32) {
      val spacebyte = ' '.toByte
      for (c <- v.size to 32)
        v += spacebyte
    }

    // Trim if it is more than 32 bytes
    if (v.size > 32) {
      v.reduceToSize(32)
    }

    // Saving Value
    v ++= value

    v
  }

  private def SaveObject(objName: String, key: String, value: Array[Byte], store: DataStore, serializerInfo: String) {
    object i extends IStorage {
      var k = makeKey(objName, key)
      var v = makeValue(value, serializerInfo)

      def Key = k
      def Value = v
      def Construct(Key: com.ligadata.keyvaluestore.Key, Value: com.ligadata.keyvaluestore.Value) = {}
    }
    store.put(i)
  }

}


