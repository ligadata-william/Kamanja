
package com.ligadata.SimpleEnvContextImpl

import scala.collection.immutable.Map
import scala.collection.mutable._
import scala.util.control.Breaks._
import scala.reflect.runtime.{ universe => ru }
import org.apache.log4j.Logger
import com.ligadata.keyvaluestore._
import com.ligadata.keyvaluestore.mapdb._
import com.ligadata.OnLEPBase._
import com.ligadata.OnLEPBase.{ EnvContext, MessageContainerBase }
import com.ligadata.olep.metadata._
import java.net.URLClassLoader
import com.ligadata.Serialize._

trait LogTrait {
  val loggerName = this.getClass.getName()
  val logger = Logger.getLogger(loggerName)
}

/**
 *  The SimpleEnvContextImpl supports kv stores that are based upon MapDb hash tables.
 */
object SimpleEnvContextImpl extends EnvContext with LogTrait {

  class TxnCtxtKey {
    var containerName: String = _
    var key: String = _
  }

  class MsgContainerInfo {
    var data: scala.collection.mutable.Map[String, MessageContainerBase] = scala.collection.mutable.Map[String, MessageContainerBase]()
    var dataStore: DataStore = null
    var containerType: BaseTypeDef = null
    var loadedAll: Boolean = false
    var tableName: String = ""
    var objFullName: String = ""
  }

  class TransactionContext(var txnId: Long) {
    /* private[this] */ val _messagesOrContainers = scala.collection.mutable.Map[String, MsgContainerInfo]()

    def getAllObjects(containerName: String): Array[MessageContainerBase] = {
      null
    }

    def getObject(containerName: String, key: String): MessageContainerBase = {
      val container = _messagesOrContainers.getOrElse(containerName.toLowerCase(), null)
      if (container != null) {
        val v = container.data.getOrElse(key.toLowerCase(), null)
        if (v != null) return v
      }
      null
    }

    def setObject(containerName: String, key: String, value: MessageContainerBase): Unit = {}

    def contains(containerName: String, key: String): Boolean = false
    def containsAny(containerName: String, keys: Array[String]): Boolean = false
    def containsAll(containerName: String, keys: Array[String]): Boolean = false

    // Adapters Keys & values
    def setAdapterUniqueKeyValue(key: String, value: String): Unit = {}
    def getAdapterUniqueKeyValue(key: String): String = null

    // Model Results Saving & retrieving. Don't return null, always return empty, if we don't find
    def saveModelsResult(key: String, value: scala.collection.mutable.Map[String, ModelResult]): Unit = {}
    def getModelsResult(key: String): scala.collection.mutable.Map[String, ModelResult] = null
  }

  private[this] val _lock = new Object()

  private[this] val _messagesOrContainers = scala.collection.mutable.Map[String, MsgContainerInfo]()
  private[this] val _txnContexts = scala.collection.mutable.Map[Long, TransactionContext]()

  private[this] var _serInfoBufBytes = 32

  private[this] var _kryoSer: Serializer = null
  private[this] var classLoader: java.lang.ClassLoader = null
  private[this] var _adapterUniqKvDataStore: DataStore = null
  private[this] var _modelsResultDataStore: DataStore = null
  private[this] val _adapterUniqKeyValData = scala.collection.mutable.Map[String, String]()
  private[this] val _modelsResult = scala.collection.mutable.Map[String, scala.collection.mutable.Map[String, ModelResult]]()

  override def SetClassLoader(cl: java.lang.ClassLoader): Unit = {
    classLoader = cl
  }

  override def Shutdown: Unit = _lock.synchronized {
    if (_adapterUniqKvDataStore != null)
      _adapterUniqKvDataStore.Shutdown
    _adapterUniqKvDataStore = null
    _adapterUniqKeyValData.clear
    _messagesOrContainers.foreach(mrc => {
      if (mrc._2.dataStore != null)
        mrc._2.dataStore.Shutdown
    })
    _messagesOrContainers.clear
  }

  // Adding new messages or Containers
  override def AddNewMessageOrContainers(mgr: MdMgr, storeType: String, dataLocation: String, schemaName: String, containerNames: Array[String], loadAllData: Boolean): Unit = _lock.synchronized {
    logger.info("AddNewMessageOrContainers => " + (if (containerNames != null) containerNames.mkString(",") else ""))
    if (_adapterUniqKvDataStore == null) {
      logger.info("AddNewMessageOrContainers => storeType:%s, dataLocation:%s, schemaName:%s".format(storeType, dataLocation, schemaName))
      _adapterUniqKvDataStore = GetDataStoreHandle(storeType, schemaName, "AdapterUniqKvData", dataLocation)
    }

    containerNames.foreach(c1 => {
      val c = c1.toLowerCase
      val names: Array[String] = c.split('.')
      val namespace: String = names.head
      val name: String = names.last
      var containerType = mgr.ActiveType(namespace, name)
      if (containerType != null) {

        val objFullName: String = containerType.FullName.toLowerCase

        val fnd = _messagesOrContainers.getOrElse(objFullName, null)

        if (fnd != null) {
          // We already have this
        } else {
          val newMsgOrContainer = new MsgContainerInfo
          val tableName = objFullName.replace('.', '_');

          newMsgOrContainer.dataStore = GetDataStoreHandle(storeType, schemaName, tableName, dataLocation)
          newMsgOrContainer.containerType = containerType
          newMsgOrContainer.objFullName = objFullName
          newMsgOrContainer.tableName = tableName

          /** create a map to cache the entries to be resurrected from the mapdb */
          _messagesOrContainers(objFullName) = newMsgOrContainer

          if (loadAllData) {
            val keys: ArrayBuffer[String] = ArrayBuffer[String]()
            val keyCollector = (key: Key) => { collectKey(key, keys) }
            newMsgOrContainer.dataStore.getAllKeys(keyCollector)
            if (keys.size > 0) {
              loadMap(containerType, keys, newMsgOrContainer)
            }
            newMsgOrContainer.loadedAll = true
          }

        }
      } else {
        logger.error("Message/Container %s not found".format(c))
      }
    })
  }

  /**
   *  For the current container, load the values for each key, coercing it to the appropriate MessageContainerBase, and storing
   *  each in the supplied map.
   *
   *  @param containerType - a ContainerDef that describes the current container.  Its typeName is used to create an instance
   *  	of the MessageContainerBase derivative.
   *  @param keys : the keys to use on the dstore to extract a given element.
   *  @param dstore : the mapdb handle
   *  @param map : the map to be updated with key/MessageContainerBase pairs.
   */
  private def loadMap(containerType: BaseTypeDef, keys: ArrayBuffer[String], msgCntrInfo: MsgContainerInfo): Unit = {

    var objs: Array[MessageContainerBase] = new Array[MessageContainerBase](1)
    keys.foreach(key => {
      try {
        val buildOne = (tupleBytes: Value) => { buildObject(tupleBytes, objs, containerType) }
        msgCntrInfo.dataStore.get(makeKey(key), buildOne)
        msgCntrInfo.data(key.toLowerCase) = objs(0)
      } catch {
        case e: ClassNotFoundException => {
          logger.error(s"unable to create a container named ${containerType.typeString}... is it defined in the metadata?")
          e.printStackTrace()
          throw e
        }
        case ooh: Throwable => {
          logger.error(s"unknown error encountered while processing ${containerType.typeString}.. stack trace = ${ooh.printStackTrace}")
          throw ooh
        }
      }
    })
    logger.trace("Loaded %d objects for %s".format(msgCntrInfo.data.size, containerType.FullName))
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

  private def buildObject(tupleBytes: Value, objs: Array[MessageContainerBase], containerType: BaseTypeDef) {
    // Get first _serInfoBufBytes bytes
    if (tupleBytes.size < _serInfoBufBytes) {
      val errMsg = s"Invalid input. This has only ${tupleBytes.size} bytes data. But we are expecting serializer buffer bytes as of size ${_serInfoBufBytes}"
      logger.error(errMsg)
      throw new Exception(errMsg)
    }

    val serInfo = getSerializeInfo(tupleBytes)

    serInfo.toLowerCase match {
      case "csv" => {
        val valInfo = getValueInfo(tupleBytes)
        objs(0) = Class.forName(containerType.typeString).newInstance().asInstanceOf[MessageContainerBase]
        val inputData = new DelimitedData(new String(valInfo), ",")
        inputData.tokens = inputData.dataInput.split(inputData.dataDelim, -1)
        inputData.curPos = 0
        objs(0).populate(inputData)
      }
      case "kryo" => {
        val valInfo = getValueInfo(tupleBytes)
        if (_kryoSer == null) {
          _kryoSer = SerializerManager.GetSerializer("kryo")
          if (_kryoSer != null && classLoader != null) {
            _kryoSer.SetClassLoader(classLoader)
          }
        }
        if (_kryoSer != null) {
          objs(0) = _kryoSer.DeserializeObjectFromByteArray(valInfo).asInstanceOf[MessageContainerBase]
        }
      }
      case _ => {
        throw new Exception("Found un-handled Serializer Info: " + serInfo)
      }
    }
  }

  private def collectKey(key: Key, keys: ArrayBuffer[String]): Unit = {
    val buffer: StringBuilder = new StringBuilder
    key.foreach(c => buffer.append(c.toChar))
    val containerKey: String = buffer.toString
    keys += containerKey
  }

  private def GetDataStoreHandle(storeType: String, storeName: String, tableName: String, dataLocation: String): DataStore = {
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
      logger.info("Getting DB Connection: " + connectinfo.mkString(","))
      KeyValueManager.Get(connectinfo)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw new Exception(e.getMessage())
      }
    }
  }

  override def getAllObjects(tempTransId: Long, containerName: String): Array[MessageContainerBase] = _lock.synchronized {
    val fnd = _messagesOrContainers.getOrElse(containerName.toLowerCase, null)
    val setVals: Array[MessageContainerBase] = if (fnd != null) {
      if (fnd.loadedAll) {
        val map: scala.collection.mutable.Map[String, MessageContainerBase] = fnd.data
        val filterVals: Array[MessageContainerBase] = map.values.toArray
        /** cache it for subsequent calls */
        filterVals
      } else {
        throw new Exception("Object %s is not loaded all at once. So, we can not get all objects here".format(fnd.tableName))
        Array[MessageContainerBase]()
      }
    } else {
      Array[MessageContainerBase]()
    }
    setVals
  }

  private def makeKey(key: String): com.ligadata.keyvaluestore.Key = {
    var k = new com.ligadata.keyvaluestore.Key
    k ++= key.toLowerCase.getBytes("UTF8")

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

  private def localGetObject(tempTransId: Long, containerName: String, key: String): MessageContainerBase = _lock.synchronized {
    val txnCtxt = _txnContexts.getOrElse(tempTransId, null)
    if (txnCtxt != null) {
      val v = txnCtxt.getObject(containerName, key)
      if (v != null) return v
    }

    val container = _messagesOrContainers.getOrElse(containerName.toLowerCase(), null)
    if (container != null) {
      val v = container.data.getOrElse(key.toLowerCase(), null)
      if (v != null) return v
      var objs: Array[MessageContainerBase] = new Array[MessageContainerBase](1)
      val buildOne = (tupleBytes: Value) => { buildObject(tupleBytes, objs, container.containerType) }
      try {
        container.dataStore.get(makeKey(key), buildOne)
      } catch {
        case e: Exception => {
          logger.trace("Data not found for key:" + key)
        }
      }
      if (objs(0) != null)
        container.data(key.toLowerCase) = objs(0)
      return objs(0)
    } else null
  }

  override def getObject(tempTransId: Long, containerName: String, key: String): MessageContainerBase = {
    localGetObject(tempTransId, containerName, key)
  }

  def localSetObject(tempTransId: Long, containerName: String, key: String, value: MessageContainerBase): Unit = _lock.synchronized {
    var txnCtxt = _txnContexts.getOrElse(tempTransId, null)
    if (txnCtxt == null) {
      txnCtxt = new TransactionContext(tempTransId)
      _txnContexts(tempTransId) = txnCtxt
    }
    txnCtxt.setObject(containerName, key, value)
  }

  override def setObject(tempTransId: Long, containerName: String, key: String, value: MessageContainerBase): Unit = _lock.synchronized {
    // localSetObject(tempTransId, containerName, key, value)
    val container = _messagesOrContainers.getOrElse(containerName.toLowerCase(), null)
    if (container != null) {
      val k = key.toLowerCase
      container.data(k) = value
      if (_kryoSer == null) {
        _kryoSer = SerializerManager.GetSerializer("kryo")
        if (_kryoSer != null && classLoader != null) {
          _kryoSer.SetClassLoader(classLoader)
        }
      }
      try {
        val v = _kryoSer.SerializeObjectToByteArray(value)
        writeThru(k, v, container.dataStore, "kryo")
      } catch {
        case e: Exception => {
          logger.error("Failed to serialize/write data.")
          e.printStackTrace
        }
      }
    }
    // bugbug: throw exception
  }

  private def writeThru(key: String, value: Array[Byte], store: DataStore, serializerInfo: String) {
    object i extends IStorage {
      val k = makeKey(key)
      val v = makeValue(value, serializerInfo)

      def Key = k
      def Value = v
      def Construct(Key: com.ligadata.keyvaluestore.Key, Value: com.ligadata.keyvaluestore.Value) = {}
    }
    store.put(i)
  }

  /**
   *   Does the supplied key exist in a container with the supplied name?
   */
  override def contains(tempTransId: Long, containerName: String, key: String): Boolean = {
    val container = _messagesOrContainers.getOrElse(containerName.toLowerCase(), null)
    val isPresent = if (container != null) {
      val lkey: String = key.toString.toLowerCase()
      container.data.contains(lkey)
    } else {
      false
    }
    isPresent
  }

  /**
   *   Does at least one of the supplied keys exist in a container with the supplied name?
   */
  override def containsAny(tempTransId: Long, containerName: String, keys: Array[String]): Boolean = {
    val container = _messagesOrContainers.getOrElse(containerName.toLowerCase(), null)
    val isPresent = if (container != null) {
      val matches: Int = keys.filter(key => container.data.contains(key.toLowerCase())).size
      (matches > 0)
    } else {
      false
    }
    isPresent
  }

  /**
   *   Do all of the supplied keys exist in a container with the supplied name?
   */
  override def containsAll(tempTransId: Long, containerName: String, keys: Array[String]): Boolean = {
    val container = _messagesOrContainers.getOrElse(containerName.toLowerCase(), null)
    val isPresent = if (container != null) {
      val matches: Int = keys.filter(key => container.data.contains(key.toLowerCase())).size
      (matches == keys.size)
    } else {
      false
    }
    isPresent
  }

  override def setAdapterUniqueKeyValue(tempTransId: Long, key: String, value: String): Unit = _lock.synchronized {
    _adapterUniqKeyValData(key) = value
    try {
      writeThru(key, value.getBytes("UTF8"), _adapterUniqKvDataStore, "CSV")
    } catch {
      case e: Exception => {
        logger.error("Failed to write data.")
        e.printStackTrace
      }
    }
  }

  private def buildAdapterUniqueValue(tupleBytes: Value, objs: Array[String]) {
    // Get first _serInfoBufBytes bytes
    if (tupleBytes.size < _serInfoBufBytes) {
      val errMsg = s"Invalid input. This has only ${tupleBytes.size} bytes data. But we are expecting serializer buffer bytes as of size ${_serInfoBufBytes}"
      logger.error(errMsg)
      throw new Exception(errMsg)
    }

    val valInfo = getValueInfo(tupleBytes)

    objs(0) = new String(valInfo)
  }

  override def getAdapterUniqueKeyValue(key: String): String = _lock.synchronized {
    val v = _adapterUniqKeyValData.getOrElse(key, null)
    if (v != null) return v
    var objs: Array[String] = new Array[String](1)
    val buildAdapOne = (tupleBytes: Value) => { buildAdapterUniqueValue(tupleBytes, objs) }
    try {
      _adapterUniqKvDataStore.get(makeKey(key), buildAdapOne)
    } catch {
      case e: Exception => {
        logger.trace("Data not found for key:" + key)
      }
    }
    if (objs(0) != null)
      _adapterUniqKeyValData(key) = objs(0)
    return objs(0)
  }

  override def saveModelsResult(tempTransId: Long, key: String, value: scala.collection.mutable.Map[String, ModelResult]): Unit = _lock.synchronized {
    _modelsResult(key) = value
    if (_kryoSer == null) {
      _kryoSer = SerializerManager.GetSerializer("kryo")
      if (_kryoSer != null && classLoader != null) {
        _kryoSer.SetClassLoader(classLoader)
      }
    }
    try {
      val v = _kryoSer.SerializeObjectToByteArray(value)
      writeThru(key, v, _modelsResultDataStore, "kryo")
    } catch {
      case e: Exception => {
        logger.error("Failed to write data.")
        e.printStackTrace
      }
    }
  }

  private def buildModelsResult(tupleBytes: Value, objs: Array[scala.collection.mutable.Map[String, ModelResult]]) {
    // Get first _serInfoBufBytes bytes
    if (tupleBytes.size < _serInfoBufBytes) {
      val errMsg = s"Invalid input. This has only ${tupleBytes.size} bytes data. But we are expecting serializer buffer bytes as of size ${_serInfoBufBytes}"
      logger.error(errMsg)
      throw new Exception(errMsg)
    }

    val serInfo = getSerializeInfo(tupleBytes)

    serInfo.toLowerCase match {
      case "kryo" => {
        val valInfo = getValueInfo(tupleBytes)
        if (_kryoSer == null) {
          _kryoSer = SerializerManager.GetSerializer("kryo")
          if (_kryoSer != null && classLoader != null) {
            _kryoSer.SetClassLoader(classLoader)
          }
        }
        if (_kryoSer != null) {
          objs(0) = _kryoSer.DeserializeObjectFromByteArray(valInfo).asInstanceOf[scala.collection.mutable.Map[String, ModelResult]]
        }
      }
      case _ => {
        throw new Exception("Found un-handled Serializer Info: " + serInfo)
      }
    }
  }

  override def getModelsResult(tempTransId: Long, key: String): scala.collection.mutable.Map[String, ModelResult] = _lock.synchronized {
    val v = _modelsResult.getOrElse(key, null)
    if (v != null) return v
    var objs = new Array[scala.collection.mutable.Map[String, ModelResult]](1)
    val buildAdapOne = (tupleBytes: Value) => { buildModelsResult(tupleBytes, objs) }
    try {
      _modelsResultDataStore.get(makeKey(key), buildAdapOne)
    } catch {
      case e: Exception => {
        logger.trace("Data not found for key:" + key)
      }
    }
    if (objs(0) != null) {
      _modelsResult(key) = objs(0)
      return objs(0)
    }
    return scala.collection.mutable.Map[String, ModelResult]()
  }

  // Save Current State of the machine
  override def PersistLocalNodeStateEntries: Unit = {
    // BUGBUG:: Persist all state on this node.
  }

  // Save Remaining State of the machine
  override def PersistRemainingStateEntriesOnLeader: Unit = {
    // BUGBUG:: Persist Remaining state (when other nodes goes down, this helps)
  }

  // Final Commit for the given transaction
  override def commitData(tempTransId: Long): Unit = {
    // BUGBUG:: Commit Data and Removed Transaction information from status
  }

  // Saving Status
  override def saveStatus(tempTransId: Long, status: String): Unit = {
    // BUGBUG:: Save status on local nodes (in memory distributed)
  }

}

