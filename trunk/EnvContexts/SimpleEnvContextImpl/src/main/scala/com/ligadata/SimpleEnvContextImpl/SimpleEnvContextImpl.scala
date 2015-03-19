
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
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

trait LogTrait {
  val loggerName = this.getClass.getName()
  val logger = Logger.getLogger(loggerName)
}

case class AdapterUniqueValueDes(Val: String, xidx: Int, xtot: Int) // Using most of the values as optional values. Just thinking about future changes. Don't know the performance issues.
case class ObjKey(Obj: String, Key: String)

/**
 *  The SimpleEnvContextImpl supports kv stores that are based upon MapDb hash tables.
 */
object SimpleEnvContextImpl extends EnvContext with LogTrait {

	/** Create an empty MessageContainerBase */
	override def NewMessageOrContainer(fqclassname : String) : MessageContainerBase = {
		val msgOrContainer : MessageContainerBase = Class.forName(fqclassname).newInstance().asInstanceOf[MessageContainerBase]
		msgOrContainer
	}
	
	/** Create an empty Object of Any kind */
	override def NewObject(fqclassname : String) : Any = {
		val anObject : Any = Class.forName(fqclassname).newInstance()
		anObject
	}
	
	override def getAnyObject(tempTransId: Long, partitionKey : String, containerName: String, key: String): Any = {
		/** FIXME: BugBug: we need an implementation for this that will either resurrect a MessageContainerBase or some
		 *  simple object that has factory object TypeImplementation[T] support like those in BaseTypes.scala
		 */
		null
	}
	

  class TxnCtxtKey {
    var containerName: String = _
    var key: String = _
  }

  class MsgContainerInfo {
    var data: scala.collection.mutable.Map[String, MessageContainerBase] = scala.collection.mutable.Map[String, MessageContainerBase]()
    var containerType: BaseTypeDef = null
    var loadedAll: Boolean = false
    var reload: Boolean = false
    var objFullName: String = ""
  }

  class TransactionContext(var txnId: Long) {
    private[this] val _messagesOrContainers = scala.collection.mutable.Map[String, MsgContainerInfo]()
    private[this] val _adapterUniqKeyValData = scala.collection.mutable.Map[String, (String, Int, Int)]()
    private[this] val _modelsResult = scala.collection.mutable.Map[String, scala.collection.mutable.Map[String, ModelResult]]()
    private[this] val _statusStrings = new ArrayBuffer[String]()

    private[this] def getMsgContainer(containerName: String, addIfMissing: Boolean): MsgContainerInfo = {
      var fnd = _messagesOrContainers.getOrElse(containerName.toLowerCase, null)
      if (fnd == null && addIfMissing) {
        fnd = new MsgContainerInfo
        _messagesOrContainers(containerName) = fnd
      }
      fnd
    }

    def getAllObjects(containerName: String): scala.collection.immutable.Map[String, MessageContainerBase] = {
      val fnd = getMsgContainer(containerName.toLowerCase, false)
      if (fnd != null)
        return fnd.data.toMap
      scala.collection.immutable.Map[String, MessageContainerBase]()
    }

    def getObject(containerName: String, key: String): MessageContainerBase = {
      val container = getMsgContainer(containerName.toLowerCase, false)
      if (container != null) {
        val v = container.data.getOrElse(key.toLowerCase, null)
        if (v != null) return v
      }
      null
    }

    def containsAny(containerName: String, keys: scala.collection.immutable.Set[String]): Boolean = {
      val container = getMsgContainer(containerName.toLowerCase, false)
      if (container != null) {
        keys.foreach(key => {
          if (container.data.contains(key.toLowerCase))
            return true
        })
      }
      false
    }

    def containsKeys(containerName: String, keys: scala.collection.immutable.Set[String]): scala.collection.immutable.Set[String] = {
      val container = getMsgContainer(containerName.toLowerCase, false)
      val matchedKeys = scala.collection.mutable.Set[String]()
      if (container != null) {
        keys.foreach(key => {
          val k = key.toLowerCase
          if (container.data.contains(k)) {
            matchedKeys += k
          }
        })
      }
      matchedKeys.toSet
    }

    def setObject(containerName: String, key: String, value: MessageContainerBase): Unit = {
      val container = getMsgContainer(containerName.toLowerCase, true)
      if (container != null) {
        val k = key.toLowerCase
        container.data(k) = value
      }
    }
    

    // Adapters Keys & values
    def setAdapterUniqueKeyValue(key: String, value: String, xformedMsgCntr: Int, totalXformedMsgs: Int): Unit = {
      _adapterUniqKeyValData(key) = (value, xformedMsgCntr, totalXformedMsgs)
    }

    def getAdapterUniqueKeyValue(key: String): (String, Int, Int) = {
      _adapterUniqKeyValData.getOrElse(key, null)
    }

    // Model Results Saving & retrieving. Don't return null, always return empty, if we don't find
    def saveModelsResult(key: String, value: scala.collection.mutable.Map[String, ModelResult]): Unit = {
      _modelsResult(key) = value
    }

    def getModelsResult(key: String): scala.collection.mutable.Map[String, ModelResult] = {
      _modelsResult.getOrElse(key, null)
    }

    def setReloadFlag(containerName: String): Unit = {
      val container = getMsgContainer(containerName.toLowerCase, false)
      if (container != null)
        container.reload = true
    }

    def saveStatus(status: String): Unit = {
      _statusStrings += status
    }

    def getAllMessagesAndContainers = _messagesOrContainers.toMap
    def getAllAdapterUniqKeyValData = _adapterUniqKeyValData.toMap
    def getAllModelsResult = _modelsResult.toMap
    def getAllStatusStrings = _statusStrings
    
    
  }

  private[this] val _buckets = 257 // Prime number

  private[this] val _locks = new Array[Object](_buckets)

  private[this] val _messagesOrContainers = scala.collection.mutable.Map[String, MsgContainerInfo]()
  private[this] val _txnContexts = new Array[scala.collection.mutable.Map[Long, TransactionContext]](_buckets)
  private[this] val _adapterUniqKeyValData = scala.collection.mutable.Map[String, (String, Int, Int)]()
  private[this] val _modelsResult = scala.collection.mutable.Map[String, scala.collection.mutable.Map[String, ModelResult]]()

  private[this] var _serInfoBufBytes = 32

  private[this] var _kryoSer: com.ligadata.Serialize.Serializer = null
  private[this] var _classLoader: java.lang.ClassLoader = null
  private[this] var _allDataDataStore: DataStore = null
  private[this] var _runningTxnsDataStore: DataStore = null
  private[this] var _checkPointAdapInfoDataStore: DataStore = null

  for (i <- 0 until _buckets) {
    _txnContexts(i) = scala.collection.mutable.Map[Long, TransactionContext]()
    _locks(i) = new Object()
  }

  private[this] def lockIdx(tempTransId: Long): Int = {
    return (tempTransId % _buckets).toInt
  }

  private[this] def getTransactionContext(tempTransId: Long, addIfMissing: Boolean): TransactionContext = {
    _locks(lockIdx(tempTransId)).synchronized {
      var txnCtxt = _txnContexts(lockIdx(tempTransId)).getOrElse(tempTransId, null)
      if (txnCtxt == null && addIfMissing) {
        txnCtxt = new TransactionContext(tempTransId)
        _txnContexts(lockIdx(tempTransId))(tempTransId) = txnCtxt
      }
      return txnCtxt
    }
  }

  private[this] def removeTransactionContext(tempTransId: Long): Unit = {
    _locks(lockIdx(tempTransId)).synchronized {
      _txnContexts(lockIdx(tempTransId)) -= tempTransId
    }
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
  private def loadMap(keys: Array[String], msgOrCont: MsgContainerInfo): Unit = {
    var objs: Array[MessageContainerBase] = new Array[MessageContainerBase](1)
    keys.foreach(key => {
      try {
        val buildOne = (tupleBytes: Value) => { buildObject(tupleBytes, objs, msgOrCont.containerType) }
        _allDataDataStore.get(makeKey(msgOrCont.objFullName, key), buildOne)
        msgOrCont.synchronized {
          msgOrCont.data(key.toLowerCase) = objs(0)
        }
      } catch {
        case e: ClassNotFoundException => {
          logger.error(s"unable to create a container named ${msgOrCont.containerType.typeString}... is it defined in the metadata?")
          e.printStackTrace()
          throw e
        }
        case ooh: Throwable => {
          logger.error(s"unknown error encountered while processing ${msgOrCont.containerType.typeString}.. stack trace = ${ooh.printStackTrace}")
          throw ooh
        }
      }
    })
    logger.trace("Loaded %d objects for %s".format(msgOrCont.data.size, msgOrCont.objFullName))
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
          if (_kryoSer != null && _classLoader != null) {
            _kryoSer.SetClassLoader(_classLoader)
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

  private def makeKey(objectname: String, key: String): com.ligadata.keyvaluestore.Key = {
    var k = new com.ligadata.keyvaluestore.Key
    // val compjson = "{\"Obj\": \"%s\", \"Key\": \"%s\"}".format(objectname, key.toLowerCase)
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

  /*
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
*/

  private def buildAdapterUniqueValue(tupleBytes: Value, objs: Array[(String, Int, Int)]) {
    // Get first _serInfoBufBytes bytes
    if (tupleBytes.size < _serInfoBufBytes) {
      val errMsg = s"Invalid input. This has only ${tupleBytes.size} bytes data. But we are expecting serializer buffer bytes as of size ${_serInfoBufBytes}"
      logger.error(errMsg)
      throw new Exception(errMsg)
    }

    val valInfo = getValueInfo(tupleBytes)

    implicit val jsonFormats: Formats = DefaultFormats
    val uniqVal = parse(new String(valInfo)).extract[AdapterUniqueValueDes]

    objs(0) = (uniqVal.Val, uniqVal.xidx, uniqVal.xtot)
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
          if (_kryoSer != null && _classLoader != null) {
            _kryoSer.SetClassLoader(_classLoader)
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

  private def loadObjFromDb(tempTransId: Long, msgOrCont: MsgContainerInfo, key: String): MessageContainerBase = {
    var objs: Array[MessageContainerBase] = new Array[MessageContainerBase](1)
    val buildOne = (tupleBytes: Value) => { buildObject(tupleBytes, objs, msgOrCont.containerType) }
    try {
      _allDataDataStore.get(makeKey(msgOrCont.objFullName, key), buildOne)
    } catch {
      case e: Exception => {
        logger.trace("Data not found for key:" + key)
      }
    }
    if (objs(0) != null) {
      val lockObj = if (msgOrCont.loadedAll) msgOrCont else _locks(lockIdx(tempTransId))
      lockObj.synchronized {
        msgOrCont.data(key.toLowerCase) = objs(0)
      }
    }
    return objs(0)
  }

  private def localGetObject(tempTransId: Long, containerName: String, key: String): MessageContainerBase = {
    val txnCtxt = getTransactionContext(tempTransId, false)
    if (txnCtxt != null) {
      val v = txnCtxt.getObject(containerName, key)
      if (v != null) return v
    }

    val container = _messagesOrContainers.getOrElse(containerName.toLowerCase, null)
    if (container != null) {
      val v = container.data.getOrElse(key.toLowerCase, null)
      if (v != null) return v
      return loadObjFromDb(tempTransId, container, key)
    }
    null
  }

  private def localGetAllKeyValues(tempTransId: Long, containerName: String): scala.collection.immutable.Map[String, MessageContainerBase] = {
    val fnd = _messagesOrContainers.getOrElse(containerName.toLowerCase, null)
    if (fnd != null) {
      if (fnd.loadedAll) {
        val txnCtxt = getTransactionContext(tempTransId, false)
        val map = {
          if (txnCtxt != null) {
            fnd.data.toMap ++ txnCtxt.getAllObjects(containerName)
          } else {
            fnd.data.toMap
          }
        }
        return map
      } else {
        throw new Exception("Object %s is not loaded all at once. So, we can not get all objects here".format(fnd.objFullName))
      }
    } else {
      return scala.collection.immutable.Map[String, MessageContainerBase]()
    }
  }

  private def localGetAllObjects(tempTransId: Long, containerName: String): Array[MessageContainerBase] = {
    val keysVals = localGetAllKeyValues(tempTransId, containerName)
    keysVals.values.toArray
  }

  private def localGetAdapterUniqueKeyValue(tempTransId: Long, key: String): (String, Int, Int) = {
    val txnCtxt = getTransactionContext(tempTransId, false)
    if (txnCtxt != null) {
      val v = txnCtxt.getAdapterUniqueKeyValue(key)
      if (v != null) return v
    }

    val v = _adapterUniqKeyValData.getOrElse(key, null)
    if (v != null) return v
    var objs: Array[(String, Int, Int)] = new Array[(String, Int, Int)](1)
    val buildAdapOne = (tupleBytes: Value) => { buildAdapterUniqueValue(tupleBytes, objs) }
    try {
      _allDataDataStore.get(makeKey("AdapterUniqKvData", key), buildAdapOne)
    } catch {
      case e: Exception => {
        logger.trace("Data not found for key:" + key)
      }
    }
    if (objs(0) != null) {
      _adapterUniqKeyValData.synchronized {
        _adapterUniqKeyValData(key) = objs(0)
      }
    }
    return objs(0)
  }

  private def localGetModelsResult(tempTransId: Long, key: String): scala.collection.mutable.Map[String, ModelResult] = {
    val txnCtxt = getTransactionContext(tempTransId, false)
    if (txnCtxt != null) {
      val v = txnCtxt.getModelsResult(key)
      if (v != null) return v
    }

    val v = _modelsResult.getOrElse(key, null)
    if (v != null) return v
    var objs = new Array[scala.collection.mutable.Map[String, ModelResult]](1)
    val buildMdlOne = (tupleBytes: Value) => { buildModelsResult(tupleBytes, objs) }
    try {
      _allDataDataStore.get(makeKey("ModelResults", key), buildMdlOne)
    } catch {
      case e: Exception => {
        logger.trace("Data not found for key:" + key)
      }
    }
    if (objs(0) != null) {
      _modelsResult.synchronized {
        _modelsResult(key) = objs(0)
      }
      return objs(0)
    }
    return scala.collection.mutable.Map[String, ModelResult]()
  }

  private def localContains(tempTransId: Long, containerName: String, key: String): Boolean = {
    val txnCtxt = getTransactionContext(tempTransId, false)
    if (txnCtxt != null) {
      if (txnCtxt.containsAny(containerName, scala.collection.immutable.Set(key)))
        return true
    }

    val container = _messagesOrContainers.getOrElse(containerName.toLowerCase, null)
    if (container != null) {
      if (container.data.contains(key.toString.toLowerCase))
        return true
      val dta = loadObjFromDb(tempTransId, container, key)
      if (dta != null)
        return true
    }
    false
  }

  /**
   *   Does at least one of the supplied keys exist in a container with the supplied name?
   */
  private def localContainsAny(tempTransId: Long, containerName: String, keys: Array[String]): Boolean = {
    val keysSet = keys.toSet
    val txnCtxt = getTransactionContext(tempTransId, false)
    if (txnCtxt != null) {
      if (txnCtxt.containsAny(containerName, keysSet))
        return true
    }

    val container = _messagesOrContainers.getOrElse(containerName.toLowerCase, null)
    if (container != null) {
      keysSet.foreach(key => {
        if (container.data.contains(key.toLowerCase))
          return true
      })

      keysSet.foreach(key => {
        val dta = loadObjFromDb(tempTransId, container, key)
        if (dta != null)
          return true
      })
    }
    false
  }

  /**
   *   Do all of the supplied keys exist in a container with the supplied name?
   */
  private def localContainsAll(tempTransId: Long, containerName: String, keys: Array[String]): Boolean = {
    val remainingKeys = scala.collection.mutable.Set[String]()
    keys.foreach(key => {
      remainingKeys += key.toLowerCase
    })

    val txnCtxt = getTransactionContext(tempTransId, false)
    if (txnCtxt != null) {
      val foundKeys = txnCtxt.containsKeys(containerName, remainingKeys.toSet)
      remainingKeys --= foundKeys
    }

    val container = _messagesOrContainers.getOrElse(containerName.toLowerCase, null)
    if (container != null) {
      val keysSet = remainingKeys.toSet
      keysSet.foreach(key => {
        if (container.data.contains(key)) // Already converted to lower case
          remainingKeys -= key
      })

      val keysSet1 = remainingKeys.toSet
      keysSet1.foreach(key => {
        val dta = loadObjFromDb(tempTransId, container, key)
        if (dta == null) {
          // This key is not found any where. Simply return false
          return false
        }
        remainingKeys -= key
      })
    }

    (remainingKeys.size == 0)
  }

  private def localSetObject(tempTransId: Long, containerName: String, key: String, value: MessageContainerBase): Unit = {
    var txnCtxt = getTransactionContext(tempTransId, true)
    if (txnCtxt != null) {
      txnCtxt.setObject(containerName, key, value)
    }
  }

  private def localSetAdapterUniqueKeyValue(tempTransId: Long, key: String, value: String, xformedMsgCntr: Int, totalXformedMsgs: Int): Unit = {
    var txnCtxt = getTransactionContext(tempTransId, true)
    if (txnCtxt != null) {
      txnCtxt.setAdapterUniqueKeyValue(key, value, xformedMsgCntr, totalXformedMsgs)
    }
  }

  private def localSaveModelsResult(tempTransId: Long, key: String, value: scala.collection.mutable.Map[String, ModelResult]): Unit = {
    var txnCtxt = getTransactionContext(tempTransId, true)
    if (txnCtxt != null) {
      txnCtxt.saveModelsResult(key, value)
    }
  }

  private def collectKey(key: Key, keys: ArrayBuffer[(String, String)]): Unit = {
    implicit val jsonFormats: Formats = DefaultFormats
    val parsed_key = parse(new String(key.toArray)).extract[ObjKey]
    keys += ((parsed_key.Obj, parsed_key.Key))
  }

  override def SetClassLoader(cl: java.lang.ClassLoader): Unit = {
    _classLoader = cl
  }

  //BUGBUG:: May be we need to lock before we do anything here
  override def Shutdown: Unit = {
    _adapterUniqKeyValData.clear
    if (_allDataDataStore != null)
      _allDataDataStore.Shutdown
    _allDataDataStore = null
    if (_runningTxnsDataStore != null)
      _runningTxnsDataStore.Shutdown
    _runningTxnsDataStore = null
    if (_checkPointAdapInfoDataStore != null)
      _checkPointAdapInfoDataStore.Shutdown
    _checkPointAdapInfoDataStore = null
    _messagesOrContainers.clear
  }

  private def getTableKeys(all_keys: ArrayBuffer[(String, String)], objName: String): Array[String] = {
    all_keys.filter(k => k._1.compareTo(objName) == 0).map(k => k._2).toArray
  }

  // Adding new messages or Containers
  //BUGBUG:: May be we need to lock before we do anything here
  override def AddNewMessageOrContainers(mgr: MdMgr, storeType: String, dataLocation: String, schemaName: String, containerNames: Array[String], loadAllData: Boolean, statusInfoStoreType: String, statusInfoSchemaName: String, statusInfoLocation: String): Unit = {
    logger.info("AddNewMessageOrContainers => " + (if (containerNames != null) containerNames.mkString(",") else ""))
    if (_allDataDataStore == null) {
      logger.info("AddNewMessageOrContainers => storeType:%s, dataLocation:%s, schemaName:%s".format(storeType, dataLocation, schemaName))
      _allDataDataStore = GetDataStoreHandle(storeType, schemaName, "AllData", dataLocation)
    }
    if (_runningTxnsDataStore == null) {
      _runningTxnsDataStore = GetDataStoreHandle(statusInfoStoreType, statusInfoSchemaName, "RunningTxns", statusInfoLocation)
    }
    if (_checkPointAdapInfoDataStore == null) {
      _checkPointAdapInfoDataStore = GetDataStoreHandle(statusInfoStoreType, statusInfoSchemaName, "checkPointAdapInfo", statusInfoLocation)
    }

    val all_keys = ArrayBuffer[(String, String)]() // All keys for all tables for now
    var keysAlreadyLoaded = false

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

          newMsgOrContainer.containerType = containerType
          newMsgOrContainer.objFullName = objFullName

          /** create a map to cache the entries to be resurrected from the mapdb */
          _messagesOrContainers(objFullName) = newMsgOrContainer
          if (loadAllData) {
            if (keysAlreadyLoaded == false) {
              val keyCollector = (key: Key) => { collectKey(key, all_keys) }
              _allDataDataStore.getAllKeys(keyCollector)
              keysAlreadyLoaded = true
            }
            val keys = getTableKeys(all_keys, objFullName)
            if (keys.size > 0) {
              loadMap(keys, newMsgOrContainer)
            }
            newMsgOrContainer.loadedAll = true
            newMsgOrContainer.reload = false
          }
        }
      } else {
        logger.error("Message/Container %s not found".format(c))
      }
    })
  }

  override def getAllObjects(tempTransId: Long, containerName: String): Array[MessageContainerBase] = {
    localGetAllObjects(tempTransId, containerName)
  }

  override def getObject(tempTransId: Long, containerName: String, key: String): MessageContainerBase = {
    localGetObject(tempTransId, containerName, key)
  }

  override def getAdapterUniqueKeyValue(tempTransId: Long, key: String): (String, Int, Int) = {
    localGetAdapterUniqueKeyValue(tempTransId, key)
  }

  override def getModelsResult(tempTransId: Long, key: String): scala.collection.mutable.Map[String, ModelResult] = {
    localGetModelsResult(tempTransId, key)
  }

  /**
   *   Does the supplied key exist in a container with the supplied name?
   */
  override def contains(tempTransId: Long, containerName: String, key: String): Boolean = {
    localContains(tempTransId, containerName, key)
  }

  /**
   *   Does at least one of the supplied keys exist in a container with the supplied name?
   */
  override def containsAny(tempTransId: Long, containerName: String, keys: Array[String]): Boolean = {
    localContainsAny(tempTransId, containerName, keys)
  }

  /**
   *   Do all of the supplied keys exist in a container with the supplied name?
   */
  override def containsAll(tempTransId: Long, containerName: String, keys: Array[String]): Boolean = {
    localContainsAll(tempTransId, containerName, keys)
  }

  override def setObject(tempTransId: Long, containerName: String, key: String, value: MessageContainerBase): Unit = {
    localSetObject(tempTransId, containerName, key, value)
  }

  override def setAdapterUniqueKeyValue(tempTransId: Long, key: String, value: String, xformedMsgCntr: Int, totalXformedMsgs: Int): Unit = {
    localSetAdapterUniqueKeyValue(tempTransId, key, value, xformedMsgCntr, totalXformedMsgs)
  }

  override def saveModelsResult(tempTransId: Long, key: String, value: scala.collection.mutable.Map[String, ModelResult]): Unit = {
    localSaveModelsResult(tempTransId, key, value)
  }

  // Final Commit for the given transaction
  override def commitData(tempTransId: Long): Unit = {
    // Commit Data and Removed Transaction information from status
    val txnCtxt = getTransactionContext(tempTransId, false)
    if (txnCtxt == null)
      return

    // Persist current transaction objects
    val messagesOrContainers = txnCtxt.getAllMessagesAndContainers
    val adapterUniqKeyValData = txnCtxt.getAllAdapterUniqKeyValData
    val modelsResult = txnCtxt.getAllModelsResult

    if (_kryoSer == null) {
      _kryoSer = SerializerManager.GetSerializer("kryo")
      if (_kryoSer != null && _classLoader != null) {
        _kryoSer.SetClassLoader(_classLoader)
      }
    }

    val storeObjects = new Array[IStorage](messagesOrContainers.size + adapterUniqKeyValData.size + modelsResult.size)
    var cntr = 0

    messagesOrContainers.foreach(v => {
      val mc = _messagesOrContainers.getOrElse(v._1, null)
      if (mc != null) {
        if (v._2.reload)
          mc.reload = true
        v._2.data.foreach(kv => {
          mc.data(kv._1) = kv._2
          try {
            val serVal = _kryoSer.SerializeObjectToByteArray(kv._2)
            object obj extends IStorage {
              val k = makeKey(mc.objFullName, kv._1)
              val v = makeValue(serVal, "kryo")

              def Key = k
              def Value = v
              def Construct(Key: com.ligadata.keyvaluestore.Key, Value: com.ligadata.keyvaluestore.Value) = {}
            }
            storeObjects(cntr) = obj
            cntr += 1
          } catch {
            case e: Exception => {
              logger.error("Failed to serialize/write data.")
              e.printStackTrace
              throw e
            }
          }
        })
      }
    })

    adapterUniqKeyValData.foreach(v1 => {
      _adapterUniqKeyValData(v1._1) = v1._2
      try {
        object obj extends IStorage {
          val k = makeKey("AdapterUniqKvData", v1._1)
          val json =
            ("Val" -> v1._2._1) ~
              ("xidx" -> v1._2._2) ~
              ("xtot" -> v1._2._3)
          val compjson = compact(render(json))
          val v = makeValue(compjson.getBytes("UTF8"), "CSV")

          def Key = k
          def Value = v
          def Construct(Key: com.ligadata.keyvaluestore.Key, Value: com.ligadata.keyvaluestore.Value) = {}
        }
        storeObjects(cntr) = obj
        cntr += 1
      } catch {
        case e: Exception => {
          logger.error("Failed to write data.")
          e.printStackTrace
          throw e
        }
      }
    })

    modelsResult.foreach(v1 => {
      _modelsResult(v1._1) = v1._2
      try {
        val serVal = _kryoSer.SerializeObjectToByteArray(v1._2)
        object obj extends IStorage {
          val k = makeKey("ModelResults", v1._1)
          val v = makeValue(serVal, "kryo")

          def Key = k
          def Value = v
          def Construct(Key: com.ligadata.keyvaluestore.Key, Value: com.ligadata.keyvaluestore.Value) = {}
        }
        storeObjects(cntr) = obj
        cntr += 1
      } catch {
        case e: Exception => {
          logger.error("Failed to write data.")
          e.printStackTrace
          throw e
        }
      }
    })
    
    val txn = _allDataDataStore.beginTx()
    try {
      _allDataDataStore.putBatch(storeObjects)
      _allDataDataStore.commitTx(txn)
    } catch {
      case e: Exception => {
        _allDataDataStore.endTx(txn)
        logger.error("Failed to write data.")
        e.printStackTrace
        throw e
      }
    }

    // Remove the current transaction
    removeTransactionContext(tempTransId)
  }

  // Set Reload Flag
  override def setReloadFlag(tempTransId: Long, containerName: String): Unit = {
    // BUGBUG:: Set Reload Flag
    val txnCtxt = getTransactionContext(tempTransId, true)
    if (txnCtxt != null) {
      txnCtxt.setReloadFlag(containerName)
    }
  }

  // Saving Status
  override def saveStatus(tempTransId: Long, status: String, persistIntermediateStatusInfo: Boolean): Unit = {
    val txnCtxt = getTransactionContext(tempTransId, true)
    if (txnCtxt == null)
      return
    if (persistIntermediateStatusInfo) { // Saving Intermediate Status Info
      val adapterUniqKeyValData = txnCtxt.getAllAdapterUniqKeyValData // Expecting only one value at this moment from here
      if (adapterUniqKeyValData.size > 0) {
        // Persists unique key & value here for this transactionId
        val storeObjects = new Array[IStorage](adapterUniqKeyValData.size)
        var cntr = 0

        adapterUniqKeyValData.foreach(kv => {
          object obj extends IStorage {
            val k = makeKey("UK", kv._1)
            val json =
              ("Val" -> kv._2._1) ~
                ("xidx" -> kv._2._2) ~
                ("xtot" -> kv._2._3)
            val compjson = compact(render(json))
            val v = makeValue(compjson.getBytes("UTF8"), "CSV")

            def Key = k
            def Value = v
            def Construct(Key: com.ligadata.keyvaluestore.Key, Value: com.ligadata.keyvaluestore.Value) = {}
          }
          storeObjects(cntr) = obj
          cntr += 1
        })

        val txn = _runningTxnsDataStore.beginTx()
        _runningTxnsDataStore.putBatch(storeObjects)
        _runningTxnsDataStore.commitTx(txn)
      }
    }
    txnCtxt.saveStatus(status)
  }

  // Clear Intermediate results before Restart processing
  //BUGBUG:: May be we need to lock before we do anything here
  override def clearIntermediateResults: Unit = {
    // BUGBUG:: What happens to containers with reload flag
    _messagesOrContainers.foreach(v => {
      if (v._2.loadedAll == false) {
        v._2.data.clear
      }
    })

    _adapterUniqKeyValData.clear

    _modelsResult.clear
  }

  // Get all Status information from intermediate table
  override def getAllIntermediateStatusInfo: Array[(String, (String, Int, Int))] = {
    val results = new ArrayBuffer[(String, (String, Int, Int))]()
    val keys = ArrayBuffer[(String, String)]()
    val keyCollector = (key: Key) => { collectKey(key, keys) }
    _runningTxnsDataStore.getAllKeys(keyCollector)
    var objs: Array[(String, Int, Int)] = new Array[(String, Int, Int)](1)
    keys.foreach(key => {
      try {
        val buildAdapOne = (tupleBytes: Value) => { buildAdapterUniqueValue(tupleBytes, objs) }
        _runningTxnsDataStore.get(makeKey("UK", key._2), buildAdapOne)
        results += ((key._2, objs(0)))
      } catch {
        case e: Exception => {
          logger.info(s"getAllIntermediateStatusInfo() -- Unable to load Status Info")
        }
      }
    })
    logger.trace("Loaded %d status informations".format(results.size))
    results.toArray
  }

  // Get Status information from intermediate table for given keys. No Transaction required here.
  override def getIntermediateStatusInfo(keys: Array[String]): Array[(String, (String, Int, Int))] = {
    val results = new ArrayBuffer[(String, (String, Int, Int))]()
    var objs: Array[(String, Int, Int)] = new Array[(String, Int, Int)](1)
    keys.foreach(key => {
      try {
        val buildAdapOne = (tupleBytes: Value) => { buildAdapterUniqueValue(tupleBytes, objs) }
        _runningTxnsDataStore.get(makeKey("UK", key), buildAdapOne)
        results += ((key, objs(0)))
      } catch {
        case e: Exception => {
          logger.info(s"getIntermediateStatusInfo() -- Unable to load Status Info")
        }
      }
    })
    logger.trace("Loaded %d status informations".format(results.size))
    results.toArray
  }

  // Get Status information from Final table
  override def getAllFinalStatusInfo(keys: Array[String]): Array[(String, (String, Int, Int))] = {
    val results = new ArrayBuffer[(String, (String, Int, Int))]()
    var objs: Array[(String, Int, Int)] = new Array[(String, Int, Int)](1)
    keys.foreach(key => {
      try {
        val buildAdapOne = (tupleBytes: Value) => { buildAdapterUniqueValue(tupleBytes, objs) }
        _allDataDataStore.get(makeKey("AdapterUniqKvData", key), buildAdapOne)
        results += ((key, objs(0)))
      } catch {
        case e: Exception => {
          logger.info(s"getAllFinalStatusInfo() -- Unable to load Status Info")
        }
      }
    })
    logger.trace("Loaded %d status informations".format(results.size))
    results.toArray
  }

  // Save Current State of the machine
  override def PersistLocalNodeStateEntries: Unit = {
    // BUGBUG:: Persist all state on this node.
  }

  // Save Remaining State of the machine
  override def PersistRemainingStateEntriesOnLeader: Unit = {
    // BUGBUG:: Persist Remaining state (when other nodes goes down, this helps)
  }

  override def PersistValidateAdapterInformation(validateUniqVals: Array[(String, String)]): Unit = {
    // Persists unique key & value here for this transactionId
    val storeObjects = new Array[IStorage](validateUniqVals.size)
    var cntr = 0

    logger.info(s"PersistValidateAdapterInformation => " + validateUniqVals.mkString(","))

    validateUniqVals.foreach(kv => {
      object obj extends IStorage {
        val key = makeKey("CP", kv._1)
        val value = kv._2
        val v = makeValue(value.getBytes("UTF8"), "CSV")

        def Key = key
        def Value = v
        def Construct(Key: com.ligadata.keyvaluestore.Key, Value: com.ligadata.keyvaluestore.Value) = {}
      }
      storeObjects(cntr) = obj
      cntr += 1
    })

    val txn = _checkPointAdapInfoDataStore.beginTx()
    _checkPointAdapInfoDataStore.putBatch(storeObjects)
    _checkPointAdapInfoDataStore.commitTx(txn)
  }

  private def buildValidateAdapInfo(tupleBytes: Value, objs: Array[String]) {
    // Get first _serInfoBufBytes bytes
    if (tupleBytes.size < _serInfoBufBytes) {
      val errMsg = s"Invalid input. This has only ${tupleBytes.size} bytes data. But we are expecting serializer buffer bytes as of size ${_serInfoBufBytes}"
      logger.error(errMsg)
      throw new Exception(errMsg)
    }

    val valInfo = getValueInfo(tupleBytes)
    objs(0) = new String(valInfo)
  }

  override def GetValidateAdapterInformation: Array[(String, String)] = {
    val results = ArrayBuffer[(String, String)]()

    val keys = ArrayBuffer[(String, String)]()
    val keyCollector = (key: Key) => { collectKey(key, keys) }
    _checkPointAdapInfoDataStore.getAllKeys(keyCollector)
    var objs: Array[String] = new Array[String](1)
    keys.foreach(key => {
      try {
        val buildAdapOne = (tupleBytes: Value) => { buildValidateAdapInfo(tupleBytes, objs) }
        _checkPointAdapInfoDataStore.get(makeKey(key._1, key._2), buildAdapOne)
        logger.info(s"GetValidateAdapterInformation -- %s -> %s".format(key._2, objs(0).toString))
        results += ((key._2, objs(0)))
      } catch {
        case e: Exception => {
          logger.info(s"GetValidateAdapterInformation() -- Unable to load Validate (Check Point) Adapter Information")
        }
      }
    })
    logger.trace("Loaded %d Validate (Check Point) Adapter Information".format(results.size))
    results.toArray
  }
}

