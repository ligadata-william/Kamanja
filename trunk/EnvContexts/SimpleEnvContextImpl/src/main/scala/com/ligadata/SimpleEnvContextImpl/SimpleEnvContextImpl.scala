
package com.ligadata.SimpleEnvContextImpl

import scala.collection.immutable.Map
import scala.collection.mutable._
import scala.util.control.Breaks._
import scala.reflect.runtime.{ universe => ru }
import org.apache.log4j.Logger
import com.ligadata.keyvaluestore._
import com.ligadata.keyvaluestore.mapdb._
import com.ligadata.FatafatBase._
import com.ligadata.FatafatBase.{ EnvContext, MessageContainerBase }
import com.ligadata.fatafat.metadata._
import com.ligadata.Exceptions._
import java.net.URLClassLoader
import com.ligadata.Serialize._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import com.ligadata.FatafatData.{ FatafatData }

case class FatafatDataKey(T: String, K: List[String], D: List[Int], V: Int)
case class InMemoryKeyData(K: List[String])

trait LogTrait {
  val loggerName = this.getClass.getName()
  val logger = Logger.getLogger(loggerName)
}

case class AdapterUniqueValueDes(Val: String, xidx: Int, xtot: Int) // Using most of the values as optional values. Just thinking about future changes. Don't know the performance issues.

/**
 *  The SimpleEnvContextImpl supports kv stores that are based upon MapDb hash tables.
 */
object SimpleEnvContextImpl extends EnvContext with LogTrait {

  override def NewMessageOrContainer(fqclassname: String): MessageContainerBase = {
    val msgOrContainer: MessageContainerBase = Class.forName(fqclassname).newInstance().asInstanceOf[MessageContainerBase]
    msgOrContainer
  }

  class TxnCtxtKey {
    var containerName: String = _
    var key: String = _
  }

  class MsgContainerInfo {
    var current_msg_cont_data: scala.collection.mutable.ArrayBuffer[MessageContainerBase] = scala.collection.mutable.ArrayBuffer[MessageContainerBase]()
    var data: scala.collection.mutable.Map[String, (Boolean, FatafatData)] = scala.collection.mutable.Map[String, (Boolean, FatafatData)]()
    var containerType: BaseTypeDef = null
    var loadedAll: Boolean = false
    var reload: Boolean = false
    var isContainer: Boolean = false
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

    def setFetchedObj(containerName: String, partKeyStr: String, fatafatData: FatafatData): Unit = {
      val container = getMsgContainer(containerName.toLowerCase, true)
      if (container != null) {
        container.data(partKeyStr) = (false, fatafatData)
      }
    }

    def getAllObjects(containerName: String): Array[MessageContainerBase] = {
      val container = getMsgContainer(containerName.toLowerCase, false)
      val arrList = ArrayBuffer[MessageContainerBase]()
      if (container != null) {
        container.data.foreach(kv => {
          arrList ++= kv._2._2.GetAllData
        })
      }
      arrList.toArray
    }

    def getObject(containerName: String, partKey: List[String], primaryKey: List[String]): (MessageContainerBase, Boolean) = {
      val container = getMsgContainer(containerName.toLowerCase, false)
      if (container != null) {
        val fatafatData = container.data.getOrElse(InMemoryKeyDataInJson(partKey), null)
        if (fatafatData != null) {
          // Search for primary key match
          return (fatafatData._2.GetMessageContainerBase(primaryKey.toArray, false), true)
        }
      }
      (null, false)
    }

    def getObjects(containerName: String, partKey: List[String], appendCurrentChanges: Boolean): (Array[MessageContainerBase], Boolean) = {
      val container = getMsgContainer(containerName.toLowerCase, false)
      if (container != null) {
        val fatafatData = container.data.getOrElse(InMemoryKeyDataInJson(partKey), null)
        if (fatafatData != null) {
          if (container.current_msg_cont_data.size > 0) {
            val allData = ArrayBuffer[MessageContainerBase]()
            if (appendCurrentChanges) {
              allData ++= fatafatData._2.GetAllData
              allData --= container.current_msg_cont_data
              allData ++= container.current_msg_cont_data // Just to get the current messages to end
            } else {
              allData ++= fatafatData._2.GetAllData
              allData --= container.current_msg_cont_data
            }
            return (allData.toArray, true)
          } else {
            return (fatafatData._2.GetAllData, true)
          }
        }
      }
      (Array[MessageContainerBase](), false)
    }

    def containsAny(containerName: String, partKeys: Array[List[String]], primaryKeys: Array[List[String]]): (Boolean, Array[List[String]]) = {
      val container = getMsgContainer(containerName.toLowerCase, false)
      if (container != null) {
        val notFndPartkeys = ArrayBuffer[List[String]]()
        for (i <- 0 until partKeys.size) {
          val fatafatData = container.data.getOrElse(InMemoryKeyDataInJson(partKeys(i)), null)
          if (fatafatData != null) {
            // Search for primary key match
            val fnd = fatafatData._2.GetMessageContainerBase(primaryKeys(i).toArray, false)
            if (fnd != null)
              return (true, Array[List[String]]())
          } else {
            notFndPartkeys += partKeys(i)
          }
        }
        return (false, notFndPartkeys.toArray)
      }
      (false, primaryKeys)
    }

    def containsKeys(containerName: String, partKeys: Array[List[String]], primaryKeys: Array[List[String]]): (Array[List[String]], Array[List[String]], Array[List[String]], Array[List[String]]) = {
      val container = getMsgContainer(containerName.toLowerCase, false)
      val matchedPartKeys = ArrayBuffer[List[String]]()
      val unmatchedPartKeys = ArrayBuffer[List[String]]()
      val matchedPrimaryKeys = ArrayBuffer[List[String]]()
      val unmatchedPrimaryKeys = ArrayBuffer[List[String]]()
      if (container != null) {
        for (i <- 0 until partKeys.size) {
          val fatafatData = container.data.getOrElse(InMemoryKeyDataInJson(partKeys(i)), null)
          if (fatafatData != null) {
            // Search for primary key match
            val fnd = fatafatData._2.GetMessageContainerBase(primaryKeys(i).toArray, false)
            if (fnd != null) {
              matchedPartKeys += partKeys(i)
              matchedPrimaryKeys += primaryKeys(i)
            } else {
              unmatchedPartKeys += partKeys(i)
              unmatchedPrimaryKeys += primaryKeys(i)
            }
          } else {
            unmatchedPartKeys += partKeys(i)
            unmatchedPrimaryKeys += primaryKeys(i)
          }
        }
      }
      (matchedPartKeys.toArray, unmatchedPartKeys.toArray, matchedPrimaryKeys.toArray, unmatchedPrimaryKeys.toArray)
    }

    def setObject(containerName: String, partKey: List[String], value: MessageContainerBase): Unit = {
      val container = getMsgContainer(containerName.toLowerCase, true)
      if (container != null) {
        val partKeyStr = InMemoryKeyDataInJson(partKey)
        val fnd = container.data.getOrElse(partKeyStr, null)
        if (fnd != null) {
          fnd._2.AddMessageContainerBase(value, true, true)
          if (fnd._1 == false) {
            container.data(partKeyStr) = (true, fnd._2)
          }
        } else {
          val ffData = new FatafatData
          ffData.SetKey(partKey.toArray)
          ffData.SetTypeName(containerName)
          ffData.AddMessageContainerBase(value, true, true)
          container.data(partKeyStr) = (true, ffData)
        }
        container.current_msg_cont_data -= value
        container.current_msg_cont_data += value // to get the value to end
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
    def saveModelsResult(key: List[String], value: scala.collection.mutable.Map[String, ModelResult]): Unit = {
      _modelsResult(InMemoryKeyDataInJson(key)) = value
    }

    def getModelsResult(key: List[String]): scala.collection.mutable.Map[String, ModelResult] = {
      val keystr = InMemoryKeyDataInJson(key)
      _modelsResult.getOrElse(keystr, null)
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
  private[this] var _mdres: MdBaseResolveInfo = null

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

  private[this] def InMemoryKeyDataInJson(keyData: List[String]): String = {
    val json = ("K" -> keyData)
    return compact(render(json))
  }

  private[this] def KeyFromInMemoryJson(keyData: String): List[String] = {
    implicit val jsonFormats: Formats = DefaultFormats
    val parsed_key = parse(keyData).extract[InMemoryKeyData]
    return parsed_key.K
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
  private def loadMap(keys: Array[FatafatDataKey], msgOrCont: MsgContainerInfo): Unit = {
    var objs: Array[FatafatData] = new Array[FatafatData](1)
    var notFoundKeys = 0
    val buildOne = (tupleBytes: Value) => { buildObject(tupleBytes, objs, msgOrCont.containerType) }
    keys.foreach(key => {
      try {
        val StartDateRange = if (key.D.size == 2) key.D(0) else 0
        val EndDateRange = if (key.D.size == 2) key.D(1) else 0
        _allDataDataStore.get(makeKey(FatafatData.PrepareKey(key.T, key.K, StartDateRange, EndDateRange)), buildOne)
        msgOrCont.synchronized {
          msgOrCont.data(InMemoryKeyDataInJson(key.K)) = (false, objs(0))
        }
      } catch {
        case e: ClassNotFoundException => {
          logger.error(s"Not found key:${key.K.mkString(",")}. Reason:${e.getCause}, Message:${e.getMessage}")
          e.printStackTrace()
          notFoundKeys += 1
        }
        case e: KeyNotFoundException => {
          logger.error(s"Not found key:${key.K.mkString(",")}. Reason:${e.getCause}, Message:${e.getMessage}")
          e.printStackTrace()
          notFoundKeys += 1
        }
        case e: Exception => {
          logger.error(s"Not found key:${key.K.mkString(",")}. Reason:${e.getCause}, Message:${e.getMessage}")
          e.printStackTrace()
          notFoundKeys += 1
        }
        case ooh: Throwable => {
          logger.error(s"Not found key:${key.K.mkString(",")}. Reason:${ooh.getCause}, Message:${ooh.getMessage}")
          throw ooh
        }
      }
    })

    if (notFoundKeys > 0) {
      logger.error("Not found some keys to load")
      throw new Exception("Not found some keys to load")
    }

    logger.debug("Loaded %d objects for %s".format(msgOrCont.data.size, msgOrCont.objFullName))
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

  private def buildObject(tupleBytes: Value, objs: Array[FatafatData], containerType: BaseTypeDef): Unit = {
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
          objs(0) = _kryoSer.DeserializeObjectFromByteArray(valInfo).asInstanceOf[FatafatData]
        }
      }
      case "manual" => {
        val valInfo = getValueInfo(tupleBytes)
        val datarec = new FatafatData
        datarec.DeserializeData(valInfo, _mdres, _classLoader)
        objs(0) = datarec
      }
      case _ => {
        throw new Exception("Found un-handled Serializer Info: " + serInfo)
      }
    }
  }

  private def GetDataStoreHandle(storeType: String, storeName: String, tableName: String, dataLocation: String, adapterSpecificConfig: String): DataStore = {
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

  private def makeKey(key: String): com.ligadata.keyvaluestore.Key = {
    var k = new com.ligadata.keyvaluestore.Key
    k ++= key.getBytes("UTF8")
    k
  }

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

  private def loadObjFromDb(tempTransId: Long, msgOrCont: MsgContainerInfo, key: List[String]): FatafatData = {
    val partKeyStr = FatafatData.PrepareKey(msgOrCont.objFullName, key, 0, 0)
    var objs: Array[FatafatData] = new Array[FatafatData](1)
    val buildOne = (tupleBytes: Value) => { buildObject(tupleBytes, objs, msgOrCont.containerType) }
    try {
      _allDataDataStore.get(makeKey(partKeyStr), buildOne)
    } catch {
      case e: Exception => {
        logger.debug("1. Data not found for key:" + partKeyStr)
      }
    }
    if (objs(0) != null) {
      val lockObj = if (msgOrCont.loadedAll) msgOrCont else _locks(lockIdx(tempTransId))
      lockObj.synchronized {
        msgOrCont.data(InMemoryKeyDataInJson(key)) = (false, objs(0))
      }
    }
    return objs(0)
  }

  private def localGetObject(tempTransId: Long, containerName: String, partKey: List[String], primaryKey: List[String]): MessageContainerBase = {
    val txnCtxt = getTransactionContext(tempTransId, false)
    if (txnCtxt != null) {
      val (v, foundPartKey) = txnCtxt.getObject(containerName, partKey, primaryKey)
      if (foundPartKey) {
        return v
      }
      if (v != null) return v // It must be null. Without finding partition key it should not find the primary key
    }

    val container = _messagesOrContainers.getOrElse(containerName.toLowerCase, null)
    if (container != null) {
      val partKeyStr = InMemoryKeyDataInJson(partKey)
      val fatafatData = container.data.getOrElse(partKeyStr, null)
      if (fatafatData != null) {
        // Search for primary key match
        val v = fatafatData._2.GetMessageContainerBase(primaryKey.toArray, false)
        if (txnCtxt != null)
          txnCtxt.setFetchedObj(containerName, partKeyStr, fatafatData._2)
        return v;
      }
      // If not found in memory, try in DB
      val loadedFfData = loadObjFromDb(tempTransId, container, partKey)
      if (loadedFfData != null) {
        // Search for primary key match
        val v = loadedFfData.GetMessageContainerBase(primaryKey.toArray, false)
        if (txnCtxt != null)
          txnCtxt.setFetchedObj(containerName, partKeyStr, loadedFfData)
        return v;
      }
      // If not found in DB, Create Empty and set to current transaction context
      if (txnCtxt != null) {
        val emptyFfData = new FatafatData
        emptyFfData.SetKey(partKey.toArray)
        emptyFfData.SetTypeName(containerName)
        txnCtxt.setFetchedObj(containerName, partKeyStr, emptyFfData)
      }
    }
    null
  }

  private def localHistoryObjects(tempTransId: Long, containerName: String, partKey: List[String], appendCurrentChanges: Boolean): Array[MessageContainerBase] = {
    val retVals = ArrayBuffer[MessageContainerBase]()
    val txnCtxt = getTransactionContext(tempTransId, false)
    if (txnCtxt != null) {
      val (objs, foundPartKey) = txnCtxt.getObjects(containerName, partKey, appendCurrentChanges)
      retVals ++= objs
      if (foundPartKey)
        return retVals.toArray
    }

    val container = _messagesOrContainers.getOrElse(containerName.toLowerCase, null)
    if (container != null) {
      val partKeyStr = InMemoryKeyDataInJson(partKey)
      val fatafatData = container.data.getOrElse(partKeyStr, null)
      if (fatafatData != null) {
        // Search for primary key match
        if (txnCtxt != null)
          txnCtxt.setFetchedObj(containerName, partKeyStr, fatafatData._2)
        retVals ++= fatafatData._2.GetAllData
      } else {
        val loadedFfData = loadObjFromDb(tempTransId, container, partKey)
        if (loadedFfData != null) {
          // Search for primary key match
          if (txnCtxt != null)
            txnCtxt.setFetchedObj(containerName, partKeyStr, loadedFfData)
          retVals ++= loadedFfData.GetAllData
        } else {
          // If not found in DB, Create Empty and set to current transaction context
          if (txnCtxt != null) {
            val emptyFfData = new FatafatData
            emptyFfData.SetKey(partKey.toArray)
            emptyFfData.SetTypeName(containerName)
            txnCtxt.setFetchedObj(containerName, partKeyStr, emptyFfData)
          }
        }
      }
    }

    retVals.toArray
  }

  private def localGetAllKeyValues(tempTransId: Long, containerName: String): Array[MessageContainerBase] = {
    val fnd = _messagesOrContainers.getOrElse(containerName.toLowerCase, null)
    if (fnd != null) {
      if (fnd.loadedAll) {
        var allObjs = ArrayBuffer[MessageContainerBase]()
        fnd.data.foreach(kv => {
          allObjs ++= kv._2._2.GetAllData
        })
        val txnCtxt = getTransactionContext(tempTransId, false)
        if (txnCtxt != null)
          allObjs ++= txnCtxt.getAllObjects(containerName)
        return allObjs.toArray
      } else {
        throw new Exception("Object %s is not loaded all at once. So, we can not get all objects here".format(fnd.objFullName))
      }
    } else {
      return Array[MessageContainerBase]()
    }
  }

  private def localGetAllObjects(tempTransId: Long, containerName: String): Array[MessageContainerBase] = {
    return localGetAllKeyValues(tempTransId, containerName)
  }

  private def localGetAdapterUniqueKeyValue(tempTransId: Long, key: String): (String, Int, Int) = {
    val txnCtxt = getTransactionContext(tempTransId, false)
    if (txnCtxt != null) {
      val v = txnCtxt.getAdapterUniqueKeyValue(key)
      if (v != null) return v
    }

    val v = _adapterUniqKeyValData.getOrElse(key, null)
    if (v != null) return v
    val partKeyStr = FatafatData.PrepareKey("AdapterUniqKvData", List(key), 0, 0)
    var objs: Array[(String, Int, Int)] = new Array[(String, Int, Int)](1)
    val buildAdapOne = (tupleBytes: Value) => { buildAdapterUniqueValue(tupleBytes, objs) }
    try {
      _allDataDataStore.get(makeKey(partKeyStr), buildAdapOne)
    } catch {
      case e: Exception => {
        logger.debug("2. Data not found for key:" + partKeyStr)
      }
    }
    if (objs(0) != null) {
      _adapterUniqKeyValData.synchronized {
        _adapterUniqKeyValData(key) = objs(0)
      }
    }
    return objs(0)
  }

  private def localGetModelsResult(tempTransId: Long, key: List[String]): scala.collection.mutable.Map[String, ModelResult] = {
    val txnCtxt = getTransactionContext(tempTransId, false)
    if (txnCtxt != null) {
      val v = txnCtxt.getModelsResult(key)
      if (v != null) return v
    }

    val keystr = InMemoryKeyDataInJson(key)
    val v = _modelsResult.getOrElse(keystr, null)
    if (v != null) return v
    var objs = new Array[scala.collection.mutable.Map[String, ModelResult]](1)
    val buildMdlOne = (tupleBytes: Value) => { buildModelsResult(tupleBytes, objs) }
    val partKeyStr = FatafatData.PrepareKey("ModelResults", key, 0, 0)
    try {
      _allDataDataStore.get(makeKey(partKeyStr), buildMdlOne)
    } catch {
      case e: Exception => {
        logger.debug("3. Data not found for key:" + partKeyStr)
      }
    }
    if (objs(0) != null) {
      _modelsResult.synchronized {
        _modelsResult(keystr) = objs(0)
      }
      return objs(0)
    }
    return scala.collection.mutable.Map[String, ModelResult]()
  }

  /**
   *   Does at least one of the supplied keys exist in a container with the supplied name?
   */
  private def localContainsAny(tempTransId: Long, containerName: String, partKeys: Array[List[String]], primaryKeys: Array[List[String]]): Boolean = {
    var remainingPartKeys = partKeys
    val txnCtxt = getTransactionContext(tempTransId, false)
    if (txnCtxt != null) {
      val (retval, notFoundPartKeys) = txnCtxt.containsAny(containerName, remainingPartKeys, primaryKeys)
      if (retval)
        return true
      remainingPartKeys = notFoundPartKeys
    }

    val container = _messagesOrContainers.getOrElse(containerName.toLowerCase, null)
    val reMainingForDb = ArrayBuffer[List[String]]()
    if (container != null) {
      for (i <- 0 until remainingPartKeys.size) {
        val fatafatData = container.data.getOrElse(InMemoryKeyDataInJson(remainingPartKeys(i)), null)
        if (fatafatData != null) {
          // Search for primary key match
          val fnd = fatafatData._2.GetMessageContainerBase(primaryKeys(i).toArray, false)
          if (fnd != null)
            return true
        } else {
          reMainingForDb += remainingPartKeys(i)
        }
      }

      for (i <- 0 until reMainingForDb.size) {
        val fatafatData = loadObjFromDb(tempTransId, container, reMainingForDb(i))
        if (fatafatData != null) {
          // Search for primary key match
          val fnd = fatafatData.GetMessageContainerBase(primaryKeys(i).toArray, false)
          if (fnd != null)
            return true
        }
      }
    }
    false
  }

  /**
   *   Do all of the supplied keys exist in a container with the supplied name?
   */
  private def localContainsAll(tempTransId: Long, containerName: String, partKeys: Array[List[String]], primaryKeys: Array[List[String]]): Boolean = {
    var remainingPartKeys: Array[List[String]] = partKeys
    var remainingPrimaryKeys: Array[List[String]] = primaryKeys
    val txnCtxt = getTransactionContext(tempTransId, false)
    if (txnCtxt != null) {
      val (matchedPartKeys, unmatchedPartKeys, matchedPrimaryKeys, unmatchedPrimaryKeys) = txnCtxt.containsKeys(containerName, remainingPartKeys, remainingPrimaryKeys)
      remainingPartKeys = unmatchedPartKeys
      remainingPrimaryKeys = unmatchedPrimaryKeys
    }

    val container = _messagesOrContainers.getOrElse(containerName.toLowerCase, null)
    if (container != null) {
      var unmatchedPartKeys = ArrayBuffer[List[String]]()
      var unmatchedPrimaryKeys = ArrayBuffer[List[String]]()

      for (i <- 0 until remainingPartKeys.size) {
        val fatafatData = container.data.getOrElse(InMemoryKeyDataInJson(remainingPartKeys(i)), null)
        if (fatafatData != null) {
          // Search for primary key match
          val fnd = fatafatData._2.GetMessageContainerBase(remainingPrimaryKeys(i).toArray, false)
          if (fnd != null) {
            // Matched
          } else {
            unmatchedPartKeys += remainingPartKeys(i)
            unmatchedPrimaryKeys += remainingPrimaryKeys(i)
          }
        } else {
          unmatchedPartKeys += remainingPartKeys(i)
          unmatchedPrimaryKeys += remainingPrimaryKeys(i)
        }
      }

      var unmatchedFinalPatKeys = ArrayBuffer[List[String]]()
      var unmatchedFinalPrimaryKeys = ArrayBuffer[List[String]]()

      // BUGBUG:: Need to check whether the 1st key loaded 2nd key also. Because same partition key can have multiple primary keys or partition key itself can be duplicated
      for (i <- 0 until unmatchedPartKeys.size) {
        val fatafatData = container.data.getOrElse(InMemoryKeyDataInJson(unmatchedPartKeys(i)), null)
        if (fatafatData != null) {
          // Search for primary key match
          val fnd = fatafatData._2.GetMessageContainerBase(unmatchedPrimaryKeys(i).toArray, false)
          if (fnd != null) {
            // Matched
          } else {
            unmatchedFinalPatKeys += unmatchedPartKeys(i)
            unmatchedFinalPrimaryKeys += unmatchedPrimaryKeys(i)
          }
        } else {
          unmatchedFinalPatKeys += unmatchedPartKeys(i)
          unmatchedFinalPrimaryKeys += unmatchedPrimaryKeys(i)
        }
      }
      remainingPartKeys = unmatchedFinalPatKeys.toArray
    }

    (remainingPartKeys.size == 0)
  }

  private def localSetObject(tempTransId: Long, containerName: String, partKey: List[String], value: MessageContainerBase): Unit = {
    var txnCtxt = getTransactionContext(tempTransId, true)
    if (txnCtxt != null) {
      txnCtxt.setObject(containerName, partKey, value)
    }
  }

  private def localSetAdapterUniqueKeyValue(tempTransId: Long, key: String, value: String, xformedMsgCntr: Int, totalXformedMsgs: Int): Unit = {
    var txnCtxt = getTransactionContext(tempTransId, true)
    if (txnCtxt != null) {
      txnCtxt.setAdapterUniqueKeyValue(key, value, xformedMsgCntr, totalXformedMsgs)
    }
  }

  private def localSaveModelsResult(tempTransId: Long, key: List[String], value: scala.collection.mutable.Map[String, ModelResult]): Unit = {
    var txnCtxt = getTransactionContext(tempTransId, true)
    if (txnCtxt != null) {
      txnCtxt.saveModelsResult(key, value)
    }
  }

  private def collectKey(key: Key, keys: ArrayBuffer[FatafatDataKey]): Unit = {
    implicit val jsonFormats: Formats = DefaultFormats
    val parsed_key = parse(new String(key.toArray)).extract[FatafatDataKey]
    keys += parsed_key
  }

  override def SetClassLoader(cl: java.lang.ClassLoader): Unit = {
    _classLoader = cl
  }

  override def SetMetadataResolveInfo(mdres: MdBaseResolveInfo): Unit = {
    _mdres = mdres
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

  private def getTableKeys(all_keys: ArrayBuffer[FatafatDataKey], objName: String): Array[FatafatDataKey] = {
    val tmpObjName = objName.toLowerCase
    all_keys.filter(k => k.T.compareTo(objName) == 0).toArray
  }

  // Adding new messages or Containers
  //BUGBUG:: May be we need to lock before we do anything here
  override def AddNewMessageOrContainers(mgr: MdMgr, storeType: String, dataLocation: String, schemaName: String, adapterSpecificConfig: String, containerNames: Array[String], loadAllData: Boolean, statusInfoStoreType: String, statusInfoSchemaName: String, statusInfoLocation: String, statusInfoadapterSpecificConfig: String): Unit = {
    logger.debug("AddNewMessageOrContainers => " + (if (containerNames != null) containerNames.mkString(",") else ""))
    if (_allDataDataStore == null) {
      logger.debug("AddNewMessageOrContainers => storeType:%s, dataLocation:%s, schemaName:%s".format(storeType, dataLocation, schemaName))
      _allDataDataStore = GetDataStoreHandle(storeType, schemaName, "AllData", dataLocation, adapterSpecificConfig)
    }
    if (_runningTxnsDataStore == null) {
      _runningTxnsDataStore = GetDataStoreHandle(statusInfoStoreType, statusInfoSchemaName, "RunningTxns", statusInfoLocation, statusInfoadapterSpecificConfig)
    }
    if (_checkPointAdapInfoDataStore == null) {
      _checkPointAdapInfoDataStore = GetDataStoreHandle(statusInfoStoreType, statusInfoSchemaName, "checkPointAdapInfo", statusInfoLocation, statusInfoadapterSpecificConfig)
    }

    val all_keys = ArrayBuffer[FatafatDataKey]() // All keys for all tables for now
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
          newMsgOrContainer.isContainer = (mgr.ActiveContainer(namespace, name) != null)

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

  override def getObject(tempTransId: Long, containerName: String, partKey: List[String], primaryKey: List[String]): MessageContainerBase = {
    localGetObject(tempTransId, containerName, partKey, primaryKey)
  }

  override def getHistoryObjects(tempTransId: Long, containerName: String, partKey: List[String], appendCurrentChanges: Boolean): Array[MessageContainerBase] = {
    localHistoryObjects(tempTransId, containerName, partKey, appendCurrentChanges)
  }

  override def getAdapterUniqueKeyValue(tempTransId: Long, key: String): (String, Int, Int) = {
    localGetAdapterUniqueKeyValue(tempTransId, key)
  }

  override def getModelsResult(tempTransId: Long, key: List[String]): scala.collection.mutable.Map[String, ModelResult] = {
    localGetModelsResult(tempTransId, key)
  }

  /**
   *   Does the supplied key exist in a container with the supplied name?
   */
  override def contains(tempTransId: Long, containerName: String, partKey: List[String], primaryKey: List[String]): Boolean = {
    localContainsAny(tempTransId, containerName, Array(partKey), Array(primaryKey))
  }

  /**
   *   Does at least one of the supplied keys exist in a container with the supplied name?
   */
  override def containsAny(tempTransId: Long, containerName: String, partKeys: Array[List[String]], primaryKeys: Array[List[String]]): Boolean = {
    localContainsAny(tempTransId, containerName, partKeys, primaryKeys)
  }

  /**
   *   Do all of the supplied keys exist in a container with the supplied name?
   */
  override def containsAll(tempTransId: Long, containerName: String, partKeys: Array[List[String]], primaryKeys: Array[List[String]]): Boolean = {
    localContainsAll(tempTransId, containerName, partKeys, primaryKeys)
  }

  override def setObject(tempTransId: Long, containerName: String, partKey: List[String], value: MessageContainerBase): Unit = {
    localSetObject(tempTransId, containerName, partKey, value)
  }

  override def setAdapterUniqueKeyValue(tempTransId: Long, key: String, value: String, xformedMsgCntr: Int, totalXformedMsgs: Int): Unit = {
    localSetAdapterUniqueKeyValue(tempTransId, key, value, xformedMsgCntr, totalXformedMsgs)
  }

  override def saveModelsResult(tempTransId: Long, key: List[String], value: scala.collection.mutable.Map[String, ModelResult]): Unit = {
    localSaveModelsResult(tempTransId, key, value)
  }

  override def getChangedData(tempTransId: Long, includeMessages: Boolean, includeContainers: Boolean): scala.collection.immutable.Map[String, List[List[String]]] = {
    val changedContainersData = scala.collection.mutable.Map[String, List[List[String]]]()

    // Commit Data and Removed Transaction information from status
    val txnCtxt = getTransactionContext(tempTransId, false)
    if (txnCtxt == null)
      return changedContainersData.toMap

    val messagesOrContainers = txnCtxt.getAllMessagesAndContainers

    messagesOrContainers.foreach(v => {
      val mc = _messagesOrContainers.getOrElse(v._1, null)
      if (mc != null) {
        val savingKeys = new ArrayBuffer[List[String]]()
        if (includeMessages && includeContainers) { // All the vlaues (includes messages & containers)
          v._2.data.foreach(kv => {
            if (kv._2._1 && kv._2._2.DataSize > 0) {
              savingKeys += kv._2._2.GetKey.toList
            }
          })
        } else if (includeMessages && /* v._2.containerType.tTypeType == ObjTypeType.tContainer && */ (mc.isContainer == false && v._2.isContainer == false)) { // Msgs
          v._2.data.foreach(kv => {
            if (kv._2._1 && kv._2._2.DataSize > 0) {
              savingKeys += kv._2._2.GetKey.toList
            }
          })
        } else if (includeContainers && /* v._2.containerType.tTypeType == ObjTypeType.tContainer && */ (mc.isContainer || v._2.isContainer)) { // Containers
          v._2.data.foreach(kv => {
            if (kv._2._1 && kv._2._2.DataSize > 0) {
              savingKeys += kv._2._2.GetKey.toList
            }
          })
        }
        if (savingKeys.size > 0)
          changedContainersData(v._1) = savingKeys.toList
      }
    })

    return changedContainersData.toMap
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

    val storeObjects = ArrayBuffer[IStorage]()
    var cntr = 0

    messagesOrContainers.foreach(v => {
      val mc = _messagesOrContainers.getOrElse(v._1, null)
      if (mc != null) {
        if (v._2.reload)
          mc.reload = true
        v._2.data.foreach(kv => {
          if (kv._2._1 && kv._2._2.DataSize > 0) {
            mc.data(kv._1) = (false, kv._2._2) // Here it is already converted to proper key type. Because we are copying from somewhere else where we applied function InMemoryKeyDataInJson
            try {
              val serVal = kv._2._2.SerializeData
              object obj extends IStorage {
                val k = makeKey(kv._2._2.SerializeKey) // FatafatData.PrepareKey(mc.objFullName, ka, 0, 0)
                val v = makeValue(serVal, "manual")
                def Key = k
                def Value = v
                def Construct(Key: com.ligadata.keyvaluestore.Key, Value: com.ligadata.keyvaluestore.Value) = {}
              }
              storeObjects += obj
              cntr += 1
            } catch {
              case e: Exception => {
                logger.error("Failed to serialize/write data.")
                e.printStackTrace
                throw e
              }
            }
          }
        })

        v._2.current_msg_cont_data.clear
      }
    })

    adapterUniqKeyValData.foreach(v1 => {
      _adapterUniqKeyValData(v1._1) = v1._2
      try {
        object obj extends IStorage {
          val k = makeKey(FatafatData.PrepareKey("AdapterUniqKvData", List(v1._1), 0, 0))
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
        storeObjects += obj
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
          val k = makeKey(FatafatData.PrepareKey("ModelResults", List(v1._1), 0, 0))
          val v = makeValue(serVal, "kryo")

          def Key = k
          def Value = v
          def Construct(Key: com.ligadata.keyvaluestore.Key, Value: com.ligadata.keyvaluestore.Value) = {}
        }
        storeObjects += obj
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
      logger.debug("Going to save " + cntr + " objects")
      storeObjects.foreach(o => {
        logger.debug("ObjKey:" + new String(o.Key.toArray) + " Value Size: " + o.Value.toArray.size)
      })
      _allDataDataStore.putBatch(storeObjects.toArray)
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
            val k = makeKey(FatafatData.PrepareKey("UK", List(kv._1), 0, 0))
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

  // Clear Intermediate results After updating them on different node or different component (like KVInit), etc
  //BUGBUG:: May be we need to lock before we do anything here
  def clearIntermediateResults(unloadMsgsContainers: Array[String]): Unit = {
    if (unloadMsgsContainers == null)
      return
    unloadMsgsContainers.foreach(mc => {
      val msgCont = _messagesOrContainers.getOrElse(mc.toLowerCase, null)
      if (msgCont != null && msgCont.data != null) {
        msgCont.data.clear
      }
    })
  }

  // Get all Status information from intermediate table
  override def getAllIntermediateStatusInfo: Array[(String, (String, Int, Int))] = {
    val results = new ArrayBuffer[(String, (String, Int, Int))]()
    val keys = ArrayBuffer[FatafatDataKey]()
    val keyCollector = (key: Key) => { collectKey(key, keys) }
    _runningTxnsDataStore.getAllKeys(keyCollector)
    var objs: Array[(String, Int, Int)] = new Array[(String, Int, Int)](1)
    keys.foreach(key => {
      try {
        val buildAdapOne = (tupleBytes: Value) => { buildAdapterUniqueValue(tupleBytes, objs) }
        _runningTxnsDataStore.get(makeKey(FatafatData.PrepareKey("UK", key.K, 0, 0)), buildAdapOne)
        results += ((key.K(0), objs(0)))
      } catch {
        case e: Exception => {
          logger.debug(s"getAllIntermediateStatusInfo() -- Unable to load Status Info")
        }
      }
    })
    logger.debug("Loaded %d status informations".format(results.size))
    results.toArray
  }

  // Get Status information from intermediate table for given keys. No Transaction required here.
  override def getIntermediateStatusInfo(keys: Array[String]): Array[(String, (String, Int, Int))] = {
    val results = new ArrayBuffer[(String, (String, Int, Int))]()
    var objs: Array[(String, Int, Int)] = new Array[(String, Int, Int)](1)
    keys.foreach(key => {
      try {
        val buildAdapOne = (tupleBytes: Value) => { buildAdapterUniqueValue(tupleBytes, objs) }
        _runningTxnsDataStore.get(makeKey(FatafatData.PrepareKey("UK", List(key), 0, 0)), buildAdapOne)
        results += ((key, objs(0)))
      } catch {
        case e: Exception => {
          logger.debug(s"getIntermediateStatusInfo() -- Unable to load Status Info")
        }
      }
    })
    logger.debug("Loaded %d status informations".format(results.size))
    results.toArray
  }

  // Get Status information from Final table
  override def getAllFinalStatusInfo(keys: Array[String]): Array[(String, (String, Int, Int))] = {
    val results = new ArrayBuffer[(String, (String, Int, Int))]()
    var objs: Array[(String, Int, Int)] = new Array[(String, Int, Int)](1)
    keys.foreach(key => {
      try {
        val buildAdapOne = (tupleBytes: Value) => { buildAdapterUniqueValue(tupleBytes, objs) }
        _allDataDataStore.get(makeKey(FatafatData.PrepareKey("AdapterUniqKvData", List(key), 0, 0)), buildAdapOne)
        results += ((key, objs(0)))
      } catch {
        case e: Exception => {
          logger.debug(s"getAllFinalStatusInfo() -- Unable to load Status Info")
        }
      }
    })
    logger.debug("Loaded %d status informations".format(results.size))
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

    logger.debug(s"PersistValidateAdapterInformation => " + validateUniqVals.mkString(","))

    validateUniqVals.foreach(kv => {
      object obj extends IStorage {
        val key = makeKey(FatafatData.PrepareKey("CP", List(kv._1), 0, 0))
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

    val keys = ArrayBuffer[FatafatDataKey]()
    val keyCollector = (key: Key) => { collectKey(key, keys) }
    _checkPointAdapInfoDataStore.getAllKeys(keyCollector)
    var objs: Array[String] = new Array[String](1)
    keys.foreach(key => {
      try {
        val buildAdapOne = (tupleBytes: Value) => { buildValidateAdapInfo(tupleBytes, objs) }
        _checkPointAdapInfoDataStore.get(makeKey(FatafatData.PrepareKey(key.T, key.K, 0, 0)), buildAdapOne)
        logger.debug(s"GetValidateAdapterInformation -- %s -> %s".format(key.K(0), objs(0).toString))
        results += ((key.K(0), objs(0)))
      } catch {
        case e: Exception => {
          logger.debug(s"GetValidateAdapterInformation() -- Unable to load Validate (Check Point) Adapter Information")
        }
      }
    })
    logger.debug("Loaded %d Validate (Check Point) Adapter Information".format(results.size))
    results.toArray
  }

  override def ReloadKeys(tempTransId: Long, containerName: String, keys: List[List[String]]): Unit = {
    val container = _messagesOrContainers.getOrElse(containerName.toLowerCase, null)
    if (container != null) {
      val dataKeys = keys.map(partKey => { FatafatDataKey(container.objFullName, partKey, List[Int](), 0) }).toArray
      loadMap(dataKeys, container)
      /*
      keys.foreach(partKey => {
        // Loading full Partition key for now.
        loadObjFromDb(tempTransId, container, partKey)
      })
      */
    }

  override def getRecent(containerName: String, partKey: List[String], tmRange: TimeRange, f: MessageContainerBase => Boolean): Option[MessageContainerBase] = {
    None
  }

  override def getRDD(containerName: String, partKey: List[String], tmRange: TimeRange, f: MessageContainerBase => Boolean): Array[MessageContainerBase] = {
    Array[MessageContainerBase]()
  }

  override def saveOne(containerName: String, partKey: List[String], value: MessageContainerBase): Unit = {
    
  }
}

