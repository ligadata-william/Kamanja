/*
 * Copyright 2015 ligaDATA
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ligadata.SimpleEnvContextImpl

import scala.collection.immutable.Map
import scala.collection.mutable._
import scala.util.control.Breaks._
import scala.reflect.runtime.{ universe => ru }
import org.apache.log4j.Logger
import com.ligadata.KvBase.{ Key, Value, TimeRange, KvBaseDefalts }
import com.ligadata.StorageBase.{ DataStore, Transaction }
import com.ligadata.KamanjaBase._
// import com.ligadata.KamanjaBase.{ EnvContext, MessageContainerBase }
import com.ligadata.kamanja.metadata._
import com.ligadata.Exceptions._
import java.net.URLClassLoader
import com.ligadata.Serialize._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import com.ligadata.Exceptions.StackTrace
import com.ligadata.KamanjaData.{ KamanjaData }
import com.ligadata.keyvaluestore.KeyValueManager
import java.io.{ ByteArrayInputStream, DataInputStream, DataOutputStream, ByteArrayOutputStream }
import java.util.{ Comparator, TreeMap };

trait LogTrait {
  val loggerName = this.getClass.getName()
  val logger = Logger.getLogger(loggerName)
}

case class AdapterUniqueValueDes(T: Long, V: String, Qs: Option[List[String]], Res: Option[List[String]]) // TransactionId, Value, Queues & Result Strings. Queues and Result Strings should be same size.  

/**
 *  The SimpleEnvContextImpl supports kv stores that are based upon MapDb hash tables.
 */
object SimpleEnvContextImpl extends EnvContext with LogTrait {

  override def setMdMgr(inMgr: MdMgr): Unit = { _mgr = inMgr }

  override def NewMessageOrContainer(fqclassname: String): MessageContainerBase = {
    try {
      Class.forName(fqclassname)
    } catch {
      case e: Exception => {
        logger.error("Failed to load Message/Container class %s with Reason:%s Message:%s".format(fqclassname, e.getCause, e.getMessage))
        throw e // Rethrow
      }
    }
    val msgOrContainer: MessageContainerBase = Class.forName(fqclassname).newInstance().asInstanceOf[MessageContainerBase]
    msgOrContainer
  }

  class TxnCtxtKey {
    var containerName: String = _
    var key: String = _
  }

  def BucketIdForBucketKey(bucketKey: Array[String]): Int = {
    if (bucketKey == null) return 0
    val prime = 31;
    var result = 1;
    bucketKey.foreach(k => {
      result = result * prime
      if (k != null)
        result += k.hashCode();
    })
    return result
  }

  case class KeyWithBucketId(bucketId: Int, key: Key);

  class BucketKeyComp extends Comparator[KeyWithBucketId] {
    override def compare(k1: KeyWithBucketId, k2: KeyWithBucketId): Int = {
      // First compare bucketId
      if (k1.bucketId < k2.bucketId)
        return -1
      if (k1.bucketId > k2.bucketId)
        return 1

      // Next compare Bucket Keys
      if (k1.key.bucketKey.size < k2.key.bucketKey.size)
        return -1
      if (k1.key.bucketKey.size > k2.key.bucketKey.size)
        return 1

      for (i <- 0 until k1.key.bucketKey.size) {
        val cmp = k1.key.bucketKey(i).compareTo(k2.key.bucketKey(i))
        if (cmp != 0)
          return cmp
      }

      // Next Compare time
      val tmCmp = k1.key.timePartition.compareTo(k2.key.timePartition)
      if (tmCmp != 0)
        return tmCmp

      // Next compare TransactionId
      if (k1.key.transactionId < k2.key.transactionId)
        return -1
      if (k1.key.transactionId > k2.key.transactionId)
        return 1

      // Next compare Row Id
      if (k1.key.rowId < k2.key.rowId)
        return -1
      if (k1.key.rowId > k2.key.rowId)
        return 1

      return 0
    }
  }

  class TimePartComp extends Comparator[KeyWithBucketId] {
    override def compare(k1: KeyWithBucketId, k2: KeyWithBucketId): Int = {
      // First Compare time
      val tmCmp = k1.key.timePartition.compareTo(k2.key.timePartition)
      if (tmCmp != 0)
        return tmCmp

      // Next compare bucketId
      if (k1.bucketId < k2.bucketId)
        return -1
      if (k1.bucketId > k2.bucketId)
        return 1

      // Next compare Bucket Keys
      if (k1.key.bucketKey.size < k2.key.bucketKey.size)
        return -1
      if (k1.key.bucketKey.size > k2.key.bucketKey.size)
        return 1

      for (i <- 0 until k1.key.bucketKey.size) {
        val cmp = k1.key.bucketKey(i).compareTo(k2.key.bucketKey(i))
        if (cmp != 0)
          return cmp
      }

      // Next compare TransactionId
      if (k1.key.transactionId < k2.key.transactionId)
        return -1
      if (k1.key.transactionId > k2.key.transactionId)
        return 1

      // Next compare Row Id
      if (k1.key.rowId < k2.key.rowId)
        return -1
      if (k1.key.rowId > k2.key.rowId)
        return 1

      return 0
    }
  }

  val bucketKeyComp = new BucketKeyComp()
  val timePartComp = new TimePartComp()

  class MsgContainerInfo {
    var current_msg_cont_data = ArrayBuffer[MessageContainerBase]()
    var dataByTmPart = new TreeMap[KeyWithBucketId, MessageContainerBase](timePartComp) // By time, BucketKey, then transactionid & rowid. This is little cheaper if we are going to get exact match, because we compare time & then bucketid
    var dataByBucketKey = new TreeMap[KeyWithBucketId, MessageContainerBase](bucketKeyComp) // By BucketKey, time, then transactionid & rowid
    var containerType: BaseTypeDef = null
    var isContainer: Boolean = false
    var objFullName: String = ""
    var dataStore: DataStore = null
  }

  object TxnContextCommonFunctions {
    def getRecentFromKamanjaData(kamanjaData: KamanjaData, tmRange: TimeRange, f: MessageContainerBase => Boolean): (MessageContainerBase, Boolean) = {
      // BUGBUG:: tmRange is not yet handled
      if (kamanjaData != null) {
        if (f != null) {
          val filterddata = kamanjaData.GetAllData.filter(v => f(v))
          if (filterddata.size > 0)
            return (filterddata(filterddata.size - 1), true)
        } else {
          val data = kamanjaData.GetAllData
          if (data.size > 0)
            return (data(data.size - 1), true)
        }
      }
      (null, false)
    }

    def getRddDataFromKamanjaData(kamanjaData: KamanjaData, tmRange: TimeRange, f: MessageContainerBase => Boolean): Array[MessageContainerBase] = {
      // BUGBUG:: tmRange is not yet handled
      if (kamanjaData != null) {
        if (f != null) {
          return kamanjaData.GetAllData.filter(v => f(v))
        } else {
          return kamanjaData.GetAllData
        }
      }
      Array[MessageContainerBase]()
    }

    def getRecent(container: MsgContainerInfo, partKey: List[String], tmRange: TimeRange, f: MessageContainerBase => Boolean): (MessageContainerBase, Boolean) = {
      //BUGBUG:: tmRange is not yet handled
      //BUGBUG:: Taking last record. But that may not be correct. Need to take max txnid one. But the issue is, if we are getting same data from multiple partitions, the txnids may be completely different.
      if (container != null) {
        val kamanjaData = container.data
        if (TxnContextCommonFunctions.IsEmptyKey(partKey) == false) {
          val kamanjaData = container.data.getOrElse(partKey)
          if (kamanjaData != null)
            return getRecentFromKamanjaData(kamanjaData._2, tmRange, f)
        } else {
          val dataAsArr = container.data.toArray
          var idx = dataAsArr.size - 1
          while (idx >= 0) {
            val (v, foundPartKey) = getRecentFromKamanjaData(dataAsArr(idx)._2._2, tmRange, f)
            if (foundPartKey)
              return (v, foundPartKey)
            idx = idx - 1
          }
        }
      }
      (null, false)
    }

    def IsEmptyKey(key: List[String]): Boolean = {
      (key == null || key.size == 0)
    }

    def IsSameKey(key1: List[String], key2: List[String]): Boolean = {
      if (key1.size != key2.size)
        return false

      for (i <- 0 until key1.size) {
        if (key1(i).compareTo(key2(i)) != 0)
          return false
      }

      return true
    }

    def IsKeyExists(keys: Array[List[String]], searchKey: List[String]): Boolean = {
      keys.foreach(k => {
        if (IsSameKey(k, searchKey))
          return true
      })
      return false
    }

    def getRddData(container: MsgContainerInfo, partKey: List[String], tmRange: TimeRange, f: MessageContainerBase => Boolean, alreadyFoundPartKeys: Array[List[String]]): (Array[MessageContainerBase], Array[List[String]]) = {
      val retResult = ArrayBuffer[MessageContainerBase]()
      var foundPartKeys = ArrayBuffer[List[String]]()
      if (container != null) {
        if (TxnContextCommonFunctions.IsEmptyKey(partKey) == false) {
          val kamanjaData = container.data.getOrElse(InMemoryKeyDataInJson(partKey), null)
          if (kamanjaData != null && IsKeyExists(alreadyFoundPartKeys, kamanjaData._2.GetKey.toList) == false) {
            retResult ++= getRddDataFromKamanjaData(kamanjaData._2, tmRange, f)
            foundPartKeys += partKey
          }
        } else {
          container.data.foreach(kv => {
            val k = kv._2._2.GetKey.toList
            if (IsKeyExists(alreadyFoundPartKeys, k) == false) {
              retResult ++= getRddDataFromKamanjaData(kv._2._2, tmRange, f)
              foundPartKeys += k.toList
            }
          })
        }
      }
      (retResult.toArray, foundPartKeys.toArray)
    }
  }

  class TransactionContext(var txnId: Long) {
    private[this] val _messagesOrContainers = scala.collection.mutable.Map[String, MsgContainerInfo]()
    private[this] val _adapterUniqKeyValData = scala.collection.mutable.Map[String, (Long, String, List[(String, String)])]()
    private[this] val _modelsResult = scala.collection.mutable.Map[Key, scala.collection.mutable.Map[String, SavedMdlResult]]()
    /*
    private[this] val _statusStrings = new ArrayBuffer[String]()
*/

    def getMsgContainer(containerName: String, addIfMissing: Boolean): MsgContainerInfo = {
      var fnd = _messagesOrContainers.getOrElse(containerName.toLowerCase, null)
      if (fnd == null && addIfMissing) {
        fnd = new MsgContainerInfo
        _messagesOrContainers(containerName) = fnd
      }
      fnd
    }

    /*
    def setFetchedObj(containerName: String, partKeyStr: String, kamanjaData: KamanjaData): Unit = {
      val container = getMsgContainer(containerName.toLowerCase, true)
      if (container != null) {
        container.data(partKeyStr) = (false, kamanjaData)
      }
    }
*/
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
      if (container != null && TxnContextCommonFunctions.IsEmptyKey(partKey) == false && TxnContextCommonFunctions.IsEmptyKey(primaryKey) == false) {
        val kamanjaData = container.data.getOrElse(InMemoryKeyDataInJson(partKey), null)
        if (kamanjaData != null) {
          // Search for primary key match
          return (kamanjaData._2.GetMessageContainerBase(primaryKey.toArray, false), true)
        }
      }
      (null, false)
    }

    def getObjects(containerName: String, partKey: List[String], appendCurrentChanges: Boolean): (Array[MessageContainerBase], Boolean) = {
      val container = getMsgContainer(containerName.toLowerCase, false)
      if (container != null) {
        val kamanjaData = container.data.getOrElse(InMemoryKeyDataInJson(partKey), null)
        if (kamanjaData != null) {
          if (container.current_msg_cont_data.size > 0) {
            val allData = ArrayBuffer[MessageContainerBase]()
            if (appendCurrentChanges) {
              allData ++= kamanjaData._2.GetAllData
              allData --= container.current_msg_cont_data
              allData ++= container.current_msg_cont_data // Just to get the current messages to end
            } else {
              allData ++= kamanjaData._2.GetAllData
              allData --= container.current_msg_cont_data
            }
            return (allData.toArray, true)
          } else {
            return (kamanjaData._2.GetAllData, true)
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
          if (TxnContextCommonFunctions.IsEmptyKey(partKeys(i)) == false) {
            val kamanjaData = container.data.getOrElse(InMemoryKeyDataInJson(partKeys(i)), null)
            if (kamanjaData != null) {
              if (TxnContextCommonFunctions.IsEmptyKey(primaryKeys(i)) == false) {
                // Search for primary key match
                val fnd = kamanjaData._2.GetMessageContainerBase(primaryKeys(i).toArray, false)
                if (fnd != null)
                  return (true, Array[List[String]]())
              }
            } else {
              notFndPartkeys += partKeys(i)
            }
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
          val kamanjaData = container.data.getOrElse(InMemoryKeyDataInJson(partKeys(i)), null)
          if (kamanjaData != null) {
            // Search for primary key match
            val fnd = kamanjaData._2.GetMessageContainerBase(primaryKeys(i).toArray, false)
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
      if (TxnContextCommonFunctions.IsEmptyKey(partKey))
        return
      val container = getMsgContainer(containerName.toLowerCase, true)
      if (container != null) {
        value.TransactionId(txnId) // Setting the current transactionid
        val partKeyStr = InMemoryKeyDataInJson(partKey)
        val fnd = container.data.getOrElse(partKeyStr, null)
        if (fnd != null) {
          fnd._2.AddMessageContainerBase(value, true, true)
          if (fnd._1 == false) {
            container.data(partKeyStr) = (true, fnd._2)
          }
        } else {
          val ffData = new KamanjaData
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
    def setAdapterUniqueKeyValue(transId: Long, key: String, value: String, outputResults: List[(String, String)]): Unit = {
      _adapterUniqKeyValData(key) = (transId, value, outputResults)
    }

    def getAdapterUniqueKeyValue(key: String): (Long, String, List[(String, String)]) = {
      _adapterUniqKeyValData.getOrElse(key, null)
    }

    // Model Results Saving & retrieving. Don't return null, always return empty, if we don't find
    def saveModelsResult(key: List[String], value: scala.collection.mutable.Map[String, SavedMdlResult]): Unit = {
      _modelsResult(Key(KvBaseDefalts.defaultTime, key.toArray, 0L, 0)) = value
    }

    def getModelsResult(k: Key): scala.collection.mutable.Map[String, SavedMdlResult] = {
      _modelsResult.getOrElse(k, null)
    }

    def setReloadFlag(containerName: String): Unit = {
      val container = getMsgContainer(containerName.toLowerCase, false)
      if (container != null)
        container.reload = true
    }

    /*
    def saveStatus(status: String): Unit = {
      _statusStrings += status
    }
*/

    def getAllMessagesAndContainers = _messagesOrContainers.toMap

    def getAllAdapterUniqKeyValData = _adapterUniqKeyValData.toMap

    def getAllModelsResult = _modelsResult.toMap

    /*
    def getAllStatusStrings = _statusStrings
*/

    def getRecent(containerName: String, partKey: List[String], tmRange: TimeRange, f: MessageContainerBase => Boolean): (MessageContainerBase, Boolean) = {
      val (v, foundPartKey) = TxnContextCommonFunctions.getRecent(getMsgContainer(containerName.toLowerCase, false), partKey, tmRange, f)
      (v, foundPartKey)
    }

    def getRddData(containerName: String, partKey: List[String], tmRange: TimeRange, f: MessageContainerBase => Boolean, alreadyFoundPartKeys: Array[List[String]]): (Array[MessageContainerBase], Array[List[String]]) = {
      return TxnContextCommonFunctions.getRddData(getMsgContainer(containerName.toLowerCase, false), partKey, tmRange, f, alreadyFoundPartKeys)
    }
  }

  private[this] val _buckets = 257 // Prime number

  private[this] val _locks = new Array[Object](_buckets)

  private[this] val _messagesOrContainers = scala.collection.mutable.Map[String, MsgContainerInfo]()
  private[this] val _txnContexts = new Array[scala.collection.mutable.Map[Long, TransactionContext]](_buckets)
  private[this] val _adapterUniqKeyValData = scala.collection.mutable.Map[String, (Long, String, List[(String, String)])]()
  private[this] val _modelsResult = scala.collection.mutable.Map[Key, scala.collection.mutable.Map[String, SavedMdlResult]]()

  private[this] var _kryoSer: com.ligadata.Serialize.Serializer = null
  private[this] var _classLoader: java.lang.ClassLoader = null
  private[this] var _defaultDataStore: DataStore = null
  private[this] var _statusinfoDataStore: DataStore = null
  private[this] var _mdres: MdBaseResolveInfo = null
  private[this] var _jarPaths: collection.immutable.Set[String] = null // Jar paths where we can resolve all jars (including dependency jars).

  for (i <- 0 until _buckets) {
    _txnContexts(i) = scala.collection.mutable.Map[Long, TransactionContext]()
    _locks(i) = new Object()
  }

  private[this] def lockIdx(transId: Long): Int = {
    return (transId % _buckets).toInt
  }

  private[this] def getTransactionContext(transId: Long, addIfMissing: Boolean): TransactionContext = {
    _locks(lockIdx(transId)).synchronized {
      var txnCtxt = _txnContexts(lockIdx(transId)).getOrElse(transId, null)
      if (txnCtxt == null && addIfMissing) {
        txnCtxt = new TransactionContext(transId)
        _txnContexts(lockIdx(transId))(transId) = txnCtxt
      }
      return txnCtxt
    }
  }

  private[this] def removeTransactionContext(transId: Long): Unit = {
    _locks(lockIdx(transId)).synchronized {
      _txnContexts(lockIdx(transId)) -= transId
    }
  }

  private def buildObject(k: Key, v: Value, objs: Array[MessageContainerBase], containerType: BaseTypeDef): Unit = {
    v.serializerType.toLowerCase match {
      case "kryo" => {
        if (_kryoSer == null) {
          _kryoSer = SerializerManager.GetSerializer("kryo")
          if (_kryoSer != null && _classLoader != null) {
            _kryoSer.SetClassLoader(_classLoader)
          }
        }
        if (_kryoSer != null) {
          objs(0) = _kryoSer.DeserializeObjectFromByteArray(v.serializedInfo).asInstanceOf[MessageContainerBase]
        }
      }
      case "manual" => {
        objs(0) = SerializeDeserialize.Deserialize(v.serializedInfo, _mdres, _classLoader, true, "")
      }
      case _ => {
        throw new Exception("Found un-handled Serializer Info: " + v.serializerType)
      }
    }
  }

  private def GetDataStoreHandle(jarPaths: collection.immutable.Set[String], dataStoreInfo: String): DataStore = {
    try {
      logger.debug("Getting DB Connection for dataStoreInfo:%s, tableName:%s".format(dataStoreInfo))
      return KeyValueManager.Get(jarPaths, dataStoreInfo)
    } catch {
      case e: Exception => {
        logger.error("Failed to GetDataStoreHandle")
        throw e
      }
    }
  }

  val results = new ArrayBuffer[(String, (Long, String, List[(String, String)]))]()

  private def buildAdapterUniqueValue(k: Key, v: Value, results: ArrayBuffer[(String, (Long, String, List[(String, String)]))]) {
    implicit val jsonFormats: Formats = DefaultFormats
    val uniqVal = parse(new String(v.serializedInfo)).extract[AdapterUniqueValueDes]

    val res = ArrayBuffer[(String, String)]()

    if (uniqVal.Qs != None && uniqVal.Res != None) {
      val Qs = uniqVal.Qs.get
      val Res = uniqVal.Res.get
      if (Qs.size == Res.size) {
        for (i <- 0 until Qs.size) {
          res += ((Qs(i), Res(i)))
        }
      }
    }

    results += ((k.bucketKey(0), (uniqVal.T, uniqVal.V, res.toList))) // taking 1st key, that is what we are expecting
  }

  private def buildModelsResult(k: Key, v: Value, objs: Array[(Key, scala.collection.mutable.Map[String, SavedMdlResult])]) {
    v.serializerType.toLowerCase match {
      case "kryo" => {
        if (_kryoSer == null) {
          _kryoSer = SerializerManager.GetSerializer("kryo")
          if (_kryoSer != null && _classLoader != null) {
            _kryoSer.SetClassLoader(_classLoader)
          }
        }
        if (_kryoSer != null) {
          objs(0) = ((k, _kryoSer.DeserializeObjectFromByteArray(v.serializedInfo).asInstanceOf[scala.collection.mutable.Map[String, SavedMdlResult]]))
        }
      }
      case _ => {
        throw new Exception("Found un-handled Serializer Info: " + v.serializerType)
      }
    }
  }

  /*
  
  private def loadObjFromDb(transId: Long, msgOrCont: MsgContainerInfo, key: List[String]): KamanjaData = {
    val partKeyStr = KamanjaData.PrepareKey(msgOrCont.objFullName, key, 0, 0)
    var objs: Array[KamanjaData] = new Array[KamanjaData](1)
    val buildOne = (tupleBytes: Value) => {
      buildObject(tupleBytes, objs, msgOrCont.containerType)
    }
    try {
      _allDataDataStore.get(makeKey(partKeyStr), buildOne)
    } catch {
      case e: Exception => {
        val stackTrace = StackTrace.ThrowableTraceString(e)
        logger.debug("Data not found for key:" + partKeyStr + "\nStackTrace:" + stackTrace)
      }
    }
    if (objs(0) != null) {
      val lockObj = if (msgOrCont.loadedAll) msgOrCont else _locks(lockIdx(transId))
      lockObj.synchronized {
        msgOrCont.data(InMemoryKeyDataInJson(key)) = (false, objs(0))
      }
    }
    return objs(0)
  }
  
  */
  /*
  private def localGetObject(transId: Long, containerName: String, partKey: List[String], primaryKey: List[String]): MessageContainerBase = {
    if (TxnContextCommonFunctions.IsEmptyKey(partKey) || TxnContextCommonFunctions.IsEmptyKey(primaryKey))
      return null
    val txnCtxt = getTransactionContext(transId, false)
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
      val kamanjaData = container.data.getOrElse(partKeyStr, null)
      if (kamanjaData != null) {
        // Search for primary key match
        val v = kamanjaData._2.GetMessageContainerBase(primaryKey.toArray, false)
        if (txnCtxt != null)
          txnCtxt.setFetchedObj(containerName, partKeyStr, kamanjaData._2)
        return v;
      }
      // If not found in memory, try in DB
      val loadedFfData = loadObjFromDb(transId, container, partKey)
      if (loadedFfData != null) {
        // Search for primary key match
        val v = loadedFfData.GetMessageContainerBase(primaryKey.toArray, false)
        if (txnCtxt != null)
          txnCtxt.setFetchedObj(containerName, partKeyStr, loadedFfData)
        return v;
      }
      // If not found in DB, Create Empty and set to current transaction context
      if (txnCtxt != null) {
        val emptyFfData = new KamanjaData
        emptyFfData.SetKey(partKey.toArray)
        emptyFfData.SetTypeName(containerName)
        txnCtxt.setFetchedObj(containerName, partKeyStr, emptyFfData)
      }
    }
    null
  }

  private def localHistoryObjects(transId: Long, containerName: String, partKey: List[String], appendCurrentChanges: Boolean): Array[MessageContainerBase] = {
    val retVals = ArrayBuffer[MessageContainerBase]()
    if (TxnContextCommonFunctions.IsEmptyKey(partKey))
      return retVals.toArray
    val txnCtxt = getTransactionContext(transId, false)
    if (txnCtxt != null) {
      val (objs, foundPartKey) = txnCtxt.getObjects(containerName, partKey, appendCurrentChanges)
      retVals ++= objs
      if (foundPartKey)
        return retVals.toArray
    }

    val container = _messagesOrContainers.getOrElse(containerName.toLowerCase, null)
    if (container != null) {
      val partKeyStr = InMemoryKeyDataInJson(partKey)
      val kamanjaData = container.data.getOrElse(partKeyStr, null)
      if (kamanjaData != null) {
        // Search for primary key match
        if (txnCtxt != null)
          txnCtxt.setFetchedObj(containerName, partKeyStr, kamanjaData._2)
        retVals ++= kamanjaData._2.GetAllData
      } else {
        val loadedFfData = loadObjFromDb(transId, container, partKey)
        if (loadedFfData != null) {
          // Search for primary key match
          if (txnCtxt != null)
            txnCtxt.setFetchedObj(containerName, partKeyStr, loadedFfData)
          retVals ++= loadedFfData.GetAllData
        } else {
          // If not found in DB, Create Empty and set to current transaction context
          if (txnCtxt != null) {
            val emptyFfData = new KamanjaData
            emptyFfData.SetKey(partKey.toArray)
            emptyFfData.SetTypeName(containerName)
            txnCtxt.setFetchedObj(containerName, partKeyStr, emptyFfData)
          }
        }
      }
    }

    retVals.toArray
  }
  private def localGetAllKeyValues(transId: Long, containerName: String): Array[MessageContainerBase] = {
    val fnd = _messagesOrContainers.getOrElse(containerName.toLowerCase, null)
    if (fnd != null) {
      if (fnd.loadedAll) {
        var allObjs = ArrayBuffer[MessageContainerBase]()
        fnd.data.foreach(kv => {
          allObjs ++= kv._2._2.GetAllData
        })
        val txnCtxt = getTransactionContext(transId, false)
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

  private def localGetAllObjects(transId: Long, containerName: String): Array[MessageContainerBase] = {
    return localGetAllKeyValues(transId, containerName)
  }
*/

  private def localGetAdapterUniqueKeyValue(transId: Long, key: String): (Long, String, List[(String, String)]) = {
    val txnCtxt = getTransactionContext(transId, false)
    if (txnCtxt != null) {
      val v = txnCtxt.getAdapterUniqueKeyValue(key)
      if (v != null) return v
    }

    val v = _adapterUniqKeyValData.getOrElse(key, null)
    if (v != null) return v
    val results = new ArrayBuffer[(String, (Long, String, List[(String, String)]))]()
    val buildAdapOne = (k: Key, v: Value) => {
      buildAdapterUniqueValue(k, v, results)
    }
    try {
      _defaultDataStore.get("AdapterUniqKvData", Array(TimeRange(KvBaseDefalts.defaultTime, KvBaseDefalts.defaultTime)), Array(Array(key)), buildAdapOne)
    } catch {
      case e: Exception => {
        val stackTrace = StackTrace.ThrowableTraceString(e)
        logger.debug("Data not found for key:" + key + "\nStackTrace:" + stackTrace)
      }
    }
    if (results.size > 0) {
      _adapterUniqKeyValData.synchronized {
        _adapterUniqKeyValData(key) = results(0)._2
      }
      return results(0)._2
    }
    return null
  }

  private def localGetModelsResult(transId: Long, key: List[String]): scala.collection.mutable.Map[String, SavedMdlResult] = {
    val k = Key(KvBaseDefalts.defaultTime, key.toArray, 0L, 0)
    val txnCtxt = getTransactionContext(transId, false)
    if (txnCtxt != null) {
      val v = txnCtxt.getModelsResult(k)
      if (v != null) return v
    }

    val v = _modelsResult.getOrElse(k, null)
    if (v != null) return v

    var objs = new Array[(Key, scala.collection.mutable.Map[String, SavedMdlResult])](1)
    val buildMdlOne = (k: Key, v: Value) => { buildModelsResult(k, v, objs) }
    try {
      _defaultDataStore.get("ModelResults", Array(TimeRange(KvBaseDefalts.defaultTime, KvBaseDefalts.defaultTime)), Array(key.toArray), buildMdlOne)
    } catch {
      case e: Exception => {
        val stackTrace = StackTrace.ThrowableTraceString(e)
        logger.debug("Data not found for key:" + key.mkString(",") + "\nStackTrace:" + stackTrace)
      }
    }
    if (objs(0) != null) {
      _modelsResult.synchronized {
        _modelsResult(objs(0)._1) = objs(0)._2
      }
      return objs(0)._2
    }
    return scala.collection.mutable.Map[String, SavedMdlResult]()
  }

  /**
   * Does at least one of the supplied keys exist in a container with the supplied name?
   */
  /*
  private def localContainsAny(transId: Long, containerName: String, partKeys: Array[List[String]], primaryKeys: Array[List[String]]): Boolean = {
    var remainingPartKeys = partKeys
    val txnCtxt = getTransactionContext(transId, false)
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
        val kamanjaData = container.data.getOrElse(InMemoryKeyDataInJson(remainingPartKeys(i)), null)
        if (kamanjaData != null) {
          // Search for primary key match
          val fnd = kamanjaData._2.GetMessageContainerBase(primaryKeys(i).toArray, false)
          if (fnd != null)
            return true
        } else {
          reMainingForDb += remainingPartKeys(i)
        }
      }

      for (i <- 0 until reMainingForDb.size) {
        val kamanjaData = loadObjFromDb(transId, container, reMainingForDb(i))
        if (kamanjaData != null) {
          // Search for primary key match
          val fnd = kamanjaData.GetMessageContainerBase(primaryKeys(i).toArray, false)
          if (fnd != null)
            return true
        }
      }
    }
    false
  }
*/
  /**
   * Do all of the supplied keys exist in a container with the supplied name?
   */
  /*
  private def localContainsAll(transId: Long, containerName: String, partKeys: Array[List[String]], primaryKeys: Array[List[String]]): Boolean = {
    var remainingPartKeys: Array[List[String]] = partKeys
    var remainingPrimaryKeys: Array[List[String]] = primaryKeys
    val txnCtxt = getTransactionContext(transId, false)
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
        val kamanjaData = container.data.getOrElse(InMemoryKeyDataInJson(remainingPartKeys(i)), null)
        if (kamanjaData != null) {
          // Search for primary key match
          val fnd = kamanjaData._2.GetMessageContainerBase(remainingPrimaryKeys(i).toArray, false)
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
        val kamanjaData = container.data.getOrElse(InMemoryKeyDataInJson(unmatchedPartKeys(i)), null)
        if (kamanjaData != null) {
          // Search for primary key match
          val fnd = kamanjaData._2.GetMessageContainerBase(unmatchedPrimaryKeys(i).toArray, false)
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

  // Same kind of code is there in localGetObject
  private def loadBeforeSetObject(txnCtxt: TransactionContext, transId: Long, containerName: String, partKey: List[String]): Unit = {
    if (TxnContextCommonFunctions.IsEmptyKey(partKey))
      return
    val container = _messagesOrContainers.getOrElse(containerName.toLowerCase, null)
    if (container != null) {
      val partKeyStr = InMemoryKeyDataInJson(partKey)
      val kamanjaData = container.data.getOrElse(partKeyStr, null)
      if (kamanjaData != null) {
        if (txnCtxt != null)
          txnCtxt.setFetchedObj(containerName, partKeyStr, kamanjaData._2)
        return ;
      }
      // If not found in memory, try in DB
      val loadedFfData = loadObjFromDb(transId, container, partKey)
      if (loadedFfData != null) {
        if (txnCtxt != null)
          txnCtxt.setFetchedObj(containerName, partKeyStr, loadedFfData)
        return ;
      }
      // If not found in DB, Create Empty and set to current transaction context
      if (txnCtxt != null) {
        val emptyFfData = new KamanjaData
        emptyFfData.SetKey(partKey.toArray)
        emptyFfData.SetTypeName(containerName)
        txnCtxt.setFetchedObj(containerName, partKeyStr, emptyFfData)
      }
    }
  }
*/
  private def localSetObject(transId: Long, containerName: String, partKey: List[String], value: MessageContainerBase): Unit = {
    var txnCtxt = getTransactionContext(transId, true)
    if (txnCtxt != null) {
      val container = txnCtxt.getMsgContainer(containerName.toLowerCase, false)
      if (container == null) {
        // Try to load the key if they exists in global storage.
        // loadBeforeSetObject(txnCtxt, transId, containerName, partKey)
      }
      txnCtxt.setObject(containerName, partKey, value)
    }
  }

  private def localSetAdapterUniqueKeyValue(transId: Long, key: String, value: String, outputResults: List[(String, String)]): Unit = {
    var txnCtxt = getTransactionContext(transId, true)
    if (txnCtxt != null) {
      txnCtxt.setAdapterUniqueKeyValue(transId, key, value, outputResults)
    }
  }

  private def localSaveModelsResult(transId: Long, key: List[String], value: scala.collection.mutable.Map[String, SavedMdlResult]): Unit = {
    var txnCtxt = getTransactionContext(transId, true)
    if (txnCtxt != null) {
      txnCtxt.saveModelsResult(key, value)
    }
  }
  /*
  private def collectKey(key: Key, keys: ArrayBuffer[KamanjaDataKey]): Unit = {
    implicit val jsonFormats: Formats = DefaultFormats
    val parsed_key = parse(new String(key.toArray)).extract[KamanjaDataKey]
    keys += parsed_key
  }
*/
  override def SetClassLoader(cl: java.lang.ClassLoader): Unit = {
    _classLoader = cl
    if (_kryoSer != null)
      _kryoSer.SetClassLoader(_classLoader)
  }

  override def SetMetadataResolveInfo(mdres: MdBaseResolveInfo): Unit = {
    _mdres = mdres
  }

  //BUGBUG:: May be we need to lock before we do anything here
  override def Shutdown: Unit = {
    _adapterUniqKeyValData.clear
    if (_defaultDataStore != null)
      _defaultDataStore.Shutdown
    _defaultDataStore = null

    if (_statusinfoDataStore != null)
      _statusinfoDataStore.Shutdown
    _statusinfoDataStore = null

    _messagesOrContainers.clear
  }

  /*
  private def getTableKeys(all_keys: ArrayBuffer[KamanjaDataKey], objName: String): Array[KamanjaDataKey] = {
    val tmpObjName = objName.toLowerCase
    all_keys.filter(k => k.T.compareTo(objName) == 0).toArray
  }
*/

  override def getPropertyValue(clusterId: String, key: String): String = {
    _mgr.GetUserProperty(clusterId, key)
  }

  override def SetJarPaths(jarPaths: collection.immutable.Set[String]): Unit = {
    if (jarPaths != null) {
      logger.debug("JarPaths:%s".format(jarPaths.mkString(",")))
    }
    _jarPaths = jarPaths
  }

  override def SetDefaultDatastore(dataDataStoreInfo: String): Unit = {
    if (dataDataStoreInfo != null)
      logger.debug("DefaultDatastore Information:%s".format(dataDataStoreInfo))
    if (_defaultDataStore == null) { // Doing it only once
      _defaultDataStore = GetDataStoreHandle(_jarPaths, dataDataStoreInfo)
    }
  }

  override def SetStatusInfoDatastore(statusDataStoreInfo: String): Unit = {
    if (statusDataStoreInfo != null)
      logger.debug("DefaultDatastore Information:%s".format(statusDataStoreInfo))
    if (_statusinfoDataStore == null) { // Doing it only once
      _statusinfoDataStore = GetDataStoreHandle(_jarPaths, statusDataStoreInfo)
    }
  }

  // Adding new messages or Containers
  override def RegisterMessageOrContainers(containersInfo: Array[ContainerNameAndDatastoreInfo]): Unit = {
    if (containersInfo != null)
      logger.info("Messages/Containers:%s".format(containersInfo.map(ci => (if (ci.containerName != null) ci.containerName else "", if (ci.dataDataStoreInfo != null) ci.dataDataStoreInfo else "")).mkString(",")))

    containersInfo.foreach(ci => {
      val c = ci.containerName.toLowerCase
      val (namespace, name) = Utils.parseNameTokenNoVersion(c)
      var containerType = _mgr.ActiveType(namespace, name)

      if (containerType != null) {
        val objFullName: String = containerType.FullName.toLowerCase
        val fnd = _messagesOrContainers.getOrElse(objFullName, null)
        if (fnd != null) {
          // We already have this
        } else {
          val newMsgOrContainer = new MsgContainerInfo
          newMsgOrContainer.containerType = containerType
          newMsgOrContainer.objFullName = objFullName
          newMsgOrContainer.isContainer = (_mgr.ActiveContainer(namespace, name) != null)

          if (ci.dataDataStoreInfo != null)
            newMsgOrContainer.dataStore = GetDataStoreHandle(_jarPaths, ci.dataDataStoreInfo)
          else
            newMsgOrContainer.dataStore = _defaultDataStore

          /** create a map to cache the entries to be resurrected from the mapdb */
          _messagesOrContainers(objFullName) = newMsgOrContainer
        }
      } else {
        var error_msg = "Message/Container %s not found".format(c)
        logger.error(error_msg)
        throw new Exception(error_msg)
      }
    })
  }

  private def Clone(vals: Array[MessageContainerBase]): Array[MessageContainerBase] = {
    if (vals == null) return null
    return vals.map(v => {
      if (v == null) null else v.Clone()
    })
  }

  private def Clone(v: MessageContainerBase): MessageContainerBase = {
    if (v == null) return null
    return v.Clone()
  }

  private def Clone(ov: Option[MessageContainerBase]): Option[MessageContainerBase] = {
    if (ov == None) return ov
    Some(ov.get.Clone())
  }

  /*
  override def getAllObjects(transId: Long, containerName: String): Array[MessageContainerBase] = {
    Clone(localGetAllObjects(transId, containerName))
  }

  override def getObject(transId: Long, containerName: String, partKey: List[String], primaryKey: List[String]): MessageContainerBase = {
    Clone(localGetObject(transId, containerName, partKey, primaryKey))
  }

  override def getHistoryObjects(transId: Long, containerName: String, partKey: List[String], appendCurrentChanges: Boolean): Array[MessageContainerBase] = {
    Clone(localHistoryObjects(transId, containerName, partKey, appendCurrentChanges))
  }

  override def getAdapterUniqueKeyValue(transId: Long, key: String): (Long, String, List[(String, String)]) = {
    localGetAdapterUniqueKeyValue(transId, key)
  }
*/

  override def getModelsResult(transId: Long, key: List[String]): scala.collection.mutable.Map[String, SavedMdlResult] = {
    localGetModelsResult(transId, key)
  }

  /**
   * Does the supplied key exist in a container with the supplied name?
   */
  /*
  override def contains(transId: Long, containerName: String, partKey: List[String], primaryKey: List[String]): Boolean = {
    localContainsAny(transId, containerName, Array(partKey), Array(primaryKey))
  }
*/
  /**
   * Does at least one of the supplied keys exist in a container with the supplied name?
   */
  /*
  override def containsAny(transId: Long, containerName: String, partKeys: Array[List[String]], primaryKeys: Array[List[String]]): Boolean = {
    localContainsAny(transId, containerName, partKeys, primaryKeys)
  }
*/
  /**
   * Do all of the supplied keys exist in a container with the supplied name?
   */
  /*
  override def containsAll(transId: Long, containerName: String, partKeys: Array[List[String]], primaryKeys: Array[List[String]]): Boolean = {
    localContainsAll(transId, containerName, partKeys, primaryKeys)
  }
*/
  override def setObject(transId: Long, containerName: String, partKey: List[String], value: MessageContainerBase): Unit = {
    localSetObject(transId, containerName, partKey, value)
  }

  override def setAdapterUniqueKeyValue(transId: Long, key: String, value: String, outputResults: List[(String, String)]): Unit = {
    localSetAdapterUniqueKeyValue(transId, key, value, outputResults)
  }

  override def saveModelsResult(transId: Long, key: List[String], value: scala.collection.mutable.Map[String, SavedMdlResult]): Unit = {
    localSaveModelsResult(transId, key, value)
  }

  override def getChangedData(tempTransId: Long, includeMessages: Boolean, includeContainers: Boolean): scala.collection.immutable.Map[String, Array[Key]] = {
    val changedContainersData = scala.collection.mutable.Map[String, Array[Key]]()

    // Commit Data and Removed Transaction information from status
    val txnCtxt = getTransactionContext(tempTransId, false)
    if (txnCtxt == null)
      return changedContainersData.toMap

    val messagesOrContainers = txnCtxt.getAllMessagesAndContainers

    messagesOrContainers.foreach(v => {
      val mc = _messagesOrContainers.getOrElse(v._1, null)
      if (mc != null) {
        var savingKeys: Array[Key] = null
        if (includeMessages && includeContainers) {
          // All the vlaues (includes messages & containers)
          savingKeys = v._2.data.map(kv => kv._1).toArray
        } else if (includeMessages && /* v._2.containerType.tTypeType == ObjTypeType.tContainer && */ (mc.isContainer == false && v._2.isContainer == false)) {
          // Msgs
          savingKeys = v._2.data.map(kv => kv._1).toArray
        } else if (includeContainers && /* v._2.containerType.tTypeType == ObjTypeType.tContainer && */ (mc.isContainer || v._2.isContainer)) {
          // Containers
          savingKeys = v._2.data.map(kv => kv._1).toArray
        }
        if (savingKeys != null && savingKeys.size > 0)
          changedContainersData(v._1) = savingKeys
      }
    })

    return changedContainersData.toMap
  }

  // Final Commit for the given transaction
  // BUGBUG:: For now we are committing all the data into default datastore. Not yet handled message level datastore.
  override def commitData(transId: Long, key: String, value: String, outResults: List[(String, String)]): Unit = {
    val outputResults = if (outResults != null) outResults else List[(String, String)]()

    // Commit Data and Removed Transaction information from status
    val txnCtxt = getTransactionContext(transId, false)
    if (txnCtxt == null && (key == null || value == null))
      return

    // Persist current transaction objects
    val messagesOrContainers = if (txnCtxt != null) txnCtxt.getAllMessagesAndContainers else Map[String, MsgContainerInfo]()

    val localValues = scala.collection.mutable.Map[String, (Long, String, List[(String, String)])]()

    if (key != null && value != null && outputResults != null)
      localValues(key) = (transId, value, outputResults)

    val adapterUniqKeyValData = if (localValues.size > 0) localValues.toMap else if (txnCtxt != null) txnCtxt.getAllAdapterUniqKeyValData else Map[String, (Long, String, List[(String, String)])]()
    val modelsResult = if (txnCtxt != null) txnCtxt.getAllModelsResult else Map[Key, scala.collection.mutable.Map[String, SavedMdlResult]]()

    if (_kryoSer == null) {
      _kryoSer = SerializerManager.GetSerializer("kryo")
      if (_kryoSer != null && _classLoader != null) {
        _kryoSer.SetClassLoader(_classLoader)
      }
    }

    val bos = new ByteArrayOutputStream(1024 * 1024)
    val dos = new DataOutputStream(bos)

    val commiting_data = ArrayBuffer[(String, Array[(Key, Value)])]()
    val dataForContainer = ArrayBuffer[(Key, Value)]()

    messagesOrContainers.foreach(v => {
      dataForContainer.clear
      val mc = _messagesOrContainers.getOrElse(v._1, null)
      if (mc != null) {
        v._2.data.foreach(kv => {
          mc.data(kv._1) = kv._2 // Assigning new data
          bos.reset
          kv._2.Serialize(dos)
          dataForContainer += ((kv._1, Value("manual", bos.toByteArray)))
        })
        v._2.current_msg_cont_data.clear
        commiting_data += ((mc.objFullName, dataForContainer.toArray))
      }
    })

    dataForContainer.clear
    adapterUniqKeyValData.foreach(v1 => {
      _adapterUniqKeyValData(v1._1) = v1._2
      val json = ("T" -> v1._2._1) ~
        ("V" -> v1._2._2) ~
        ("Qs" -> v1._2._3.map(qsres => { qsres._1 })) ~
        ("Res" -> v1._2._3.map(qsres => { qsres._2 }))
      val compjson = compact(render(json))
      dataForContainer += ((Key(KvBaseDefalts.defaultTime, Array(v1._1), 0, 0), Value("json", compjson.getBytes("UTF8"))))
    })
    commiting_data += (("AdapterUniqKvData", dataForContainer.toArray))

    dataForContainer.clear
    modelsResult.foreach(v1 => {
      _modelsResult(v1._1) = v1._2
      dataForContainer += ((v1._1, Value("kryo", _kryoSer.SerializeObjectToByteArray(v1._2))))
    })
    commiting_data += (("ModelResults", dataForContainer.toArray))

    /*
    dataForContainer.clear
    if (adapterUniqKeyValData.size > 0) {
      adapterUniqKeyValData.foreach(v1 => {
        if (v1._2._3 != null && v1._2._3.size > 0) { // If we have output then only commit this, otherwise ignore 

          val json = ("T" -> v1._2._1) ~
            ("V" -> v1._2._2) ~
            ("Qs" -> v1._2._3.map(qsres => { qsres._1 })) ~
            ("Res" -> v1._2._3.map(qsres => { qsres._2 }))
          val compjson = compact(render(json))
          dataForContainer += ((Key(KvBaseDefalts.defaultTime, Array(v1._1), 0, 0), Value("json", compjson.getBytes("UTF8"))))
        }
      })
      commiting_data += (("UK", dataForContainer.toArray))
    }
*/

    dos.close()
    bos.close()

    try {
      logger.debug("Going to commit data into datastore.")
      commiting_data.foreach(cd => {
        cd._2.foreach(kv => {
          logger.debug("ObjKey:(%d, %s, %d, %s), Value Info:(Ser:%s, Size:%d)".format(kv._1.timePartition.getTime(), kv._1.bucketKey.mkString(","), kv._1.transactionId, kv._1.rowId), kv._2.serializerType, kv._2.serializedInfo.size)
        })
      })
      _defaultDataStore.put(commiting_data.toArray)
    } catch {
      case e: Exception => {
        logger.error("Failed to write data")
        throw e
      }
    }

    // Remove the current transaction
    removeTransactionContext(transId)
  }

  // Set Reload Flag
  override def setReloadFlag(transId: Long, containerName: String): Unit = {
    // BUGBUG:: Set Reload Flag
    val txnCtxt = getTransactionContext(transId, true)
    if (txnCtxt != null) {
      txnCtxt.setReloadFlag(containerName)
    }
  }

  // Clear Intermediate results before Restart processing
  //BUGBUG:: May be we need to lock before we do anything here
  override def clearIntermediateResults: Unit = {
    _messagesOrContainers.foreach(v => {
      v._2.data.clear
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
      val msgCont = _messagesOrContainers.getOrElse(mc.trim.toLowerCase, null)
      if (msgCont != null && msgCont.data != null) {
        msgCont.data.clear
      }
    })
  }

  // Get Status information from Final table
  override def getAllAdapterUniqKvDataInfo(keys: Array[String]): Array[(String, (Long, String, List[(String, String)]))] = {
    val results = new ArrayBuffer[(String, (Long, String, List[(String, String)]))]()

    val buildAdapOne = (k: Key, v: Value) => {
      buildAdapterUniqueValue(k, v, results)
    }

    _defaultDataStore.get("AdapterUniqKvData", Array(TimeRange(KvBaseDefalts.defaultTime, KvBaseDefalts.defaultTime)), keys.map(k => Array(k)), buildAdapOne)

    logger.debug("Loaded %d committing informations".format(results.size))
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
    val ukvs = validateUniqVals.map(kv => {
      (Key(KvBaseDefalts.defaultTime, Array(kv._1), 0L, 0), Value("", kv._2.getBytes("UTF8")))
    })

    _defaultDataStore.put(Array(("ValidateAdapterPartitionInfo", ukvs)))
  }

  private def buildValidateAdapInfo(k: Key, v: Value, results: ArrayBuffer[(String, String)]): Unit = {
    results += ((k.bucketKey(0), new String(v.serializedInfo)))
  }

  override def GetValidateAdapterInformation: Array[(String, String)] = {
    logger.debug(s"GetValidateAdapterInformation() -- Entered")
    val results = ArrayBuffer[(String, String)]()
    val collectorValidateAdapInfo = (k: Key, v: Value) => {
      buildValidateAdapInfo(k, v, results)
    }
    _defaultDataStore.get("CheckPointInformation", Array(TimeRange(KvBaseDefalts.defaultTime, KvBaseDefalts.defaultTime)), collectorValidateAdapInfo)
    logger.debug("Loaded %d Validate (Check Point) Adapter Information".format(results.size))
    results.toArray
  }

  override def ReloadKeys(tempTransId: Long, containerName: String, keys: List[Key]): Unit = {
    val container = _messagesOrContainers.getOrElse(containerName.toLowerCase, null)
    if (container != null) {
      val dataKeys = keys.map(partKey => {
        KamanjaDataKey(container.objFullName, partKey, List[Int](), 0)
      }).toArray
      loadMap(dataKeys, container)

      // _defaultDataStore.get(containerName, keys, callbackFunction: (Key, Value) => Unit)

      /*
      keys.foreach(partKey => {
        // Loading full Partition key for now.
        loadObjFromDb(tempTransId, container, partKey)
      })
      */
    }
  }

  private def getLocalRecent(transId: Long, containerName: String, partKey: List[String], tmRange: TimeRange, f: MessageContainerBase => Boolean): Option[MessageContainerBase] = {
    if (TxnContextCommonFunctions.IsEmptyKey(partKey))
      None
    val txnCtxt = getTransactionContext(transId, false)
    if (txnCtxt != null) {
      val (v, foundPartKey) = txnCtxt.getRecent(containerName, partKey, tmRange, f)
      if (foundPartKey) {
        return Some(v)
      }
      if (v != null) return Some(v) // It must be null. Without finding partition key it should not find the primary key
    }
    val container = _messagesOrContainers.getOrElse(containerName.toLowerCase, null)

    val (v, foundPartKey) = TxnContextCommonFunctions.getRecent(container, partKey, tmRange, f)

    if (foundPartKey)
      return Some(v)

    if (container != null) { // Loading for partition id
      // If not found in memory, try in DB
      val loadedFfData = loadObjFromDb(transId, container, partKey)
      if (loadedFfData != null) {
        val (v1, foundPartKey1) = TxnContextCommonFunctions.getRecentFromKamanjaData(loadedFfData, tmRange, f)
        if (foundPartKey1)
          return Some(v1)
      }
      return None // If partition key exists, tried DB also, no need to go down
    }

    None
  }

  override def getRecent(transId: Long, containerName: String, partKey: List[String], tmRange: TimeRange, f: MessageContainerBase => Boolean): Option[MessageContainerBase] = {
    Clone(getLocalRecent(transId, containerName, partKey, tmRange, f))
  }

  override def getRDD(transId: Long, containerName: String, partKey: List[String], tmRange: TimeRange, f: MessageContainerBase => Boolean): Array[MessageContainerBase] = {
    Clone(getLocalRDD(transId, containerName, partKey, tmRange, f))
  }

  private def getLocalRDD(transId: Long, containerName: String, partKey: List[String], tmRange: TimeRange, f: MessageContainerBase => Boolean): Array[MessageContainerBase] = {
    val foundPartKeys = ArrayBuffer[List[String]]()
    val retResult = ArrayBuffer[MessageContainerBase]()
    val txnCtxt = getTransactionContext(transId, false)
    if (txnCtxt != null) {
      val (res, foundPartKeys1) = txnCtxt.getRddData(containerName, partKey, tmRange, f, foundPartKeys.toArray)
      if (foundPartKeys1.size > 0) {
        foundPartKeys ++= foundPartKeys1
        retResult ++= res
        if (TxnContextCommonFunctions.IsEmptyKey(partKey) == false) // Already found the key, no need to go down
          return retResult.toArray
      }
    }

    val container = _messagesOrContainers.getOrElse(containerName.toLowerCase, null)
    // In memory
    if (container != null) {
      val (res1, foundPartKeys2) = TxnContextCommonFunctions.getRddData(container, partKey, tmRange, f, foundPartKeys.toArray)
      if (foundPartKeys2.size > 0) {
        foundPartKeys ++= foundPartKeys2
        retResult ++= res1 // Add only if we find all here
        if (TxnContextCommonFunctions.IsEmptyKey(partKey) == false) // Already found the key, no need to go down
          return retResult.toArray
      }
    }

    if (container != null && TxnContextCommonFunctions.IsEmptyKey(partKey) == false) { // Loading for partition id
      // If not found in memory, try in DB
      val loadedFfData = loadObjFromDb(transId, container, partKey)
      if (loadedFfData != null) {
        val res2 = TxnContextCommonFunctions.getRddDataFromKamanjaData(loadedFfData, tmRange, f)
        retResult ++= res2
      }
      return retResult.toArray
    }

    if (container != null) {
      if (container.loadedAll) {
        // Nothing to be loaded from database
      } else {
        // Need to get all keys for this message/container and take all the data 
        val all_keys = ArrayBuffer[KamanjaDataKey]() // All keys for all tables for now
        val keyCollector = (key: Key) => { collectKey(key, all_keys) }
        _allDataDataStore.getAllKeys(keyCollector)
        val keys = getTableKeys(all_keys, containerName.toLowerCase)
        if (keys.size > 0) {
          var objs: Array[KamanjaData] = new Array[KamanjaData](1)
          val buildOne = (tupleBytes: Value) => { buildObject(tupleBytes, objs, container.containerType) }
          val alreadyFoundPartKeys = foundPartKeys.toArray // No need to add to this list anymore. This is the final place we used this to check
          keys.foreach(key => {
            if (TxnContextCommonFunctions.IsKeyExists(alreadyFoundPartKeys, key.K) == false) {
              val StartDateRange = if (key.D.size == 2) key.D(0) else 0
              val EndDateRange = if (key.D.size == 2) key.D(1) else 0
              objs(0) = null
              try {
                _allDataDataStore.get(makeKey(KamanjaData.PrepareKey(key.T, key.K, StartDateRange, EndDateRange)), buildOne)

              } catch {
                case e: Exception => {
                  val stackTrace = StackTrace.ThrowableTraceString(e)
                  logger.debug("\nStackTrace:" + stackTrace)
                }
                case t: Throwable => {
                  val stackTrace = StackTrace.ThrowableTraceString(t)
                  logger.debug("\nStackTrace:" + stackTrace)
                }
              }
              if (objs(0) != null) {
                retResult ++= TxnContextCommonFunctions.getRddDataFromKamanjaData(objs(0), tmRange, f)
              }
            }
          })
        }
      }
    }

    return retResult.toArray
  }

  override def saveOne(transId: Long, containerName: String, partKey: List[String], value: MessageContainerBase): Unit = {
    if (value != null)
      localSetObject(transId, containerName, partKey, value)
  }

  override def saveRDD(transId: Long, containerName: String, values: Array[MessageContainerBase]): Unit = {
    if (values == null)
      return
    //BUGBUG:: For now we are looping thru and saving on Partition key
    values.foreach(v => {
      localSetObject(transId, containerName, v.PartitionKeyData.toList, v)
    })
  }
}