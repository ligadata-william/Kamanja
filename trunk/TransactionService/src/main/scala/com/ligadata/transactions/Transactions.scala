package com.ligadata.transactions

import com.ligadata.KamanjaBase.{ EnvContext }

import org.apache.log4j.Logger
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import scala.collection.mutable.ArrayBuffer

import com.ligadata.ZooKeeper._
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.locks.InterProcessMutex
import com.ligadata.StorageBase.{ DataStore, Transaction, IStorage, Key, Value, StorageAdapterObj }
import com.ligadata.keyvaluestore.KeyValueManager
import com.ligadata.KamanjaData.{ KamanjaData }

object NodeLevelTransService {
  private[this] val LOG = Logger.getLogger(getClass);
  private[this] var startTxnRangeIdx: Long = 1
  private[this] var endTxnRangeIdx: Long = 0
  private[this] val _lock = new Object
  private[this] var zkcForGetRange: CuratorFramework = null
  private[this] var zkConnectString: String = ""
  private[this] var zkSessionTimeoutMs: Int = 0
  private[this] var zkConnectionTimeoutMs: Int = 0
  private[this] var zkDistributeLockBasePath: String = ""
  private[this] var txnIdsRangeForNode: Int = 0
  private[this] var dataDataStoreInfo: String = ""
  private[this] var initialized: Boolean = false
  private[this] var txnsDataStore: DataStore = null
  private[this] var jarPaths: collection.immutable.Set[String] = null
  private[this] var _serInfoBufBytes = 32

  private def GetDataStoreHandle(jarPaths: collection.immutable.Set[String], dataStoreInfo: String, tableName: String): DataStore = {
    try {
      LOG.debug("Getting DB Connection for dataStoreInfo:%s, tableName:%s".format(dataStoreInfo, tableName))
      return KeyValueManager.Get(jarPaths, dataStoreInfo, tableName)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw new Exception(e.getMessage())
      }
    }
  }

  private def getValueInfo(tupleBytes: Value): Array[Byte] = {
    if (tupleBytes.size < _serInfoBufBytes) return null
    val valInfoBytes = new Array[Byte](tupleBytes.size - _serInfoBufBytes)
    Array.copy(tupleBytes.toArray, _serInfoBufBytes, valInfoBytes, 0, tupleBytes.size - _serInfoBufBytes)
    valInfoBytes
  }

  private def makeKey(key: String): Key = {
    var k = new Key
    k ++= key.getBytes("UTF8")
    k
  }

  private def makeValue(value: Array[Byte], serializerInfo: String): Value = {
    var v = new Value
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

  private def buildTxnStartOffset(tupleBytes: Value, objs: Array[Long]) {
    // Get first _serInfoBufBytes bytes
    if (tupleBytes.size < _serInfoBufBytes) {
      val errMsg = s"Invalid input. This has only ${tupleBytes.size} bytes data. But we are expecting serializer buffer bytes as of size ${_serInfoBufBytes}"
      LOG.error(errMsg)
      throw new Exception(errMsg)
    }

    val valInfo = getValueInfo(tupleBytes)

    val uniqVal = new String(valInfo).toLong

    objs(0) = uniqVal
  }

  // We are not locking anything here. Just pulling from db and returning as requested. 
  private def getNextTransactionRange(requestedRange: Int): (Long, Long) = {
    if (requestedRange <= 0) {
      return (0, -1)
    }

    var startTxnIdx: Long = 0
    var endTxnIdx: Long = -1
    var objs: Array[Long] = new Array[Long](1)
    try {
      if (txnsDataStore == null) {
        throw new Exception("Not found Status DataStore to save Status.")
      }

      val buildAdapOne = (tupleBytes: Value) => {
        buildTxnStartOffset(tupleBytes, objs)
      }

      val keystr = KamanjaData.PrepareKey("Txns", List[String]("Transactions"), 0, 0)
      val key = makeKey(keystr)

      try {
        objs(0) = 0
        txnsDataStore.get(key, buildAdapOne)
        startTxnIdx = objs(0)
      } catch {
        case e: Exception => LOG.debug("Key %s not found. Reason:%s, Message:%s".format(keystr, e.getCause, e.getMessage))
        case t: Throwable => LOG.debug("Key %s not found. Reason:%s, Message:%s".format(keystr, t.getCause, t.getMessage))
      }

      if (startTxnIdx <= 0)
        startTxnIdx = 1
      endTxnIdx = startTxnIdx + requestedRange - 1

      // Persists next start transactionId
      val storeObjects = new Array[IStorage](1)
      var cntr = 0
      val nextTxnStart = endTxnIdx + 1

      object obj extends IStorage {
        val k = key

        val v = makeValue(nextTxnStart.toString.getBytes("UTF8"), "CSV")

        def Key = k

        def Value = v

        def Construct(Key: Key, Value: Value) = {}
      }
      storeObjects(cntr) = obj
      cntr += 1

      val txn = txnsDataStore.beginTx()
      txnsDataStore.putBatch(storeObjects)
      txnsDataStore.commitTx(txn)
    } catch {
      case e: Exception => {
        LOG.debug(s"getNextTransactionRange() -- Unable to get Next Transaction Range")
      }
    }
    (startTxnIdx, endTxnIdx)
  }

  def init(zkConnectString1: String, zkSessionTimeoutMs1: Int, zkConnectionTimeoutMs1: Int, zkDistributeLockBasePath1: String, txnIdsRangeForNode1: Int, dataDataStoreInfo1: String, jarPaths1: collection.immutable.Set[String]): Unit = {
    zkConnectString = zkConnectString1
    zkSessionTimeoutMs = zkSessionTimeoutMs1
    zkConnectionTimeoutMs = zkConnectionTimeoutMs1
    zkDistributeLockBasePath = zkDistributeLockBasePath1
    txnIdsRangeForNode = txnIdsRangeForNode1
    dataDataStoreInfo = dataDataStoreInfo1
    jarPaths = jarPaths1
    initialized = true
  }

  def getNextTransRange(requestedPartitionRange: Int): (Long, Long) = _lock.synchronized {
    if (requestedPartitionRange <= 0) {
      return (0, -1)
    }
    LOG.info("Start Requesting another Range %d for Partition".format(requestedPartitionRange))
    if (startTxnRangeIdx > endTxnRangeIdx) {
      if (initialized == false) {
        throw new Exception("Transaction Service is not yet initialized")
      }
      LOG.info("Start Requesting another Range %d for Node from Storage".format(requestedPartitionRange))

      // Connect to DB if not connected already
      if (txnsDataStore == null) {
        LOG.debug("Transaction DB Info:" + dataDataStoreInfo)
        txnsDataStore = GetDataStoreHandle(jarPaths, dataDataStoreInfo, "ClusterCounts")
        if (txnsDataStore == null)
          throw new Exception("Unable to connect to DataStore with " + dataDataStoreInfo)
      }

      // Do Distributed zookeeper lock here
      if (zkcForGetRange == null) {
        CreateClient.CreateNodeIfNotExists(zkConnectString, zkDistributeLockBasePath) // Creating 
        zkcForGetRange = CreateClient.createSimple(zkConnectString, zkSessionTimeoutMs, zkConnectionTimeoutMs)
      }
      if (zkcForGetRange == null)
        throw new Exception("Failed to connect to Zookeeper with connection string:" + zkConnectString)
      val lockPath = zkDistributeLockBasePath + "/distributed-transaction-lock"

      try {
        val lock = new InterProcessMutex(zkcForGetRange, lockPath);
        try {
          lock.acquire();
          val (startRng, endRng) = getNextTransactionRange(txnIdsRangeForNode)
          startTxnRangeIdx = startRng
          endTxnRangeIdx = endRng
        } catch {
          case e: Exception => throw e
        } finally {
          if (lock != null)
            lock.release();
        }

      } catch {
        case e: Exception => throw e
      } finally {
      }

      LOG.info("Done Requesting another Range %d for Node from Storage".format(requestedPartitionRange))
    }
    val retStartIdx = startTxnRangeIdx
    val retMaxEndIdx = startTxnRangeIdx + requestedPartitionRange - 1
    val retEndIdx = if (retMaxEndIdx > endTxnRangeIdx) endTxnRangeIdx else retMaxEndIdx
    startTxnRangeIdx = retEndIdx + 1
    LOG.info("Done Requesting another Range %d for Partition".format(requestedPartitionRange))
    return (retStartIdx, retEndIdx)
  }

  def Shutdown = _lock.synchronized {
    if (zkcForGetRange != null)
      zkcForGetRange.close()
    zkcForGetRange = null
    if (txnsDataStore != null)
      txnsDataStore.Shutdown
    txnsDataStore = null
  }

}

class SimpleTransService {
  private[this] val LOG = Logger.getLogger(getClass);
  private[this] var startTxnRangeIdx: Long = 1
  private[this] var endTxnRangeIdx: Long = 0
  private[this] var txnIdsRangeForPartition: Int = 1

  def init(txnIdsRangeForPartition1: Int): Unit = {
    txnIdsRangeForPartition = txnIdsRangeForPartition1
  }

  def getNextTransId: Long = {
    if (startTxnRangeIdx > endTxnRangeIdx) {
      val (startRng, endRng) = NodeLevelTransService.getNextTransRange(txnIdsRangeForPartition)
      startTxnRangeIdx = startRng
      endTxnRangeIdx = endRng
    }
    val retval = startTxnRangeIdx
    startTxnRangeIdx += 1
    retval
  }
}
