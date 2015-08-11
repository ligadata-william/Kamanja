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

object NodeLevelTransService {
  private[this] val LOG = Logger.getLogger(getClass);
  private[this] var startTxnRangeIdx: Long = 1
  private[this] var endTxnRangeIdx: Long = 0
  private[this] val _lock = new Object
  private[this] var zkcForGetRange: CuratorFramework = null
  private[this] var zkConnectString: String = ""
  private[this] var zkSessionTimeoutMs: Int = 0
  private[this] var zkConnectionTimeoutMs: Int = 0
  private[this] var zkDistributeLockPath: String = ""
  private[this] var txnIdsRangeForNode: Int = 0

  def init(zkConnectString1: String, zkSessionTimeoutMs1: Int, zkConnectionTimeoutMs1: Int, zkDistributeLockPath1: String, txnIdsRangeForNode1: Int): Unit = {
    zkConnectString = zkConnectString1
    zkSessionTimeoutMs = zkSessionTimeoutMs1
    zkConnectionTimeoutMs = zkConnectionTimeoutMs1
    zkDistributeLockPath = zkDistributeLockPath1
    txnIdsRangeForNode = txnIdsRangeForNode1
  }

  def getNextTransRange(envCtxt: EnvContext, requestedPartitionRange: Int): (Long, Long) = _lock.synchronized {
    LOG.info("Start Requesting another Range %d for Partition".format(requestedPartitionRange))
    if (startTxnRangeIdx > endTxnRangeIdx) {
      LOG.info("Start Requesting another Range %d for Node from Storage".format(requestedPartitionRange))
      // Do Distributed zookeeper lock here
      if (zkcForGetRange == null) {
        CreateClient.CreateNodeIfNotExists(zkConnectString, zkDistributeLockPath) // Creating 
        zkcForGetRange = CreateClient.createSimple(zkConnectString, zkSessionTimeoutMs, zkConnectionTimeoutMs)
      }
      if (zkcForGetRange == null)
        throw new Exception("Failed to connect to Zookeeper with connection string:" + zkConnectString)
      val lockPath = zkDistributeLockPath

      try {
        val lock = new InterProcessMutex(zkcForGetRange, lockPath);
        try {
          lock.acquire();
          val (startRng, endRng) = envCtxt.getNextTransactionRange(txnIdsRangeForNode)
          startTxnRangeIdx = startRng
          endTxnRangeIdx = endRng
        } catch {
          case e: Exception => throw e
        } finally {
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
  }

}

class SimpleTransService {
  private[this] val LOG = Logger.getLogger(getClass);
  private[this] var startTxnRangeIdx: Long = 1
  private[this] var endTxnRangeIdx: Long = 0
  private[this] var txnIdsRangeForPartition: Int = 0

  def init(txnIdsRangeForPartition1: Int): Unit = {
    txnIdsRangeForPartition = txnIdsRangeForPartition1
  }

  def getNextTransId(envCtxt: EnvContext): Long = {
    if (startTxnRangeIdx > endTxnRangeIdx) {
      val (startRng, endRng) = NodeLevelTransService.getNextTransRange(envCtxt, txnIdsRangeForPartition)
      startTxnRangeIdx = startRng
      endTxnRangeIdx = endRng
    }
    val retval = startTxnRangeIdx
    startTxnRangeIdx += 1
    retval
  }
}
