package com.ligadata.metadataapiservice

import com.ligadata.MetadataAPI._
import com.ligadata.olep.metadata._
import com.ligadata.olep.metadata.MdMgr._

import com.ligadata.Serialize._
import com.ligadata.ZooKeeper._
import org.apache.curator.framework._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.log4j._

object MetadataAPIServiceListener {
  private[this] val LOG = Logger.getLogger(getClass);
  private[this] val lock = new Object()
  private[this] var zkListener: ZooKeeperListener = _

  // Assuming mdMgr is locked at this moment for not to update while doing this operation
  def UpdateMetadata(receivedJsonStr: String): Unit = {
    try{
      LOG.debug("Process ZooKeeper notification " + receivedJsonStr)
      
      // Assuming Only LeaderNode process API requests and post the changes the zookeeper
      if ( MetadataAPIServiceLeader.IsLeader ){
	LOG.info("Leader posts zookeeper messages after updating MdMgr, no need to update MdMgr")
	return
      }

      if (receivedJsonStr == null || receivedJsonStr.size == 0) {
	LOG.info("Unexpected null message from zookeeper")
	return
      }

      val zkTransaction = JsonSerializer.parseZkTransaction(receivedJsonStr, "JSON")
      if (zkTransaction == null || zkTransaction.Notifications.size == 0) {
	LOG.info("Failed to parse the json message from zookeeper")
	return
      }
      MetadataAPIImpl.UpdateMdMgr(zkTransaction)
    } catch {
      case e: Exception => {
        LOG.error("Failed to initialize MdMgr Based on Zookeeper notification. Reason:%s Message:%s".format(e.getCause, e.getMessage))
        throw e
      }
    }
  }
  
  def Init(nodeId: String,zkConnStr:String,znodePath:String,sessTimeOut: Int,conTimeOut: Int): Unit = {
    try {
      CreateClient.CreateNodeIfNotExists(zkConnStr, znodePath)
      zkListener = new ZooKeeperListener
      zkListener.CreateListener(zkConnStr, znodePath, UpdateMetadata, sessTimeOut, conTimeOut)
      LOG.info("ZooKeeperListener started (" + zkConnStr + "," + znodePath + ")")
    } catch {
      case e: Exception => {
        LOG.error("Failed to initialize ZooKeeper Connection. Reason:%s Message:%s".format(e.getCause, e.getMessage))
        throw e
      }
    }
  }

  def Shutdown: Unit = {
    if (zkListener != null)
      zkListener.Shutdown
    zkListener = null
  }

}

