
package com.ligadata.OnLEPManager

import com.ligadata.OnLEPBase._

import com.ligadata.olep.metadata.{ BaseElem, MappedMsgTypeDef, BaseAttributeDef, StructTypeDef, EntityType, AttributeDef, ArrayBufTypeDef, MessageDef, ContainerDef, ModelDef }
import com.ligadata.olep.metadata._
import com.ligadata.olep.metadata.MdMgr._

import com.ligadata.olep.metadataload.MetadataLoad
import scala.collection.mutable.TreeSet
import scala.util.control.Breaks._
import com.ligadata.OnLEPBase.{ MdlInfo, MessageContainerObjBase, BaseMsgObj, BaseContainer, ModelBaseObj, TransformMessage, EnvContext }
import scala.collection.mutable.HashMap
import org.apache.log4j.Logger
import scala.collection.mutable.ArrayBuffer
import com.ligadata.Serialize._
import com.ligadata.ZooKeeper._

object OnLEPLeader {
  private[this] val LOG = Logger.getLogger(getClass);
  private[this] val lock = new Object()
  private[this] var clusterStatus = ClusterStatus("", false, "", null)
  private[this] var zkLeaderLatch: ZkLeaderLatch = _
  private[this] var nodeId: String = _
  private[this] var zkConnectString: String = _
  private[this] var engineLeaderZkNodePath: String = _
  private[this] var engineDistributionZkNodePath: String = _
  private[this] var adaptersStatusPath: String = _
  private[this] var zkSessionTimeoutMs: Int = _
  private[this] var zkConnectionTimeoutMs: Int = _
  private[this] var zkEngineDistributionNodeListener: ZooKeeperListener = _
  private[this] var zkAdapterStatusNodeListener: ZooKeeperListener = _

  private def UpdatePartitionsIfNeededOnLeader(cs: ClusterStatus): Unit = lock.synchronized {
    if (cs.leader != nodeId) return // This is not leader, just return from here. This is same as (cs.leader != cs.nodeId)
    // Update New partitions for all nodes and Set the text 
  }

  // Here Leader can change or Participants can change
  private def EventChangeCallback(cs: ClusterStatus): Unit = {
    clusterStatus = cs

    if (cs.leader == cs.nodeId) // Leader node
      UpdatePartitionsIfNeededOnLeader(cs)

    val isLeader = if (cs.isLeader) "true" else "false"
    LOG.info("NodeId:%s, IsLeader:%s, Leader:%s, AllParticipents:{%s}".format(cs.nodeId, isLeader, cs.leader, cs.participants.mkString(",")))
  }

  private def RestartInputAdapters(receivedJsonStr: String): Unit = {
    if (receivedJsonStr == null || receivedJsonStr.size == 0) {
      // nothing to do
      return
    }
    // Stop processing

    // Update New partitions

    // Wait for Few seconds

    // Start Processing
  }

  def Init(nodeId1: String, zkConnectString1: String, engineLeaderZkNodePath1: String, engineDistributionZkNodePath1: String, adaptersStatusPath1: String, zkSessionTimeoutMs1: Int, zkConnectionTimeoutMs1: Int): Unit = {
    nodeId = nodeId1.toLowerCase
    zkConnectString = zkConnectString1
    engineLeaderZkNodePath = engineLeaderZkNodePath1
    engineDistributionZkNodePath = engineDistributionZkNodePath1
    adaptersStatusPath = adaptersStatusPath1
    zkSessionTimeoutMs = zkSessionTimeoutMs1
    zkConnectionTimeoutMs = zkConnectionTimeoutMs1

    if (zkConnectString != null && zkConnectString.isEmpty() == false && engineLeaderZkNodePath != null && engineLeaderZkNodePath.isEmpty() == false && engineDistributionZkNodePath != null && engineDistributionZkNodePath.isEmpty() == false) {
      try {
        val adaptrStatusPathForNode = adaptersStatusPath + "/" + nodeId
        CreateClient.CreateNodeIfNotExists(zkConnectString, engineDistributionZkNodePath) // Creating 
        CreateClient.CreateNodeIfNotExists(zkConnectString, adaptrStatusPathForNode) // Creating path for Adapter Statues
        zkEngineDistributionNodeListener = new ZooKeeperListener
        zkEngineDistributionNodeListener.CreateListener(zkConnectString, engineDistributionZkNodePath, RestartInputAdapters, zkSessionTimeoutMs, zkConnectionTimeoutMs)
        zkAdapterStatusNodeListener =  new ZooKeeperListener
        // zkAdapterStatusNodeListener.CreatePathChildrenCacheListener(zkConnectString, adaptrStatusPathForNode, RestartInputAdapters, zkSessionTimeoutMs, zkConnectionTimeoutMs)
        zkLeaderLatch = new ZkLeaderLatch(zkConnectString, engineLeaderZkNodePath, nodeId, EventChangeCallback, zkSessionTimeoutMs, zkConnectionTimeoutMs)
        zkLeaderLatch.SelectLeader
      } catch {
        case e: Exception => {
          LOG.error("Failed to initialize ZooKeeper Connection." + e.getMessage)
          throw e
        }
      }
    } else {
      LOG.error("Not connected to elect Leader and not distributing data between nodes.")
    }
  }

  def Shutdown: Unit = {
    if (zkEngineDistributionNodeListener != null)
      zkEngineDistributionNodeListener.Shutdown
    zkEngineDistributionNodeListener = null
    if (zkAdapterStatusNodeListener != null)
      zkAdapterStatusNodeListener.Shutdown
    zkAdapterStatusNodeListener = null
  }
}


