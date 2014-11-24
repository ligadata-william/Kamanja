
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
import org.apache.curator.framework._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.apache.curator.utils.ZKPaths

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
  private[this] var zkcForSetData: CuratorFramework = null
  private[this] var distributionMap = scala.collection.mutable.Map[String, ArrayBuffer[PartitionUniqueRecordKey]]()
  private[this] var nodesStatus = scala.collection.mutable.Map[String, String]() // NodeId & actionId
  private[this] var curNodesAction: String = _

  private def UpdatePartitionsNodeData(eventType: String, eventPath: String, eventPathData: Array[Byte]): Unit = lock.synchronized {
    println("Got Event" + eventType)
    /*
    val participants = if (clusterStatus.participants != null) clusterStatus.participants.toSet else Set[String]()

    if (childs != null) {
      val allNodesNameAndData = childs.map(c => ((ZKPaths.getNodeFromPath(c._1), c._2)))
      val validNodesNameAndData = allNodesNameAndData.filter(nodeInfo => participants(nodeInfo._1))
    }
*/
    // BUGBUG:: On Leader node, Go thru all active Participent nodes data and see whether it is stopped or not before we send "distribute" action 
    // If we get redistribute from any node, just go and redistribute again
    // UpdatePartitionsIfNeededOnLeader(clusterStatus)

  }

  private def UpdatePartitionsIfNeededOnLeader(cs: ClusterStatus): Unit = lock.synchronized {
    if (cs.isLeader && cs.leader != nodeId) return // This is not leader, just return from here. This is same as (cs.leader != cs.nodeId)

    // Clear Previous Distribution Map
    distributionMap.clear

    var tmpDistMap = ArrayBuffer[(String, ArrayBuffer[PartitionUniqueRecordKey])]()

    if (cs.participants != null) {
      // Create ArrayBuffer for each node participating at this moment
      cs.participants.foreach(p => {
        tmpDistMap += ((p, new ArrayBuffer[PartitionUniqueRecordKey]))
      })

      // BUGBUG:: Get all PartitionUniqueRecordKey for all Input Adapters
      val allPartitionUniqueRecordKeys = Array[PartitionUniqueRecordKey]()

      // Update New partitions for all nodes and Set the text
      var cntr: Int = 0
      val totalParticipents: Int = cs.participants.size
      if (allPartitionUniqueRecordKeys != null) {
        allPartitionUniqueRecordKeys.foreach(k => {
          tmpDistMap(cntr % totalParticipents)._2 += k
          cntr += 1
        })
      }

      tmpDistMap.foreach(tup => {
        distributionMap(tup._1) = tup._2
      })
    }

    curNodesAction = "stop"
    // Set STOP Action on engineDistributionZkNodePath
    zkcForSetData.setData().forPath(engineDistributionZkNodePath, "{ \"action\": \"stop\" }".getBytes("UTF8"))
  }

  // Here Leader can change or Participants can change
  private def EventChangeCallback(cs: ClusterStatus): Unit = {
    clusterStatus = cs

    if (cs.isLeader && cs.leader == cs.nodeId) // Leader node
      UpdatePartitionsIfNeededOnLeader(cs)

    val isLeader = if (cs.isLeader) "true" else "false"
    LOG.info("NodeId:%s, IsLeader:%s, Leader:%s, AllParticipents:{%s}".format(cs.nodeId, isLeader, cs.leader, cs.participants.mkString(",")))
  }

  private def ActionOnAdaptersDistribution(receivedJsonStr: String): Unit = {
    if (receivedJsonStr == null || receivedJsonStr.size == 0) {
      // nothing to do
      return
    }

    try {
      // Perform the action here (STOP or DISTRIBUTE for now)
      val json = parse(receivedJsonStr)
      if (json == null || json.values != null) // Not doing any action if not found valid json
        return
      val values = json.values.asInstanceOf[Map[String, Any]]
      val action = values.getOrElse("action", "").toString.toLowerCase

      action match {
        case "stop" => {
          // BUGBUG:: STOP all Input Adapters on local node

          // Set STOPPED action in adaptersStatusPath + "/" + nodeId path
          val adaptrStatusPathForNode = adaptersStatusPath + "/" + nodeId
          zkcForSetData.setData().forPath(adaptrStatusPathForNode, "{ \"action\": \"stopped\" }".getBytes("UTF8"))
        }
        case "distribute" => {
          // get Unique Keys for this nodeId
          val uniqueKeysForNode = values.getOrElse(nodeId, null)
          if (uniqueKeysForNode != null) {
            // BUGBUG:: START all Input Adapters on local node from uniqueKeysForNode (new distribution map) 
          }

          // Set DISTRIBUTED action in adaptersStatusPath + "/" + nodeId path
          val adaptrStatusPathForNode = adaptersStatusPath + "/" + nodeId
          zkcForSetData.setData().forPath(adaptrStatusPathForNode, "{ \"action\": \"distributed\" }".getBytes("UTF8"))
        }
        case _ => {
          LOG.info("No action performed, because of invalid action %s in json %s".format(action, receivedJsonStr))
        }
      }

      // 
    } catch {
      case e: Exception => {
        LOG.info("Found invalid JSON: %s".format(receivedJsonStr))
      }
    }

  }

  private def ParticipentsAdaptersStatus(eventType: String, eventPath: String, eventPathData: Array[Byte], childs: Array[(String, Array[Byte])]): Unit = {
    if (clusterStatus.isLeader == false || clusterStatus.leader != clusterStatus.nodeId) // Not Leader node
      return
    UpdatePartitionsNodeData(eventType, eventPath, eventPathData)
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
        zkcForSetData = CreateClient.createSimple(zkConnectString, zkSessionTimeoutMs, zkConnectionTimeoutMs)
        zkEngineDistributionNodeListener = new ZooKeeperListener
        zkEngineDistributionNodeListener.CreateListener(zkConnectString, engineDistributionZkNodePath, ActionOnAdaptersDistribution, zkSessionTimeoutMs, zkConnectionTimeoutMs)
        zkAdapterStatusNodeListener = new ZooKeeperListener
        zkAdapterStatusNodeListener.CreatePathChildrenCacheListener(zkConnectString, adaptersStatusPath, false, ParticipentsAdaptersStatus, zkSessionTimeoutMs, zkConnectionTimeoutMs)
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
    if (zkLeaderLatch != null)
      zkLeaderLatch.Shutdown
    zkLeaderLatch = null
    if (zkEngineDistributionNodeListener != null)
      zkEngineDistributionNodeListener.Shutdown
    zkEngineDistributionNodeListener = null
    if (zkAdapterStatusNodeListener != null)
      zkAdapterStatusNodeListener.Shutdown
    zkAdapterStatusNodeListener = null
    if (zkcForSetData != null)
      zkcForSetData.close
    zkcForSetData = null
  }
}


