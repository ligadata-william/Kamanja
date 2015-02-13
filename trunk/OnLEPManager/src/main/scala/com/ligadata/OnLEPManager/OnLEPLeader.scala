
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
import scala.actors.threadpool.{ Executors, ExecutorService }

case class AdapMaxPartitions(Adap: String, MaxParts: Int)
case class PartAndTxn(Part: String, Txn: Long)
case class NodeDistMap(Adap: String, Parts: List[PartAndTxn])
case class DistributionMap(Node: String, Adaps: List[NodeDistMap])
case class FoundKeysInValidation(K: String, V1: String, V2: Int, V3: Int, V4: Long)
case class ActionOnAdaptersMap(action: String, adaptermaxpartitions: Option[List[AdapMaxPartitions]], distributionmap: Option[List[DistributionMap]], foundKeysInValidation: Option[List[FoundKeysInValidation]])

object OnLEPLeader {
  private[this] val LOG = Logger.getLogger(getClass);
  private[this] val lock = new Object()
  private[this] val lock1 = new Object()
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
  private[this] var distributionMap = scala.collection.mutable.Map[String, scala.collection.mutable.Map[String, ArrayBuffer[(String, Long)]]]() // Nodeid & Unique Keys (adapter unique name & unique key)
  private[this] var foundKeysInValidation: scala.collection.immutable.Map[String, (String, Int, Int, Long)] = _
  private[this] var adapterMaxPartitions = scala.collection.mutable.Map[String, Int]() // Adapters & Max Partitions
  private[this] var allPartitionsToValidate = scala.collection.mutable.Map[String, Set[String]]()
  private[this] var nodesStatus = scala.collection.mutable.Set[String]() // NodeId
  private[this] var expectedNodesAction: String = _
  private[this] var curParticipents = Set[String]() // Derived from clusterStatus.participants
  private[this] var canRedistribute = false
  private[this] var inputAdapters: ArrayBuffer[InputAdapter] = _
  private[this] var outputAdapters: ArrayBuffer[OutputAdapter] = _
  private[this] var statusAdapters: ArrayBuffer[OutputAdapter] = _
  private[this] var validateInputAdapters: ArrayBuffer[InputAdapter] = _
  private[this] var envCtxt: EnvContext = _
  private[this] var updatePartitionsFlag = false
  private[this] var distributionExecutor = Executors.newFixedThreadPool(1)

  private def SetCanRedistribute(redistFlag: Boolean): Unit = lock.synchronized {
    canRedistribute = redistFlag
  }

  private def UpdatePartitionsNodeData(eventType: String, eventPath: String, eventPathData: Array[Byte]): Unit = lock.synchronized {
    try {
      val evntPthData = if (eventPathData != null) (new String(eventPathData)) else "{}"
      val extractedNode = ZKPaths.getNodeFromPath(eventPath)
      LOG.info("UpdatePartitionsNodeData => eventType: %s, eventPath: %s, eventPathData: %s, Extracted Node:%s".format(eventType, eventPath, evntPthData, extractedNode))

      if (eventType.compareToIgnoreCase("CHILD_UPDATED") == 0) {
        if (curParticipents(extractedNode)) { // If this node is one of the participent, then work on this, otherwise ignore
          try {
            val json = parse(evntPthData)
            if (json == null || json.values == null) // Not doing any action if not found valid json
              return
            val values = json.values.asInstanceOf[Map[String, Any]]
            val action = values.getOrElse("action", "").toString.toLowerCase

            if (expectedNodesAction.compareToIgnoreCase(action) == 0) {
              nodesStatus += extractedNode
              if (nodesStatus.size == curParticipents.size && expectedNodesAction == "stopped" && (nodesStatus -- curParticipents).isEmpty) {
                envCtxt.PersistRemainingStateEntriesOnLeader
                nodesStatus.clear
                expectedNodesAction = "distributed"

                val fndKeyInVal = if (foundKeysInValidation == null) scala.collection.immutable.Map[String, (String, Int, Int, Long)]() else foundKeysInValidation

                // Set DISTRIBUTE Action on engineDistributionZkNodePath
                // Send all Unique keys to corresponding nodes 
                val distribute =
                  ("action" -> "distribute") ~
                    ("adaptermaxpartitions" -> adapterMaxPartitions.map(kv =>
                      ("Adap" -> kv._1) ~
                        ("MaxParts" -> kv._2))) ~
                    ("distributionmap" -> distributionMap.map(kv =>
                      ("Node" -> kv._1) ~
                        ("Adaps" -> kv._2.map(kv1 => ("Adap" -> kv1._1) ~
                          ("Parts" -> kv1._2.toList.map(av => ("Part" -> av._1) ~
                            ("Txn" -> av._2))))))) ~
                    ("foundKeysInValidation" -> fndKeyInVal.map(kv =>
                      ("K" -> kv._1) ~
                        ("V1" -> kv._2._1) ~
                        ("V2" -> kv._2._2) ~
                        ("V3" -> kv._2._3) ~
                        ("V4" -> kv._2._4)))
                val sendJson = compact(render(distribute))
                zkcForSetData.setData().forPath(engineDistributionZkNodePath, sendJson.getBytes("UTF8"))
              }
            } else {
              val redStr = if (canRedistribute) "canRedistribute is true, Redistributing" else "canRedistribute is false, waiting until next call"
              // Got different action. May be re-distribute. For now any non-expected action we will redistribute
              LOG.info("UpdatePartitionsNodeData => eventType: %s, eventPath: %s, eventPathData: %s, Extracted Node:%s. Expected Action:%s, Recieved Action:%s %s.".format(eventType, eventPath, evntPthData, extractedNode, expectedNodesAction, action, redStr))
              if (canRedistribute)
                SetUpdatePartitionsFlag
            }
          } catch {
            case e: Exception => {
              LOG.error("UpdatePartitionsNodeData => Failed eventType: %s, eventPath: %s, eventPathData: %s, Reason:%s, Message:%s".format(eventType, eventPath, evntPthData, e.getCause, e.getMessage))
            }
          }
        }

      } else if (eventType.compareToIgnoreCase("CHILD_REMOVED") == 0) {
        // Not expected this. Need to check what is going on
        LOG.error("UpdatePartitionsNodeData => eventType: %s, eventPath: %s, eventPathData: %s".format(eventType, eventPath, evntPthData))
      } else if (eventType.compareToIgnoreCase("CHILD_ADDED") == 0) {
        // Not doing anything here
      }
    } catch {
      case e: Exception => {
        LOG.error("Exception while UpdatePartitionsNodeData, reason %s, message %s".format(e.getCause, e.getMessage))
      }
    }
  }

  private def UpdatePartitionsIfNeededOnLeader: Unit = lock.synchronized {
    val cs = GetClusterStatus
    if (cs.isLeader == false || cs.leader != cs.nodeId) return // This is not leader, just return from here. This is same as (cs.leader != cs.nodeId)

    LOG.info("Distribution NodeId:%s, IsLeader:%s, Leader:%s, AllParticipents:{%s}".format(cs.nodeId, cs.isLeader.toString, cs.leader, cs.participants.mkString(",")))

    // Clear Previous Distribution Map
    distributionMap.clear
    adapterMaxPartitions.clear
    nodesStatus.clear
    expectedNodesAction = ""
    curParticipents = if (cs.participants != null) cs.participants.toSet else Set[String]()

    try {
      breakable {
        var tmpDistMap = ArrayBuffer[(String, scala.collection.mutable.Map[String, ArrayBuffer[(String, Long)]])]()

        if (cs.participants != null) {

          // Create ArrayBuffer for each node participating at this moment
          cs.participants.foreach(p => {
            tmpDistMap += ((p, scala.collection.mutable.Map[String, ArrayBuffer[(String, Long)]]()))
          })

          val allPartitionUniqueRecordKeys = ArrayBuffer[(String, String)]()

          // Get all PartitionUniqueRecordKey for all Input Adapters
          inputAdapters.foreach(ia => {
            val uk = ia.GetAllPartitionUniqueRecordKey
            val name = ia.UniqueName
            val ukCnt = if (uk != null) uk.size else 0
            adapterMaxPartitions(name) = ukCnt
            if (ukCnt > 0) {
              val serUK = uk.map(k => {
                val kstr = k.Serialize
                LOG.info("Unique Key in %s => %s".format(name, kstr))
                (name, kstr)
              })
              allPartitionUniqueRecordKeys ++= serUK
              AddPartitionsToValidate(name, serUK.map(k => k._2).toSet)
            } else {
              AddPartitionsToValidate(name, Set[String]())
            }
          })

          val savedValidatedAdaptInfo = envCtxt.GetValidateAdapterInformation
          val map = scala.collection.mutable.Map[String, String]()

          LOG.info("savedValidatedAdaptInfo: " + savedValidatedAdaptInfo.mkString(","))

          savedValidatedAdaptInfo.foreach(kv => {
            map(kv._1.toLowerCase) = kv._2
          })

          CollectKeyValsFromValidation.clear
          validateInputAdapters.foreach(via => {
            // Get all Begin values for Unique Keys
            val begVals = via.getAllPartitionBeginValues
            val finalVals = new ArrayBuffer[(PartitionUniqueRecordKey, PartitionUniqueRecordValue, Long, (PartitionUniqueRecordValue, Int, Int))](begVals.size)

            // Replace the newly found ones
            val nParts = begVals.size
            for (i <- 0 until nParts) {
              val key = begVals(i)._1.Serialize.toLowerCase
              val foundVal = map.getOrElse(key, null)
              if (foundVal != null) {
                val desVal = via.DeserializeValue(foundVal)
                finalVals += ((begVals(i)._1, desVal, 0, (desVal, 0, 0)))
              } else {
                finalVals += ((begVals(i)._1, begVals(i)._2, 0, (begVals(i)._2, 0, 0)))
              }
            }
            via.StartProcessing(finalVals.size, finalVals.toArray, false)
          })

          var stillWait = true
          while (stillWait) {
            try {
              Thread.sleep(1000) // sleep 1000 ms and then check
            } catch {
              case e: Exception => {
              }
            }
            if ((System.nanoTime - CollectKeyValsFromValidation.getLastUpdateTime) < 1000 * 1000000) // 1000ms
              stillWait = true
            else
              stillWait = false
          }

          if (distributionExecutor.isShutdown)
            break

          // Stopping the Adapters
          validateInputAdapters.foreach(via => {
            via.StopProcessing
          })

          // Expecting keys are lower case here
          foundKeysInValidation = CollectKeyValsFromValidation.get
          LOG.info("foundKeysInValidation: " + foundKeysInValidation.map(v => { (v._1.toString, v._2.toString) }).mkString(","))

          var maxUsedTxnIdInValidationRes: Long = 0

          foundKeysInValidation.foreach(k => {
            if (maxUsedTxnIdInValidationRes < k._2._4)
              maxUsedTxnIdInValidationRes = k._2._4
          })

          // Update New partitions for all nodes and Set the text
          val totalParticipents: Int = cs.participants.size
          if (allPartitionUniqueRecordKeys != null && allPartitionUniqueRecordKeys.size > 0) {
            LOG.info("allPartitionUniqueRecordKeys: %d".format(allPartitionUniqueRecordKeys.size))
            var cntr: Int = 0
            val txnIdCntrPerPartition: Long = com.ligadata.Utils.Utils.MaxTransactionsPerPartition
            var nextTxnIdCntrIdx: Long = (maxUsedTxnIdInValidationRes / txnIdCntrPerPartition) + 1
            allPartitionUniqueRecordKeys.foreach(k => {
              val fnd = foundKeysInValidation.getOrElse(k._2.toLowerCase, null)
              val startTxnId = if (fnd != null) (fnd._4 + 10) else (nextTxnIdCntrIdx * txnIdCntrPerPartition)
              if (fnd == null)
                nextTxnIdCntrIdx += 1
              val af = tmpDistMap(cntr % totalParticipents)._2.getOrElse(k._1, null)
              if (af == null) {
                val af1 = new ArrayBuffer[(String, Long)]
                af1 += ((k._2, startTxnId))
                tmpDistMap(cntr % totalParticipents)._2(k._1) = af1
              } else {
                af += ((k._2, startTxnId))
              }
              cntr += 1
            })
          }

          tmpDistMap.foreach(tup => {
            distributionMap(tup._1) = tup._2
          })
        }

        expectedNodesAction = "stopped"
        // Set STOP Action on engineDistributionZkNodePath
        val act = ("action" -> "stop")
        val sendJson = compact(render(act))
        zkcForSetData.setData().forPath(engineDistributionZkNodePath, sendJson.getBytes("UTF8"))
      }
    } catch {
      case e: Exception => {}
    }
  }

  private def AddPartitionsToValidate(adapName: String, partitions: Set[String]): Unit = lock1.synchronized {
    allPartitionsToValidate(adapName) = partitions
  }

  private def GetPartitionsToValidate(adapName: String): Set[String] = lock1.synchronized {
    allPartitionsToValidate.getOrElse(adapName, null)
  }

  private def GetAllAdaptersPartitionsToValidate: Map[String, Set[String]] = lock1.synchronized {
    allPartitionsToValidate.toMap
  }

  private def SetClusterStatus(cs: ClusterStatus): Unit = lock1.synchronized {
    clusterStatus = cs
    updatePartitionsFlag = true
  }

  private def GetClusterStatus: ClusterStatus = lock1.synchronized {
    return clusterStatus
  }

  private def IsLeaderNode: Boolean = lock1.synchronized {
    return (clusterStatus.isLeader && clusterStatus.leader == clusterStatus.nodeId)
  }

  private def IsLeaderNodeAndUpdatePartitionsFlagSet: Boolean = lock1.synchronized {
    if (clusterStatus.isLeader && clusterStatus.leader == clusterStatus.nodeId)
      return updatePartitionsFlag
    else
      return false
  }

  private def SetUpdatePartitionsFlag: Unit = lock1.synchronized {
    updatePartitionsFlag = true
  }

  private def GetUpdatePartitionsFlag: Boolean = lock1.synchronized {
    return updatePartitionsFlag
  }

  private def GetUpdatePartitionsFlagAndReset: Boolean = lock1.synchronized {
    val retVal = updatePartitionsFlag
    updatePartitionsFlag = false
    retVal
  }

  // Here Leader can change or Participants can change
  private def EventChangeCallback(cs: ClusterStatus): Unit = {
    LOG.info("EventChangeCallback => Enter")
    SetClusterStatus(cs)
    LOG.info("NodeId:%s, IsLeader:%s, Leader:%s, AllParticipents:{%s}".format(cs.nodeId, cs.isLeader.toString, cs.leader, cs.participants.mkString(",")))
    LOG.info("EventChangeCallback => Exit")
  }

  private def GetUniqueKeyValue(uk: String): (String, Int, Int) = {
    envCtxt.getAdapterUniqueKeyValue(0, uk)
  }

  private def StartNodeKeysMap(nodeKeysMap: scala.collection.immutable.Map[String, Array[(String, Long)]], receivedJsonStr: String, adapMaxPartsMap: Map[String, Int], foundKeysInVald: Map[String, (String, Int, Int, Long)]): Boolean = {
    if (nodeKeysMap == null || nodeKeysMap.size == 0) {
      return true
    }
    inputAdapters.foreach(ia => {
      val name = ia.UniqueName
      try {
        val uAK = nodeKeysMap.getOrElse(name, null)
        if (uAK != null) {
          val uKV = uAK.map(uk => { GetUniqueKeyValue(uk._1) })
          val tmpProcessingKVs = envCtxt.getIntermediateStatusInfo(uAK.map(uk => uk._1).toArray) // Get all Status information from intermediate table.
          val processingKVs = scala.collection.mutable.Map[String, (String, Int, Int)]()
          tmpProcessingKVs.foreach(kv => {
            processingKVs(kv._1.toLowerCase) = kv._2
          })
          val maxParts = adapMaxPartsMap.getOrElse(name, 0)
          LOG.info("On Node %s for Adapter %s with Max Partitions %d UniqueKeys %s, UniqueValues %s".format(nodeId, name, maxParts, uAK.mkString(","), uKV.mkString(",")))

          LOG.info("Deserializing Keys")
          val keys = uAK.map(k => ia.DeserializeKey(k._1))

          LOG.info("Deserializing Values")
          val vals = uKV.map(v => ia.DeserializeValue(if (v != null) v._1 else null))

          var indx = 0
          LOG.info("Deserializing Processing Values")
          val processingvals = uAK.map(uk => {
            val keyVal = uk._1.toLowerCase
            val ptmpV = processingKVs.getOrElse(keyVal, null)
            var pV: PartitionUniqueRecordValue = null
            var processingMsg = 0
            var totalMsgs = 0
            if (ptmpV != null) {
              val doneVal = uKV(indx)
              if (doneVal._1.compareTo(ptmpV._1) == 0) {
                // Already written what ever we were processing. Just take this in the ignored list
                pV = vals(indx)
                // LOG.info("====================================> Key:%s found in process(ed) keys, Value is :%s. ProcessedVal: %s".format(uk._1, pV.Serialize, vals(indx).Serialize))
              } else {
                // Processing some other record than what we persisted.
                val sentOutAdap = foundKeysInVald.getOrElse(keyVal, null)
                if (sentOutAdap != null) {
                  // Found in Sent into Output Adapter
                  processingMsg = sentOutAdap._2
                  totalMsgs = sentOutAdap._3
                  pV = ia.DeserializeValue(sentOutAdap._1)
                  // LOG.info("====================================> Key:%s found in process(ing) keys, found in Sent to Output Adap, Value is :%s (%d, %d). ProcessedVal: %s".format(uk._1, sentOutAdap._1, sentOutAdap._2, sentOutAdap._3, vals(indx).Serialize))
                } else {
                  // If the key is not there in the above adapter, just take same vals object (pV = vals(indx)), otherwise deserialize the current one and ignore till that point. For now we are treating ignore send output unitl we fix reading from OuputAdapter
                  processingMsg = 0 // Need to update it
                  totalMsgs = 0 // Need to update it
                  pV = ia.DeserializeValue(ptmpV._1)
                  // LOG.info("====================================> Key:%s found in process(ing) keys, not found in Sent to Output Adap, Value is :%s. ProcessedVal: %s".format(uk._1, ptmpV._1, vals(indx).Serialize))
                }
              }
            } else { // May not be processed at all before. just take the vals(indx) 
              pV = vals(indx)
              // LOG.info("====================================> Key:%s found in processing keys, Value is :%s. ProcessedVal: %s".format(uk._1, ptmpV._1, vals(indx).Serialize))
            }
            indx += 1
            (pV, processingMsg, totalMsgs)
          })
          LOG.info("Deserializing Keys & Values done")

          val quads = new ArrayBuffer[(PartitionUniqueRecordKey, PartitionUniqueRecordValue, Long, (PartitionUniqueRecordValue, Int, Int))](keys.size)

          for (i <- 0 until keys.size) {
            val key = keys(i)
            quads += ((key, vals(i), uAK(i)._2, processingvals(i)))
          }

          LOG.info(ia.UniqueName + " ==> Processing Keys & values: " + quads.map(q => { (q._1.Serialize, q._2.Serialize, q._3, (q._4._1.Serialize, q._4._2, q._4._2)) }).mkString(","))
          ia.StartProcessing(maxParts, quads.toArray, true)
        }
      } catch {
        case e: Exception => {
          LOG.error("Failed to print final Unique Keys. JsonString:%s, Reason:%s, Message:%s".format(receivedJsonStr, e.getCause, e.getMessage))
        }
      }
    })

    return true
  }

  def GetAdaptersMaxPartitioinsMap(adaptermaxpartitions: Option[List[AdapMaxPartitions]]): Map[String, Int] = {
    val adapterMax = scala.collection.mutable.Map[String, Int]() // Adapters & Max Partitions
    if (adaptermaxpartitions != None && adaptermaxpartitions != null) {
      adaptermaxpartitions.get.foreach(adapMaxParts => {
        adapterMax(adapMaxParts.Adap) = adapMaxParts.MaxParts
      })
    }

    adapterMax.toMap
  }

  private def GetDistMapForNodeId(distributionmap: Option[List[DistributionMap]], nodeId: String): scala.collection.immutable.Map[String, Array[(String, Long)]] = {
    val retVals = scala.collection.mutable.Map[String, Array[(String, Long)]]()
    if (distributionmap != None && distributionmap != null) {
      val nodeDistMap = distributionmap.get.filter(ndistmap => { (ndistmap.Node.compareToIgnoreCase(nodeId) == 0) })
      if (nodeDistMap != None && nodeDistMap != null) { // At most 1 value, but not enforcing
        nodeDistMap.foreach(ndistmap => {
          ndistmap.Adaps.map(ndm => {
            retVals(ndm.Adap) = ndm.Parts.map(vt => (vt.Part, vt.Txn)).toArray
          })
        })
      }
    }

    retVals.toMap
  }

  // Using canRedistribute as startup mechanism here, because until we do bootstap ignore all the messages from this 
  private def ActionOnAdaptersDistImpl(receivedJsonStr: String): Unit = lock.synchronized {
    // LOG.info("ActionOnAdaptersDistImpl => receivedJsonStr: " + receivedJsonStr)

    if (receivedJsonStr == null || receivedJsonStr.size == 0 || canRedistribute == false) {
      // nothing to do
      LOG.info("ActionOnAdaptersDistImpl => Exit. receivedJsonStr: " + receivedJsonStr)
      return
    }

    if (IsLeaderNodeAndUpdatePartitionsFlagSet) {
      LOG.info("Already got Re-distribution request. Ignoring any actions from ActionOnAdaptersDistImpl") // Atleast this happens on main node
      return
    }

    LOG.info("ActionOnAdaptersDistImpl => receivedJsonStr: " + receivedJsonStr)

    try {
      // Perform the action here (STOP or DISTRIBUTE for now)
      val json = parse(receivedJsonStr)
      if (json == null || json.values == null) { // Not doing any action if not found valid json
        LOG.info("ActionOnAdaptersDistImpl => Exit. receivedJsonStr: " + receivedJsonStr)
        return
      }

      implicit val jsonFormats: Formats = DefaultFormats
      val actionOnAdaptersMap = json.extract[ActionOnAdaptersMap]

      actionOnAdaptersMap.action match {
        case "stop" => {
          try {
            breakable {
              // STOP all Input Adapters on local node
              inputAdapters.foreach(ia => {
                ia.StopProcessing
              })

              // Sleep for a sec
              try {
                Thread.sleep(1000)
              } catch {
                case e: Exception => {
                  // Not doing anything
                }
              }

              if (distributionExecutor.isShutdown)
                break

              envCtxt.PersistLocalNodeStateEntries
              envCtxt.clearIntermediateResults

              // Set STOPPED action in adaptersStatusPath + "/" + nodeId path
              val adaptrStatusPathForNode = adaptersStatusPath + "/" + nodeId
              val act = ("action" -> "stopped")
              val sendJson = compact(render(act))
              zkcForSetData.setData().forPath(adaptrStatusPathForNode, sendJson.getBytes("UTF8"))
            }
          } catch {
            case e: Exception => { LOG.error("Failed to get Input Adapters partitions. Reason:%s Message:%s".format(e.getCause, e.getMessage)) }
          }
        }
        case "distribute" => {
          envCtxt.clearIntermediateResults // We may not need it here. But anyway safe side
          var distributed = true
          try {
            // get Unique Keys for this nodeId
            // Distribution Map 
            if (actionOnAdaptersMap.distributionmap != None && actionOnAdaptersMap.distributionmap != null) {
              val adapMaxPartsMap = GetAdaptersMaxPartitioinsMap(actionOnAdaptersMap.adaptermaxpartitions)
              val nodeDistMap = GetDistMapForNodeId(actionOnAdaptersMap.distributionmap, nodeId)

              var foundKeysInVald = scala.collection.mutable.Map[String, (String, Int, Int, Long)]()

              if (actionOnAdaptersMap.foundKeysInValidation != None && actionOnAdaptersMap.foundKeysInValidation != null) {
                actionOnAdaptersMap.foundKeysInValidation.get.foreach(ks => {
                  foundKeysInVald(ks.K.toLowerCase) = (ks.V1, ks.V2, ks.V3, ks.V4)
                })

              }
              StartNodeKeysMap(nodeDistMap, receivedJsonStr, adapMaxPartsMap, foundKeysInVald.toMap)
            }

          } catch {
            case e: Exception => {
              LOG.error("distribute action failed with reason %s, message %s".format(e.getCause, e.getMessage))
              distributed = false
            }
          }

          val adaptrStatusPathForNode = adaptersStatusPath + "/" + nodeId
          var sentDistributed = false
          if (distributed) {
            try {
              // Set DISTRIBUTED action in adaptersStatusPath + "/" + nodeId path
              val act = ("action" -> "distributed")
              val sendJson = compact(render(act))
              zkcForSetData.setData().forPath(adaptrStatusPathForNode, sendJson.getBytes("UTF8"))
              sentDistributed = true
            } catch {
              case e: Exception => {
                LOG.error("distribute action failed with reason %s, message %s".format(e.getCause, e.getMessage))
              }
            }
          }

          if (sentDistributed == false) {
            // Set RE-DISTRIBUTED action in adaptersStatusPath + "/" + nodeId path
            val act = ("action" -> "re-distribute")
            val sendJson = compact(render(act))
            zkcForSetData.setData().forPath(adaptrStatusPathForNode, sendJson.getBytes("UTF8"))
          }
        }
        case _ => {
          LOG.info("No action performed, because of invalid action %s in json %s".format(actionOnAdaptersMap.action, receivedJsonStr))
        }
      }

      // 
    } catch {
      case e: Exception => {
        LOG.info("Found invalid JSON: %s".format(receivedJsonStr))
      }
    }

    LOG.info("ActionOnAdaptersDistImpl => Exit. receivedJsonStr: " + receivedJsonStr)
  }

  private def ActionOnAdaptersDistribution(receivedJsonStr: String): Unit = {
    ActionOnAdaptersDistImpl(receivedJsonStr)
  }

  private def ParticipentsAdaptersStatus(eventType: String, eventPath: String, eventPathData: Array[Byte], childs: Array[(String, Array[Byte])]): Unit = {
    // LOG.info("ParticipentsAdaptersStatus => Enter, eventType:%s, eventPath:%s ".format(eventType, eventPath))
    if (IsLeaderNode == false) { // Not Leader node
      // LOG.info("ParticipentsAdaptersStatus => Exit, eventType:%s, eventPath:%s ".format(eventType, eventPath))
      return
    }

    if (IsLeaderNodeAndUpdatePartitionsFlagSet) {
      LOG.info("Already got Re-distribution request. Ignoring any actions from ParticipentsAdaptersStatus")
      return
    }

    UpdatePartitionsNodeData(eventType, eventPath, eventPathData)
    // LOG.info("ParticipentsAdaptersStatus => Exit, eventType:%s, eventPath:%s ".format(eventType, eventPath))
  }

  private def CheckForPartitionsChange: Unit = {
    if (inputAdapters != null) {
      try {
        breakable {
          inputAdapters.foreach(ia => {
            val uk = ia.GetAllPartitionUniqueRecordKey
            val name = ia.UniqueName
            val ukCnt = if (uk != null) uk.size else 0
            val prevParts = GetPartitionsToValidate(name)
            val prevCnt = if (prevParts != null) prevParts.size else 0
            if (prevCnt != ukCnt) {
              // Number of partitions does not match
              SetUpdatePartitionsFlag
              break;
            }
            if (ukCnt > 0) {
              // Check the real content
              val serUKSet = uk.map(k => { k.Serialize }).toSet
              if ((serUKSet -- prevParts).isEmpty == false) {
                // Partition keys does not match
                SetUpdatePartitionsFlag
                break;
              }
            }
          })
        }
      } catch {
        case e: Exception => { LOG.error("Failed to get Input Adapters partitions. Reason:%s Message:%s".format(e.getCause, e.getMessage)) }
      }
    }
  }

  private def GetEndPartitionsValuesForValidateAdapters: Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)] = {
    val uniqPartKeysValues = ArrayBuffer[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)]()

    if (validateInputAdapters != null) {
      try {
        validateInputAdapters.foreach(ia => {
          val uKeysVals = ia.getAllPartitionEndValues
          uniqPartKeysValues ++= uKeysVals
        })
      } catch {
        case e: Exception => { LOG.error("Failed to get Validate Input Adapters partitions. Reason:%s Message:%s".format(e.getCause, e.getMessage)) }
      }
    }

    uniqPartKeysValues.toArray
  }

  def Init(nodeId1: String, zkConnectString1: String, engineLeaderZkNodePath1: String, engineDistributionZkNodePath1: String, adaptersStatusPath1: String, inputAdap: ArrayBuffer[InputAdapter], outputAdap: ArrayBuffer[OutputAdapter], statusAdap: ArrayBuffer[OutputAdapter], validateInputAdap: ArrayBuffer[InputAdapter], enviCxt: EnvContext, zkSessionTimeoutMs1: Int, zkConnectionTimeoutMs1: Int): Unit = {
    nodeId = nodeId1.toLowerCase
    zkConnectString = zkConnectString1
    engineLeaderZkNodePath = engineLeaderZkNodePath1
    engineDistributionZkNodePath = engineDistributionZkNodePath1
    adaptersStatusPath = adaptersStatusPath1
    zkSessionTimeoutMs = zkSessionTimeoutMs1
    zkConnectionTimeoutMs = zkConnectionTimeoutMs1
    inputAdapters = inputAdap
    outputAdapters = outputAdap
    statusAdapters = statusAdap
    validateInputAdapters = validateInputAdap
    envCtxt = enviCxt

    if (zkConnectString != null && zkConnectString.isEmpty() == false && engineLeaderZkNodePath != null && engineLeaderZkNodePath.isEmpty() == false && engineDistributionZkNodePath != null && engineDistributionZkNodePath.isEmpty() == false) {
      try {
        val adaptrStatusPathForNode = adaptersStatusPath + "/" + nodeId
        LOG.info("ZK Connecting. adaptrStatusPathForNode:%s, zkConnectString:%s, engineLeaderZkNodePath:%s, engineDistributionZkNodePath:%s".format(adaptrStatusPathForNode, zkConnectString, engineLeaderZkNodePath, engineDistributionZkNodePath))
        CreateClient.CreateNodeIfNotExists(zkConnectString, engineDistributionZkNodePath) // Creating 
        CreateClient.CreateNodeIfNotExists(zkConnectString, adaptrStatusPathForNode) // Creating path for Adapter Statues
        zkcForSetData = CreateClient.createSimple(zkConnectString, zkSessionTimeoutMs, zkConnectionTimeoutMs)
        zkAdapterStatusNodeListener = new ZooKeeperListener
        zkAdapterStatusNodeListener.CreatePathChildrenCacheListener(zkConnectString, adaptersStatusPath, false, ParticipentsAdaptersStatus, zkSessionTimeoutMs, zkConnectionTimeoutMs)
        zkEngineDistributionNodeListener = new ZooKeeperListener
        zkEngineDistributionNodeListener.CreateListener(zkConnectString, engineDistributionZkNodePath, ActionOnAdaptersDistribution, zkSessionTimeoutMs, zkConnectionTimeoutMs)
        try {
          Thread.sleep(500)
        } catch {
          case e: Exception => {
            // Not doing anything
          }
        }

        distributionExecutor.execute(new Runnable() {
          override def run() = {
            var updatePartsCntr = 0
            var getValidateAdapCntr = 0
            var wait4ValidateCheck = 0
            var validateUniqVals: Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)] = null
            while (distributionExecutor.isShutdown == false) {
              Thread.sleep(1000) // Waiting for 1sec
              if (distributionExecutor.isShutdown == false) {
                if (GetUpdatePartitionsFlagAndReset) {
                  UpdatePartitionsIfNeededOnLeader
                  wait4ValidateCheck = 180 // When ever rebalancing it should be 180 secs
                  updatePartsCntr = 0
                  getValidateAdapCntr = 0
                } else if (IsLeaderNode) {
                  if (GetUpdatePartitionsFlag == false) {
                    // Get Partitions for every N secs and see whether any partitions changes from previous get
                    if (updatePartsCntr >= 30) { // for every 30 secs
                      CheckForPartitionsChange
                      updatePartsCntr = 0
                    } else {
                      updatePartsCntr += 1
                    }

                    if (wait4ValidateCheck > 0) {
                      // Get Partitions keys and values for every M secs
                      if (getValidateAdapCntr >= wait4ValidateCheck) { // for every waitForValidateCheck secs
                        // Persists the previous ones if we have any
                        if (validateUniqVals != null && validateUniqVals.size > 0) {
                          envCtxt.PersistValidateAdapterInformation(validateUniqVals.map(kv => (kv._1.Serialize, kv._2.Serialize)))
                        }
                        // Get the latest ones
                        validateUniqVals = GetEndPartitionsValuesForValidateAdapters
                        getValidateAdapCntr = 0
                        wait4ValidateCheck = 60 // Next time onwards it is 60 secs
                      } else {
                        getValidateAdapCntr += 1
                      }
                    } else {
                      getValidateAdapCntr = 0
                    }
                  }
                } else {
                  wait4ValidateCheck = 0 // Not leader node, don't check for it until we set it in redistribute
                  getValidateAdapCntr = 0
                }
              }
            }
          }
        })

        SetCanRedistribute(true)
        zkLeaderLatch = new ZkLeaderLatch(zkConnectString, engineLeaderZkNodePath, nodeId, EventChangeCallback, zkSessionTimeoutMs, zkConnectionTimeoutMs)
        zkLeaderLatch.SelectLeader
        /*
        // Set RE-DISTRIBUTED action in adaptersStatusPath + "/" + nodeId path
        val act = ("action" -> "re-distribute")
        val sendJson = compact(render(act))
        zkcForSetData.setData().forPath(adaptrStatusPathForNode, sendJson.getBytes("UTF8"))
        */
      } catch {
        case e: Exception => {
          LOG.error("Failed to initialize ZooKeeper Connection. Reason:%s Message:%s".format(e.getCause, e.getMessage))
          throw e
        }
      }
    } else {
      LOG.error("Not connected to elect Leader and not distributing data between nodes.")
    }
  }

  def Shutdown: Unit = {
    distributionExecutor.shutdown
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


