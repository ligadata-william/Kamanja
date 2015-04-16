
package com.ligadata.InputAdapters

import scala.actors.threadpool.{ Executors, ExecutorService }
import java.util.Properties
import kafka.consumer.{ ConsumerConfig, Consumer, ConsumerConnector }
import scala.collection.mutable.ArrayBuffer
import org.apache.log4j.Logger
import com.ligadata.OnLEPBase.{ EnvContext, AdapterConfiguration, InputAdapter, InputAdapterObj, OutputAdapter, ExecContext, MakeExecContext, CountersAdapter, PartitionUniqueRecordKey, PartitionUniqueRecordValue }
import com.ligadata.AdaptersConfiguration.{ KafkaQueueAdapterConfiguration, KafkaPartitionUniqueRecordKey, KafkaPartitionUniqueRecordValue }
import scala.util.control.Breaks._
import org.apache.zookeeper.data.Stat
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNoNodeException
import kafka.utils.ZKStringSerializer
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import kafka.consumer.ConsoleConsumer

object KafkaConsumer extends InputAdapterObj {
  def CreateInputAdapter(inputConfig: AdapterConfiguration, output: Array[OutputAdapter], envCtxt: EnvContext, mkExecCtxt: MakeExecContext, cntrAdapter: CountersAdapter): InputAdapter = new KafkaConsumer(inputConfig, output, envCtxt, mkExecCtxt, cntrAdapter)
}

class KafkaConsumer(val inputConfig: AdapterConfiguration, val output: Array[OutputAdapter], val envCtxt: EnvContext, val mkExecCtxt: MakeExecContext, cntrAdapter: CountersAdapter) extends InputAdapter {
  private[this] val LOG = Logger.getLogger(getClass);
  private[this] val props = new Properties
  private[this] val fetchsize = 64 * 1024
  private[this] val rebalance_backoff_ms = 30000
  private[this] val zookeeper_session_timeout_ms = 30000
  private[this] val zookeeper_connection_timeout_ms = 30000
  private[this] val zookeeper_sync_time_ms = 5000
  private[this] val auto_commit_time = 365 * 24 * 60 * 60 * 1000

  //BUGBUG:: Not Checking whether inputConfig is really QueueAdapterConfiguration or not. 
  private[this] val qc = KafkaQueueAdapterConfiguration.GetAdapterConfig(inputConfig)
  private[this] val lock = new Object()
  private[this] val kvs = scala.collection.mutable.Map[Int, (KafkaPartitionUniqueRecordKey, KafkaPartitionUniqueRecordValue, Long, (KafkaPartitionUniqueRecordValue, Int, Int))]()

  val groupName = "T" + hashCode.toString

  //BUGBUG:: Not validating the values in QueueAdapterConfiguration 
  props.put("zookeeper.connect", qc.hosts.mkString(","))
  props.put("group.id", groupName)
  props.put("rebalance.backoff.ms", rebalance_backoff_ms.toString)
  props.put("rebalance.max.retries", 1.toString)
  props.put("zookeeper.session.timeout.ms", zookeeper_session_timeout_ms.toString)
  props.put("zookeeper.connection.timeout.ms", zookeeper_connection_timeout_ms.toString)
  props.put("zookeeper.sync.time.ms", zookeeper_sync_time_ms.toString)
  props.put("auto.commit.enable", "false")
  props.put("auto.commit.interval.ms", auto_commit_time.toString)
  props.put("auto.offset.reset", if (true) "smallest" else "largest")

  val consumerConfig = new ConsumerConfig(props)
  var consumerConnector: ConsumerConnector = _
  var executor: ExecutorService = _
  val input = this

  override def Shutdown: Unit = lock.synchronized {
    StopProcessing
  }

  override def StopProcessing: Unit = lock.synchronized {
    LOG.debug("===============> Called StopProcessing")
    //BUGBUG:: Make sure we finish processing the current running messages.
    if (consumerConnector != null)
      consumerConnector.shutdown
    if (executor != null) {
      executor.shutdownNow()
      while (executor.isTerminated == false) {
        Thread.sleep(100) // sleep 100ms and then check
      }
    }

    consumerConnector = null
    executor = null
  }

  // Each value in partitionInfo is (PartitionUniqueRecordKey, PartitionUniqueRecordValue, Long, PartitionUniqueRecordValue) key, processed value, Start transactionid, Ignore Output Till given Value (Which is written into Output Adapter) 
  override def StartProcessing(maxParts: Int, partitionInfo: Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue, Long, (PartitionUniqueRecordValue, Int, Int))], ignoreFirstMsg: Boolean): Unit = lock.synchronized {
    LOG.debug("===============> Called StartProcessing")
    if (partitionInfo == null || partitionInfo.size == 0)
      return

    try {
      // Cleaning GroupId so that we can start from begining
      ConsoleConsumer.tryCleanupZookeeper(qc.hosts.mkString(","), groupName)
    } catch {
      case e: Exception => {
      }
    }

    consumerConnector = Consumer.create(consumerConfig)

    val partInfo = partitionInfo.map(quad => { (quad._1.asInstanceOf[KafkaPartitionUniqueRecordKey], quad._2.asInstanceOf[KafkaPartitionUniqueRecordValue], quad._3, (quad._4._1.asInstanceOf[KafkaPartitionUniqueRecordValue], quad._4._2, quad._4._3)) })

    qc.instancePartitions = partInfo.map(trip => { trip._1.PartitionId }).toSet

    // var threads: Int = if (qc.maxPartitions > qc.instancePartitions.size) qc.maxPartitions else qc.instancePartitions.size
    var threads: Int = maxParts
    if (threads == 0)
      threads = if (qc.instancePartitions == null) 0 else qc.instancePartitions.size
    if (threads == 0)
      threads = 1

    // create the consumer streams
    val topicMessageStreams = consumerConnector.createMessageStreams(Predef.Map(qc.topic -> threads))
    LOG.debug("All Message Streams")

    executor = Executors.newFixedThreadPool(threads)

    kvs.clear

    LOG.debug("Creating KV Map")

    partInfo.foreach(quad => {
      kvs(quad._1.PartitionId) = quad
    })

    LOG.debug("KV Map =>")
    kvs.foreach(kv => {
      LOG.debug("Key:%s => Val:%s".format(kv._2._1.Serialize, kv._2._2.Serialize))
    })

    try {
      LOG.debug("Trying to Prepare Streams => Topic:%s, TotalPartitions:%d, Partitions:%s".format(qc.topic, maxParts, qc.instancePartitions.mkString(",")))
      // get the streams for the topic
      val testTopicStreams = topicMessageStreams.get(qc.topic).get
      LOG.debug("Prepare Streams => Topic:%s, TotalPartitions:%d, Partitions:%s".format(qc.topic, maxParts, qc.instancePartitions.mkString(",")))

      for (stream <- testTopicStreams) {
        // LOG.debug("Streams Creating => ")
        executor.execute(new Runnable() {
          override def run() {
            val topicMessageStrmsPtr = topicMessageStreams
            val testTopicStrmsPtr = testTopicStreams
            var curPartitionId = -1
            var checkForPartition = true
            var execThread: ExecContext = null
            var cntr: Long = 0
            var currentOffset: Long = -1
            var processingXformMsg = 0
            var totalXformMsg = 0
            val uniqueKey = new KafkaPartitionUniqueRecordKey
            val uniqueVal = new KafkaPartitionUniqueRecordValue

            uniqueKey.Name = qc.Name
            uniqueKey.TopicName = qc.topic

            var tempTransId: Long = 0
            var ignoreOff: Long = -1

            try {
              breakable {
                for (message <- stream) {
                  // LOG.debug("Partition:%d Message:%s".format(message.partition, new String(message.message)))
                  if (qc.instancePartitions(message.partition)) {
                    if (message.offset > currentOffset) {
                      currentOffset = message.offset
                      var readTmNs = System.nanoTime
                      var readTmMs = System.currentTimeMillis
                      var executeCurMsg = true
                      if (checkForPartition) {
                        // For first message, check whether this stream we are going to handle it or not
                        // If not handle, just return
                        curPartitionId = message.partition
                        var isValid = false
                        qc.instancePartitions.foreach(p => {
                          if (p == curPartitionId)
                            isValid = true
                        })
                        LOG.debug("Name:%s, Topic:%s, PartitionId:%d, isValid:%s".format(qc.Name, qc.topic, curPartitionId, isValid.toString))
                        if (isValid == false) {
                          LOG.debug("Returning from stream of Partitionid : " + curPartitionId)
                          return ;
                        }
                        checkForPartition = false
                        uniqueKey.PartitionId = curPartitionId
                        execThread = mkExecCtxt.CreateExecContext(input, curPartitionId, output, envCtxt)
                        val kv = kvs.getOrElse(curPartitionId, null)
                        if (kv != null) {
                          if (kv._2.Offset != -1) {
                            if (message.offset < kv._2.Offset) {
                              executeCurMsg = false
                              currentOffset = if (ignoreFirstMsg) kv._2.Offset else (kv._2.Offset - 1) // Later just checking whether it is > or not
                            } else if (message.offset == kv._2.Offset) {
                              if (ignoreFirstMsg)
                                executeCurMsg = false
                              currentOffset = kv._2.Offset
                            }

                          }
                          tempTransId = kv._3
                          ignoreOff = if (ignoreFirstMsg) kv._4._1.Offset else kv._4._1.Offset - 1 
                          processingXformMsg = kv._4._2
                          totalXformMsg = kv._4._3
                        }
                      }
                      if (executeCurMsg) {
                        try {
                          // Creating new string to convert from Byte Array to string
                          val msg = new String(message.message)
                          uniqueVal.Offset = currentOffset
                          if (message.offset >= ignoreOff) {
                            processingXformMsg = 0
                            totalXformMsg = 0
                          }
                          execThread.execute(tempTransId, msg, qc.formatOrInputAdapterName, uniqueKey, uniqueVal, readTmNs, readTmMs, message.offset <= ignoreOff, processingXformMsg, totalXformMsg)
                          tempTransId += 1
                          // consumerConnector.commitOffsets // BUGBUG:: Bad way of calling to save all offsets
                          cntr += 1
                          val key = Category + "/" + qc.Name + "/evtCnt"
                          cntrAdapter.addCntr(key, 1) // for now adding each row
                        } catch {
                          case e: Exception => LOG.error("Failed with Message:" + e.getMessage)
                        }
                      } else {
                        LOG.debug("Ignoring Message:%s".format(new String(message.message)))
                      }
                    } else {
                      LOG.debug("Ignoring Message:%s".format(new String(message.message)))
                    }
                  }
                  if (executor.isShutdown) {
                    LOG.debug("Executor is shutting down for partitionId: " + curPartitionId)
                    break
                  }
                }
              }
            } catch {
              case e: Exception => {
                LOG.error("Failed with Reason:%s Message:%s".format(e.getCause, e.getMessage))
              }
            }
            LOG.debug("===========================> Exiting Thread for Partition:" + curPartitionId)
          }
        });
      }
    } catch {
      case e: Exception => {
        LOG.error("Failed to setup Streams. Reason:%s Message:%s".format(e.getCause, e.getMessage))
      }
    }
  }

  // *********** These are temporary methods -- Start *********** ///
  private def getTopicPath(topic: String): String = {
    val BrokerTopicsPath = "/brokers/topics"
    BrokerTopicsPath + "/" + topic
  }

  private def getTopicPartitionsPath(topic: String): String = {
    getTopicPath(topic) + "/partitions"
  }

  private def readDataMaybeNull(client: ZkClient, path: String): (Option[String], Stat) = {
    val stat: Stat = new Stat()
    val dataAndStat = try {
      (Some(client.readData(path, stat)), stat)
    } catch {
      case e: ZkNoNodeException =>
        (None, stat)
      case e2: Exception => throw e2
    }
    dataAndStat
  }

  private def GetAllPartitionsUniqueKeys: Array[PartitionUniqueRecordKey] = lock.synchronized {
    val zkClient = new ZkClient(qc.hosts.mkString(","), 30000, 30000, ZKStringSerializer)

    val jsonPartitionMapOpt = readDataMaybeNull(zkClient, getTopicPath(qc.topic))._1

    zkClient.close

    if (jsonPartitionMapOpt == None) {
      LOG.error("Not found any JSON Partitions for Queue: " + qc.topic)
      return null
    }

    LOG.debug("JSON Partitions:%s".format(jsonPartitionMapOpt.get))

    val json = parse(jsonPartitionMapOpt.get)
    if (json == null || json.values == null) // Not doing anything
      return null

    val values1 = json.values.asInstanceOf[Map[String, Any]]
    val values2 = values1.getOrElse("partitions", null)
    if (values2 == null)
      return null
    val values3 = values2.asInstanceOf[Map[String, Seq[String]]]
    if (values3 == null || values3.size == 0)
      return null

    values3.map(p => (p._1.toInt)).map(pid => {
      val uniqueKey = new KafkaPartitionUniqueRecordKey
      uniqueKey.Name = qc.Name
      uniqueKey.TopicName = qc.topic
      uniqueKey.PartitionId = pid
      uniqueKey
    }).toArray
  }

  // *********** These are temporary methods -- End *********** ///

  override def GetAllPartitionUniqueRecordKey: Array[PartitionUniqueRecordKey] = lock.synchronized {
    GetAllPartitionsUniqueKeys
  }

  override def DeserializeKey(k: String): PartitionUniqueRecordKey = {
    val key = new KafkaPartitionUniqueRecordKey
    try {
      LOG.debug("Deserializing Key:" + k)
      key.Deserialize(k)
    } catch {
      case e: Exception => {
        LOG.error("Failed to deserialize Key:%s. Reason:%s Message:%s".format(k, e.getCause, e.getMessage))
        throw e
      }
    }
    key
  }

  override def DeserializeValue(v: String): PartitionUniqueRecordValue = {
    val vl = new KafkaPartitionUniqueRecordValue
    if (v != null) {
      try {
        LOG.debug("Deserializing Value:" + v)
        vl.Deserialize(v)
      } catch {
        case e: Exception => {
          LOG.error("Failed to deserialize Value:%s. Reason:%s Message:%s".format(v, e.getCause, e.getMessage))
          throw e
        }
      }
    }
    vl
  }

  // Not yet implemented
  override def getAllPartitionBeginValues: Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)] = {
    return Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)]()
  }

  // Not yet implemented
  override def getAllPartitionEndValues: Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)] = {
    return Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)]()
  }

}

