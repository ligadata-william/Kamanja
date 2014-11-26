
package com.ligadata.InputAdapters

import scala.actors.threadpool.{ Executors, ExecutorService }
import java.util.Properties
import kafka.consumer.{ ConsumerConfig, Consumer, ConsumerConnector }
import scala.collection.mutable.ArrayBuffer
import org.apache.log4j.Logger
import com.ligadata.OnLEPBase.{ EnvContext, AdapterConfiguration, InputAdapter, InputAdapterObj, OutputAdapter, ExecContext, MakeExecContext, CountersAdapter, PartitionUniqueRecordKey }
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
  private[this] val auto_commit_time = 24 * 60 * 60 * 1000

  //BUGBUG:: Not Checking whether inputConfig is really QueueAdapterConfiguration or not. 
  private[this] val qc = new KafkaQueueAdapterConfiguration
  private[this] val lock = new Object()
  private[this] val kvs = scala.collection.mutable.Map[Int, (KafkaPartitionUniqueRecordKey, KafkaPartitionUniqueRecordValue)]()

  qc.Typ = inputConfig.Typ
  qc.Name = inputConfig.Name
  qc.className = inputConfig.className
  qc.jarName = inputConfig.jarName
  qc.dependencyJars = inputConfig.dependencyJars

  // For Kafka Queue we expect the format "Type~Name~Host/Brokers~Group/Client~MaxPartitions~InstancePartitions~ClassName~JarName~DependencyJars"
  if (inputConfig.adapterSpecificTokens.size != 4) {
    val err = "We should find only Type, Name, ClassName, JarName, DependencyJarsm Host/Brokers, Group/Client, MaxPartitions & Set of Handled Partitions for Kafka Queue Adapter Config:" + inputConfig.Name
    LOG.error(err)
    throw new Exception(err)
  }

  qc.hosts = inputConfig.adapterSpecificTokens(0).split(",").map(str => str.trim).filter(str => str.size > 0)
  qc.groupName = inputConfig.adapterSpecificTokens(1)
  if (inputConfig.adapterSpecificTokens(2).size > 0)
    qc.maxPartitions = inputConfig.adapterSpecificTokens(2).toInt
  if (inputConfig.adapterSpecificTokens(3).size > 0)
    qc.instancePartitions = inputConfig.adapterSpecificTokens(3).split(",").map(str => str.trim).filter(str => str.size > 0).map(str => str.toInt).toSet

  //BUGBUG:: Not validating the values in QueueAdapterConfiguration 
  props.put("zookeeper.connect", qc.hosts.mkString(","))
  props.put("group.id", qc.groupName)
  props.put("rebalance.backoff.ms", rebalance_backoff_ms.toString)
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
    LOG.info("===============> Called StopProcessing")
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

  override def StartProcessing(maxParts: Int, partitionUniqueRecordKeys: Array[String], partitionUniqueRecordValues: Array[String]): Unit = lock.synchronized {
    LOG.info("===============> Called StartProcessing")
    if (partitionUniqueRecordKeys == null || partitionUniqueRecordKeys.size == 0)
      return

    if (partitionUniqueRecordKeys.size != partitionUniqueRecordValues.size) {
      LOG.error("Keys Size:%d not same as Values Size:%d".format(partitionUniqueRecordKeys.size, partitionUniqueRecordValues.size))
      return
    }

    // Cleaning GroupId so that we can start from begining
    ConsoleConsumer.tryCleanupZookeeper(qc.hosts.mkString(","), qc.groupName)

    consumerConnector = Consumer.create(consumerConfig)

    LOG.info("Deserializing Keys")
    val keys = partitionUniqueRecordKeys.map(k => {
      LOG.info("Deserializing Key:" + k)
      val key = new KafkaPartitionUniqueRecordKey
      try {
        key.Deserialize(k)
      } catch {
        case e: Exception => {
          LOG.error("Failed to deserialize Key:%s. Reason:%s Message:%s".format(k, e.getCause, e.getMessage))
          throw e
        }
      }
      key
    })

    LOG.info("Deserializing Values")
    val vals = partitionUniqueRecordValues.map(v => {
      val vl = new KafkaPartitionUniqueRecordValue
      if (v != null) {
        try {
          LOG.info("Deserializing Value:" + v)
          vl.Deserialize(v)
        } catch {
          case e: Exception => {
            LOG.error("Failed to deserialize Value:%s. Reason:%s Message:%s".format(v, e.getCause, e.getMessage))
            throw e
          }
        }
      }
      vl
    })
    LOG.info("Deserializing Keys & Values done")

    qc.instancePartitions = keys.map(k => { k.PartitionId }).toSet

    // var threads: Int = if (qc.maxPartitions > qc.instancePartitions.size) qc.maxPartitions else qc.instancePartitions.size
    var threads: Int = maxParts
    if (threads == 0)
      threads = if (qc.instancePartitions == null) 0 else qc.instancePartitions.size
    if (threads == 0)
      threads = 1

    // create the consumer streams
    val topicMessageStreams = consumerConnector.createMessageStreams(Predef.Map(qc.Name -> threads))
    LOG.info("All Message Streams")

    executor = Executors.newFixedThreadPool(threads)

    kvs.clear

    LOG.info("Creating KV Map")
    for (i <- 0 until keys.size) {
      val key = keys(i)
      kvs(key.PartitionId) = ((key, vals(i)))
    }

    LOG.info("KV Map =>")
    kvs.foreach(kv => {
      LOG.info("Key:%s => Val:%s".format(kv._2._1.Serialize, kv._2._2.Serialize))
    })

    try {
      LOG.info("Trying to Prepare Streams => Topic:%s, TotalPartitions:%d, Partitions:%s".format(qc.Name, qc.maxPartitions, qc.instancePartitions.mkString(",")))
      // get the streams for the topic
      val testTopicStreams = topicMessageStreams.get(qc.Name).get
      LOG.info("Prepare Streams => Topic:%s, TotalPartitions:%d, Partitions:%s".format(qc.Name, qc.maxPartitions, qc.instancePartitions.mkString(",")))

      for (stream <- testTopicStreams) {
        LOG.info("Streams Creating => ")
        executor.execute(new Runnable() {
          override def run() {
            val topicMessageStrmsPtr = topicMessageStreams
            val testTopicStrmsPtr = testTopicStreams
            var curPartitionId = 0
            var checkForPartition = true
            var execThread: ExecContext = null
            var cntr: Long = 0
            var currentOffset: Long = -1
            val uniqueKey = new KafkaPartitionUniqueRecordKey
            val uniqueVal = new KafkaPartitionUniqueRecordValue

            uniqueKey.TopicName = qc.Name

            try {
              breakable {
                for (message <- stream) {
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
                      LOG.info("Topic:%s, PartitionId:%d, isValid:%s".format(qc.Name, curPartitionId, isValid.toString))
                      if (isValid == false)
                        return ;
                      checkForPartition = false
                      uniqueKey.PartitionId = curPartitionId
                      execThread = mkExecCtxt.CreateExecContext(input, curPartitionId, output, envCtxt)
                      val kv = kvs.getOrElse(curPartitionId, null)
                      if (kv != null && kv._2.Offset != -1 && message.offset <= kv._2.Offset) {
                        executeCurMsg = false
                        currentOffset = kv._2.Offset
                      }
                    }
                    if (executeCurMsg) {
                      try {
                        // Creating new string to convert from Byte Array to string
                        val msg = new String(message.message)
                        uniqueVal.Offset = currentOffset
                        execThread.execute(msg, uniqueKey, uniqueVal, readTmNs, readTmMs)
                        // consumerConnector.commitOffsets // BUGBUG:: Bad way of calling to save all offsets
                        cntr += 1
                        val key = Category + "/" + qc.Name + "/evtCnt"
                        cntrAdapter.addCntr(key, 1) // for now adding each row
                      } catch {
                        case e: Exception => LOG.error("Failed with Message:" + e.getMessage)
                      }
                    } else {
                      LOG.info("Ignoring Message:%s".format(new String(message.message)))
                    }
                  } else {
                    LOG.info("Ignoring Message:%s".format(new String(message.message)))
                  }
                  if (executor.isShutdown)
                    break
                }
              }
            } catch {
              case e: Exception => {
                LOG.error("Failed with Reason:%s Message:%s".format(e.getCause, e.getMessage))
              }
            }
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

  private def GetAllPartitionsUniqueKeys: Array[String] = lock.synchronized {
    val zkClient = new ZkClient(qc.hosts.mkString(","), 30000, 30000, ZKStringSerializer)

    val jsonPartitionMapOpt = readDataMaybeNull(zkClient, getTopicPath(qc.Name))._1

    zkClient.close

    if (jsonPartitionMapOpt == None) {
      LOG.info("Not found any JSON Partitions for Queue: " + qc.Name)
      return null
    }

    LOG.info("JSON Partitions:%s".format(jsonPartitionMapOpt.get))

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

    // val values4 = values3.map(p => (p._1.toInt, p._2.map(_.toInt)))
    values3.map(p => (p._1.toInt)).map(pid => {
      val uniqueKey = new KafkaPartitionUniqueRecordKey
      uniqueKey.TopicName = qc.Name
      uniqueKey.PartitionId = pid
      uniqueKey
    }).map(k => { k.Serialize }).toArray
  }

  // *********** These are temporary methods -- End *********** ///

  override def GetAllPartitionUniqueRecordKey: Array[String] = lock.synchronized {
    GetAllPartitionsUniqueKeys
  }
}

