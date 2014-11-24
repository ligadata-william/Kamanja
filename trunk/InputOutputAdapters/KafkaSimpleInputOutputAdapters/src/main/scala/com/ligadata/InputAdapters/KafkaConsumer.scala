
package com.ligadata.InputAdapters

import scala.actors.threadpool.Executors
import java.util.Properties
import kafka.consumer.{ ConsumerConfig, Consumer, ConsumerConnector }
import scala.collection.mutable.ArrayBuffer
import org.apache.log4j.Logger
import com.ligadata.OnLEPBase.{ EnvContext, AdapterConfiguration, InputAdapter, InputAdapterObj, OutputAdapter, ExecContext, MakeExecContext, CountersAdapter, PartitionUniqueRecordKey }
import com.ligadata.AdaptersConfiguration.{ KafkaQueueAdapterConfiguration, KafkaPartitionUniqueRecordKey, KafkaPartitionUniqueRecordValue }

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

  //BUGBUG:: Not Checking whether inputConfig is really QueueAdapterConfiguration or not. 
  private[this] val qc = new KafkaQueueAdapterConfiguration
  private[this] val uniqueKeys = new ArrayBuffer[KafkaPartitionUniqueRecordKey]
  private[this] val lock = new Object()

  private def AddUniqueKey(uniqueKey: KafkaPartitionUniqueRecordKey): Unit = lock.synchronized {
    uniqueKeys += uniqueKey
  }

  private def GetUniqueKeys: Array[KafkaPartitionUniqueRecordKey] = lock.synchronized {
    uniqueKeys.toArray
  }

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
  // props.put("fetch.size", fetchsize.toString)
  // props.put("auto.offset.reset", "smallest")
  props.put("rebalance.backoff.ms", rebalance_backoff_ms.toString)
  props.put("zookeeper.session.timeout.ms", zookeeper_session_timeout_ms.toString)
  props.put("zookeeper.connection.timeout.ms", zookeeper_connection_timeout_ms.toString)
  props.put("zookeeper.sync.time.ms", zookeeper_sync_time_ms.toString)

  val consumerConfig = new ConsumerConfig(props)
  val consumerConnector: ConsumerConnector = Consumer.create(consumerConfig)

  // instancePartitions

  var threads: Int = if (qc.maxPartitions > qc.instancePartitions.size) qc.maxPartitions else qc.instancePartitions.size
  if (threads == 0)
    threads = 1

  // create the consumer streams
  val topicMessageStreams = consumerConnector.createMessageStreams(Predef.Map(qc.Name -> threads))

  val executor = Executors.newFixedThreadPool(threads)

  val input = this

  // get the streams for the topic
  val testTopicStreams = topicMessageStreams.get(qc.Name).get
  LOG.debug("Prepare Streams => Topic:%s, TotalPartitions:%d, Partitions:%s".format(qc.Name, qc.maxPartitions, qc.instancePartitions.mkString(",")))

  for (stream <- testTopicStreams) {
    executor.execute(new Runnable() {
      override def run() {
        var curPartitionId = 0
        var checkForPartition = true
        var execThread: ExecContext = null
        var cntr: Long = 0
        var currentOffset: Long = -1
        val uniqueKey = new KafkaPartitionUniqueRecordKey
        val uniqueVal = new KafkaPartitionUniqueRecordValue

        uniqueKey.TopicName = qc.Name

        for (message <- stream) {
          if (message.offset > currentOffset) {
            currentOffset = message.offset
            var readTmNs = System.nanoTime
            var readTmMs = System.currentTimeMillis
            if (checkForPartition) {
              // For first message, check whether this stream we are going to handle it or not
              // If not handle, just return
              curPartitionId = message.partition
              var isValid = false
              qc.instancePartitions.foreach(p => {
                if (p == curPartitionId)
                  isValid = true
              })
              LOG.debug("Topic:%s, PartitionId:%d, isValid:%s".format(qc.Name, curPartitionId, isValid.toString))
              if (isValid == false)
                return ;
              checkForPartition = false
              uniqueKey.PartitionId = curPartitionId
              AddUniqueKey(uniqueKey)
              execThread = mkExecCtxt.CreateExecContext(input, curPartitionId, output, envCtxt)
            }
            try {
              // Creating new string to convert from Byte Array to string
              val msg = new String(message.message)
              uniqueVal.Offset = currentOffset
              execThread.execute(msg, uniqueKey, uniqueVal, readTmNs, readTmMs)
              cntr += 1
              val key = Category + "/" + qc.Name + "/evtCnt"
              cntrAdapter.addCntr(key, 1) // for now adding each row
            } catch {
              case e: Exception => LOG.error("Failed with Message:" + e.getMessage)
            }
          }

        }
      }
    });
  }

  override def Shutdown: Unit = {
    consumerConnector.shutdown
    executor.shutdownNow()
  }

  override def StopProcessing: Unit = {

  }

  override def StartProcessing(partitionUniqueRecordKeys: Array[String]): Unit = {

  }

  override def GetAllPartitionUniqueRecordKey: Array[String] = {
    val uniqueKeys = GetUniqueKeys
    if (uniqueKeys != null) {
      return uniqueKeys.map(k => { k.Serialize })
    }
    null
  }

}

