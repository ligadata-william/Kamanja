
package com.ligadata.InputAdapters

import com.ligadata.AdaptersConfiguration.{ KafkaPartitionUniqueRecordKey, KafkaPartitionUniqueRecordValue, KafkaQueueAdapterConfiguration }
import com.ligadata.OnLEPBase._
import kafka.api._
import kafka.common.TopicAndPartition
import scala.actors.threadpool.{ TimeUnit, ExecutorService, Executors }
import scala.util.control.Breaks._
import kafka.consumer.{ SimpleConsumer }
import java.net.{ InetAddress }
import org.apache.log4j.Logger
import scala.collection.mutable.Map

object KafkaSimpleConsumer extends InputAdapterObj {
  val METADATA_REQUEST_CORR_ID = 2
  val QUEUE_FETCH_REQUEST_TYPE = 1
  val METADATA_REQUEST_TYPE = "metadataLookup"
  val MAX_FAILURES = 2
  val MONITOR_FREQUENCY = 10000 // Monitor Topic queues every 20 seconds
  val SLEEP_DURATION = 1000 // Allow 1 sec between unsucessful fetched
  val MAXSLEEP = 8001
  var CURRENT_BROKER: String = _
  val FETCHSIZE = 64 * 1024
  val ZOOKEEPER_CONNECTION_TIMEOUT_MS = 3000
  def CreateInputAdapter(inputConfig: AdapterConfiguration, output: Array[OutputAdapter], envCtxt: EnvContext, mkExecCtxt: MakeExecContext, cntrAdapter: CountersAdapter): InputAdapter = new KafkaSimpleConsumer(inputConfig, output, envCtxt, mkExecCtxt, cntrAdapter)
}

class KafkaSimpleConsumer(val inputConfig: AdapterConfiguration, val output: Array[OutputAdapter], val envCtxt: EnvContext, val mkExecCtxt: MakeExecContext, cntrAdapter: CountersAdapter) extends InputAdapter {
  val input = this
  private val lock = new Object()
  private val LOG = Logger.getLogger(getClass);

  if (inputConfig.adapterSpecificTokens.size == 0) {
    val err = "We should find only Name, Format, ClassName, JarName, DependencyJars, Host/Brokers and topicname for Kafka Queue Adapter Config:" + inputConfig.Name
    LOG.error(err)
    throw new Exception(err)
  }

  private val qc = new KafkaQueueAdapterConfiguration
  // get the config values from the input
  qc.Name = inputConfig.Name
  qc.className = inputConfig.className
  qc.formatOrInputAdapterName = inputConfig.formatOrInputAdapterName
  qc.jarName = inputConfig.jarName
  qc.dependencyJars = inputConfig.dependencyJars
  qc.hosts = inputConfig.adapterSpecificTokens(0).split(",").map(str => str.trim).filter(str => str.size > 0).map(str => convertIp(str))
  qc.topic = inputConfig.adapterSpecificTokens(1).trim
  qc.instancePartitions = Array[Int]().toSet // Some methods on this class require this to be set... maybe need to create at instanciation time

  LOG.info("KAFKA ADAPTER: allocating kafka adapter for " + qc.hosts.size + " broker hosts")

  private var numberOfErrors: Int = _
  private var replicaBrokers: Set[String] = Set()
  private var readExecutor: ExecutorService = _
  private val kvs = scala.collection.mutable.Map[Int, (KafkaPartitionUniqueRecordKey, KafkaPartitionUniqueRecordValue, Long, (KafkaPartitionUniqueRecordValue, Int, Int))]()
  private var clientName: String = _

  // Heartbeat monitor related variables.
  private var hbRunning: Boolean = false
  private var hbTopicPartitionNumber = -1
  private val hbExecutor = Executors.newFixedThreadPool(qc.hosts.size)

  /**
   *  This will stop all the running reading threads and close all the underlying connections to Kafka.
   */
  override def Shutdown(): Unit = lock.synchronized {
    StopProcessing
  }

  /**
   * Will stop all the running read threads only - a call to StartProcessing will restart the reading process
   */
  def StopProcessing(): Unit = {
    terminateReaderTasks
    terminateHBTasks
  }

  /**
   * Start processing - will start a number of threads to read the Kafka queues for a topic.  The list of Hosts servicing a
   * given topic, and the topic have been set when this KafkaConsumer_V2 Adapter was instantiated.  The partitionIds should be
   * obtained via a prior call to the adapter.  One of the hosts will be a chosen as a leader to service the requests by the
   * spawned threads.
   * @param maxParts Int - Number of Partitions
   * @param partitionIds Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue, Long, PartitionUniqueRecordValue)] - an Array of partition ids
   */
  def StartProcessing(maxParts: Int, partitionIds: Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue, Long, (PartitionUniqueRecordValue, Int, Int))]): Unit = lock.synchronized {

    LOG.info("KAFKA-ADAPTER: Starting to read Kafka queues for topic: " + qc.topic)

    if (partitionIds == null || partitionIds.size == 0) {
      LOG.error("KAFKA-ADAPTER: Cannot process the kafka queue request, invalid parameters - number")
      return
    }

    // Get the data about the request and set the instancePartition list.
    val partitionInfo = partitionIds.map(quad => {
      (quad._1.asInstanceOf[KafkaPartitionUniqueRecordKey],
        quad._2.asInstanceOf[KafkaPartitionUniqueRecordValue],
        quad._3,
        (quad._4._1.asInstanceOf[KafkaPartitionUniqueRecordValue], quad._4._2, quad._4._3))
    })

    qc.instancePartitions = partitionInfo.map(partQuad => { partQuad._1.PartitionId }).toSet

    // Make sure the data passed was valid.
    if (qc.instancePartitions == null) {
      LOG.error("KAFKA-ADAPTER: Cannot process the kafka queue request, invalid parameters - partition instance list")
      return
    }

    // Figure out the size of the thread pool to use and create that pool
    var threads = maxParts
    if (threads == 0) {
      if (qc.instancePartitions.size == 0)
        threads = 1
      else
        threads = qc.instancePartitions.size
    }

    readExecutor = Executors.newFixedThreadPool(threads)

    // Create a Map of all the partiotion Ids.
    kvs.clear
    partitionInfo.foreach(quad => {
      kvs(quad._1.PartitionId) = quad
    })

    LOG.debug("KAFKA-ADAPTER: Starting " + kvs.size + " threads to process partitions")

    // Schedule a task to perform a read from a give partition.
    kvs.foreach(kvsElement => {
      readExecutor.execute(new Runnable() {
        override def run() {
          val partitionId = kvsElement._1
          val partition = kvsElement._2

          // if the offset is -1, then the server wants to start from the begining, else, it means that the server
          // knows what its doing and we start from that offset.
          var readOffset: Long = -1
          var transactionId = partition._3
          val uniqueRecordValue = partition._4._1.Offset
          var processingXformMsg = partition._4._2
          var totalXformMsg = partition._4._3

          var sleepDuration = KafkaSimpleConsumer.SLEEP_DURATION
          var messagesProcessed: Long = 0
          var execThread: ExecContext = null
          val uniqueKey = new KafkaPartitionUniqueRecordKey
          val uniqueVal = new KafkaPartitionUniqueRecordValue
          var readerRunning = true

          clientName = "Client" + qc.Name + "/" + partitionId
          uniqueKey.Name = qc.Name
          uniqueKey.TopicName = qc.topic
          uniqueKey.PartitionId = partitionId

          // Figure out which of the hosts is the leader for the given partition
          val leadBroker: String = getKafkaConfigId(findLeader(qc.hosts, partitionId))

          // Start processing from either a beginning or a number specified by the OnLEPMananger
          readOffset = getKeyValueForPartition(leadBroker, partitionId, kafka.api.OffsetRequest.EarliestTime)
          if (partition._2.Offset > readOffset) {
            readOffset = partition._2.Offset
          }

          // See if we can determine the right offset, bail if we can't
          if (readOffset == -1) {
            LOG.error("KAFKA-ADAPTER: Unable to initialize new reader thread for partition {" + partitionId + "} starting at offset " + readOffset + " on server - " + leadBroker + ", Invalid OFFSET")
            return
          }

          LOG.info("KAFKA-ADAPTER: Initializing new reader thread for partition {" + partitionId + "} starting at offset " + readOffset + " on server - " + leadBroker)

          // If we are forced to retry in case of a failure, get the new Leader.
          //val consumer = brokerConfiguration(leadBroker)
          val brokerId = convertIp(leadBroker)
          val brokerName = brokerId.split(":")
          val consumer = new SimpleConsumer(brokerName(0), brokerName(1).toInt,
            KafkaSimpleConsumer.ZOOKEEPER_CONNECTION_TIMEOUT_MS,
            KafkaSimpleConsumer.FETCHSIZE,
            KafkaSimpleConsumer.METADATA_REQUEST_TYPE)

          // Keep processing until you fail enough times.
          while (readerRunning) {
            val fetchReq = new FetchRequestBuilder().clientId(clientName).addFetch(qc.topic, partitionId, readOffset, KafkaSimpleConsumer.FETCHSIZE).build();

            // Call the broker and get a response.
            val readTmNs = System.nanoTime
            val readTmMs = System.currentTimeMillis
            val fetchResp = consumer.fetch(fetchReq)

            // Check for errors
            if (fetchResp.hasError) {
              LOG.error("KAFKA-ADAPTER: Error occured reading from " + leadBroker + " " + ", error code is " +
                fetchResp.errorCode(qc.topic, partitionId))
              numberOfErrors = numberOfErrors + 1

              if (numberOfErrors > KafkaSimpleConsumer.MAX_FAILURES) {
                LOG.error("KAFKA-ADAPTER: Too many failures reading from kafka adapters.")
                if (consumer != null) {
                  consumer.close
                }
                return
              }
            }

            // Successfuly read from the Kafka Adapter - Process messages
            fetchResp.messageSet(qc.topic, partitionId).foreach(msgBuffer => {
              val bufferPayload = msgBuffer.message.payload
              val message: Array[Byte] = new Array[Byte](bufferPayload.limit)
              readOffset = msgBuffer.nextOffset
              breakable {
                messagesProcessed = messagesProcessed + 1

                // Engine in interested in message at OFFSET + 1, Because I cannot guarantee that offset for a partition
                // is increasing by one, and I cannot simple set the offset to offset++ since that can cause our of
                // range errors on the read, we simple ignore the message by with the offset specified by the engine.
                if (msgBuffer.offset == partition._2.Offset) {
                  LOG.debug("KAFKA-ADAPTER: skipping a message at  Broker: " + leadBroker + "_" + partitionId + " OFFSET " + msgBuffer.offset + " " + new String(message, "UTF-8") + " - previously processed! ")
                  break
                }

                // OK, present this message to the Engine.
                bufferPayload.get(message)
                LOG.debug("KAFKA-ADAPTER: Broker: " + leadBroker + "_" + partitionId + " OFFSET " + msgBuffer.offset + " Message: " + new String(message, "UTF-8"))

                // Create a new EngineMessage and call the engine.
                if (execThread == null) {
                  execThread = mkExecCtxt.CreateExecContext(input, partitionId, output, envCtxt)
                }

                uniqueVal.Offset = msgBuffer.offset
                val dontSendOutputToOutputAdap = uniqueVal.Offset < uniqueRecordValue
                if (dontSendOutputToOutputAdap == false) {
                  processingXformMsg = 0
                  totalXformMsg = 0
                }
                execThread.execute(transactionId, new String(message, "UTF-8"), qc.formatOrInputAdapterName, uniqueKey, uniqueVal, readTmNs, readTmMs, dontSendOutputToOutputAdap, processingXformMsg, totalXformMsg)

                val key = Category + "/" + qc.Name + "/evtCnt"
                cntrAdapter.addCntr(key, 1) // for now adding each row
                transactionId = transactionId + 1
              }

            })

            // Progressively slow down the reading of the Kafka queues while there is nothing in the queues.  Reset to the
            // initial frequency once something is present in the queue.
            if (messagesProcessed > 0) {
              messagesProcessed = 0
              sleepDuration = KafkaSimpleConsumer.SLEEP_DURATION
            } else {
              if ((sleepDuration * 2) > KafkaSimpleConsumer.MAXSLEEP) {
                sleepDuration = KafkaSimpleConsumer.MAXSLEEP
              } else {
                sleepDuration = sleepDuration * 2
              }
            }
            try {
              Thread.sleep(sleepDuration)
            } catch {
              case e: java.lang.InterruptedException =>
                LOG.info("KAFKA ADAPTER: Shutting down the Consumer Reader thread")
                readerRunning = false
            }
          }
          if (consumer != null) { consumer.close }
        }
      })
    })

  }

  /**
   * getServerInfo - returns information about hosts and their coresponding partitions.
   * @return Array[PartitionUniqueRecordKey] - return data
   */
  def GetAllPartitionUniqueRecordKey: Array[PartitionUniqueRecordKey] = lock.synchronized {
    // iterate through all the simple consumers - collect the metadata about this topic on each specified host

    val topics: Array[String] = Array(qc.topic)
    val metaDataReq = new TopicMetadataRequest(topics, KafkaSimpleConsumer.METADATA_REQUEST_CORR_ID)
    var partitionNames: List[KafkaPartitionUniqueRecordKey] = List()

    LOG.info("KAFKA-ADAPTER - Querying kafka for Topic " + qc.topic + " metadata(partitions)")

    qc.hosts.foreach(broker => {
      val brokerName = broker.split(":")
      val partConsumer = new SimpleConsumer(brokerName(0), brokerName(1).toInt,
        KafkaSimpleConsumer.ZOOKEEPER_CONNECTION_TIMEOUT_MS,
        KafkaSimpleConsumer.FETCHSIZE,
        KafkaSimpleConsumer.METADATA_REQUEST_TYPE)
      try {
        val metaDataResp: kafka.api.TopicMetadataResponse = partConsumer.send(metaDataReq)
        val metaData = metaDataResp.topicsMetadata
        metaData.foreach(topicMeta => {
          topicMeta.partitionsMetadata.foreach(partitionMeta => {
            val uniqueKey = new KafkaPartitionUniqueRecordKey
            uniqueKey.PartitionId = partitionMeta.partitionId
            uniqueKey.Name = qc.Name
            uniqueKey.TopicName = qc.topic
            partitionNames = uniqueKey :: partitionNames
          })
        })
      } catch {
        case e: java.lang.InterruptedException => LOG.error("KAFKA-ADAPTER: Communication interrupted with broker " + broker + " while getting a list of partitions")
      } finally {
        if (partConsumer != null) { partConsumer.close }
      }
    })
    return partitionNames.toArray
  }

  /**
   * Return an array of PartitionUniqueKey/PartitionUniqueRecordValues whre key is the partion and value is the offset
   * within the kafka queue where it begins.
   * @return Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)]
   */
  override def getAllPartitionBeginValues: Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)] = lock.synchronized {
    return getKeyValues(kafka.api.OffsetRequest.EarliestTime)
  }

  /**
   * Return an array of PartitionUniqueKey/PartitionUniqueRecordValues whre key is the partion and value is the offset
   * within the kafka queue where it eds.
   * @return Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)]
   */
  override def getAllPartitionEndValues: Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)] = lock.synchronized {
    return getKeyValues(kafka.api.OffsetRequest.LatestTime)
  }

  override def DeserializeKey(k: String): PartitionUniqueRecordKey = {
    val key = new KafkaPartitionUniqueRecordKey
    try {
      LOG.info("Deserializing Key:" + k)
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
  }

  /**
   *  Find a leader of for this topic for a given partition.
   */
  private def findLeader(brokers: Array[String], inPartition: Int): kafka.api.PartitionMetadata = lock.synchronized {
    var leaderMetadata: kafka.api.PartitionMetadata = null

    LOG.info("KAFKA-ADAPTER: Looking for Kafka Topic Leader for partition " + inPartition)
    try {
      breakable {
        brokers.foreach(broker => {
          // Create a connection to this broker to obtain the metadata for this broker.
          val brokerName = broker.split(":")
          val llConsumer = new SimpleConsumer(brokerName(0), brokerName(1).toInt, KafkaSimpleConsumer.ZOOKEEPER_CONNECTION_TIMEOUT_MS,
            KafkaSimpleConsumer.FETCHSIZE, KafkaSimpleConsumer.METADATA_REQUEST_TYPE)
          val topics: Array[String] = Array(qc.topic)
          val llReq = new TopicMetadataRequest(topics, KafkaSimpleConsumer.METADATA_REQUEST_CORR_ID)

          // get the metadata on the llConsumer
          try {
            val llResp: kafka.api.TopicMetadataResponse = llConsumer.send(llReq)
            val metaData = llResp.topicsMetadata

            // look at each piece of metadata, and analyze its partitions
            metaData.foreach(metaDatum => {
              val partitionMetadata = metaDatum.partitionsMetadata
              partitionMetadata.foreach(partitionMetadatum => {
                // If we found the partitionmetadatum for the desired partition then this must be the leader.
                if (partitionMetadatum.partitionId == inPartition) {
                  // Create a list of replicas to be used in case of a fetch failure here.
                  replicaBrokers.empty
                  partitionMetadatum.replicas.foreach(replica => {
                    replicaBrokers = replicaBrokers + (replica.host)
                  })
                  leaderMetadata = partitionMetadatum
                }
              })
            })
          } catch {
            case e: Exception => { LOG.info("KAFKA-ADAPTER: Communicatin problem with broker " + broker + " trace " + e.printStackTrace()) }
          } finally {
            if (llConsumer != null) llConsumer.close()
          }
        })
      }

    } catch {
      case e: Exception => { LOG.info("KAFKA ADAPTER - Fatal Error for FindLeader for partition " + inPartition) }
    }
    return leaderMetadata;
  }

  /*
* getKeyValues - get the values from the OffsetMetadata call and combine them into an array
 */
  private def getKeyValues(time: Long): Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)] = {
    var infoList = List[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)]()
    // Always get the complete list of partitions for this.
    val kafkaKnownParitions = GetAllPartitionUniqueRecordKey
    val partitionList = kafkaKnownParitions.map(uKey => { uKey.asInstanceOf[KafkaPartitionUniqueRecordKey].PartitionId }).toSet

    // Now that we know for sure we have a partition list.. process them
    partitionList.foreach(partitionId => {
      val offset = getKeyValueForPartition(getKafkaConfigId(findLeader(qc.hosts, partitionId)), partitionId, time)
      val rKey = new KafkaPartitionUniqueRecordKey
      val rValue = new KafkaPartitionUniqueRecordValue
      rKey.PartitionId = partitionId
      rKey.Name = qc.Name
      rKey.TopicName = qc.topic
      rValue.Offset = offset
      infoList = (rKey, rValue) :: infoList
    })
    return infoList.toArray
  }

  /**
   * getKeyValueForPartition - get the valid offset range in a given partition.
   */
  def getKeyValueForPartition(leadBroker: String, partitionId: Int, timeFrame: Long): Long = {
    var offset: Long = -1
    var llConsumer: kafka.javaapi.consumer.SimpleConsumer = null
    val brokerName = leadBroker.split(":")
    try {
      llConsumer = new kafka.javaapi.consumer.SimpleConsumer(brokerName(0), brokerName(1).toInt,
        KafkaSimpleConsumer.ZOOKEEPER_CONNECTION_TIMEOUT_MS,
        KafkaSimpleConsumer.FETCHSIZE,
        KafkaSimpleConsumer.METADATA_REQUEST_TYPE)

      // Set up the request object
      val jtap: kafka.common.TopicAndPartition = kafka.common.TopicAndPartition(qc.topic.toString, partitionId)
      val requestInfo: java.util.HashMap[TopicAndPartition, PartitionOffsetRequestInfo] = new java.util.HashMap[TopicAndPartition, PartitionOffsetRequestInfo]()
      requestInfo.put(jtap, new PartitionOffsetRequestInfo(timeFrame, 1))
      val offsetRequest: kafka.javaapi.OffsetRequest = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion, clientName)

      // Issue the call
      val response: kafka.javaapi.OffsetResponse = llConsumer.getOffsetsBefore(offsetRequest)

      // Return the value, or handle the error
      if (response.hasError) {
        LOG.error("KAFKA ADAPTER: error occured trying to find out the valid range for partition {" + partitionId + "}")
      } else {
        val offsets: Array[Long] = response.offsets(qc.topic.toString, partitionId)
        offset = offsets(0)
      }
    } catch {
      case e: java.lang.Exception => { LOG.error("KAFKA ADAPTER: Exception during offset inquiry request for partiotion {" + partitionId + "}") }
    } finally {
      if (llConsumer != null) { llConsumer.close }
    }
    return offset
  }

  /**
   *  Previous request failed, need to find a new leader
   */
  private def findNewLeader(oldBroker: String, partitionId: Int): kafka.api.PartitionMetadata = {
    // There are moving parts in Kafka under the failure condtions, we may not have an immediately availabe new
    // leader, so lets try 3 times to get the new leader before bailing
    for (i <- 0 until 3) {
      try {
        val leaderMetaData = findLeader(replicaBrokers.toArray[String], partitionId)
        // Either new metadata leader is not available or the the new broker has not been updated in kafka
        if (leaderMetaData == null || leaderMetaData.leader == null ||
          (leaderMetaData.leader.get.host.equalsIgnoreCase(oldBroker) && i == 0)) {
          Thread.sleep(KafkaSimpleConsumer.SLEEP_DURATION)
        } else {
          return leaderMetaData
        }
      } catch {
        case e: InterruptedException => { LOG.info("Adapter terminated during findNewLeader") }
      }
    }
    return null
  }

  /**
   * beginHeartbeat - This adapter will begin monitoring the partitions for the specified topic
   */
  def beginHeartbeat(): Unit = lock.synchronized {
    LOG.info("Starting monitor for Kafka QUEUE: " + qc.topic)
    startHeartBeat()
  }

  /**
   *  stopHeartbeat - signal this adapter to shut down the monitor thread
   */
  def stopHearbeat(): Unit = lock.synchronized {
    try {
      hbRunning = false
      hbExecutor.shutdownNow()
    } catch {
      case e: java.lang.InterruptedException => LOG.info("Heartbeat terminated")
    }
  }

  /**
   * Private method to start a heartbeat task, and the code that the heartbeat task will execute.....
   */
  private def startHeartBeat(): Unit = {
    // only start 1 heartbeat
    if (hbRunning) return

    // Block any more heartbeats from being spawned
    hbRunning = true

    // start new heartbeat here.
    hbExecutor.execute(new Runnable() {
      override def run() {
        // Get a connection to each server
        val hbConsumers: Map[String, SimpleConsumer] = Map()
        qc.hosts.foreach(host => {
          val brokerName = host.split(":")
          hbConsumers(host) = new SimpleConsumer(brokerName(0), brokerName(1).toInt,
            KafkaSimpleConsumer.ZOOKEEPER_CONNECTION_TIMEOUT_MS,
            KafkaSimpleConsumer.FETCHSIZE,
            KafkaSimpleConsumer.METADATA_REQUEST_TYPE)
        })

        val topics = Array[String](qc.topic)
        // Get the metadata for each monitored Topic and see if it changed.  If so, notify the engine

        try {
          while (hbRunning) {
            LOG.debug("Heartbeat checking status of " + hbConsumers.size + " broker(s)")
            hbConsumers.foreach {
              case (key, consumer) => {
                val req = new TopicMetadataRequest(topics, KafkaSimpleConsumer.METADATA_REQUEST_CORR_ID)
                val resp: kafka.api.TopicMetadataResponse = consumer.send(req)
                resp.topicsMetadata.foreach(metaTopic => {
                  if (metaTopic.partitionsMetadata.size != hbTopicPartitionNumber) {
                    // TODO: Need to know how to call back to the Engine
                    // first time through the heartbeat
                    if (hbTopicPartitionNumber != -1) {
                      LOG.info("Partitions changed for TOPIC - " + qc.topic + " on broker " + key + ", it is now" + metaTopic.partitionsMetadata.size)
                    }
                    hbTopicPartitionNumber = metaTopic.partitionsMetadata.size
                  }
                })
              }
            }
            try {
              Thread.sleep(KafkaSimpleConsumer.MONITOR_FREQUENCY)
            } catch {
              case e: java.lang.InterruptedException =>
                LOG.info("Shutting down the Monitor heartbeat")
                hbRunning = false
            }
          }
        } catch {
          case e: java.lang.Exception => LOG.error("Heartbeat forced down due to exception + " + e.printStackTrace())
        } finally {
          hbConsumers.foreach({ case (key, consumer) => { consumer.close } })
          hbRunning = false
          LOG.info("Monitor is down")
        }
      }
    })
  }

  /**
   *  Convert the "localhost:XXXX" into an actual IP address.
   */
  private def convertIp(inString: String): String = {
    val brokerName = inString.split(":")
    if (brokerName(0).equalsIgnoreCase("localhost")) {
      brokerName(0) = InetAddress.getLocalHost().getHostAddress()
    }
    val brokerId = brokerName(0) + ":" + brokerName(1)
    brokerId
  }

  /**
   * combine the ip address and port number into a Kafka Configuratio ID
   */
  private def getKafkaConfigId(metadata: kafka.api.PartitionMetadata): String = {
    return metadata.leader.get.host + ":" + metadata.leader.get.port;
  }

  /**
   *
   */
  private def terminateHBTasks(): Unit = {
    if (hbExecutor == null) return

    hbExecutor.shutdown
    try {
      // Wait a while for existing tasks to terminate
      if (!hbExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
        hbExecutor.shutdownNow
        if (!hbExecutor.awaitTermination(10, TimeUnit.SECONDS))
          LOG.info("heartbeat pool did not terminate")
      }
    } catch {
      case e: InterruptedException =>
        LOG.error("FATAL + " + e.printStackTrace())
        Thread.currentThread().interrupt();
    }
  }

  /**
   *
   */
  private def terminateReaderTasks(): Unit = {

    if (readExecutor == null) return

    readExecutor.shutdown
    try {
      // Wait a while for existing tasks to terminate
      if (!readExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
        readExecutor.shutdownNow
        if (!readExecutor.awaitTermination(10, TimeUnit.SECONDS))
          LOG.info("readpool did not terminate")
      }
    } catch {
      case e: InterruptedException =>
        LOG.error("FATAL + " + e.printStackTrace())
        Thread.currentThread().interrupt();
    }
  }

}
