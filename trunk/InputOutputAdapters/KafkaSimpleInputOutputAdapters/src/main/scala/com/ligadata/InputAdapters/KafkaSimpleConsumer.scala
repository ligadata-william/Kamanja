
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
  var CURRENT_BROKER: String = _
  val FETCHSIZE = 64 * 1024
  val ZOOKEEPER_CONNECTION_TIMEOUT_MS = 3000
  def CreateInputAdapter(inputConfig: AdapterConfiguration, output: Array[OutputAdapter], envCtxt: EnvContext, mkExecCtxt: MakeExecContext, cntrAdapter: CountersAdapter): InputAdapter = new KafkaSimpleConsumer(inputConfig, output, envCtxt, mkExecCtxt, cntrAdapter)

  /**
   *  convert the ip:port string into the structure that we can use.
   */
  private def convertIp(inString: String): (String, Boolean, String, Int) =  {
    val brokerName = inString.split(":")
    var isLocalHost: Boolean = false

    if (brokerName(0).equalsIgnoreCase("localhost")) {
      brokerName(0) = getLocalHostIP
      isLocalHost = true
    }
    val brokerId = brokerName(0) + ":" + brokerName(1)
    return (brokerId,isLocalHost,brokerName(0),brokerName(1).toInt)
  }

  /*
  * Get the ip of the local host
   */
  private def getLocalHostIP(): String = {
    return InetAddress.getLocalHost().getHostAddress()
  }

  /*
  * Used to see if the LOCALHOST at the time of this specific method invocation is different that it was prior.  LOCALHOST
  * IP address can actually change, and the each Read Thread Consumer is tied to a physical IP address and needs to be
  * refreshed.
   */
  private def didIpChange(current: String, past: String ): Boolean = {
    val brokerName = current.split(":")
    if (brokerName(0).equalsIgnoreCase("localhost")) {
      brokerName(0) =  getLocalHostIP()
      if (!past.equalsIgnoreCase(brokerName(0))) {
        return true
      }
    }
    return false
  }
}

class KafkaSimpleConsumer(val inputConfig: AdapterConfiguration, val output: Array[OutputAdapter], val envCtxt: EnvContext, val mkExecCtxt: MakeExecContext, cntrAdapter: CountersAdapter) extends InputAdapter {
  val input = this
  private val lock = new Object()
  private val LOG = Logger.getLogger(getClass)
  private var isQuiesced = false
  private var startTime: Long = 0

  private val qc = KafkaQueueAdapterConfiguration.GetAdapterConfig(inputConfig)

  LOG.info("KAFKA ADAPTER: allocating kafka adapter for " + qc.hosts.size + " broker hosts")

  private var numberOfErrors: Int = _
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
  def StartProcessing(maxParts: Int, partitionIds: Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue, Long, (PartitionUniqueRecordValue, Int, Int))], ignoreFirstMsg: Boolean): Unit = lock.synchronized {

    // Check to see if this already started
    if (startTime > 0) {
      LOG.error("KAFKA-ADAPTER: already started, or in the process of shutting down")
    }
    startTime = System.nanoTime
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

    // Enable the adapter to process
    isQuiesced = false
    LOG.debug("KAFKA-ADAPTER: Starting " + kvs.size + " threads to process partitions")

    // Schedule a task to perform a read from a give partition.
    kvs.foreach(kvsElement => {
      readExecutor.execute(new Runnable() {
        override def run() {
          var replicaBrokers: Set[String] = Set()
          var isLocalHost = false
          var localHostResolvedAddress = ""
          val partitionId = kvsElement._1
          val partition = kvsElement._2

          // if the offset is -1, then the server wants to start from the begining, else, it means that the server
          // knows what its doing and we start from that offset.
          var readOffset: Long = -1
          var transactionId = partition._3
          val uniqueRecordValue = if (ignoreFirstMsg) partition._4._1.Offset else partition._4._1.Offset - 1
          var processingXformMsg = partition._4._2
          var totalXformMsg = partition._4._3

          var sleepDuration = KafkaSimpleConsumer.SLEEP_DURATION
          var messagesProcessed: Long = 0
          var execThread: ExecContext = null
          val uniqueKey = new KafkaPartitionUniqueRecordKey
          val uniqueVal = new KafkaPartitionUniqueRecordValue

          clientName = "Client" + qc.Name + "/" + partitionId
          uniqueKey.Name = qc.Name
          uniqueKey.TopicName = qc.topic
          uniqueKey.PartitionId = partitionId

          // Figure out which of the hosts is the leader for the given partition
          val leadBrokerMeta = findLeader(qc.hosts, partitionId)
          replicaBrokers = leadBrokerMeta._2
          isLocalHost = leadBrokerMeta._3
          var leadBroker: String = getKafkaConfigId(leadBrokerMeta._1)
          val brokerId = KafkaSimpleConsumer.convertIp(leadBroker)
          localHostResolvedAddress = brokerId._3

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

          // Create a new consumer object for this read thread.
          var consumer = new SimpleConsumer(brokerId._3, brokerId._4,
                                            KafkaSimpleConsumer.ZOOKEEPER_CONNECTION_TIMEOUT_MS,
                                            KafkaSimpleConsumer.FETCHSIZE,
                                            KafkaSimpleConsumer.METADATA_REQUEST_TYPE)

          // Keep processing until you fail enough times.
          while (!isQuiesced) {
            val fetchReq = new FetchRequestBuilder().clientId(clientName).addFetch(qc.topic, partitionId, readOffset, KafkaSimpleConsumer.FETCHSIZE).build();

            // Call the broker and get a response.
            val readTmNs = System.nanoTime
            val readTmMs = System.currentTimeMillis

            // Call the Kafka server...
            val fetchResp = consumer.fetch(fetchReq)

            // Check for errors -
            //  if too many occured.. bail.  otherwise, check to see if LocalHost changed under us and try reading again.
            //  if that was not the issue, then we'll fall into a path to find a new leader and try reading again.
            //  eventually, we'll run out of replic server and bail from this READER loop.
            if (fetchResp.hasError) {
              LOG.error("KAFKA-ADAPTER: Error occured reading from " + leadBroker + " " + ", error code is " + fetchResp.errorCode(qc.topic, partitionId))

              // for now we dont have scenarios when we expect an error on the FETCH call that do not require rebuilding of the consumer object...
              if (consumer != null) consumer.close

              // A weird scenario here.  A LOCALHOST/Or another alias ip udress can technically change from under us...  This not really an
              // error that Kafka can tell us via a bad Reason Code, so we check here that its not the case.
              // NOTE: we dont worry about a port number change here, since that would need to be done explicitely.
              if (isLocalHost && KafkaSimpleConsumer.didIpChange(leadBroker, localHostResolvedAddress)) {
                val newip = KafkaSimpleConsumer.getLocalHostIP
                val curBrokerInfo = leadBroker.split(":")
                localHostResolvedAddress = newip
                consumer = new SimpleConsumer(newip, curBrokerInfo(1).toInt,
                                              KafkaSimpleConsumer.ZOOKEEPER_CONNECTION_TIMEOUT_MS,
                                              KafkaSimpleConsumer.FETCHSIZE,
                                              KafkaSimpleConsumer.METADATA_REQUEST_TYPE)
              } else {
                val newLeaderMeta = findNewLeader(leadBroker, partitionId, replicaBrokers)
                replicaBrokers = newLeaderMeta._2
                isLocalHost = newLeaderMeta._3
                leadBroker = getKafkaConfigId(newLeaderMeta._1)

                // if we reach the end of the road here, return.
                if (leadBroker.equalsIgnoreCase("")) {
                  return
                }
                val brokerId = KafkaSimpleConsumer.convertIp(leadBroker)
                localHostResolvedAddress = brokerId._3
                consumer = new SimpleConsumer(brokerId._3, brokerId._4,
                                              KafkaSimpleConsumer.ZOOKEEPER_CONNECTION_TIMEOUT_MS,
                                              KafkaSimpleConsumer.FETCHSIZE,
                                              KafkaSimpleConsumer.METADATA_REQUEST_TYPE)
              }
            }

            val ignoreTillOffset = if (ignoreFirstMsg) partition._2.Offset else partition._2.Offset - 1

            // Successfuly read from the Kafka Adapter - Process messages.  If an error occured, then this loop will not get
            // executed and we will try to read again.
            fetchResp.messageSet(qc.topic, partitionId).foreach(msgBuffer => {
              val bufferPayload = msgBuffer.message.payload
              val message: Array[Byte] = new Array[Byte](bufferPayload.limit)
              readOffset = msgBuffer.nextOffset
              breakable {
                messagesProcessed = messagesProcessed + 1

                // Engine in interested in message at OFFSET + 1, Because I cannot guarantee that offset for a partition
                // is increasing by one, and I cannot simple set the offset to offset++ since that can cause our of
                // range errors on the read, we simple ignore the message by with the offset specified by the engine.
                if (msgBuffer.offset <= ignoreTillOffset) {
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
                val dontSendOutputToOutputAdap = uniqueVal.Offset <= uniqueRecordValue
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

            try {
              // Sleep here, Note, the input constant may be zero (if it is zero, balls to the wall!) better be a very busy system.
              if (qc.noDataSleepTimeInMs > 0) {
                Thread.sleep(qc.noDataSleepTimeInMs)
              }
            } catch {
              case e: java.lang.InterruptedException =>
                LOG.info("KAFKA ADAPTER: Forcing down the Consumer Reader thread")
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
  private def findLeader(brokers: Array[String], inPartition: Int): (kafka.api.PartitionMetadata, Set[String], Boolean) = lock.synchronized {
    var leaderMetadata: kafka.api.PartitionMetadata = null
    var isLocalHost: Boolean = false
    var replicaBrokers: Set[String] = Set()

    LOG.info("KAFKA-ADAPTER: Looking for Kafka Topic Leader for partition " + inPartition)
    breakable {
      try {
          brokers.foreach(broker => {
            // Create a connection to this broker to obtain the metadata for this broker.
            //val brokerName = broker.split(":")
            isLocalHost = false
            val brokerStructure = KafkaSimpleConsumer.convertIp(broker)
            if (brokerStructure._2) {
              isLocalHost = true
            }
            val llConsumer = new SimpleConsumer(brokerStructure._3, brokerStructure._4,
                                                KafkaSimpleConsumer.ZOOKEEPER_CONNECTION_TIMEOUT_MS,
                                                KafkaSimpleConsumer.FETCHSIZE,
                                                KafkaSimpleConsumer.METADATA_REQUEST_TYPE)
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
                    partitionMetadatum.replicas.foreach(replica => {
                      replicaBrokers = replicaBrokers + (replica.host)
                    })
                    leaderMetadata = partitionMetadatum
                    break
                  }
                })
              })
            } catch {
              case e: Exception => {
                LOG.info("KAFKA-ADAPTER: Communicatin problem with broker " + broker + " trace " + e.printStackTrace())
              }
            } finally {
              if (llConsumer != null) llConsumer.close()
            }
          })
      } catch {
        case e: Exception => {
          LOG.error("KAFKA ADAPTER - Fatal Error for FindLeader for partition " + inPartition)
        }
      }
    }
    return (leaderMetadata,replicaBrokers,isLocalHost);
  }

  /*
* getKeyValues - get the values from the OffsetMetadata call and combine them into an array
 */
  private def getKeyValues(timeFrame: Long): Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)] = {
    var infoList = List[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)]()
    // Always get the complete list of partitions for this.
    val kafkaKnownParitions = GetAllPartitionUniqueRecordKey
    val partitionList = kafkaKnownParitions.map(uKey => { uKey.asInstanceOf[KafkaPartitionUniqueRecordKey].PartitionId }).toSet

    // Now that we know for sure we have a partition list.. process them
    partitionList.foreach(partitionId => {
      val offset = getKeyValueForPartition(getKafkaConfigId(findLeader(qc.hosts, partitionId)._1), partitionId, timeFrame)
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
  private def findNewLeader(oldBroker: String, partitionId: Int, replicaBrokers: Set[String]): (kafka.api.PartitionMetadata, Set[String], Boolean)= {
    // There are moving parts in Kafka under the failure condtions, we may not have an immediately availabe new
    // leader, so lets try 3 times to get the new leader before bailing
    for (i <- 0 until 3) {
      try {
        val leaderMetaData = findLeader(replicaBrokers.toArray[String], partitionId)
        // Either new metadata leader is not available or the the new broker has not been updated in kafka
        if (leaderMetaData == null || leaderMetaData._1.leader == null ||
          (leaderMetaData._1.leader.get.host.equalsIgnoreCase(oldBroker) && i == 0)) {
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
   * combine the ip address and port number into a Kafka Configuratio ID
   */
  private def getKafkaConfigId(metadata: kafka.api.PartitionMetadata): String = {
    if(metadata != null)
      return metadata.leader.get.host + ":" + metadata.leader.get.port;
    else
      return "";
  }

  /**
   * terminateHBTasks - Just what it says
   */
  private def terminateHBTasks(): Unit = {
    if (hbExecutor == null) return
    hbExecutor.shutdownNow
    while (hbExecutor.isTerminated == false ) {
      Thread.sleep(100) // sleep 100ms and then check
    }
  }

  /**
   *  terminateReaderTasks - well, just what it says
   */
  private def terminateReaderTasks(): Unit = {
    if (readExecutor == null) return

    // Tell all thread to stop processing on the next interval, and shutdown the Excecutor.
    quiesce

    // Give the threads to gracefully stop their reading cycles, and then execute them with extreme prejudice.
    Thread.sleep(qc.noDataSleepTimeInMs + 1)
    readExecutor.shutdownNow
    while (readExecutor.isTerminated == false ) {
      Thread.sleep(100)
    }

    LOG.info("KAFKA_ADAPTER - Shutdown Complete")
    readExecutor = null
    startTime = 0
  }

  /* no need for any synchronization here... it can only go one way.. worst case scenario, a reader thread gets to try to
  *  read the kafka queue one extra time (100ms lost)
   */
  private def quiesce: Unit = {
    isQuiesced = true
  }

}

