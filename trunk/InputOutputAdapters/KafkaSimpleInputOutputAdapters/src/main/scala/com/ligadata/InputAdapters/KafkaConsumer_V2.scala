
package com.ligadata.InputAdapters

import com.ligadata.AdaptersConfiguration.{KafkaPartitionUniqueRecordValue, KafkaPartitionUniqueRecordKey, KafkaQueueAdapterConfiguration}
import com.ligadata.OnLEPBase._
import kafka.api.{FetchRequestBuilder,TopicMetadataRequest}
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.apache.zookeeper.data.Stat
import scala.actors.threadpool.{TimeUnit, ExecutorService, Executors}
import scala.util.control.Breaks._
import scala.collection.mutable._
import kafka.consumer.{ SimpleConsumer }
import java.net.{ InetAddress }
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.log4j.Logger


object KafkaConsumer_V2 extends InputAdapterObj {
  val METADATA_REQUEST_CORR_ID = 2
  val QUEUE_FETCH_REQUEST_TYPE = 1
  val METADATA_REQUEST_TYPE = "metadataLookup"
  val MAX_FAILURES = 2
  val MONITOR_FREQUENCY = 10000  // Monitor Topic queues every 20 seconds
  val SLEEP_DURATION = 1000 // Allow 1 sec between unsucessful fetched
  val MAXSLEEP = 8001
  var CURRENT_BROKER: String = _
  val FETCHSIZE = 64 * 1024
  val ZOOKEEPER_CONNECTION_TIMEOUT_MS = 3000
  def CreateInputAdapter(inputConfig: AdapterConfiguration, output: Array[OutputAdapter], envCtxt: EnvContext, mkExecCtxt: MakeExecContext, cntrAdapter: CountersAdapter): InputAdapter = new KafkaConsumer_V2(inputConfig, output, envCtxt, mkExecCtxt, cntrAdapter)
}

class KafkaConsumer_V2(val inputConfig: AdapterConfiguration, val output: Array[OutputAdapter], val envCtxt: EnvContext, val mkExecCtxt: MakeExecContext, cntrAdapter: CountersAdapter) extends InputAdapter {
  val input = this
  private val lock = new Object()
  private val LOG = Logger.getLogger(getClass);
  // For Kafka Queue we expect the format "Type~Name~Host/Brokers~Group/Client~MaxPartitions~InstancePartitions~ClassName~JarName~DependencyJars"
  if (inputConfig.adapterSpecificTokens.size != 4) {
    val err = "We should find only Type, Name, ClassName, JarName, DependencyJarsm Host/Brokers, Group/Client, MaxPartitions & Set of Handled Partitions for Kafka Queue Adapter Config:" + inputConfig.Name
    LOG.error(err)
    throw new Exception(err)
  }

  private val brokerConfiguration: Map[String,SimpleConsumer] = Map()
  private var numberOfErrors: Int = _

  //Set up the QC to store the adapter configuration
  private val qc = new KafkaQueueAdapterConfiguration

  // get the config values from the input
  qc.Typ = inputConfig.Typ
  qc.Name = inputConfig.Name
  qc.className = inputConfig.className
  qc.jarName = inputConfig.jarName
  qc.dependencyJars = inputConfig.dependencyJars
  qc.hosts = inputConfig.adapterSpecificTokens(0).split(",").map(str => str.trim).filter(str => str.size > 0).map(str => convertIp(str))
  qc.groupName = inputConfig.adapterSpecificTokens(1)
  qc.instancePartitions = Array[Int]().toSet

  // Prepare the cache of Simple Consumers to service later request.
  qc.hosts.foreach(str => {
    val brokerId = convertIp(str)
    val brokerName =str.split(":")
    brokerConfiguration(brokerId) = new SimpleConsumer(brokerName(0), brokerName(1).toInt,
                                                       KafkaConsumer_V2.ZOOKEEPER_CONNECTION_TIMEOUT_MS,
                                                       KafkaConsumer_V2.FETCHSIZE,
                                                       KafkaConsumer_V2.METADATA_REQUEST_TYPE)
  })

  // We only read 1 partition at a time here, so  we need a thread for a heartbeat and a thread for a fetch.
  private var replicaBrokers: Set[String] = Set()
  private var readExecutor: ExecutorService =  _

  // Heartbeat monitor related variables.
  private var hbRunning: Boolean = false
  private var hbTopicPartitionNumber = -1
  private var monitoredTopics: Map[String,Int] = Map()
  private val hbExecutor = Executors.newFixedThreadPool(qc.hosts.size)

  /**
   *  This will stop all the running reading threads and close all the underlying connections to Kafka.
   */
  override def Shutdown(): Unit = lock.synchronized {
    StopProcessing
    brokerConfiguration.foreach { case (key, consumer) => { consumer.close }}
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
   * obtained via a prior getServerInfo call.  One of the hosts will be a chosen as a leader to service the requests by the
   * spawned threads.
   * @param maxParts Int - Number of Partitions
   * @param partitionIds Array[String] - an Array of partition ids
   * @param partitionOffsets Array[String] - a coresponding array of start offsets.
   */
  def StartProcessing(maxParts: Int, partitionIds: Array[String], partitionOffsets: Array[String]): Unit = lock.synchronized {

    LOG.info("Starting to read Kafka queues for topic: "+qc.Name)

    // Check for some error stuff.
    if (partitionIds.size != partitionOffsets.size || partitionIds.size == 0 || partitionOffsets.size == 0) {
      LOG.error("ERROR: invalid parameters given to start processing kafka adapter for topic " + qc.Name)
      LOG.error("ERROR: partitionIds size = " + partitionIds.size + ", and partitionOffsets size = " + partitionOffsets.size)
      return
    }

    // Create a Map of target partition to Offset
    val allPartitions = Map[Int,Long]()
    for(i <- 0 until partitionIds.size) {
      allPartitions(partitionIds(i).toInt) = partitionOffsets(i).toLong
    }

    // create a list of of partitions to be used during this processing.
    qc.instancePartitions = partitionIds.map(k => {k.toInt}).toSet

    // Figure out the size of the thread pool to use and create that pool
    var threads = maxParts
    if (threads == 0) {
      if (qc.instancePartitions.size == 0)
        threads = 1
      else
        threads = qc.instancePartitions.size
    }
    readExecutor = Executors.newFixedThreadPool(threads)

    // Schedule a task to perform a read from a give partition.
    allPartitions.foreach(partition =>{
      readExecutor.execute(new Runnable() {
        override def run() {
          // Set up a fetch call to this broker
          val partitionId = partition._1
          var readOffset = partition._2
          val clientName: String = "Client_" + qc.Name + "_" + partitionId;
          var sleepDuration = KafkaConsumer_V2.SLEEP_DURATION
          var messagesProcessed: Long = 0
          var execThread: ExecContext = null
          val uniqueKey = new KafkaPartitionUniqueRecordKey
          val uniqueVal = new KafkaPartitionUniqueRecordValue
          var readerRunning = true

          uniqueKey.TopicName = qc.Name
          uniqueKey.PartitionId = partitionId

          // Figure out which of the hosts is the leader for the given partition
          val leadBroker = getKafkaConfigId(findLeader(qc.hosts, partitionId))

          LOG.info("Initializing new reader thread for partition {"+partitionId+"} on server - " + leadBroker)

          // Keep processing until you fail enough times.
          while (readerRunning) {

            // If we are forced to retry in case of a failure, get the new Leader.
            val consumer = brokerConfiguration(leadBroker)
            val fetchReq = new FetchRequestBuilder().clientId(clientName).addFetch(qc.Name, partitionId, readOffset, KafkaConsumer_V2.FETCHSIZE).build();

            // Call the broker and get a response.
            val readTmNs = System.nanoTime
            val readTmMs = System.currentTimeMillis
            val fetchResp = consumer.fetch(fetchReq)

            // Check for errors
            if (fetchResp.hasError) {
              LOG.error("Error occured reading from " + leadBroker + " " + ", error code is " +
                         fetchResp.errorCode(qc.Name, partitionId))
              numberOfErrors = numberOfErrors + 1

              if (numberOfErrors > KafkaConsumer_V2.MAX_FAILURES) {
                LOG.error("Too many failures reading from kafka adapters.")
                if (consumer != null) {
                  consumer.close
                }
                return
              }
            }

            // Successfuly read from the Kafka Adapter - Process messages
            fetchResp.messageSet(qc.groupName, partitionId).foreach(msgBuffer => {
              val bufferPayload = msgBuffer.message.payload
              val message: Array[Byte] = new Array[Byte](bufferPayload.limit)
              readOffset = msgBuffer.nextOffset
              messagesProcessed = messagesProcessed + 1
              bufferPayload.get(message)

              if (true) {
                LOG.info("Broker: "+leadBroker+"_"+partitionId+ " OFFSET " + msgBuffer.offset + " Message: " + new String(message, "UTF-8"))
              } else {
                //  Create a new EngineMessage and call the engine.
                if (execThread == null) {
                  execThread = mkExecCtxt.CreateExecContext(input, partitionId, output, envCtxt)
                }

                uniqueVal.Offset = msgBuffer.offset
                execThread.execute(new String(message, "UTF-8"), uniqueKey, uniqueVal, readTmNs, readTmMs)

                val key = Category + "/" + qc.Name + "/evtCnt"
                cntrAdapter.addCntr(key, 1) // for now adding each row
              }

            })

            if (messagesProcessed > 0) {
              messagesProcessed = 0
              sleepDuration = KafkaConsumer_V2.SLEEP_DURATION
            } else {
              if ((sleepDuration * 2) > KafkaConsumer_V2.MAXSLEEP) {
                sleepDuration = KafkaConsumer_V2.MAXSLEEP
              } else {
                sleepDuration = sleepDuration * 2
              }
            }
            try {
              Thread.sleep(sleepDuration)
            } catch {
              case e: java.lang.InterruptedException => LOG.info("Shutting down the Consumer Reader thread")
                readerRunning = false
            }
          }
        }
      })
    })

  }


  /**
   * getServerInfo - returns information about hosts and their coresponding partitions.
   * @return Map[String,List[Int]] - return data
   */
  def getAdapterMetadata(): Map[String,List[Int]] =  lock.synchronized {
    var returnInfo:  Map[String,List[Int]]  = Map[String,List[Int]]()

    // iterate through all the simple consumers - collect the metadata about this topic on each specified host
    val topics: Array[String] = Array(qc.Name)
    val metaDataReq = new TopicMetadataRequest(topics, KafkaConsumer_V2.METADATA_REQUEST_CORR_ID)

    brokerConfiguration.foreach {
      case (key,consumer) => {
        try {
          val metaDataResp: kafka.api.TopicMetadataResponse = consumer.send(metaDataReq)
          val metaData = metaDataResp.topicsMetadata
          metaData.foreach(topicMeta => {
            var partitionNames: List[Int] = List()
            topicMeta.partitionsMetadata.foreach(partitionMeta => {
              partitionNames = partitionMeta.partitionId :: partitionNames
            })
            returnInfo(key) = partitionNames
            //returnInfo =  returnInfo.++(Map(key -> partitionNames))
          })
        } catch {
          case e: Exception => {LOG.error("Communication problem with broker " + key)}
        }
      }
    }
    return returnInfo
  }

  /**
   * Need to clarify with pokouri as to what the use case for this method is.
   * @return - Array[String]
   */
  def GetAllPartitionUniqueRecordKey: Array[String] = lock.synchronized {
    GetAllPartitionsUniqueKeys
  }

  /**
   *  Find a leader of for this topic for a given partition.
   */
  private def findLeader(brokers: Array[String], inPartition: Int): kafka.api.PartitionMetadata = lock.synchronized{
    var leaderMetadata: kafka.api.PartitionMetadata = null

    try {
      breakable {
        brokers.foreach(broker => {
          // Create a connection to this broker to obtain the metadata for this broker.
          val brokerName = broker.split(":")
          val llConsumer = new SimpleConsumer(brokerName(0), brokerName(1).toInt, KafkaConsumer_V2.ZOOKEEPER_CONNECTION_TIMEOUT_MS,
                                              KafkaConsumer_V2.FETCHSIZE, KafkaConsumer_V2.METADATA_REQUEST_TYPE)
          val topics: Array[String] = Array(qc.Name)
          val llReq = new TopicMetadataRequest(topics, KafkaConsumer_V2.METADATA_REQUEST_CORR_ID)

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
            case e: Exception => {LOG.info("Communicatin problem with broker " + broker + " trace "+e.printStackTrace())}
          } finally {
            if (llConsumer != null) llConsumer.close()
          }
        })
      }

    } catch {
      case e: Exception => {LOG.info("FindLeader ERROR for partition "+ inPartition)}
    }
    return leaderMetadata;
  }

  /**
   *  Previous request failed, need to find a new leader
   */
  private def findNewLeader(oldBroker: String, partitionId: Int ): kafka.api.PartitionMetadata = {
    // There are moving parts in Kafka under the failure condtions, we may not have an immediately availabe new
    // leader, so lets try 3 times to get the new leader before bailing
    for (i <- 0 until 3) {
      try {
        val leaderMetaData = findLeader(replicaBrokers.toArray[String], partitionId)
        // Either new metadata leader is not available or the the new broker has not been updated in kafka
        if (leaderMetaData == null || leaderMetaData.leader == null ||
            (leaderMetaData.leader.get.host.equalsIgnoreCase(oldBroker) && i == 0)) {
          Thread.sleep(KafkaConsumer_V2.SLEEP_DURATION)
        }
        else {
          return leaderMetaData
        }
      } catch {
        case e: InterruptedException => {LOG.info("Adapter terminated during findNewLeader")}
      }
    }
    return null
  }

  /**
   *
   */
  private def GetAllPartitionsUniqueKeys: Array[String] = {
    val zkClient = new ZkClient(qc.hosts.mkString(","), 30000, 30000, ZKStringSerializer)
    val jsonPartitionMapOpt = readDataMaybeNull(zkClient, getTopicPath(qc.Name))._1
    zkClient.close

    if (jsonPartitionMapOpt == None) {
      LOG.error("Not found any JSON Partitions for Queue: " + qc.Name)
      return null
    }

    LOG.info("JSON Partitions:%s".format(jsonPartitionMapOpt.get))

    val json = {
      parse(jsonPartitionMapOpt.get)
    }

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

  /**
   *
   */
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

  /**
   *
   */
  private def getTopicPath(topic: String): String = {
    val BrokerTopicsPath = "/brokers/topics"
    BrokerTopicsPath + "/" + topic
  }

  /**
   * beginHeartbeat - This adapter will begin monitoring the partitions for the specified topic
   */
  def beginHeartbeat (): Unit = lock.synchronized{
    LOG.info("Starting monitor for Kafka QUEUE: " + qc.Name)
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
        val hbConsumers: Map[String,SimpleConsumer] = Map()
        qc.hosts.foreach(host => {
          val brokerName = host.split(":")
          hbConsumers(host) = new SimpleConsumer(brokerName(0), brokerName(1).toInt,
                                                KafkaConsumer_V2.ZOOKEEPER_CONNECTION_TIMEOUT_MS,
                                                KafkaConsumer_V2.FETCHSIZE,
                                                KafkaConsumer_V2.METADATA_REQUEST_TYPE)
        })

        val topics = Array[String](qc.Name)
        // Get the metadata for each monitored Topic and see if it changed.  If so, notify the engine

        try {
          while (hbRunning) {
            LOG.info("Heartbeat checking status of " + hbConsumers.size + " broker(s)")
            hbConsumers.foreach {
              case (key,consumer) => {
                val req = new TopicMetadataRequest  (topics, KafkaConsumer_V2.METADATA_REQUEST_CORR_ID)
                val resp: kafka.api.TopicMetadataResponse = consumer.send(req)
                resp.topicsMetadata.foreach(metaTopic => {
                  if (metaTopic.partitionsMetadata.size != hbTopicPartitionNumber) {
                    // TODO: Need to know how to call back to the Engine
                    // first time through the heartbeat
                    if (hbTopicPartitionNumber != -1) {
                      LOG.info("Partitions changed for TOPIC - " + qc.Name + " on broker " + key + ", it is now" + metaTopic.partitionsMetadata.size)
                    }
                    hbTopicPartitionNumber = metaTopic.partitionsMetadata.size
                  }
                })
              }
            }
            try {
              Thread.sleep(KafkaConsumer_V2.MONITOR_FREQUENCY)
            } catch {
              case e: java.lang.InterruptedException => LOG.info("Shutting down the Monitor heartbeat")
                hbRunning = false
            }
          }
        } catch {
          case e: java.lang.Exception => LOG.error("Heartbeat forced down due to exception + " + e.printStackTrace())
        } finally {
          hbConsumers.foreach({ case (key, consumer) => { consumer.close }})
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
    val brokerId = brokerName(0) +":"+brokerName(1)
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
  private def terminateHBTasks (): Unit = {
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
      case e: InterruptedException => LOG.error("FATAL + " + e.printStackTrace())
        Thread.currentThread().interrupt();
    }
  }

  /**
   *
   */
  private def terminateReaderTasks (): Unit = {

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
      case e: InterruptedException => LOG.error("FATAL + " + e.printStackTrace())
        Thread.currentThread().interrupt();
    }
  }

}

