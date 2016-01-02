
/*
 * Copyright 2015 ligaDATA
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ligadata.OutputAdapters

import java.util.{ Properties, Arrays }
import kafka.common.{ QueueFullException, FailedToSendMessageException }
import org.apache.logging.log4j.{ Logger, LogManager }
import com.ligadata.InputOutputAdapterInfo.{ AdapterConfiguration, OutputAdapter, OutputAdapterObj, CountersAdapter }
import com.ligadata.AdaptersConfiguration.{ KafkaConstants, KafkaQueueAdapterConfiguration }
import com.ligadata.Exceptions.{ FatalAdapterException, StackTrace }
import scala.collection.mutable.ArrayBuffer

import org.apache.kafka.clients.producer.{ Callback, RecordMetadata, ProducerRecord }
import org.apache.kafka.common.serialization.{ ByteArraySerializer /*, StringSerializer */ }
import java.util.concurrent.{ TimeUnit, Future }
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong
import scala.actors.threadpool.{ TimeUnit, ExecutorService, Executors }
import java.util.concurrent.locks.ReentrantReadWriteLock;

object KafkaProducer extends OutputAdapterObj {
  def CreateOutputAdapter(inputConfig: AdapterConfiguration, cntrAdapter: CountersAdapter): OutputAdapter = new KafkaProducer(inputConfig, cntrAdapter)
}

// http://kafka.apache.org/documentation.html
// New Producer configs are found @ http://kafka.apache.org/082/documentation.html#newproducerconfigs
// We still have ordering issues with Kafka. Once case is, if Kafka goes down and comes back and if we have list of new messages to send before it trigger failure, the new messages may go first

class KafkaProducer(val inputConfig: AdapterConfiguration, cntrAdapter: CountersAdapter) extends OutputAdapter {
  private[this] val LOG = LogManager.getLogger(getClass);

  //BUGBUG:: Not Checking whether inputConfig is really QueueAdapterConfiguration or not. 
  private[this] val qc = KafkaQueueAdapterConfiguration.GetAdapterConfig(inputConfig)

  val default_compression_type = "none" // Valida values at this moment are none, gzip, or snappy.
  val default_value_serializer = "org.apache.kafka.common.serialization.ByteArraySerializer"
  val default_key_serializer = "org.apache.kafka.common.serialization.ByteArraySerializer"
  val default_batch_size = "1024"
  val default_linger_ms = "50" // 50ms
  // val default_retries = "0"
  val default_block_on_buffer_full = "true" // true or false
  val default_buffer_memory = "16777216" // 16MB
  val default_client_id = qc.Name + "_" + hashCode.toString
  val default_request_timeout_ms = "10000"
  val default_timeout_ms = "10000"
  val default_metadata_fetch_timeout_ms = "10000"
  val defrault_metadata_max_age_ms = "20000"
  val default_max_block_ms = "20000"
  val default_max_buffer_full_block_ms = "100"
  val default_network_request_timeout_ms = "20000"
  val default_outstanding_messages = "2048"

  val linger_ms = qc.otherconfigs.getOrElse("linger.ms", default_linger_ms).toString.trim()
  val timeout_ms = qc.otherconfigs.getOrElse("timeout.ms", default_timeout_ms).toString.trim()
  val metadata_fetch_timeout_ms = qc.otherconfigs.getOrElse("metadata.fetch.timeout.ms", default_metadata_fetch_timeout_ms).toString.trim()

  // Set up some properties for the Kafka Producer
  val props = new Properties()
  props.put("bootstrap.servers", qc.hosts.mkString(",")); // ProducerConfig.BOOTSTRAP_SERVERS_CONFIG
  props.put("compression.type", qc.otherconfigs.getOrElse("compression.type", default_compression_type).toString.trim()); // ProducerConfig.COMPRESSION_TYPE_CONFIG
  props.put("value.serializer", qc.otherconfigs.getOrElse("value.serializer", default_value_serializer).toString.trim()); // ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG
  props.put("key.serializer", qc.otherconfigs.getOrElse("key.serializer", default_key_serializer).toString.trim()); // ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG
  props.put("batch.size", qc.otherconfigs.getOrElse("batch.size", default_batch_size).toString.trim()); // ProducerConfig.BATCH_SIZE_CONFIG
  props.put("linger.ms", linger_ms) // ProducerConfig.LINGER_MS_CONFIG
  // props.put("retries", qc.otherconfigs.getOrElse("retries", default_retries).toString.trim()) // ProducerConfig.RETRIES_CONFIG
  props.put("block.on.buffer.full", qc.otherconfigs.getOrElse("block.on.buffer.full", default_block_on_buffer_full).toString.trim()) // ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG
  props.put("buffer.memory", qc.otherconfigs.getOrElse("buffer.memory", default_buffer_memory).toString.trim()) // ProducerConfig.BUFFER_MEMORY_CONFIG
  props.put("client.id", qc.otherconfigs.getOrElse("client.id", default_client_id).toString.trim()) // ProducerConfig.CLIENT_ID_CONFIG
  props.put("request.timeout.ms", qc.otherconfigs.getOrElse("request.timeout.ms", default_request_timeout_ms).toString.trim())
  props.put("timeout.ms", timeout_ms)
  props.put("metadata.fetch.timeout.ms", metadata_fetch_timeout_ms)
  props.put("metadata.max.age.ms", qc.otherconfigs.getOrElse("metadata.max.age.ms", defrault_metadata_max_age_ms).toString.trim())
  props.put("max.block.ms", qc.otherconfigs.getOrElse("max.block.ms", default_max_block_ms).toString.trim())
  props.put("max.buffer.full.block.ms", qc.otherconfigs.getOrElse("max.buffer.full.block.ms", default_max_buffer_full_block_ms).toString.trim())
  props.put("network.request.timeout.ms", qc.otherconfigs.getOrElse("network.request.timeout.ms", default_network_request_timeout_ms).toString.trim())

  val max_outstanding_messages = qc.otherconfigs.getOrElse("max.outstanding.messages", default_outstanding_messages).toString.trim().toInt

  case class MsgDataRecievedCnt(cntrToOrder: Long, msg: ProducerRecord[Array[Byte], Array[Byte]])

  val partitionsMap = new ConcurrentHashMap[Int, ConcurrentHashMap[Long, MsgDataRecievedCnt]](128);
  val failedMsgsMap = new ConcurrentHashMap[Int, ConcurrentHashMap[Long, MsgDataRecievedCnt]](128); // We just need Array Buffer as Innser value. But the only issue is we need to make sure we handle it for multiple threads.

  var reqCntr: Int = 0
  var msgInOrder = new AtomicLong

  val producer = new org.apache.kafka.clients.producer.KafkaProducer[Array[Byte], Array[Byte]](props)

  var topicPartitionsCount = producer.partitionsFor(qc.topic).size()
  var partitionsGetTm = System.currentTimeMillis
  val refreshPartitionTime = 60 * 1000 // 60 secs

  val randomPartitionCntr = new java.util.Random

  val key = Category + "/" + qc.Name + "/evtCnt"

  var shutdown = false

  private var retryExecutor: ExecutorService = Executors.newFixedThreadPool(1)

  retryExecutor.execute(new RetryFailedMessages())

  class RetryFailedMessages extends Runnable {
    def run() {
      val statusPrintTm = 60000 // for every 1 min
      var nextPrintTimeCheck = System.currentTimeMillis + statusPrintTm
      while (shutdown == false) {
        try {
          Thread.sleep(5000) // Sleeping for 5Sec
        } catch {
          case e: Exception => {}
          case e: Throwable => {}
        }
        if (shutdown == false) {
          var outstandingMsgs = outstandingMsgCount
          var allFailedMsgs = failedMsgCount
          if (outstandingMsgs > 0 || allFailedMsgs > 0 || nextPrintTimeCheck < System.currentTimeMillis) {
            LOG.warn("KAFKA PRODUCER: Topic: %s - current outstanding messages:%d & failed messages:%d".format(qc.topic, outstandingMsgs, allFailedMsgs))
            nextPrintTimeCheck = System.currentTimeMillis + statusPrintTm
          }
          // Get all failed records and resend for each partitions
          val keysIt = failedMsgsMap.keySet().iterator()

          while (keysIt.hasNext() && shutdown == false) {
            val partId = keysIt.next();

            val failedMsgs = failedMsgsMap.get(partId)
            val sz = failedMsgs.size()
            if (sz > 0) {
              val keyMessages = new ArrayBuffer[MsgDataRecievedCnt](sz)

              val allmsgsit = failedMsgs.entrySet().iterator()
              while (allmsgsit.hasNext() && shutdown == false) {
                val ent = allmsgsit.next();
                keyMessages += ent.getValue
              }
              if (shutdown == false) {
                val km = keyMessages.sortWith(_.cntrToOrder < _.cntrToOrder) // Sending in the same order as inserted before.
                sendInfinitely(km, true)
              }
            }
          }
        }
      }
    }
  }

  private def failedMsgCount: Int = {
    var failedMsgs = 0

    val allFailedPartitions = failedMsgsMap.elements()
    while (allFailedPartitions.hasMoreElements()) {
      val nxt = allFailedPartitions.nextElement();
      failedMsgs += nxt.size()
    }

    failedMsgs
  }

  private def outstandingMsgCount: Int = {
    var outstandingMsgs = 0

    val allPartitions = partitionsMap.elements()
    while (allPartitions.hasMoreElements()) {
      val nxt = allPartitions.nextElement();
      outstandingMsgs += nxt.size()
    }

    outstandingMsgs
  }

  /*
  private def addMsgToMap(partId: Int, msgAndCntr: MsgDataRecievedCnt): Unit = {
    var msgMap = partitionsMap.get(partId)
    if (msgMap == null) {
      partitionsMap.synchronized {
        msgMap = partitionsMap.get(partId)
        if (msgMap == null) {
          val tmpMsgMap = new ConcurrentHashMap[Long, MsgDataRecievedCnt](1024);
          partitionsMap.put(partId, tmpMsgMap)
          msgMap = tmpMsgMap
        }
      }
    }

    if (msgMap != null) {
      try {
        msgMap.put(msgAndCntr.cntrToOrder, msgAndCntr)
      } catch {
        case e: Exception => {
          // Failed to insert into Map
          throw e
        }
      }
    }
  }
*/

  private def addMsgsToMap(partId: Int, keyMessages: ArrayBuffer[MsgDataRecievedCnt]): Unit = {
    var msgMap = partitionsMap.get(partId)
    if (msgMap == null) {
      partitionsMap.synchronized {
        msgMap = partitionsMap.get(partId)
        if (msgMap == null) {
          val tmpMsgMap = new ConcurrentHashMap[Long, MsgDataRecievedCnt](1024);
          partitionsMap.put(partId, tmpMsgMap)
          msgMap = tmpMsgMap
        }
      }
    }

    if (msgMap != null) {
      try {
        val allKeys = new java.util.HashMap[Long, MsgDataRecievedCnt]()
        keyMessages.foreach(m => {
          allKeys.put(m.cntrToOrder, m)
        })
        msgMap.putAll(allKeys)
      } catch {
        case e: Exception => {
          // Failed to insert into Map
          throw e
        }
      }
    }
  }

  private def removeMsgFromMap(msgAndCntr: MsgDataRecievedCnt): Unit = {
    if (msgAndCntr == null) return
    val partId = msgAndCntr.msg.partition()
    val msgMap = partitionsMap.get(partId)
    if (msgMap != null) {
      try {
        msgMap.remove(msgAndCntr.cntrToOrder) // This must present. Because we are adding the records into partitionsMap before we send messages. If it does not present we simply ignore it.
      } catch {
        case e: Exception => {}
        case e: Throwable => {}
      }
    }
  }

  private def addToFailedMap(msgAndCntr: MsgDataRecievedCnt): Unit = {
    if (msgAndCntr == null) return
    val partId = msgAndCntr.msg.partition()
    var msgMap = failedMsgsMap.get(partId)
    if (msgMap == null) {
      failedMsgsMap.synchronized {
        msgMap = failedMsgsMap.get(partId)
        if (msgMap == null) {
          val tmpMsgMap = new ConcurrentHashMap[Long, MsgDataRecievedCnt](1024);
          failedMsgsMap.put(partId, tmpMsgMap)
          msgMap = tmpMsgMap
        }
      }
    }

    if (msgMap != null) {
      try {
        msgMap.put(msgAndCntr.cntrToOrder, msgAndCntr)
      } catch {
        case e: Exception => {
          // Failed to insert into Map
          throw e
        }
      }
    }
  }

  private def removeMsgFromFailedMap(msgAndCntr: MsgDataRecievedCnt): Unit = {
    if (msgAndCntr == null) return
    val partId = msgAndCntr.msg.partition()
    val msgMap = failedMsgsMap.get(partId)
    if (msgMap != null) {
      try {
        msgMap.remove(msgAndCntr.cntrToOrder)
      } catch {
        case e: Exception => {}
        case e: Throwable => {}
      }
    }
  }

  private def getPartition(key: Array[Byte], numPartitions: Int): Int = {
    if (numPartitions == 0) return 0
    if (key != null) {
      try {
        return (scala.math.abs(Arrays.hashCode(key)) % numPartitions)
      } catch {
        case e: Exception => { throw e }
        case e: Throwable => { throw e }
      }
    }
    return randomPartitionCntr.nextInt(numPartitions)
  }

  // To send an array of messages. messages.size should be same as partKeys.size
  override def send(messages: Array[Array[Byte]], partKeys: Array[Array[Byte]]): Unit = {
    if (messages.size != partKeys.size) {
      val szMsg = "KAFKA PRODUCER: Message and Partition Keys should has same number of elements. Message has %d and Partition Keys has %d".format(messages.size, partKeys.size)
      LOG.error(szMsg)
      throw new Exception(szMsg)
    }
    if (messages.size == 0) return

    // Refreshing Partitions for every refreshPartitionTime.
    // BUGBUG:: This may execute multiple times from multiple threads. For now it does not hard too much.
    if ((System.currentTimeMillis - partitionsGetTm) > refreshPartitionTime) {
      topicPartitionsCount = producer.partitionsFor(qc.topic).size()
      partitionsGetTm = System.currentTimeMillis
    }

    try {
      var partitionsMsgMap = scala.collection.mutable.Map[Int, ArrayBuffer[MsgDataRecievedCnt]]();

      for (i <- 0 until messages.size) {
        val partId = getPartition(partKeys(i), topicPartitionsCount)
        var ab = partitionsMsgMap.getOrElse(partId, null)
        if (ab == null) {
          ab = new ArrayBuffer[MsgDataRecievedCnt](256)
          partitionsMsgMap(partId) = ab
        }
        val pr = new ProducerRecord(qc.topic, partId, partKeys(i), messages(i))
        ab += MsgDataRecievedCnt(msgInOrder.getAndIncrement, pr)
      }

      var outstandingMsgs = outstandingMsgCount
      // LOG.debug("KAFKA PRODUCER: current outstanding messages for topic %s are %d".format(qc.topic, outstandingMsgs))

      var osRetryCount = 0
      var osWaitTm = 5000
      while (outstandingMsgs > max_outstanding_messages) {
        LOG.warn("KAFKA PRODUCER: %d outstanding messages in queue to write. Waiting for them to flush before we write new messages. Retrying after %dms. Retry count:%d".format(outstandingMsgs, osWaitTm, osRetryCount))
        try {
          Thread.sleep(osWaitTm)
        } catch {
          case e: Exception => throw e
          case e: Throwable => throw e
        }
        outstandingMsgs = outstandingMsgCount
      }

      partitionsMsgMap.foreach(partIdAndRecs => {
        val partId = partIdAndRecs._1
        val keyMessages = partIdAndRecs._2

        // first push all messages to partitionsMap before we really send. So that callback is guaranteed to find the message in partitionsMap
        addMsgsToMap(partId, keyMessages)
        sendInfinitely(keyMessages, false)
      })

    } catch {
      case fae: FatalAdapterException => throw fae
      case e: Exception               => throw FatalAdapterException("Unknown exception", e)
      case e: Throwable               => throw FatalAdapterException("Unknown exception", e)
    }
  }

  private def sendInfinitely(keyMessages: ArrayBuffer[MsgDataRecievedCnt], removeFromFailedMap: Boolean): Unit = {
    var sendStatus = KafkaConstants.KAFKA_NOT_SEND
    var retryCount = 0
    var waitTm = 15000
    // We keep on retry until we succeed on this thread
    while (sendStatus != KafkaConstants.KAFKA_SEND_SUCCESS && shutdown == false) {
      try {
        sendStatus = doSend(keyMessages, removeFromFailedMap)
      } catch {
        case e: Exception => {
          LOG.error("KAFKA PRODUCER: Error sending to kafka, Retrying after %dms. Retry count:%d".format(waitTm, retryCount), e)
          try {
            Thread.sleep(waitTm)
          } catch {
            case e: Exception => throw e
            case e: Throwable => throw e
          }
          if (waitTm < 60000) {
            waitTm = waitTm * 2
            if (waitTm > 60000)
              waitTm = 60000
          }
        }
      }
    }
  }

  private def addBackFailedToSendRec(lastAccessRec: MsgDataRecievedCnt): Unit = {
    if (lastAccessRec != null)
      addToFailedMap(lastAccessRec)
  }

  private def doSend(keyMessages: ArrayBuffer[MsgDataRecievedCnt], removeFromFailedMap: Boolean): Int = {
    var sentMsgsCntr = 0
    var lastAccessRec: MsgDataRecievedCnt = null
    try {
      // We already populated partitionsMap before we really send. So that callback is guaranteed to find the message in partitionsMap
      keyMessages.map(msgAndCntr => {
        if (shutdown)
          throw new Exception("Shutting down")
        lastAccessRec = msgAndCntr
        if (removeFromFailedMap)
          removeMsgFromFailedMap(lastAccessRec)
        // Send the request to Kafka
        producer.send(msgAndCntr.msg, new Callback {
          override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
            val localMsgAndCntr = msgAndCntr
            if (exception != null) {
              LOG.warn("Failed to send message into " + localMsgAndCntr.msg.topic, exception)
              addToFailedMap(localMsgAndCntr)
            } else {
              // Succeed
              removeMsgFromMap(localMsgAndCntr)
            }
          }
        })
        lastAccessRec = null
        sentMsgsCntr += 1
        cntrAdapter.addCntr(key, 1)
      })
      keyMessages.clear()
    } catch {
      case ftsme: FailedToSendMessageException => { if (sentMsgsCntr > 0) keyMessages.remove(0, sentMsgsCntr); addBackFailedToSendRec(lastAccessRec); throw new FatalAdapterException("Kafka sending to Dead producer", ftsme) }
      case qfe: QueueFullException             => { if (sentMsgsCntr > 0) keyMessages.remove(0, sentMsgsCntr); addBackFailedToSendRec(lastAccessRec); throw new FatalAdapterException("Kafka queue full", qfe) }
      case e: Exception                        => { if (sentMsgsCntr > 0) keyMessages.remove(0, sentMsgsCntr); addBackFailedToSendRec(lastAccessRec); throw new FatalAdapterException("Unknown exception", e) }
      case e: Throwable                        => { if (sentMsgsCntr > 0) keyMessages.remove(0, sentMsgsCntr); addBackFailedToSendRec(lastAccessRec); throw new FatalAdapterException("Unknown exception", e) }
    }
    return KafkaConstants.KAFKA_SEND_SUCCESS
  }

  override def Shutdown(): Unit = {
    shutdown = true

    // First shutdown retry executor
    if (retryExecutor != null) {
      retryExecutor.shutdownNow
      while (retryExecutor.isTerminated == false) {
        Thread.sleep(100)
      }
    }

    if (producer != null)
      producer.close
  }
}

