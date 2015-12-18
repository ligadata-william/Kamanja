
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
// import java.util.concurrent.{ TimeUnit, Future }

object KafkaProducer extends OutputAdapterObj {
  def CreateOutputAdapter(inputConfig: AdapterConfiguration, cntrAdapter: CountersAdapter): OutputAdapter = new KafkaProducer(inputConfig, cntrAdapter)
}

class KafkaProducer(val inputConfig: AdapterConfiguration, cntrAdapter: CountersAdapter) extends OutputAdapter {
  private[this] val LOG = LogManager.getLogger(getClass);

  //BUGBUG:: Not Checking whether inputConfig is really QueueAdapterConfiguration or not. 
  private[this] val qc = KafkaQueueAdapterConfiguration.GetAdapterConfig(inputConfig)

  val default_compression_type = "none" // Valida values at this moment are none, gzip, or snappy.
  val default_value_serializer = "org.apache.kafka.common.serialization.ByteArraySerializer"
  val default_key_serializer = "org.apache.kafka.common.serialization.ByteArraySerializer"
  val default_batch_size = "1024"
  val default_linger_ms = "50" // 50ms
  val default_producer_type = "async"
  // val default_retries = "0"
  val default_block_on_buffer_full = "true" // true or false
  val default_buffer_memory = "16777216" // 16MB
  val default_client_id = qc.Name + "_" + hashCode.toString
  val default_request_timeout_ms = "10000"
  val MAX_RETRY = 3

  val producer_type = qc.otherconfigs.getOrElse("producer.type", default_producer_type).toString.trim()
  val isSyncProducer = (producer_type.compareToIgnoreCase("sync") == 0)

  // Set up some properties for the Kafka Producer
  // New Producer configs are found @ http://kafka.apache.org/082/documentation.html#newproducerconfigs
  val props = new Properties()
  props.put("bootstrap.servers", qc.hosts.mkString(",")); // ProducerConfig.BOOTSTRAP_SERVERS_CONFIG
  props.put("compression.type", qc.otherconfigs.getOrElse("compression.type", default_compression_type).toString.trim()); // ProducerConfig.COMPRESSION_TYPE_CONFIG
  props.put("value.serializer", qc.otherconfigs.getOrElse("value.serializer", default_value_serializer).toString.trim()); // ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG
  props.put("key.serializer", qc.otherconfigs.getOrElse("key.serializer", default_key_serializer).toString.trim()); // ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG
  props.put("batch.size", qc.otherconfigs.getOrElse("batch.size", default_batch_size).toString.trim()); // ProducerConfig.BATCH_SIZE_CONFIG
  props.put("linger.ms", qc.otherconfigs.getOrElse("linger.ms", default_linger_ms).toString.trim()) // ProducerConfig.LINGER_MS_CONFIG
  // props.put("retries", qc.otherconfigs.getOrElse("retries", default_retries).toString.trim()) // ProducerConfig.RETRIES_CONFIG
  // props.put("producer.type", producer_type)
  props.put("block.on.buffer.full", qc.otherconfigs.getOrElse("block.on.buffer.full", default_block_on_buffer_full).toString.trim()) // ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG
  props.put("buffer.memory", qc.otherconfigs.getOrElse("buffer.memory", default_buffer_memory).toString.trim()) // ProducerConfig.BUFFER_MEMORY_CONFIG
  props.put("client.id", qc.otherconfigs.getOrElse("client.id", default_client_id).toString.trim()) // ProducerConfig.CLIENT_ID_CONFIG
  props.put("request.timeout.ms", qc.otherconfigs.getOrElse("request.timeout.ms", default_request_timeout_ms).toString.trim())

  val tmpProducersStr = qc.otherconfigs.getOrElse("numberofconcurrentproducers", "1").toString.trim()
  var producersCnt = 1
  if (tmpProducersStr.size > 0) {
    try {
      val tmpProducers = tmpProducersStr.toInt
      if (tmpProducers > 0)
        producersCnt = tmpProducers
    } catch {
      case e: Exception => {}
      case e: Throwable => {}
    }
  }

  var reqCntr: Int = 0

  val producers = new Array[org.apache.kafka.clients.producer.KafkaProducer[Array[Byte], Array[Byte]]](producersCnt)

  for (i <- 0 until producersCnt) {
    LOG.info("Creating Producer:" + (i + 1))
    // create the producer object
    producers(i) = new org.apache.kafka.clients.producer.KafkaProducer[Array[Byte], Array[Byte]](props)
  }

  var topicPartitionsCount = producers(0).partitionsFor(qc.topic).size()
  var partitionsGetTm = System.currentTimeMillis
  val refreshPartitionTime = 60 * 1000 // 60 secs

  val randomPartitionCntr = new java.util.Random

  private def getPartition(key: Array[Byte], numPartitions: Int): Int = {
    if (numPartitions == 0) return 0
    if (key != null) {
      try {
        return (scala.math.abs(Arrays.hashCode(key)) % numPartitions)
      } catch {
        case e: Exception => {
        }
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

    if (reqCntr > 500000000)
      reqCntr = 0
    reqCntr += 1
    val cntr = reqCntr

    // Refreshing Partitions for every refreshPartitionTime.
    // BUGBUG:: This may execute multiple times from multiple threads. For now it does not hard too much.
    if ((System.currentTimeMillis - partitionsGetTm) > refreshPartitionTime) {
      topicPartitionsCount = producers(cntr % producersCnt).partitionsFor(qc.topic).size()
      partitionsGetTm = System.currentTimeMillis
    }

    try {
      val keyMessages = new ArrayBuffer[ProducerRecord[Array[Byte], Array[Byte]]](messages.size)
      for (i <- 0 until messages.size) {
        keyMessages += new ProducerRecord(qc.topic, getPartition(partKeys(i), topicPartitionsCount), partKeys(i), messages(i))
      }

      var sendStatus = KafkaConstants.KAFKA_NOT_SEND
      var retryCount = 0
      while (sendStatus != KafkaConstants.KAFKA_SEND_SUCCESS) {
        val result = doSend(cntr, keyMessages.toArray)
        sendStatus = result._1
        // Queue is full, wait and retry
        if (sendStatus == KafkaConstants.KAFKA_SEND_Q_FULL) {
          LOG.warn("KAFKA PRODUCER: " + qc.topic + " is temporarily full, retrying.")
          Thread.sleep(1000)
        }

        // Something wrong in sending messages,  Producer will handle internal failover, so we want to retry but only
        //  3 times.
        if (sendStatus == KafkaConstants.KAFKA_SEND_DEAD_PRODUCER) {
          retryCount += 1
          if (retryCount < MAX_RETRY) {
            LOG.warn("KAFKA PRODUCER: Error sending to kafka, Retrying " + retryCount + "/" + MAX_RETRY)
          } else {
            LOG.error("KAFKA PRODUCER: Error sending to kafka,  MAX_RETRY reached... shutting down")
            throw FatalAdapterException("Unable to send to Kafka, MAX_RETRY reached", result._2.getOrElse(null))
          }
        }
      }

      val key = Category + "/" + qc.Name + "/evtCnt"
      cntrAdapter.addCntr(key, messages.size) // for now adding rows
    } catch {
      case fae: FatalAdapterException => throw fae
      case e: Exception               => throw FatalAdapterException("Unknown exception", e)
    }
  }

  private def doSend(cntr: Int, keyMessages: Array[ProducerRecord[Array[Byte], Array[Byte]]]): (Int, Option[Exception]) = {
    try {
      if (isSyncProducer) {
        val respFutures = keyMessages.map(msg => {
          // Send the request to Kafka
          producers(cntr % producersCnt).send(msg, new Callback {
            override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
              val localMsg = msg
              if (exception != null) {
                LOG.warn("Failed to send message into the " + msg.topic, exception) // BUGBUG:: Yet to add more logic here
              }
            }
          })
        })
        val responses = respFutures.map(f => f.get)
/*
        LOG.warn(responses.map(rm1 => {
          val t = rm1.topic()
          val p = rm1.partition()
          val o = rm1.offset()
          "topic:%s, partition:%d, offset:%d".format(t, p, o);
        }).mkString("~"))
*/
      } else {
        val respFutures = keyMessages.map(msg => {
          // Send the request to Kafka
          producers(cntr % producersCnt).send(msg, new Callback {
            override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
              val localMsg = msg
              if (exception != null) {
                LOG.warn("Failed to send message into the " + msg.topic, exception) // BUGBUG:: Yet to add more logic here
              }
            }
          })
        })
      }
    } catch {
      case ftsme: FailedToSendMessageException => { /* LOG.error("Got FailedToSendMessageException", ftsme); */ return (KafkaConstants.KAFKA_SEND_DEAD_PRODUCER, Some(ftsme)) }
      case qfe: QueueFullException             => { /* LOG.error("Got FailedToSendMessageException", qfe); */ return (KafkaConstants.KAFKA_SEND_Q_FULL, None) }
      case e: Exception                        => { /* LOG.error("Got FailedToSendMessageException", e); */ throw new FatalAdapterException("Unknown exception", e) }
    }
    return (KafkaConstants.KAFKA_SEND_SUCCESS, None)
  }

  override def Shutdown(): Unit = {
    for (i <- 0 until producersCnt) {
      if (producers(i) != null)
        producers(i).close
      producers(i) = null

    }
  }
}

