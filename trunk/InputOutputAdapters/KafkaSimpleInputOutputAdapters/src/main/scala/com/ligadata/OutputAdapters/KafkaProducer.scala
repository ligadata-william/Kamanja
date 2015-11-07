
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

import java.util.Properties
import kafka.message._
import kafka.producer.{ ProducerConfig, Producer, KeyedMessage }
import org.apache.log4j.Logger
import com.ligadata.InputOutputAdapterInfo.{ AdapterConfiguration, OutputAdapter, OutputAdapterObj, CountersAdapter }
import com.ligadata.AdaptersConfiguration.KafkaQueueAdapterConfiguration
import com.ligadata.Exceptions.StackTrace
import scala.collection.mutable.ArrayBuffer

object KafkaProducer extends OutputAdapterObj {
  def CreateOutputAdapter(inputConfig: AdapterConfiguration, cntrAdapter: CountersAdapter): OutputAdapter = new KafkaProducer(inputConfig, cntrAdapter)
}

class KafkaProducer(val inputConfig: AdapterConfiguration, cntrAdapter: CountersAdapter) extends OutputAdapter {
  private[this] val LOG = Logger.getLogger(getClass);

  //BUGBUG:: Not Checking whether inputConfig is really QueueAdapterConfiguration or not. 
  private[this] val qc = KafkaQueueAdapterConfiguration.GetAdapterConfig(inputConfig)

  val clientId = qc.Name + "_" + hashCode.toString

  val compress: Boolean = false
  val synchronously: Boolean = false
  val batchSize: Integer = 1024
  val queueTime: Integer = 100
  val bufferMemory: Integer = 16 * 1024 * 1024
  val messageSendMaxRetries: Integer = 3
  val requestRequiredAcks: Integer = 1
  val ackTimeout = 10000

  val codec = if (compress) DefaultCompressionCodec.codec else NoCompressionCodec.codec

  val props = new Properties()
  props.put("compression.codec", if (qc.otherconfigs.contains("compression.codec")) qc.otherconfigs.get("compression.codec").toString else codec.toString)
  props.put("producer.type", if (qc.otherconfigs.contains("producer.type")) qc.otherconfigs.get("producer.type").toString else if (synchronously) "sync" else "async")
  props.put("metadata.broker.list", qc.hosts.mkString(","))
  props.put("batch.num.messages", if (qc.otherconfigs.contains("batch.num.messages")) qc.otherconfigs.get("batch.num.messages").toString else batchSize.toString)
  props.put("queue.buffering.max.messages", if (qc.otherconfigs.contains("queue.buffering.max.messages")) qc.otherconfigs.get("queue.buffering.max.messages").toString else batchSize.toString)
  props.put("queue.buffering.max.ms", if (qc.otherconfigs.contains("queue.buffering.max.ms")) qc.otherconfigs.get("queue.buffering.max.ms").toString else queueTime.toString)
  props.put("message.send.max.retries", if (qc.otherconfigs.contains("message.send.max.retries")) qc.otherconfigs.get("message.send.max.retries").toString else messageSendMaxRetries.toString)
  props.put("request.required.acks", if (qc.otherconfigs.contains("request.required.acks")) qc.otherconfigs.get("request.required.acks").toString else requestRequiredAcks.toString)
  props.put("send.buffer.bytes", if (qc.otherconfigs.contains("send.buffer.bytes")) qc.otherconfigs.get("send.buffer.bytes").toString else bufferMemory.toString)
  props.put("request.timeout.ms", if (qc.otherconfigs.contains("request.timeout.ms")) qc.otherconfigs.get("request.timeout.ms").toString else ackTimeout.toString)
  props.put("client.id", if (qc.otherconfigs.contains("client.id")) qc.otherconfigs.get("client.id").toString else clientId)

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

  val producers = new Array[Producer[String, Array[Byte]]](producersCnt)

  for (i <- 0 until producersCnt) {
    LOG.info("Creating Producer:" + (i + 1))
    producers(i) = new Producer[String, Array[Byte]](new ProducerConfig(props))
  }

  // To send an array of messages. messages.size should be same as partKeys.size
  override def send(messages: Array[Array[Byte]], partKeys: Array[Array[Byte]]): Unit = {
    if (messages.size != partKeys.size) {
      LOG.error("Message and Partition Keys hould has same number of elements. Message has %d and Partition Keys has %d".format(messages.size, partKeys.size))
      return
    }
    if (messages.size == 0) return
    try {
      val keyMessages = new ArrayBuffer[KeyedMessage[String, Array[Byte]]](messages.size)
      for (i <- 0 until messages.size) {
        keyMessages += new KeyedMessage(qc.topic, new String(partKeys(i)), messages(i))
      }
      if (reqCntr > 500000000)
        reqCntr = 0
      reqCntr += 1
      producers(reqCntr % producersCnt).send(keyMessages: _*) // Thinking this op is atomic for (can write multiple partitions into one queue, but don't know whether it is atomic per partition in the queue).
      val key = Category + "/" + qc.Name + "/evtCnt"
      cntrAdapter.addCntr(key, messages.size) // for now adding rows
    } catch {
      case e: Exception => {
        LOG.error("Failed to send :" + e.getMessage)
      }
    }
  }

  override def Shutdown(): Unit = {
    for (i <- 0 until producersCnt) {
      if (producers(i) != null)
        producers(i).close
      producers(i) = null

    }
  }
}

