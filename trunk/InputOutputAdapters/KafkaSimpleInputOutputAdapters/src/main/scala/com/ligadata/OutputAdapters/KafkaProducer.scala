
package com.ligadata.OutputAdapters

import java.util.Properties
import kafka.message._
import kafka.producer.{ ProducerConfig, Producer, KeyedMessage }
import org.apache.log4j.Logger
import com.ligadata.OnLEPBase.{ AdapterConfiguration, OutputAdapter, OutputAdapterObj, CountersAdapter }
import com.ligadata.AdaptersConfiguration.KafkaQueueAdapterConfiguration

object KafkaProducer extends OutputAdapterObj {
  def CreateOutputAdapter(inputConfig: AdapterConfiguration, cntrAdapter: CountersAdapter): OutputAdapter = new KafkaProducer(inputConfig, cntrAdapter)
}

class KafkaProducer(val inputConfig: AdapterConfiguration, cntrAdapter: CountersAdapter) extends OutputAdapter {
  private[this] val LOG = Logger.getLogger(getClass);

  //BUGBUG:: Not Checking whether inputConfig is really QueueAdapterConfiguration or not. 
  private[this] val qc = KafkaQueueAdapterConfiguration.GetAdapterConfig(inputConfig)

  val clientId = qc.Name + "_" + hashCode.toString

  val compress: Boolean = false
  val synchronously: Boolean = true // This parameter specifies whether the messages are sent asynchronously in a background thread. Valid values are (1) async for asynchronous send and (2) sync for synchronous send. By setting the producer to async we allow batching together of requests (which is great for throughput) but open the possibility of a failure of the client machine dropping unsent data.
  val batchSize: Integer = 1 // Each message send immediately
  val queueTime: Integer = 5
  // val queueSize: Integer = 1000000
  // val bufferMemory: Integer = 67108864
  val messageSendMaxRetries: Integer = 3
  val requestRequiredAcks: Integer = -1 // which means that the producer gets an acknowledgement after all in-sync replicas have received the data. This option provides the best durability, we guarantee that no messages will be lost as long as at least one in sync replica remains.

  val codec = if (compress) DefaultCompressionCodec.codec else NoCompressionCodec.codec

  val props = new Properties()
  props.put("compression.codec", codec.toString)
  props.put("producer.type", if (synchronously) "sync" else "async")
  props.put("metadata.broker.list", qc.hosts.mkString(","))
  props.put("batch.num.messages", batchSize.toString)
  props.put("batch.size", batchSize.toString)
  props.put("queue.time", queueTime.toString)
  // props.put("queue.size", queueSize.toString)
  props.put("message.send.max.retries", messageSendMaxRetries.toString)
  props.put("request.required.acks", requestRequiredAcks.toString)
  // props.put("buffer.memory", bufferMemory.toString)
  // props.put("buffer.size", bufferMemory.toString)
  // props.put("socket.send.buffer", bufferMemory.toString)
  // props.put("socket.receive.buffer", bufferMemory.toString)
  props.put("client.id", clientId)

  val producer = new Producer[AnyRef, AnyRef](new ProducerConfig(props)) // Not closing this producer at this moment

  override def send(message: String, partKey: String): Unit = send(message.getBytes("UTF8"), partKey.getBytes("UTF8"))

  override def send(message: Array[Byte], partKey: Array[Byte]): Unit = {
    try {
      producer.send(new KeyedMessage(qc.Name, partKey, message))
      val key = Category + "/" + qc.Name + "/evtCnt"
      cntrAdapter.addCntr(key, 1) // for now adding each row
    } catch {
      case e: Exception => {
        LOG.error("Failed to send :" + e.getMessage)
      }
    }
  }

  override def Shutdown(): Unit = {
    producer.close
  }
}

