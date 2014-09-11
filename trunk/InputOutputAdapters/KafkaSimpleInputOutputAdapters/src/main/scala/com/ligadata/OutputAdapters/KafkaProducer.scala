
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
  private[this] val qc = new KafkaQueueAdapterConfiguration

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

  val compress: Boolean = false
  val synchronously: Boolean = false
  val batchSize: Integer = 1024
  val queueTime: Integer = 5
  // val queueSize: Integer = 1000000
  // val bufferMemory: Integer = 67108864
  val messageSendMaxRetries: Integer = 3
  val requestRequiredAcks: Integer = 1

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
  props.put("client.id", qc.groupName)

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

