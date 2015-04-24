
package com.ligadata.AdaptersConfiguration

import com.ligadata.FatafatBase.{ AdapterConfiguration, PartitionUniqueRecordKey, PartitionUniqueRecordValue }
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._

class KafkaQueueAdapterConfiguration extends AdapterConfiguration {
  var hosts: Array[String] = _ // each host is HOST:PORT
  var topic: String = _ // topic name
  var noDataSleepTimeInMs: Int = 300 // No Data Sleep Time in milli secs. Default is 300 ms
  var instancePartitions: Set[Int] = _ // Valid only for Input Queues. These are the partitions we handle for this Queue. For now we are treating Partitions as Ints. (Kafka handle it as ints)
}

object KafkaQueueAdapterConfiguration {
  def GetAdapterConfig(inputConfig: AdapterConfiguration): KafkaQueueAdapterConfiguration = {
    if (inputConfig.adapterSpecificCfg == null || inputConfig.adapterSpecificCfg.size == 0) {
      val err = "Not found Host/Brokers and topicname for Kafka Adapter Config:" + inputConfig.Name
      throw new Exception(err)
    }

    val qc = new KafkaQueueAdapterConfiguration
    qc.Name = inputConfig.Name
    qc.formatOrInputAdapterName = inputConfig.formatOrInputAdapterName
    qc.className = inputConfig.className
    qc.jarName = inputConfig.jarName
    qc.dependencyJars = inputConfig.dependencyJars

    val adapCfg = parse(inputConfig.adapterSpecificCfg)
    if (adapCfg == null || adapCfg.values == null) {
      val err = "Not found Host/Brokers and topicname for Kafka Adapter Config:" + inputConfig.Name
      throw new Exception(err)
    }
    val values = adapCfg.values.asInstanceOf[Map[String, String]]

    values.foreach(kv => {
      if (kv._1.compareToIgnoreCase("HostList") == 0) {
        qc.hosts = kv._2.split(",").map(str => str.trim).filter(str => str.size > 0)
      } else if (kv._1.compareToIgnoreCase("TopicName") == 0) {
        qc.topic = kv._2.trim
      } else if (kv._1.compareToIgnoreCase("NoDataSleepTimeInMs") == 0) {
        qc.noDataSleepTimeInMs = kv._2.trim.toInt
        if (qc.noDataSleepTimeInMs < 0)
          qc.noDataSleepTimeInMs = 0
      }
    })

    qc.instancePartitions = Set[Int]()

    qc
  }
}

case class KafkaKeyData(Version: Int, Type: String, Name: String, TopicName: Option[String], PartitionId: Option[Int]) // Using most of the values as optional values. Just thinking about future changes. Don't know the performance issues.

class KafkaPartitionUniqueRecordKey extends PartitionUniqueRecordKey {
  val Version: Int = 1
  val Type: String = "Kafka"
  var Name: String = _ // Name
  var TopicName: String = _ // Topic Name
  var PartitionId: Int = _ // Partition Id

  override def Serialize: String = { // Making String from key
    val json =
      ("Version" -> Version) ~
        ("Type" -> Type) ~
        ("Name" -> Name) ~
        ("TopicName" -> TopicName) ~
        ("PartitionId" -> PartitionId)
    compact(render(json))
  }

  override def Deserialize(key: String): Unit = { // Making Key from Serialized String
    implicit val jsonFormats: Formats = DefaultFormats
    val keyData = parse(key).extract[KafkaKeyData]
    if (keyData.Version == Version && keyData.Type.compareTo(Type) == 0) {
      Name = keyData.Name
      TopicName = keyData.TopicName.get
      PartitionId = keyData.PartitionId.get
    }
    // else { } // Not yet handling other versions
  }
}

case class KafkaRecData(Version: Int, Offset: Option[Long]) // Using most of the values as optional values. Just thinking about future changes. Don't know the performance issues.

class KafkaPartitionUniqueRecordValue extends PartitionUniqueRecordValue {
  val Version: Int = 1
  var Offset: Long = -1 // Offset 

  override def Serialize: String = { // Making String from Value
    val json =
      ("Version" -> Version) ~
        ("Offset" -> Offset)
    compact(render(json))
  }

  override def Deserialize(key: String): Unit = { // Making Value from Serialized String
    implicit val jsonFormats: Formats = DefaultFormats
    val recData = parse(key).extract[KafkaRecData]
    if (recData.Version == Version) {
      Offset = recData.Offset.get
    }
    // else { } // Not yet handling other versions
  }
}

