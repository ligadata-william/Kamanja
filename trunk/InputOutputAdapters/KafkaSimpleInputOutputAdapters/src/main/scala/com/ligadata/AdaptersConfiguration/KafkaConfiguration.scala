
package com.ligadata.AdaptersConfiguration

import com.ligadata.OnLEPBase.{ AdapterConfiguration, PartitionUniqueRecordKey, PartitionUniqueRecordValue }
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._

class KafkaQueueAdapterConfiguration extends AdapterConfiguration {
  var hosts: Array[String] = _ // each host is HOST:PORT
  var groupName: String = _ // for Kafka it is Group Id or Client Id
  var instancePartitions: Set[Int] = _ // Valid only for Input Queues. These are the partitions we handle for this Queue. For now we are treating Partitions as Ints. (Kafka handle it as ints)
}

case class KafkaKeyData(Version: Int, Type: String, TopicName: Option[String], PartitionId: Option[Int]) // Using most of the values as optional values. Just thinking about future changes. Don't know the performance issues.

class KafkaPartitionUniqueRecordKey extends PartitionUniqueRecordKey {
  val Version: Int = 1
  val Type: String = "Kafka"
  var TopicName: String = _ // Topic Name
  var PartitionId: Int = _ // Partition Id

  override def Serialize: String = { // Making String from key
    val json =
      ("Version" -> Version) ~
        ("Type" -> Type) ~
        ("TopicName" -> TopicName) ~
        ("PartitionId" -> PartitionId)
    compact(render(json))
  }

  override def Deserialize(key: String): Unit = { // Making Key from Serialized String
    implicit val jsonFormats: Formats = DefaultFormats
    val keyData = parse(key).extract[KafkaKeyData]
    if (keyData.Version == Version && keyData.Type.compareTo(Type) == 0) {
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

