
package com.ligadata.AdaptersConfiguration

import com.ligadata.OnLEPBase.AdapterConfiguration

class KafkaQueueAdapterConfiguration extends AdapterConfiguration {
  var hosts: Array[String] = _ // each host is HOST:PORT
  var groupName: String = _ // for Kafka it is Group Id or Client Id
  var maxPartitions: Int = _ // Max number of partitions for this queue
  var instancePartitions: Set[Int] = _ // Valid only for Input Queues. These are the partitions we handle for this Queue. For now we are treating Partitions as Ints. (Kafka handle it as ints)
}

