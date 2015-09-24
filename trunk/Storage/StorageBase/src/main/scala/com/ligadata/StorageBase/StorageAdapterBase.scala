/*
 * This interface is primarily used for implementing storage adapters where
 * row data is partitioned by a time stamp column and also contains one or more key
 * columns
 *
 */
package com.ligadata.StorageBase

import com.ligadata.Utils.{ KamanjaLoaderInfo }
import java.util.Date

class Key {
  var date_part: Date = _
  var bucket_key: Array[String] = _
  var transactionId: Long = _
}

class Value {
  var serializerType: String = _
  var serializedInfo: Array[Byte] = _
}

class TimeRange {
  var begin_time: Date = _
  var end_time: Date = _
}

trait DataStoreOperations {
  // update operations, add & update semantics are different for relational databases
  def put(containerName: String, key: Key, value: Value): Unit
  // def put(containerName: String, data_list: Array[(Key, Value)]): Unit
  def put(data_list: Array[(String, Array[(Key, Value)])]): Unit // data_list has List of container names, and each container has list of key & value

  // delete operations
  def del(containerName: String, key: Key): Unit
  def delRange(containerName: String, time: TimeRange, bucket_key: Array[String]): Unit // For the given bucket_key, delete the values with in given date range

  // get operations
  def get(containerName: String, callbackFunction: (Key, Value) => Unit): Unit
  def get(containerName: String, time_ranges: Array[TimeRange], callbackFunction: (Key, Value) => Unit): Unit // Range of dates
  def get(containerName: String, time_ranges: Array[TimeRange], bucket_keys: Array[Array[String]], callbackFunction: (Key, Value) => Unit): Unit
  def get(containerName: String, bucket_keys: Array[Array[String]], callbackFunction: (Key, Value) => Unit): Unit
/*
  // Passing filter to storage
  def get(containerName: String, filterFunction: (Key, Value) => Boolean, callbackFunction: (Key, Value) => Unit): Unit
  def get(containerName: String, time_ranges: Array[TimeRange], filterFunction: (Key, Value) => Boolean, callbackFunction: (Key, Value) => Unit): Unit // Range of dates
  def get(containerName: String, time_ranges: Array[TimeRange], bucket_keys: Array[Array[String]], filterFunction: (Key, Value) => Boolean, callbackFunction: (Key, Value) => Unit): Unit
  def get(containerName: String, bucket_keys: Array[Array[String]], filterFunction: (Key, Value) => Boolean, callbackFunction: (Key, Value) => Unit): Unit
*/
}

trait DataStore extends DataStoreOperations {
  def beginTx(): Transaction
  def endTx(tx: Transaction): Unit // Same as commit
  def commitTx(tx: Transaction): Unit
  def rollbackTx(tx: Transaction): Unit

  // clean up operations
  def Shutdown(): Unit
  def TruncateContainer(containerNames: Array[String]): Unit
  def DropContainer(containerNames: Array[String]): Unit
  def CreateContainer(containerNames: Array[String]): Unit
}

trait Transaction extends DataStoreOperations {
  val parent: DataStore // Parent Data Store
}

// Storage Adapter Object to create storage adapter
trait StorageAdapterObj {
  def CreateStorageAdapter(kvManagerLoader: KamanjaLoaderInfo, datastoreConfig: String): DataStore
}
