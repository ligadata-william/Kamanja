/*
 * This interface is primarily used for implementing storage adapters where
 * row data is partitioned by a time stamp column and also contains one or more key
 * columns
 *
 */
package com.ligadata.StorageBase

import scala.collection.mutable.ArrayBuffer
import com.ligadata.Utils.{ KamanjaLoaderInfo }
import java.util.Date

trait DataStoreOperations {
  type ByteArray = Array[Byte]

  // update operations, add & update semantics are different for relational databases
  def put(containerName: String, part_date: Date, key: Array[String], transactionId: Long, value: ByteArray): Unit
  def putBatch(data_list: Array[(String, Array[(Date, Array[String], Long, ByteArray)])]): Unit // data_list has List of container names, and each container has list of part_date, key, transactionid, value

  // delete operations
  def del(containerName: String, begin_time: Date, end_time: Date, transactionId: Long, key: Array[String]): Unit
  //def delBatch(del_data: Array[(String, Array[(Date, Date, Long, Array[Array[String]])])]): Unit // Begine Time Range, End Time Range, Array of Keys.

  def del(containerName: String, begin_time: Date, end_time: Date, key: Array[String]): Unit
  def delBatch(del_data: Array[(String, Array[(Date, Date, Array[Array[String]])])]): Unit // Begine Time Range, End Time Range, Array of Keys.

  // get operations
  def get(containerName: String, callbackFunction: (Date, Array[String], Long, ByteArray) => Unit): Unit
  def get(containerName: String, begin_time: Date, end_time: Date, callbackFunction: (Date, Array[String], Long, ByteArray) => Unit): Unit
  def get(containerName: String, date_range: Array[(Date, Date)], callbackFunction: (Date, Array[String], Long, ByteArray) => Unit): Unit
  def get(containerName: String, begin_time: Date, end_time: Date, key: Array[String], callbackFunction: (Date, Array[String], Long, ByteArray) => Unit): Unit
  def get(containerName: String, begin_time: Date, end_time: Date, keys: Array[Array[String]], callbackFunction: (Date, Array[String], Long, ByteArray) => Unit): Unit
  def get(containerName: String, key: Array[String], callbackFunction: (Date, Array[String], Long, ByteArray) => Unit): Unit
  def get(containerName: String, keys: Array[Array[String]], callbackFunction: (Date, Array[String], Long, ByteArray) => Unit): Unit
  // def get(containerName: String, begin_time: Date, end_time: Date, key: Array[String], filterFunction: (Date, Array[String], Long, Value) => Boolean, callbackFunction: (Date, Array[String], Long, Value) => Unit): Unit
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
