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

/*
 * Takes any object and stores it with the key
 *
 */
package com.ligadata.StorageBase

import scala.collection.mutable.ArrayBuffer
import com.ligadata.Utils.{ KamanjaLoaderInfo }

class ByteArray extends ArrayBuffer[Byte]

class Key extends ByteArray

class Value extends ByteArray

trait IStorage {
  def Key: Key
  def Value: Value
  def Construct(Key: Key, Value: Value): Unit
}

trait DataStoreOperations {
  def add(source: IStorage): Unit
  def put(source: IStorage): Unit
  def get(key: Key, target: IStorage): Unit
  def get(key: Key, handler: (Value) => Unit): Unit
  def del(key: Key): Unit
  def del(source: IStorage): Unit
  def getAllKeys(handler: (Key) => Unit): Unit
  def putBatch(sourceArray: Array[IStorage]): Unit
  def delBatch(keyArray: Array[Key]): Unit
}

trait DataStore extends DataStoreOperations {
  def beginTx(): Transaction
  def endTx(tx: Transaction): Unit
  def commitTx(tx: Transaction): Unit

  def Shutdown(): Unit
  def TruncateStore(): Unit
}

trait Transaction extends DataStoreOperations {
  val parent: DataStore // Parent Data Store
}

/*
datastoreConfig has the following format

// Mandatory fields
 * 	for built-in databases connections like hbase, cassandra, hashmap, treemap & redis you can use - StoreType or (ClassName,JarName &  DependencyJars)
 *  for non built-in databases connections you must provide ClassName,JarName &  DependencyJars

// Other database related stuff. Like SchemaName, Location or hostlist, etc

Ex: Sample1 
{
  "StoreType": "hashmap",
  "SchemaName": "testdata",
  "Location": "{InstallDirectory}/storage"
}

      
Ex: Sample2      
{
  "ClassName": "com.ligadata.OutputAdapters.KafkaProducer$",
  "JarName": "kafkasimpleinputoutputadapters_2.10-1.0.jar",
  "DependencyJars": [
    "jopt-simple-3.2.jar",
    "kafka_2.10-0.8.1.1.jar",
    "metrics-core-2.2.0.jar",
    "zkclient-0.3.jar",
    "kamanjabase_2.10-1.0.jar"
  ],
  "SchemaName": "statusinfo",
  "Location": "{InstallDirectory}/storage"
}
*/

// Storage Adapter Object to create storage adapter
trait StorageAdapterObj {
  def CreateStorageAdapter(kvManagerLoader: KamanjaLoaderInfo, datastoreConfig: String, tableName: String): DataStore
}

