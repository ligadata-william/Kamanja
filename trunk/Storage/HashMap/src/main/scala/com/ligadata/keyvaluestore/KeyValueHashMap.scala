package com.ligadata.keyvaluestore

import com.ligadata.StorageBase.{ Key, Value, IStorage, DataStoreOperations, DataStore, Transaction, StorageAdapterObj }
import org.mapdb._;
import java.io.File;
import java.nio.ByteBuffer
import org.apache.log4j._
import com.ligadata.Exceptions._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import com.ligadata.Utils.{ KamanjaLoaderInfo }

/*
datastoreConfig should have the following:
	Mandatory Options:
		path/Location

	Optional Options:
		inmemory
		withtransaction

		All the optional values may come from "AdapterSpecificConfig" also. That is the old way of giving more information specific to Adapter
*/

/*

No schema setup

 */

class KeyValueHashMapTx(val parent: DataStore) extends Transaction {
  override def add(source: IStorage): Unit = { parent.add(source) }
  override def put(source: IStorage): Unit = { parent.put(source) }
  override def get(key: Key, target: IStorage): Unit = { parent.get(key, target) }
  override def get(key: Key, handler: (Value) => Unit): Unit = { parent.get(key, handler) }
  override def del(key: Key): Unit = { parent.del(key) }
  override def del(source: IStorage): Unit = { parent.del(source) }
  override def getAllKeys(handler: (Key) => Unit): Unit = { parent.getAllKeys(handler) }
  override def putBatch(sourceArray: Array[IStorage]): Unit = { parent.putBatch(sourceArray) }
  override def delBatch(keyArray: Array[Key]): Unit = { parent.delBatch(keyArray) }
}

class KeyValueHashMap(val kvManagerLoader: KamanjaLoaderInfo, val datastoreConfig: String, val tableName: String) extends DataStore {
  val adapterConfig = if (datastoreConfig != null) datastoreConfig.trim else ""
  val loggerName = this.getClass.getName
  val logger = Logger.getLogger(loggerName)

  if (adapterConfig.size == 0) {
    throw new Exception("Not found valid HashMap Configuration.")
  }

  logger.debug("HashMap configuration:" + adapterConfig)
  var parsed_json: Map[String, Any] = null
  try {
    val json = parse(adapterConfig)
    if (json == null || json.values == null) {
      logger.error("Failed to parse HashMap JSON configuration string:" + adapterConfig)
      throw new Exception("Failed to parse HashMap JSON configuration string:" + adapterConfig)
    }
    parsed_json = json.values.asInstanceOf[Map[String, Any]]
  } catch {
    case e: Exception => {
      logger.error("Failed to parse HashMap JSON configuration string:%s. Reason:%s Message:%s".format(adapterConfig, e.getCause, e.getMessage))
      throw e
    }
  }

  // Getting AdapterSpecificConfig if it has
  var adapterSpecificConfig_json: Map[String, Any] = null

  if (parsed_json.contains("AdapterSpecificConfig")) {
    val adapterSpecificStr = parsed_json.getOrElse("AdapterSpecificConfig", "").toString.trim
    if (adapterSpecificStr.size > 0) {
      try {
        val json = parse(adapterSpecificStr)
        if (json == null || json.values == null) {
          logger.error("Failed to parse Cassandra Adapter Specific JSON configuration string:" + adapterSpecificStr)
          throw new Exception("Failed to parse Cassandra Adapter Specific JSON configuration string:" + adapterSpecificStr)
        }
        adapterSpecificConfig_json = json.values.asInstanceOf[Map[String, Any]]
      } catch {
        case e: Exception => {
          logger.error("Failed to parse Cassandra Adapter Specific JSON configuration string:%s. Reason:%s Message:%s".format(adapterSpecificStr, e.getCause, e.getMessage))
          throw e
        }
      }
    }
  }

  private def getOptionalField(key: String, main_json: Map[String, Any], adapterSpecific_json: Map[String, Any], default: Any): Any = {
    if (main_json != null) {
      val mainVal = main_json.getOrElse(key, null)
      if (mainVal != null)
        return mainVal
    }
    if (adapterSpecific_json != null) {
      val mainVal1 = adapterSpecific_json.getOrElse(key, null)
      if (mainVal1 != null)
        return mainVal1
    }
    return default
  }

  val path = if (parsed_json.contains("path")) parsed_json.getOrElse("path", ".").toString.trim else parsed_json.getOrElse("Location", "localhost").toString.trim
  val table = tableName.trim
  val keyspace = table // using table name as schema name
  val InMemory = getOptionalField("inmemory", parsed_json, adapterSpecificConfig_json, "false").toString.trim.toBoolean
  val withTransactions = getOptionalField("withtransaction", parsed_json, adapterSpecificConfig_json, "false").toString.trim.toBoolean

  var db: DB = null

  if (InMemory == true) {
    db = DBMaker.newMemoryDB()
      .make()
  } else {
    val dir = new File(path);
    if (!dir.exists()) {
      // attempt to create the directory here
      dir.mkdir();
    }
    db = DBMaker.newFileDB(new File(path + "/" + keyspace + ".hdb"))
      .closeOnJvmShutdown()
      .mmapFileEnable()
      .transactionDisable()
      .commitFileSyncDisable()
      .make()
  }

  var map = db.createHashMap(table)
    .hasher(Hasher.BYTE_ARRAY)
    .makeOrGet[Array[Byte], Array[Byte]]()

  override def add(source: IStorage): Unit = {
    map.putIfAbsent(source.Key.toArray[Byte], source.Value.toArray[Byte])
    if (withTransactions)
      db.commit() //persist changes into disk
  }

  override def put(source: IStorage): Unit = {
    map.put(source.Key.toArray[Byte], source.Value.toArray[Byte])
    if (withTransactions)
      db.commit() //persist changes into disk
  }

  override def putBatch(sourceArray: Array[IStorage]): Unit = {
    sourceArray.foreach(source => {
      map.put(source.Key.toArray[Byte], source.Value.toArray[Byte])
    })
    if (withTransactions)
      db.commit() //persist changes into disk
  }

  override def delBatch(keyArray: Array[Key]): Unit = {
    keyArray.foreach(k => {
      map.remove(k.toArray[Byte])
    })
    if (withTransactions)
      db.commit() //persist changes into disk
  }

  override def get(key: Key, handler: (Value) => Unit): Unit = {
    val buffer = map.get(key.toArray[Byte])

    // Construct the output value
    val value = new Value
    if (buffer != null) {
      value ++= buffer
    } else {
      throw new KeyNotFoundException("Key Not found")
    }

    handler(value)
  }

  override def get(key: Key, target: IStorage): Unit = {
    val buffer = map.get(key.toArray[Byte])

    // Construct the output value
    val value = new Value
    if (buffer != null) {
      value ++= buffer
    } else {
      throw new KeyNotFoundException("Key Not found")
    }

    target.Construct(key, value)
  }

  override def del(key: Key): Unit = {
    map.remove(key.toArray[Byte])
    if (withTransactions)
      db.commit(); //persist changes into disk
  }

  override def del(source: IStorage): Unit = { del(source.Key) }

  override def beginTx(): Transaction = { new KeyValueHashMapTx(this) }

  override def endTx(tx: Transaction): Unit = {}

  override def commitTx(tx: Transaction): Unit = {}

  override def Shutdown(): Unit = {
    if (db != null && db.isClosed() == false) {
      logger.debug("Trying to shutdown hashmap db")
      try {
        db.commit(); //persist changes into disk
        db = null
        map.close();
      } catch {
        case e: NullPointerException => {
          logger.error("Unexpected Null pointer exception when closing hashmap, seems like internal bug related to mapdb ")
        }
        case e: Exception => {
          logger.error("Unexpected error when closing hashmap " + e.getMessage())
        }
      }
    }
  }

  override def TruncateStore(): Unit = {
    map.clear()
    if (withTransactions)
      db.commit() //persist changes into disk

    // Defrag on startup
    db.compact()
  }

  override def getAllKeys(handler: (Key) => Unit): Unit = {
    var iter = map.keySet().iterator()
    while (iter.hasNext()) {
      val buffer = iter.next()

      // Construct the output value
      // BUGBUG-jh-20140703: There should be a more concise way to get the data
      //
      val key = new Key
      for (b <- buffer)
        key += b

      handler(key)
    }
  }
}

// To create Hashmap Datastore instance
object KeyValueHashMap extends StorageAdapterObj {
  override def CreateStorageAdapter(kvManagerLoader: KamanjaLoaderInfo, datastoreConfig: String, tableName: String): DataStore = new KeyValueHashMap(kvManagerLoader, datastoreConfig, tableName)
}
