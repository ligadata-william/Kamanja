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

package com.ligadata.keyvaluestore

import com.ligadata.StorageBase.{ Key, Value, IStorage, DataStoreOperations, DataStore, Transaction, StorageAdapterObj }
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.{ BufferedMutator, BufferedMutatorParams, Connection, ConnectionFactory }

import org.apache.hadoop.hbase._
import org.apache.log4j._

import java.nio.ByteBuffer
import java.io.IOException
import org.apache.hadoop.security.UserGroupInformation;
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import com.ligadata.Exceptions._
import com.ligadata.Utils.{ KamanjaLoaderInfo }

import scala.collection.JavaConversions._

/*
datastoreConfig should have the following:
	Mandatory Options:
		hostlist/Location
		schema/SchemaName

	Optional Options:
		authentication
		regionserver_principal
		master_principal
		principal
		keytab

		All the optional values may come from "AdapterSpecificConfig" also. That is the old way of giving more information specific to Adapter
*/

//import org.apache.hadoop.hbase.util.Bytes;
/*
 * create 'default', 'value'
 *
 * put 'default', 'KEYKEY', 'value', 'ValueValue'
 *
 * scan 'default'
 *
 */

class KeyValueHBaseTx(val parent: DataStore) extends Transaction {
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

class KeyValueHBase(val kvManagerLoader: KamanjaLoaderInfo, val datastoreConfig: String, val tableName: String) extends DataStore {
  val adapterConfig = if (datastoreConfig != null) datastoreConfig.trim else ""
  val loggerName = this.getClass.getName
  val logger = Logger.getLogger(loggerName)

  if (adapterConfig.size == 0) {
    throw new Exception("Not found valid HBase Configuration.")
  }

  logger.debug("HBase configuration:" + adapterConfig)
  var parsed_json: Map[String, Any] = null
  try {
    val json = parse(adapterConfig)
    if (json == null || json.values == null) {
      logger.error("Failed to parse HBase JSON configuration string:" + adapterConfig)
      throw new Exception("Failed to parse HBase JSON configuration string:" + adapterConfig)
    }
    parsed_json = json.values.asInstanceOf[Map[String, Any]]
  } catch {
    case e: Exception => {
      logger.error("Failed to parse HBase JSON configuration string:%s. Reason:%s Message:%s".format(adapterConfig, e.getCause, e.getMessage))
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

  val hostnames = if (parsed_json.contains("hostlist")) parsed_json.getOrElse("hostlist", "localhost").toString.trim else parsed_json.getOrElse("Location", "localhost").toString.trim
  val keyspace = if (parsed_json.contains("schema")) parsed_json.getOrElse("schema", "default").toString.trim else parsed_json.getOrElse("SchemaName", "default").toString.trim
  val tablename = tableName

  val table = keyspace + ":" + tablename
  val config = HBaseConfiguration.create();

  config.setInt("zookeeper.session.timeout", getOptionalField("zookeeper_session_timeout", parsed_json, adapterSpecificConfig_json, "5000").toString.trim.toInt);
  config.setInt("zookeeper.recovery.retry", getOptionalField("zookeeper_recovery_retry", parsed_json, adapterSpecificConfig_json, "1").toString.trim.toInt);
  config.setInt("hbase.client.retries.number", getOptionalField("hbase_client_retries_number", parsed_json, adapterSpecificConfig_json, "3").toString.trim.toInt);
  config.setInt("hbase.client.pause", getOptionalField("hbase_client_pause", parsed_json, adapterSpecificConfig_json, "5000").toString.trim.toInt);
  config.set("hbase.zookeeper.quorum", hostnames);

  val keyMaxSz = getOptionalField("hbase_client_keyvalue_maxsize", parsed_json, adapterSpecificConfig_json, "104857600").toString.trim.toInt
  var clntWrtBufSz = getOptionalField("hbase_client_write_buffer", parsed_json, adapterSpecificConfig_json, "104857600").toString.trim.toInt

  if (clntWrtBufSz < keyMaxSz)
    clntWrtBufSz = keyMaxSz + 1024 // 1K Extra

  config.setInt("hbase.client.keyvalue.maxsize", keyMaxSz);
  config.setInt("hbase.client.write.buffer", clntWrtBufSz);

  var isKerberos: Boolean = false
  var ugi: UserGroupInformation = null

  val auth = getOptionalField("authentication", parsed_json, adapterSpecificConfig_json, "").toString.trim
  if (auth.size > 0) {
    isKerberos = auth.compareToIgnoreCase("kerberos") == 0
    if (isKerberos) {
      try {
        val regionserver_principal = getOptionalField("regionserver_principal", parsed_json, adapterSpecificConfig_json, "").toString.trim
        val master_principal = getOptionalField("master_principal", parsed_json, adapterSpecificConfig_json, "").toString.trim
        val principal = getOptionalField("principal", parsed_json, adapterSpecificConfig_json, "").toString.trim
        val keytab = getOptionalField("keytab", parsed_json, adapterSpecificConfig_json, "").toString.trim

        logger.debug("HBase info => Hosts:" + hostnames + ", Keyspace:" + keyspace + ", Principal:" + principal + ", Keytab:" + keytab + ", hbase.regionserver.kerberos.principal:" + regionserver_principal + ", hbase.master.kerberos.principal:" + master_principal)

        config.set("hadoop.proxyuser.hdfs.groups", "*")
        config.set("hadoop.security.authorization", "true")
        config.set("hbase.security.authentication", "kerberos")
        config.set("hadoop.security.authentication", "kerberos")
        config.set("hbase.regionserver.kerberos.principal", regionserver_principal)
        config.set("hbase.master.kerberos.principal", master_principal)

        org.apache.hadoop.security.UserGroupInformation.setConfiguration(config);

        UserGroupInformation.loginUserFromKeytab(principal, keytab);

        ugi = UserGroupInformation.getLoginUser
      } catch {
        case e: Exception => {
          logger.error("HBase issue from JSON configuration string:%s. Reason:%s Message:%s".format(adapterConfig, e.getCause, e.getMessage))
          throw e
        }
      }
    } else {
      logger.error("Not handling any authentication other than KERBEROS. AdapterSpecificConfig:" + adapterConfig)
      throw new Exception("Not handling any authentication other than KERBEROS. AdapterSpecificConfig:" + adapterConfig)
    }
  } else {
    logger.debug("HBase info => Hosts:" + hostnames + ", Keyspace:" + keyspace)
  }

  val listener = new BufferedMutator.ExceptionListener() {
    override def onException(e: RetriesExhaustedWithDetailsException, mutator: BufferedMutator) {
      for (i <- 0 until e.getNumExceptions) {
        logger.error("Failed to sent put: " + e.getRow(i))
      }
    }
  }

  val valOfTbl = TableName.valueOf(table)
  val params = new BufferedMutatorParams(valOfTbl).listener(listener);

  val valBytes = Bytes.toBytes("value")
  val baseBytes = Bytes.toBytes("base")

  var conn: Connection = _
  var mutator: BufferedMutator = _
  try {
    conn = ConnectionFactory.createConnection(config);
    mutator = conn.getBufferedMutator(params)
  } catch {
    case e: Exception => {
      if (conn != null)
        conn.close()
      throw new ConnectionFailedException("Unable to connect to hbase at " + hostnames + ":" + e.getMessage())
    }
  }

  createNamespace(keyspace)
  createTable(table)

  private def relogin: Unit = {
    try {
      if (ugi != null)
        ugi.checkTGTAndReloginFromKeytab
    } catch {
      case e: Exception => {
        logger.error("Failed to relogin into HBase. Message:" + e.getMessage())
        // Not throwing exception from here
      }
    }
  }

  def createNamespace(nameSpace: String): Unit = {
    relogin
    val admin = new HBaseAdmin(config);
    try {
      val nsd = admin.getNamespaceDescriptor(nameSpace)
      return
    } catch {
      case e: NamespaceNotFoundException => {
        logger.info("Namespace " + nameSpace + " does not exist, create it.")
      }
      case e: Exception => {
        logger.info("Namespace " + nameSpace + " may not be existing, create it. Message:" + e.getMessage + ", Cause:" + e.getCause)
      }
    }
    try {
      admin.createNamespace(NamespaceDescriptor.create(nameSpace).build)
    } catch {
      case e: Exception => {
        val stackTrace = StackTrace.ThrowableTraceString(e)
        logger.error("Failed to create namespace. StackTrace:" + stackTrace)
        throw new ConnectionFailedException("Unable to create hbase name space " + nameSpace + ":" + e.getMessage())
      }
    }
  }

  private def createTable(tableName: String): Unit = {
    relogin
    val admin = new HBaseAdmin(config);
    var tableExists = false
    try {
      tableExists = admin.tableExists(tableName)
    } catch {
      case e: Exception => {
        logger.info("Table " + tableName + " may not be existing. Message:" + e.getMessage + ", Cause:" + e.getCause)
        // If we get execption we are treating the table does not exists 
      }
    }

    if (!tableExists) {
      val tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
      val colDesc1 = new HColumnDescriptor("key".getBytes())
      val colDesc2 = new HColumnDescriptor("value".getBytes())
      // colDesc2.setMobEnabled(true);
      // colDesc2.setMobThreshold(102400L);
      tableDesc.addFamily(colDesc1)
      tableDesc.addFamily(colDesc2)
      try {
        admin.createTable(tableDesc);
      } catch {
        case e: TableExistsException => {
          // Table already exists.
        }
        case e: Exception => {
          val stackTrace = StackTrace.ThrowableTraceString(e)
          logger.error("Failed to create table. StackTrace:" + stackTrace)
          throw e
        }
      }
    }
  }

  override def add(source: IStorage): Unit = {
    relogin
    var p = new Put(source.Key.toArray[Byte])

    p.addColumn(valBytes, baseBytes, source.Value.toArray[Byte])

    mutator.mutate(p)
  }

  override def put(source: IStorage): Unit = {
    relogin
    var p = new Put(source.Key.toArray[Byte])

    p.addColumn(valBytes, baseBytes, source.Value.toArray[Byte])

    mutator.mutate(p)
  }

  override def putBatch(sourceArray: Array[IStorage]): Unit = {
    relogin

    val puts = sourceArray.map(source => {
      var p = new Put(source.Key.toArray[Byte])
      p.addColumn(valBytes, baseBytes, source.Value.toArray[Byte])
      p
    }).toList

    mutator.mutate(puts)
  }

  override def delBatch(keyArray: Array[Key]): Unit = {
    relogin

    val dels = keyArray.map(k => {
      val p = new Delete(k.toArray[Byte])
      p
    }).toList

    mutator.mutate(dels)
  }

  override def get(key: Key, handler: (Value) => Unit): Unit = {
    relogin
    try {
      var p = new Get(key.toArray[Byte])

      p.addColumn(valBytes, baseBytes)

      val tableHBase = conn.getTable(valOfTbl);
      try {
        val result = tableHBase.get(p)
        val v = result.getValue(valBytes, baseBytes)

        val value = new Value
        value ++= v

        handler(value)
      } catch {
        case e: Exception => throw e
      } finally {
        if (tableHBase != null)
          tableHBase.close
      }
    } catch {
      case e: Exception => {
        throw new KeyNotFoundException(e.getMessage())
      }
    }
  }

  override def get(key: Key, target: IStorage): Unit = {
    relogin
    try {
      var p = new Get(key.toArray[Byte])

      p.addColumn(valBytes, baseBytes)

      val tableHBase = conn.getTable(valOfTbl);
      try {
        val result = tableHBase.get(p)

        val v = result.getValue(valBytes, baseBytes)

        val value = new Value
        value ++= v

        target.Construct(key, value)
      } catch {
        case e: Exception => throw e
      } finally {
        if (tableHBase != null)
          tableHBase.close
      }
    } catch {
      case e: Exception => {
        throw new KeyNotFoundException(e.getMessage())
      }
    }
  }

  override def del(key: Key): Unit = {
    relogin
    val p = new Delete(key.toArray[Byte])
    mutator.mutate(p)
  }

  override def del(source: IStorage): Unit = { del(source.Key) }

  override def beginTx(): Transaction = { new KeyValueHBaseTx(this) }

  override def endTx(tx: Transaction): Unit = {}

  override def commitTx(tx: Transaction): Unit = {
    relogin
    if (mutator != null)
      mutator.flush()
  }

  override def Shutdown(): Unit = {
    if (mutator != null) {
      mutator.flush()
      mutator.close()
      mutator = null
    }
    if (conn != null) {
      conn.close()
      conn = null
    }
  }

  override def TruncateStore(): Unit = {
    relogin
    /*
		val a = new HBaseAdmin(connection)

		if (a.isTableEnabled(table))
			a.disableTable(table);

		a.deleteTable(table);

		a.createTable(tableHBase.getTableDescriptor(), Array(Bytes.toBytes("value:base") ) )

		a.close()
*/
    getAllKeys({ (key: Key) => del(key) })

  }

  override def getAllKeys(handler: (Key) => Unit): Unit = {
    relogin
    var p = new Scan()

    val tableHBase = conn.getTable(valOfTbl);
    try {
      val iter = tableHBase.getScanner(p)

      try {
        var fContinue = true

        do {
          val row = iter.next()
          if (row != null) {
            val v = row.getRow()
            val key = new Key
            key ++= v

            handler(key)
          } else {
            fContinue = false;
          }
        } while (fContinue)

      } finally {
        iter.close()
      }
    } catch {
      case e: Exception => throw e
    } finally {
      if (tableHBase != null)
        tableHBase.close
    }
  }
}

// To create HBase Datastore instance
object KeyValueHBase extends StorageAdapterObj {
  override def CreateStorageAdapter(kvManagerLoader: KamanjaLoaderInfo, datastoreConfig: String, tableName: String): DataStore = new KeyValueHBase(kvManagerLoader, datastoreConfig, tableName)
}

