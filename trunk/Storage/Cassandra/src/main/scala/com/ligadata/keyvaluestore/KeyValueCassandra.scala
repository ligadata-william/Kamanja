package com.ligadata.keyvaluestore

import com.ligadata.StorageBase.{ Key, Value, IStorage, DataStoreOperations, DataStore, Transaction, StorageAdapterObj }
import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Session
import com.datastax.driver.core.querybuilder.Insert
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.BatchStatement
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
		hostlist/Location
		schema/SchemaName

	Optional Options:
		user
		password
		replication_class
		replication_factor
		ConsistencyLevelRead
		ConsistencyLevelWrite
		ConsistencyLevelDelete
		
		All the optional values may come from "AdapterSpecificConfig" also. That is the old way of giving more information specific to Adapter
*/

/*
  	You open connection to a cluster hostname[,hostname]:port
  	You could provide username/password

 	You can operator on keyspace / table

 	if key space is missing we will try to create
 	if table is missing we will try to create

	-- Lets start with this schema
	--
	CREATE KEYSPACE default WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '4' };
	USE default;
	CREATE TABLE default (key blob, value blob, primary key(key) );
 */

class KeyValueCassandraTx(val parent: DataStore) extends Transaction {
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

class KeyValueCassandra(val kvManagerLoader: KamanjaLoaderInfo, val datastoreConfig: String, val tableName: String) extends DataStore {
  val adapterConfig = if (datastoreConfig != null) datastoreConfig.trim else ""
  val loggerName = this.getClass.getName
  val logger = Logger.getLogger(loggerName)

  if (adapterConfig.size == 0) {
    throw new Exception("Not found valid Cassandra Configuration.")
  }

  logger.debug("Cassandra configuration:" + adapterConfig)
  var parsed_json: Map[String, Any] = null
  try {
    val json = parse(adapterConfig)
    if (json == null || json.values == null) {
      logger.error("Failed to parse Cassandra JSON configuration string:" + adapterConfig)
      throw new Exception("Failed to parse Cassandra JSON configuration string:" + adapterConfig)
    }
    parsed_json = json.values.asInstanceOf[Map[String, Any]]
  } catch {
    case e: Exception => {
      logger.error("Failed to parse Cassandra JSON configuration string:%s. Reason:%s Message:%s".format(adapterConfig, e.getCause, e.getMessage))
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

  // Read all cassandra parameters
  val hostnames = if (parsed_json.contains("hostlist")) parsed_json.getOrElse("hostlist", "localhost").toString.trim else parsed_json.getOrElse("Location", "localhost").toString.trim
  val keyspace = if (parsed_json.contains("schema")) parsed_json.getOrElse("schema", "default").toString.trim else parsed_json.getOrElse("SchemaName", "default").toString.trim
  val table = tableName.trim
  val replication_class = getOptionalField("replication_class", parsed_json, adapterSpecificConfig_json, "SimpleStrategy").toString.trim
  val replication_factor = getOptionalField("replication_factor", parsed_json, adapterSpecificConfig_json, "1").toString.trim
  val consistencylevelRead = ConsistencyLevel.valueOf(getOptionalField("ConsistencyLevelRead", parsed_json, adapterSpecificConfig_json, "ONE").toString.trim)
  val consistencylevelWrite = ConsistencyLevel.valueOf(getOptionalField("ConsistencyLevelWrite", parsed_json, adapterSpecificConfig_json, "ANY").toString.trim)
  val consistencylevelDelete = ConsistencyLevel.valueOf(getOptionalField("ConsistencyLevelDelete", parsed_json, adapterSpecificConfig_json, "ANY").toString.trim)
  val clusterBuilder = Cluster.builder()
  var cluster: Cluster = _
  var session: Session = _

  // getOptionalField("ConsistencyLevelRead", parsed_json, adapterSpecificConfig_json, "ONE")

  var keyspace_exists = false

  try {
    clusterBuilder.addContactPoints(hostnames)
    val usr = getOptionalField("user", parsed_json, adapterSpecificConfig_json, null)
    if (usr != null)
      clusterBuilder.withCredentials(usr.toString.trim, getOptionalField("password", parsed_json, adapterSpecificConfig_json, "").toString.trim)
    cluster = clusterBuilder.build()

    if (cluster.getMetadata().getKeyspace(keyspace) == null) {
      logger.warn("The keyspace " + keyspace + " doesn't exist yet, we will create a new keyspace and continue")
      // create a session that is not associated with a key space yet so we can create one if needed
      session = cluster.connect();
      // create keyspace if not exists
      val createKeySpaceStmt = "CREATE KEYSPACE IF NOT EXISTS " + keyspace + " with replication = {'class':'" + replication_class + "', 'replication_factor':" + replication_factor + "};"
      try {
        session.execute(createKeySpaceStmt);
      } catch {
        case e: Exception => {
          throw new CreateKeySpaceFailedException("Unable to create keyspace " + keyspace + ":" + e.getMessage())
        }
      }
      // make sure the session is associated with the new tablespace, can be expensive if we create recycle sessions  too often
      session.close()
      session = cluster.connect(keyspace)
    } else {
      keyspace_exists = true
      session = cluster.connect(keyspace)
    }
  } catch {
    case e: Exception => {
      throw new ConnectionFailedException("Unable to connect to cassandra at " + hostnames + ":" + e.getMessage())
    }
  }

  // Check if table exists or create if needed
  val createTblStmt = "CREATE TABLE IF NOT EXISTS " + table + " (key blob, value blob, primary key(key) );"
  session.execute(createTblStmt);

  //
  var insertStmt = session.prepare("INSERT INTO " + table + " (key, value) values(?, ?);")
  var insertStmt1 = session.prepare("INSERT INTO " + table + " (key, value) values(?, ?) IF NOT EXISTS;")
  var selectStmt = session.prepare("SELECT value FROM " + table + " WHERE key = ?;")
  var selectAllKeysStmt = session.prepare("SELECT key FROM " + table + ";")
  var deleteStmt = session.prepare("DELETE from " + table + " WHERE Key=?;")
  var updateStmt = session.prepare("UPDATE " + table + " SET value = ? WHERE Key=?;")

  override def add(source: IStorage): Unit = {
    var key = ByteBuffer.wrap(source.Key.toArray[Byte]);
    var value = ByteBuffer.wrap(source.Value.toArray[Byte]);
    val e = session.execute(insertStmt1.bind(key, value).setConsistencyLevel(consistencylevelWrite))

    if (e.getColumnDefinitions().size() > 1)
      throw new Exception("not applied")
  }

  override def put(source: IStorage): Unit = {
    var key = ByteBuffer.wrap(source.Key.toArray[Byte]);
    var value = ByteBuffer.wrap(source.Value.toArray[Byte]);
    session.execute(updateStmt.bind(value, key).setConsistencyLevel(consistencylevelWrite))
  }

  override def putBatch(sourceArray: Array[IStorage]): Unit = {
    val batch = new BatchStatement
    sourceArray.foreach(source => {
      var key = ByteBuffer.wrap(source.Key.toArray[Byte]);
      var value = ByteBuffer.wrap(source.Value.toArray[Byte]);
      batch.add(updateStmt.bind(value, key));
    })
    session.execute(batch);
  }

  override def delBatch(keyArray: Array[Key]): Unit = {
    val batch = new BatchStatement
    keyArray.foreach(k => {
      var key = ByteBuffer.wrap(k.toArray[Byte]);
      batch.add(deleteStmt.bind(key));
    })
    session.execute(batch);
  }

  override def get(key: Key, handler: (Value) => Unit): Unit = {
    val key1 = ByteBuffer.wrap(key.toArray[Byte]);
    val rs = session.execute(selectStmt.bind(key1).setConsistencyLevel(consistencylevelRead))

    if (rs.getAvailableWithoutFetching() == 0) {
      throw new KeyNotFoundException("Key Not found")
    }

    // Construct the output value
    // BUGBUG-jh-20140703: There should be a more concise way to get the data
    //
    val value = new Value
    val buffer: ByteBuffer = rs.one().getBytes(0)
    if (buffer != null) {
      while (buffer.hasRemaining())
        value += buffer.get()
    } else {
      throw new KeyNotFoundException("Key Not found")
    }
    handler(value)
  }

  override def get(key: Key, target: IStorage): Unit = {
    val key1 = ByteBuffer.wrap(key.toArray[Byte]);
    val rs = session.execute(selectStmt.bind(key1).setConsistencyLevel(consistencylevelRead))

    if (rs.getAvailableWithoutFetching() == 0) {
      throw new KeyNotFoundException("Key Not found")
    }
    // Construct the output value
    // BUGBUG-jh-20140703: There should be a more concise way to get the data
    //
    val value = new Value

    val buffer: ByteBuffer = rs.one().getBytes(0)
    if (buffer != null) {
      while (buffer.hasRemaining())
        value += buffer.get()
    } else {
      throw new KeyNotFoundException("Key Not found")
    }
    target.Construct(key, value)
  }

  override def del(key: Key): Unit = {
    val key1 = ByteBuffer.wrap(key.toArray[Byte]);
    session.execute(deleteStmt.bind(key1).setConsistencyLevel(consistencylevelDelete))
  }

  override def del(source: IStorage): Unit = { del(source.Key) }

  override def beginTx(): Transaction = { new KeyValueCassandraTx(this) }

  override def endTx(tx: Transaction): Unit = {}

  override def commitTx(tx: Transaction): Unit = {}

  override def Shutdown(): Unit = {
    session.close()

    cluster.close()
  }

  override def TruncateStore(): Unit = {
    var stmt = session.prepare("truncate " + table + ";")
    val rs = session.execute(stmt.bind().setConsistencyLevel(consistencylevelDelete))
  }

  override def getAllKeys(handler: (Key) => Unit): Unit = {
    val rs = session.execute(selectAllKeysStmt.bind().setConsistencyLevel(consistencylevelRead))

    val iter = rs.iterator();
    while (iter.hasNext()) {
      if (rs.getAvailableWithoutFetching() == 100 && !rs.isFullyFetched())
        rs.fetchMoreResults();

      val row = iter.next()

      val key = new Key
      val buffer: ByteBuffer = row.getBytes(0)
      while (buffer.hasRemaining())
        key += buffer.get()

      handler(key)
    }
  }

}

// To create Cassandra Datastore instance
object KeyValueCassandra extends StorageAdapterObj {
  override def CreateStorageAdapter(kvManagerLoader: KamanjaLoaderInfo, datastoreConfig: String, tableName: String): DataStore = new KeyValueCassandra(kvManagerLoader, datastoreConfig, tableName)
}

