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
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.Connection
import com.ligadata.StorageBase.{ Key,Value,DataStoreOperationsV2,DataStoreV2,TransactionV2,StorageAdapterObjV2 }
import java.nio.ByteBuffer
import org.apache.log4j._
import com.ligadata.Exceptions._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import com.ligadata.Utils.{ KamanjaLoaderInfo }
import java.util.Date

class SqlServerAdapter(val kvManagerLoader: KamanjaLoaderInfo, val datastoreConfig: String) extends DataStoreV2 {

  val adapterConfig = if (datastoreConfig != null) datastoreConfig.trim else ""
  val loggerName = this.getClass.getName
  val logger = Logger.getLogger(loggerName)

  if (adapterConfig.size == 0) {
    throw new Exception("Not found valid SqlServer Configuration.")
  }

  logger.debug("SqlServer configuration:" + adapterConfig)
  var parsed_json: Map[String, Any] = null
  try {
    val json = parse(adapterConfig)
    if (json == null || json.values == null) {
      logger.error("Failed to parse SqlServer JSON configuration string:" + adapterConfig)
      throw new Exception("Failed to parse SqlServer JSON configuration string:" + adapterConfig)
    }
    parsed_json = json.values.asInstanceOf[Map[String, Any]]
  } catch {
    case e: Exception => {
      logger.error("Failed to parse SqlServer JSON configuration string:%s. Reason:%s Message:%s".format(adapterConfig, e.getCause, e.getMessage))
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
          logger.error("Failed to parse SqlServer Adapter Specific JSON configuration string:" + adapterSpecificStr)
          throw new Exception("Failed to parse SqlServer Adapter Specific JSON configuration string:" + adapterSpecificStr)
        }
        adapterSpecificConfig_json = json.values.asInstanceOf[Map[String, Any]]
      } catch {
        case e: Exception => {
          logger.error("Failed to parse SqlServer Adapter Specific JSON configuration string:%s. Reason:%s Message:%s".format(adapterSpecificStr, e.getCause, e.getMessage))
          throw e
        }
      }
    }
  }

  // Read all sqlServer parameters
  var hostname:String = null;
  if (parsed_json.contains("hostname")) {
    hostname = parsed_json.get("hostname").toString.trim
  }
  else {
    throw new ConnectionFailedException("Unable to find hostname in adapterConfig ")
  }

  var database:String = null;
  if (parsed_json.contains("database")) {
    database = parsed_json.get("database").toString.trim
  }
  else {
    throw new ConnectionFailedException("Unable to find database in adapterConfig ")
  }

  var user:String = null;
  if (parsed_json.contains("user")) {
    user = parsed_json.get("user").toString.trim
  }
  else {
    throw new ConnectionFailedException("Unable to find user in adapterConfig ")
  }

  var password:String = null;
  if (parsed_json.contains("password")) {
    password = parsed_json.get("password").toString.trim
  }
  else {
    throw new ConnectionFailedException("Unable to find password in adapterConfig ")
  }

  var jdbcUrl = "jdbc:sqlserver://" + hostname + ";databaseName=" + database + ";user=" + user + ";password=" + password
  Class.forName("com.mysql.jdbc.Driver");


  override def put(containerName:String, part_date: Date,key: Array[String], transactionId: Long, value: Value): Unit = {
    var con:Connection = null
    var stmt:PreparedStatement = null
    try{
      con = DriverManager.getConnection(jdbcUrl);
      var insertSql = "insert into " + containerName + "(part_date,key_str,transactionId,value) values(?,?,?,?)"
      stmt = con.prepareStatement(insertSql)
      stmt.setDate(1,new java.sql.Date(part_date.getTime))
      stmt.setString(2,key.mkString(":"))
      stmt.setLong(3,transactionId)
      stmt.setBinaryStream(4,new java.io.ByteArrayInputStream(value),value.length)
      stmt.executeUpdate();
    } catch{
      case e:Exception => {
        val stackTrace = StackTrace.ThrowableTraceString(e)
        logger.debug("Stacktrace:"+stackTrace)
	throw new Exception("Failed to save an object in the table " + containerName + ":" + e.getMessage())
      }
    } finally {
      stmt.close
      con.close
    }
  }

  override def putBatch(data_list: Array[(String, Array[(Date, Array[String], Long, Value)])]): Unit = {
    logger.info("not implemented yet")
  }


  // delete operations
  override def del(containerName: String, begin_time: Date, end_time: Date, transactionId: Long, key: Array[String]): Unit = {
    logger.info("not implemented yet")
  }

  /*
   override def delBatch(del_data: Array[(String, Array[(Date, Date, Long, Array[Array[String]])])]): Unit = {
   // Begine Time Range, End Time Range, Array of Keys.
   logger.info("not implemented yet")
  }
  */

  override def del(containerName: String, begin_time: Date, end_time: Date, key: Array[String]): Unit = {
    logger.info("not implemented yet")
  }

  override def delBatch(del_data: Array[(String, Array[(Date, Date, Array[Array[String]])])]): Unit = {
    // Begine Time Range, End Time Range, Array of Keys.
    logger.info("not implemented yet")
  }

  // get operations
  override def get(containerName: String, callbackFunction: (Date, Array[String], Long, Value) => Unit): Unit = {
    logger.info("not implemented yet")
  }
    
  override def get(containerName: String, begin_time: Date, end_time: Date, callbackFunction: (Date, Array[String], Long, Value) => Unit): Unit = {
    logger.info("not implemented yet")
  }

  override def get(containerName: String, date_range: Array[(Date, Date)], callbackFunction: (Date, Array[String], Long, Value) => Unit): Unit = {
    logger.info("not implemented yet")
  }

  override def get(containerName: String, begin_time: Date, end_time: Date, key: Array[String], callbackFunction: (Date, Array[String], Long, Value) => Unit): Unit = {
    logger.info("not implemented yet")
  }

  override def get(containerName: String, begin_time: Date, end_time: Date, keys: Array[Array[String]], callbackFunction: (Date, Array[String], Long, Value) => Unit): Unit = {
    logger.info("not implemented yet")
  }

  override def get(containerName: String, key: Array[String], callbackFunction: (Date, Array[String], Long, Value) => Unit): Unit = {
    logger.info("not implemented yet")
  }

  override def get(containerName: String, keys: Array[Array[String]], callbackFunction: (Date, Array[String], Long, Value) => Unit): Unit = {
    logger.info("not implemented yet")
  }

  override def beginTx(): TransactionV2 = { 
    new SqlServerAdapterTx(this)
  }

  override def endTx(tx: TransactionV2): Unit = {}

  override def commitTx(tx: TransactionV2): Unit = {}

  override def rollbackTx(tx: TransactionV2): Unit = {}

  override def Shutdown(): Unit = {
   logger.info("close the connection pool") 
  }

  override def TruncateStore(containerName: String): Unit = {
   logger.info("Truncate the container table") 
  }
  override def DropStore(containerName: String): Unit = {
   logger.info("drop the container table") 
  }
}


class SqlServerAdapterTx(val parent: DataStoreV2) extends TransactionV2 {

  val loggerName = this.getClass.getName
  val logger = Logger.getLogger(loggerName)

  override def put(containerName:String, part_date: Date,key: Array[String], transactionId: Long, value: Value): Unit = {
    parent.put(containerName,part_date,key,transactionId,value)
  }

  override def putBatch(data_list: Array[(String, Array[(Date, Array[String], Long, Value)])]): Unit = {
    logger.info("not implemented yet")
  }


  // delete operations
  override def del(containerName: String, begin_time: Date, end_time: Date, transactionId: Long, key: Array[String]): Unit = {
    logger.info("not implemented yet")
  }

  /*
  override def delBatch(del_data: Array[(String, Array[(Date, Date, Long, Array[Array[String]])])]): Unit = {
    // Begine Time Range, End Time Range, Array of Keys.
    logger.info("not implemented yet")
  }
  */

  override def del(containerName: String, begin_time: Date, end_time: Date, key: Array[String]): Unit = {
    logger.info("not implemented yet")
  }

  override def delBatch(del_data: Array[(String, Array[(Date, Date, Array[Array[String]])])]): Unit = {
    // Begine Time Range, End Time Range, Array of Keys.
    logger.info("not implemented yet")
  }

  // get operations
  override def get(containerName: String, callbackFunction: (Date, Array[String], Long, Value) => Unit): Unit = {
    logger.info("not implemented yet")
  }
    
  override def get(containerName: String, begin_time: Date, end_time: Date, callbackFunction: (Date, Array[String], Long, Value) => Unit): Unit = {
    logger.info("not implemented yet")
  }

  override def get(containerName: String, date_range: Array[(Date, Date)], callbackFunction: (Date, Array[String], Long, Value) => Unit): Unit = {
    logger.info("not implemented yet")
  }

  override def get(containerName: String, begin_time: Date, end_time: Date, key: Array[String], callbackFunction: (Date, Array[String], Long, Value) => Unit): Unit = {
    logger.info("not implemented yet")
  }

  override def get(containerName: String, begin_time: Date, end_time: Date, keys: Array[Array[String]], callbackFunction: (Date, Array[String], Long, Value) => Unit): Unit = {
    logger.info("not implemented yet")
  }

  override def get(containerName: String, key: Array[String], callbackFunction: (Date, Array[String], Long, Value) => Unit): Unit = {
    logger.info("not implemented yet")
  }

  override def get(containerName: String, keys: Array[Array[String]], callbackFunction: (Date, Array[String], Long, Value) => Unit): Unit = {
    logger.info("not implemented yet")
  }
}

// To create SqlServer Datastore instance
object SqlServerAdapter extends StorageAdapterObjV2 {
  override def CreateStorageAdapter(kvManagerLoader: KamanjaLoaderInfo, datastoreConfig: String): DataStoreV2 = new SqlServerAdapter(kvManagerLoader, datastoreConfig)
}
