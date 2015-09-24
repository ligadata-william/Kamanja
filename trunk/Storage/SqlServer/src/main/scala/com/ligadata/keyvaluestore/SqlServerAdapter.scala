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
import java.sql.{Statement,PreparedStatement,DatabaseMetaData,ResultSet}
import java.sql.Connection
import com.ligadata.StorageBase.{ DataStore, Transaction, StorageAdapterObj, Key, Value, StorageTimeRange }
import java.nio.ByteBuffer
import org.apache.log4j._
import com.ligadata.Exceptions._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import com.ligadata.Utils.{ KamanjaLoaderInfo }
import java.util.Date
import java.io.File
import java.net.{ URL, URLClassLoader }
import scala.collection.mutable.TreeSet
import java.sql.{ Driver, DriverPropertyInfo }
import java.util.Properties

class JdbcClassLoader(urls: Array[URL], parent: ClassLoader) extends URLClassLoader(urls, parent) {
  override def addURL(url: URL) {
    super.addURL(url)
  }
}

class DriverShim(d: Driver) extends Driver {
  private var driver: Driver = d

  def connect(u: String, p: Properties): Connection = this.driver.connect(u, p)

  def acceptsURL(u: String): Boolean = this.driver.acceptsURL(u)

  def getPropertyInfo(u: String, p: Properties): Array[DriverPropertyInfo] = this.driver.getPropertyInfo(u, p)

  def getMajorVersion(): Int = this.driver.getMajorVersion

  def getMinorVersion(): Int = this.driver.getMinorVersion

  def jdbcCompliant(): Boolean = this.driver.jdbcCompliant()

  def getParentLogger(): java.util.logging.Logger = this.driver.getParentLogger()
}

class SqlServerAdapter(val kvManagerLoader: KamanjaLoaderInfo, val datastoreConfig: String) extends DataStore {

  val adapterConfig = if (datastoreConfig != null) datastoreConfig.trim else ""
  val loggerName = this.getClass.getName
  val logger = Logger.getLogger(loggerName)
  private val loadedJars: TreeSet[String] = new TreeSet[String];
  private val clsLoader = new JdbcClassLoader(ClassLoader.getSystemClassLoader().asInstanceOf[URLClassLoader].getURLs(), getClass().getClassLoader())

  //val classLoader = new KamanjaLoaderInfo
  //val classLoader = URLClassLoader

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
    hostname = parsed_json.get("hostname").get.toString.trim
  }
  else {
    throw new ConnectionFailedException("Unable to find hostname in adapterConfig ")
  }

  var database:String = null;
  if (parsed_json.contains("database")) {
    database = parsed_json.get("database").get.toString.trim
  }
  else {
    throw new ConnectionFailedException("Unable to find database in adapterConfig ")
  }

  var user:String = null;
  if (parsed_json.contains("user")) {
    user = parsed_json.get("user").get.toString.trim
  }
  else {
    throw new ConnectionFailedException("Unable to find user in adapterConfig ")
  }

  var password:String = null;
  if (parsed_json.contains("password")) {
    password = parsed_json.get("password").get.toString.trim
  }
  else {
    throw new ConnectionFailedException("Unable to find password in adapterConfig ")
  }

  var jarpaths:String = null;
  if (parsed_json.contains("jarpaths")) {
    jarpaths = parsed_json.get("jarpaths").get.toString.trim
  }
  else {
    throw new ConnectionFailedException("Unable to find jarpaths in adapterConfig ")
  }

  var jdbcJar:String = null;
  if (parsed_json.contains("jdbcJar")) {
    jdbcJar = parsed_json.get("jdbcJar").get.toString.trim
  }
  else {
    throw new ConnectionFailedException("Unable to find jdbcJar in adapterConfig ")
  }

  logger.info("hostname => " + hostname)
  logger.info("jarpaths => " + jarpaths)  
  logger.info("jdbcJar  => " + jdbcJar)

  var jdbcUrl = "jdbc:sqlserver://" + hostname + ";databaseName=" + database + ";user=" + user + ";password=" + password

  //logger.info("jdbcUrl  => " + jdbcUrl)

  var jars = new Array[String](0)
  var jar = jarpaths + "/" + jdbcJar
  jars = jars :+ jar
  LoadJars(jars)

  val driverType = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  Class.forName(driverType,true,clsLoader)

  val d = Class.forName(driverType, true, clsLoader).newInstance.asInstanceOf[Driver]
  logger.info("%s:Registering Driver".format(GetCurDtTmStr))
  DriverManager.registerDriver(new DriverShim(d));

  private def GetCurDtTmStr: String = {
    new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date(System.currentTimeMillis))
  }

  /**
   * loadJar - load the specified jar into the classLoader
   */

  private def LoadJars(jars: Array[String]): Unit = {
    // Loading all jars
    for (j <- jars) {
      val jarNm = j.trim
      logger.debug("%s:Processing Jar: %s".format(GetCurDtTmStr, jarNm))
      val fl = new File(jarNm)
      if (fl.exists) {
        try {
          if (loadedJars(fl.getPath())) {
            logger.debug("%s:Jar %s already loaded to class path.".format(GetCurDtTmStr, jarNm))
          } else {
            clsLoader.addURL(fl.toURI().toURL())
            logger.debug("%s:Jar %s added to class path.".format(GetCurDtTmStr, jarNm))
            loadedJars += fl.getPath()
          }
        } catch {
          case e: Exception => {
            val errMsg = "Jar " + jarNm + " failed added to class path. Reason:%s Message:%s".format(e.getCause, e.getMessage)
            logger.error("Error:" + errMsg)
            throw new Exception(errMsg)
          }
        }
      } else {
        val errMsg = "Jar " + jarNm + " not found"
        throw new Exception(errMsg)
      }
    }
  }

  override def put(containerName:String, key: Key, value: Value): Unit = {
    var con:Connection = null
    var stmt:PreparedStatement = null
    try{
      con = DriverManager.getConnection(jdbcUrl);
      var insertSql = "insert into " + containerName + "(part_date,key_str,transactionId,value) values(?,?,?,?)"
      stmt = con.prepareStatement(insertSql)
      stmt.setDate(1,new java.sql.Date(key.date_part.getTime))
      stmt.setString(2,key.bucket_key.mkString(":"))
      stmt.setLong(3,key.transactionId)
      stmt.setBinaryStream(4,new java.io.ByteArrayInputStream(value.serializedInfo),
			   value.serializedInfo.length)
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

  override def put(data_list: Array[(String, Array[(Key, Value)])]): Unit = {
    logger.info("not implemented yet")
  }

  // delete operations
  override def del(containerName: String, key: Key): Unit = {
    logger.info("not implemented yet")
  }

  override def del(containerName: String, time: StorageTimeRange, keys: Array[Key]): Unit = {
    logger.info("not implemented yet")
  }

  // get operations
  override def get(containerName: String, callbackFunction: (Key, Value) => Unit): Unit = {
    
    logger.info("not implemented yet")
  }

  override def get(containerName: String, time_ranges: Array[StorageTimeRange], callbackFunction: (Key, Value) => Unit): Unit = {
    logger.info("not implemented yet")
  }

  override def get(containerName: String, time_ranges: Array[StorageTimeRange], bucket_keys: Array[Array[String]], callbackFunction: (Key, Value) => Unit): Unit = {
    logger.info("not implemented yet")
  }
  override def get(containerName: String, bucket_keys: Array[Array[String]], callbackFunction: (Key, Value) => Unit): Unit = {
    logger.info("not implemented yet")
  }

  override def beginTx(): Transaction = { 
    new SqlServerAdapterTx(this)
  }

  override def endTx(tx: Transaction): Unit = {}

  override def commitTx(tx: Transaction): Unit = {}

  override def rollbackTx(tx: Transaction): Unit = {}

  override def Shutdown(): Unit = {
   logger.info("close the connection pool") 
  }

  override def TruncateContainer(containerNames: Array[String]): Unit = {
   logger.info("Truncate the container tables") 
  }

  private def DropContainer(containerName: String) : Unit = {
    var con:Connection = null
    var stmt:Statement = null
    var rs: ResultSet = null
    try{
      con = DriverManager.getConnection(jdbcUrl);
      // check if the container already dropped
      val dbm = con.getMetaData();
      rs = dbm.getTables(null, null, containerName, null);
      if (!rs.next()) {
	logger.warn("The table " + containerName + " may have beem dropped already ")
      }
      else{
	var query = "drop table " + containerName
	stmt = con.createStatement()
	stmt.executeUpdate(query);
      }
    } catch{
      case e:Exception => {
        val stackTrace = StackTrace.ThrowableTraceString(e)
        logger.debug("Stacktrace:"+stackTrace)
	throw new Exception("Failed to drop the table " + containerName + ":" + e.getMessage())
      }
    } finally {
      if(rs != null) {
	rs.close
      }
      if(stmt != null){
	stmt.close
      }
      if( con != null ){
	con.close
      }
    }
  }

  override def DropContainer(containerNames: Array[String]): Unit = {
    logger.info("drop the container tables") 
    containerNames.foreach( cont => {
      logger.info("drop the container " + cont)
      DropContainer(cont)
    })
  }


  private def CreateContainer(containerName: String) : Unit = {
    var con:Connection = null
    var stmt:Statement = null
    var rs: ResultSet = null
    try{
      con = DriverManager.getConnection(jdbcUrl);
      // check if the container already exists
      val dbm = con.getMetaData();
      rs = dbm.getTables(null, null, containerName, null);
      if (rs.next()) {
	logger.warn("The table " + containerName + " already exists ")
      }
      else{
	var query = "create table " + containerName + "(part_date date,key_str varchar(100), transactionId bigint, value varbinary(max))"
	stmt = con.createStatement()
	stmt.executeUpdate(query);
      }
    } catch{
      case e:Exception => {
        val stackTrace = StackTrace.ThrowableTraceString(e)
        logger.debug("Stacktrace:"+stackTrace)
	throw new Exception("Failed to create the table " + containerName + ":" + e.getMessage())
      }
    } finally {
      if(rs != null) {
	rs.close
      }
      if(stmt != null){
	stmt.close
      }
      if( con != null ){
	con.close
      }
    }
  }

  override def CreateContainer(containerNames: Array[String]): Unit = {
    logger.info("create the container tables") 
    containerNames.foreach( cont => {
      logger.info("create the container " + cont)
      CreateContainer(cont)
    })
  }
}


class SqlServerAdapterTx(val parent: DataStore) extends Transaction {

  val loggerName = this.getClass.getName
  val logger = Logger.getLogger(loggerName)

  override def put(containerName:String, key: Key, value: Value): Unit = {
    parent.put(containerName,key,value)
  }

  override def put(data_list: Array[(String, Array[(Key, Value)])]): Unit = {
    logger.info("not implemented yet")
  }

  // delete operations
  override def del(containerName: String, key: Key): Unit = {
    logger.info("not implemented yet")
  }

  override def del(containerName: String, time: StorageTimeRange, keys: Array[Key]): Unit = {
    logger.info("not implemented yet")
  }

  // get operations
  override def get(containerName: String, callbackFunction: (Key, Value) => Unit): Unit = {
    logger.info("not implemented yet")
  }

  override def get(containerName: String, time_ranges: Array[StorageTimeRange], callbackFunction: (Key, Value) => Unit): Unit = {
    logger.info("not implemented yet")
  }

  override def get(containerName: String, time_ranges: Array[StorageTimeRange], bucket_keys: Array[Array[String]], callbackFunction: (Key, Value) => Unit): Unit = {
    logger.info("not implemented yet")
  }
  override def get(containerName: String, bucket_keys: Array[Array[String]], callbackFunction: (Key, Value) => Unit): Unit = {
    logger.info("not implemented yet")
  }
}

// To create SqlServer Datastore instance
object SqlServerAdapter extends StorageAdapterObj {
  override def CreateStorageAdapter(kvManagerLoader: KamanjaLoaderInfo, datastoreConfig: String): DataStore = new SqlServerAdapter(kvManagerLoader, datastoreConfig)
}
