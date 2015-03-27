package com.ligadata.keyvaluestore.cassandra

import com.ligadata.keyvaluestore._
import com.ligadata.olep.metadata._

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Session
import com.datastax.driver.core.querybuilder.Insert
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.BatchStatement

import java.nio.ByteBuffer
import org.apache.log4j._
import java.util.Date
import java.util.Calendar

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

class AuditStoreCassandra(parameter: PropertyMap) extends AuditStore {
  val loggerName = this.getClass.getName
  val logger = Logger.getLogger(loggerName)

  // Read all cassandra parameters
  var hostnames = parameter.getOrElse("hostlist", "localhost");
  var keyspace = parameter.getOrElse("schema", "default");
  var table = parameter.getOrElse("table", "default");
  var replication_class = parameter.getOrElse("replication_class", "SimpleStrategy")
  var replication_factor = parameter.getOrElse("replication_factor", "1")
  val consistencylevelRead = ConsistencyLevel.valueOf(parameter.getOrElse("ConsistencyLevelRead", "ONE"))
  val consistencylevelWrite = ConsistencyLevel.valueOf(parameter.getOrElse("ConsistencyLevelWrite", "ANY"))
  val consistencylevelDelete = ConsistencyLevel.valueOf(parameter.getOrElse("ConsistencyLevelDelete", "ANY"))
  var clusterBuilder = Cluster.builder()
  var cluster: Cluster = _
  var session: Session = _

  var keyspace_exists = false

  try {
    clusterBuilder.addContactPoints(hostnames)
    if (parameter.contains("user"))
      clusterBuilder.withCredentials(parameter("user"), parameter.getOrElse("password", ""))
    cluster = clusterBuilder.build()

    if (cluster.getMetadata().getKeyspace(keyspace) == null){
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
    }
    else{
      keyspace_exists = true
      logger.debug("The keyspace " + keyspace + " exists, connect to existing keyspace and continue")
      session = cluster.connect(keyspace)
    }
  } catch {
    case e: Exception => {
      throw new ConnectionFailedException("Unable to connect to cassandra at " + hostnames + ":" + e.getMessage())
    }
  }

  // Check if table exists or create if needed
  val createTblStmt = "CREATE TABLE IF NOT EXISTS " + table + 
    " (auditYear  int " +
    " ,actionTime  bigint " +
    " ,userOrRole varchar" +
    " ,userPrivilege varchar" +
    " ,action varchar" +
    " ,objectAccessed varchar" +
    " ,success varchar" +
    " ,transactionId varchar" +
    " ,notes varchar " +
    " ,primary key(auditYear,actionTime) " +
    ");"

  session.execute(createTblStmt);

  //

  var insertSql = "INSERT INTO " + table + 
	   " (audityear,actiontime,userorrole,userprivilege,action,objectaccessed,success,transactionid,notes) " +
	   " values(?,?,?,?,?,?,?,?,?);"

  var insertStmt = session.prepare(insertSql)
  //logger.debug(insertSql)

  def getYear(dt: Long) : Int = {
    val cal = Calendar.getInstance();
    cal.setTime(new Date(dt));
    val year = cal.get(Calendar.YEAR);
    year
  }    

  def add(rec: AuditRecord) = {
    try{
      var at:java.lang.Long = rec.actionTime.toLong

      var year:java.lang.Integer = 0
      year = getYear(at)

      val e = session.execute(insertStmt.bind(year,
					      at,
					      rec.userOrRole,
					      rec.userPrivilege,
					      rec.action,
					      rec.objectAccessed,
					      rec.success,
					      rec.transactionId,
					      rec.notes).setConsistencyLevel(consistencylevelWrite))

      if (e.getColumnDefinitions().size() > 1){
	logger.warn("Unexpected value for " + e.getColumnDefinitions().size())
      }
    }catch {
      case e: Exception => 
	e.printStackTrace()
	throw new Exception(e.getMessage())
    }
  }

  def get(startTime: Date, endTime: Date, userOrRole: String, action: String, objectAccessed: String): Array[AuditRecord] = {
    var auditRecords = new Array[AuditRecord](0)
    try{
      var selectSql = "SELECT actiontime,action,userorrole,objectAccessed,success,userprivilege,notes,transactionid from " + table + " where "
      var stime = (new Date().getTime() - 10 * 60 * 1000L)
      if( startTime != null ){
	stime = startTime.getTime()
      }

      var year:java.lang.Integer = 0;
      year = getYear(stime)

      selectSql = selectSql + " audityear = " +  year.toString()

      selectSql = selectSql + " and actiontime >= " +  stime.toString()
      var etime = new Date().getTime()
      if( endTime != null ){
	etime = endTime.getTime()
      }
      selectSql = selectSql + " and actiontime <= " +  etime.toString()
      if( userOrRole != null ){
	selectSql = selectSql + " and userOrRole = '" + userOrRole + "'"
      }
      if( action != null ){
	selectSql = selectSql + " and action = '" + action + "'"
      }
      if( objectAccessed != null ){
	selectSql = selectSql + " and objectAccessed = '" + objectAccessed + "'"
      }
      
      logger.debug(selectSql)
      val rs = session.execute(selectSql)

      var i = 0
      val rowList = rs.all()
      var rowCount = rowList.size()
      for(i <- 0 to rowCount - 1){
	var row = rowList.get(i)
	val a = new AuditRecord()
	a.actionTime     = row.getLong("actiontime").toString()
	a.userOrRole     = row.getString("userorrole")
	a.userPrivilege  = row.getString("userprivilege")
	a.action         = row.getString("action")
	a.objectAccessed = row.getString("objectAccessed")
	a.success        = row.getString("success")
	a.transactionId  = row.getString("transactionId")
	a.notes          = row.getString("notes")
	auditRecords = auditRecords :+ a
      }
      auditRecords
    } catch {
      case e:Exception => {
	throw new Exception("Failed to fetch audit records: " + e.getMessage())
      }
    }
  }

  override def Shutdown() =  {
    session.close()
    cluster.close()
  }
}

