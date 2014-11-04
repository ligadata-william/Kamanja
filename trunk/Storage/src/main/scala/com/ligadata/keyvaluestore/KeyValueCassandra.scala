package com.ligadata.keyvaluestore.cassandra

import com.ligadata.keyvaluestore._
import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Session
import com.datastax.driver.core.querybuilder.Insert
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.ConsistencyLevel
import java.nio.ByteBuffer

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


class KeyValueCassandraTx(owner : DataStore) extends Transaction
{
	var parent :DataStore = owner

	def add(source: IStorage) = { owner.add(source) }
	def put(source: IStorage) = { owner.put(source) }
	def get(key: Key, target: IStorage) = { owner.get(key, target) }
	def get( key: Key, handler : (Value) => Unit) = { owner.get(key, handler) }
	def del(key: Key) = { owner.del(key) }
	def del(source: IStorage) = { owner.del(source) }
	
	def getAllKeys( handler : (Key) => Unit) = { owner.getAllKeys(handler) }
}

class KeyValueCassandra(parameter: PropertyMap) extends DataStore
{
	var hostnames = parameter.getOrElse("hostlist", "localhost") ;
	var keyspace = parameter.getOrElse("schema", "default") ;
	var table = parameter.getOrElse("table", "default") ;
	var clusterBuilder = Cluster.builder()

	clusterBuilder.addContactPoints(hostnames)

	if(parameter.contains("user"))
		clusterBuilder.withCredentials(parameter("user") ,  parameter.getOrElse("password", ""))

	val cluster = clusterBuilder.build()
	val session = cluster.connect(keyspace);
	val consistencylevelRead = ConsistencyLevel.valueOf(parameter.getOrElse("ConsistencyLevelRead", "ONE"))
	val consistencylevelWrite = ConsistencyLevel.valueOf(parameter.getOrElse("ConsistencyLevelWrite", "ANY"))
	val consistencylevelDelete = ConsistencyLevel.valueOf(parameter.getOrElse("ConsistencyLevelDelete", "ANY"))

	// Check if table exists or create if needed
	// This is just for convenience until the process is defined
	//

	var insertStmt = session.prepare("INSERT INTO " + table + " (key, value) values(?, ?);")
	var insertStmt1 = session.prepare("INSERT INTO " + table + " (key, value) values(?, ?) IF NOT EXISTS;")
	var selectStmt = session.prepare("SELECT value FROM " + table  + " WHERE key = ?;")
	var deleteStmt = session.prepare("DELETE from " + table  + " WHERE Key=?;")
	var updateStmt = session.prepare("UPDATE " + table  + " SET value = ? WHERE Key=?;")

	def add(source: IStorage) =
	{
		var key = ByteBuffer.wrap(source.Key.toArray[Byte]);
		var value = ByteBuffer.wrap(source.Value.toArray[Byte]);
		val e = session.execute(insertStmt1.bind(key, value).setConsistencyLevel(consistencylevelWrite))

		if(e.getColumnDefinitions().size()>1)
			throw new Exception("not applied")
	}

	def put(source: IStorage) =
	{
		var key = ByteBuffer.wrap(source.Key.toArray[Byte]);
		var value = ByteBuffer.wrap(source.Value.toArray[Byte]);
		session.execute(updateStmt.bind(value,key).setConsistencyLevel(consistencylevelWrite))
	}

	def get(key: Key, handler : (Value) => Unit) =
	{
		val key1 = ByteBuffer.wrap(key.toArray[Byte]);
		val rs = session.execute(selectStmt.bind(key1).setConsistencyLevel(consistencylevelRead))

	        if( rs.getAvailableWithoutFetching() == 0 ){
		  throw new KeyNotFoundException("Key Not found")
		}

		// Construct the output value
		// BUGBUG-jh-20140703: There should be a more concise way to get the data
		//
		val value = new Value
		val buffer : ByteBuffer = rs.one().getBytes(0)
		if (buffer != null) {
		  while(buffer.hasRemaining())
		  value+= buffer.get()
		} else {
		  throw new KeyNotFoundException("Key Not found")
		}
		handler(value)
	}
	
	def get(key: Key, target: IStorage)  =
	{
		val key1 = ByteBuffer.wrap(key.toArray[Byte]);
		val rs = session.execute(selectStmt.bind(key1).setConsistencyLevel(consistencylevelRead))

	        if( rs.getAvailableWithoutFetching() == 0 ){
		  throw new KeyNotFoundException("Key Not found")
		}
		// Construct the output value
		// BUGBUG-jh-20140703: There should be a more concise way to get the data
		//
		val value = new Value

		val buffer : ByteBuffer = rs.one().getBytes(0)
		if (buffer != null) {
		  while(buffer.hasRemaining())
		  value+= buffer.get()
		} else {
		  throw new KeyNotFoundException("Key Not found")
		}
		target.Construct(key, value)
	}

	def del(key: Key) =
	{
		val key1 = ByteBuffer.wrap(key.toArray[Byte]);
		session.execute(deleteStmt.bind(key1).setConsistencyLevel(consistencylevelDelete))
	}

	def del(source: IStorage) = { del(source.Key) }

	def beginTx() : Transaction = { new KeyValueCassandraTx(this) }

	def endTx(tx : Transaction) = {}

	def commitTx(tx : Transaction) = {}

	override def Shutdown() =
	{
		session.close()

		cluster.close()
	}

	def TruncateStore()
	{
		var stmt = session.prepare("truncate " + table  + ";")
		val rs = session.execute(stmt.bind().setConsistencyLevel(consistencylevelDelete))
	}

	def getAllKeys( handler : (Key) => Unit) =
	{
		var stmt = session.prepare("SELECT key FROM " + table  + ";")
		val rs = session.execute(stmt.bind().setConsistencyLevel(consistencylevelRead))

		val iter = rs.iterator();
		while(iter.hasNext())
		{
			if (rs.getAvailableWithoutFetching() == 100 && !rs.isFullyFetched())
				rs.fetchMoreResults();

		       val row = iter.next()

			val key = new Key
			val buffer : ByteBuffer = row.getBytes(0)
			while(buffer.hasRemaining())
				key+= buffer.get()

			handler(key)
   		}
	}

}

