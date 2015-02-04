package com.ligadata.keyvaluestore.hbase

import com.ligadata.keyvaluestore._
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

import org.apache.hadoop.hbase._
import org.apache.log4j._

import java.nio.ByteBuffer
import java.io.IOException

//import org.apache.hadoop.hbase.util.Bytes;
/*
 * create 'default', 'value'
 *
 * put 'default', 'KEYKEY', 'value', 'ValueValue'
 *
 * scan 'default'
 *
 */

class KeyValueHBaseTx(owner : DataStore) extends Transaction
{
	var parent :DataStore = owner

	def add(source: IStorage) = { owner.add(source) }
	def put(source: IStorage) = { owner.put(source) }
	def get(key: Key, target: IStorage) = { owner.get(key, target) }
	def get( key: Key, handler : (Value) => Unit) = { owner.get(key, handler) }
	def del(key: Key) = { owner.del(key) }
	def del(source: IStorage) = { owner.del(source) }
	def getAllKeys( handler : (Key) => Unit) = { owner.getAllKeys(handler) }
	def putBatch(sourceArray: Array[IStorage]) = { owner.putBatch(sourceArray) }
	def delBatch(keyArray: Array[Key]) = { owner.delBatch(keyArray) }
}

class KeyValueHBase(parameter: PropertyMap) extends DataStore
{
	val loggerName = this.getClass.getName
	val logger = Logger.getLogger(loggerName)

	var keyspace = parameter.getOrElse("schema", "default") ;
	var hostnames = parameter.getOrElse("hostlist", "localhost") ;
	var table = parameter.getOrElse("table", "default")

	var config = new org.apache.hadoop.conf.Configuration
	config.setInt("zookeeper.session.timeout", 5000);
	config.setInt("zookeeper.recovery.retry", 1);
	config.setInt("hbase.client.retries.number", 3);
	config.setInt("hbase.client.pause", 5000);
	
	config.set("hbase.zookeeper.quorum", hostnames);

	var connection:HConnection = _
	try{
	  connection = HConnectionManager.createConnection(config);
	}
        catch{
	  case e:Exception => {
	     throw new ConnectionFailedException("Unable to connect to hbase at " + hostnames + ":" + e.getMessage())
	  }
	}
	
	createTable(table)
	var tableHBase = connection.getTable(table);

        def createTable(tableName:String) : Unit = {
	  val  admin = new HBaseAdmin(config);
	  if (! admin.tableExists(tableName)) {
	    val  tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
	    val  colDesc1 =  new HColumnDescriptor("key".getBytes())
	    val  colDesc2 =  new HColumnDescriptor("value".getBytes())
	    tableDesc.addFamily(colDesc1)
	    tableDesc.addFamily(colDesc2)
	    admin.createTable(tableDesc);
	  }
	}

	def add(source: IStorage) =
	{
		var p = new Put(source.Key.toArray[Byte])

		p.add(Bytes.toBytes("value"), Bytes.toBytes("base"),source.Value.toArray[Byte] )

		val succeeded = tableHBase.checkAndPut(p.getRow(), Bytes.toBytes("value"), Bytes.toBytes("base"), null, p)
		if(!succeeded)
		{
			throw new Exception("not applied")
		}
	}

	def put(source: IStorage) =
	{
		var p = new Put(source.Key.toArray[Byte])

		p.add(Bytes.toBytes("value"), Bytes.toBytes("base"), source.Value.toArray[Byte] )

		tableHBase.put(p)

	}

	def putBatch(sourceArray: Array[IStorage]) =
	{
	  sourceArray.foreach( source => {
	    var p = new Put(source.Key.toArray[Byte])
	    p.add(Bytes.toBytes("value"), Bytes.toBytes("base"), source.Value.toArray[Byte] )
	    tableHBase.put(p)
	  })
	}

	def delBatch(keyArray: Array[Key]) = {
	  keyArray.foreach( k => {
	    val p = new Delete(k.toArray[Byte])
	    val result = tableHBase.delete(p)
	  })
	}

	def get(key: Key, handler : (Value) => Unit) =
	{
	  try{
		var p = new Get(key.toArray[Byte])

		p.addColumn(Bytes.toBytes("value"), Bytes.toBytes("base") )

		val result = tableHBase.get(p)

		val v = result.getValue(Bytes.toBytes("value"), Bytes.toBytes("base") )

		val value = new Value
		for(b <- v)
			value+=b

		handler(value)
	  } catch {
	    case e:Exception => {
	      throw new KeyNotFoundException(e.getMessage())
	    }
	  }
	}
	
	def get(key: Key, target: IStorage)  =
	{
	  try{
		var p = new Get(key.toArray[Byte])

		p.addColumn(Bytes.toBytes("value"), Bytes.toBytes("base") )

		val result = tableHBase.get(p)

		val v = result.getValue(Bytes.toBytes("value"), Bytes.toBytes("base") )

		val value = new Value
		for(b <- v)
			value+=b

		target.Construct(key, value)
	  } catch {
	    case e:Exception => {
	      throw new KeyNotFoundException(e.getMessage())
	    }
	  }
	}

	def del(key: Key) =
	{
		val p = new Delete(key.toArray[Byte])

		val result = tableHBase.delete(p)
	}

	def del(source: IStorage) = { del(source.Key) }

	def beginTx() : Transaction = { new KeyValueHBaseTx(this) }

	def endTx(tx : Transaction) = {}

	def commitTx(tx : Transaction) = { tableHBase.flushCommits }

	override def Shutdown() =
	{
	  if(tableHBase != null ){
	    tableHBase.close()
	    tableHBase = null
	  }
	  if( connection != null ){
	    connection.close()
	    connection = null
	  }
	}

	def TruncateStore() =
	{
/*
		val a = new HBaseAdmin(connection)

		if (a.isTableEnabled(table))
			a.disableTable(table);

		a.deleteTable(table);

		a.createTable(tableHBase.getTableDescriptor(), Array(Bytes.toBytes("value:base") ) )

		a.close()
*/
		getAllKeys( {(key : Key) => del(key) } )

	}

	def getAllKeys( handler : (Key) => Unit)
	{
		var p = new Scan()

		val iter = tableHBase.getScanner(p)

		try
		{
			var fContinue = true

			do
			{
				val row = iter.next()
				if(row!=null)
				{
					val v = row.getRow()
					val key = new Key
					for(b <- v)
						key+=b

					handler(key)
				}
				else
				{
					fContinue = false;
				}
			}
			while(fContinue)

		}
		finally
		{
			iter.close()
		}

	}
}

