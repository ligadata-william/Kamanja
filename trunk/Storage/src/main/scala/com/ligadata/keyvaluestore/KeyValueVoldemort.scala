package com.ligadata.keyvaluestore.voldemort

// This libraries need to come first or import won't find them
//
import voldemort.client._
import voldemort.client.ClientConfig
import voldemort.client.SocketStoreClientFactory
import voldemort.client.StoreClient
import voldemort.versioning.Versioned;

import org.apache.commons.codec.binary.Base64

import java.nio.ByteBuffer

import com.ligadata.keyvaluestore._


class KeyValueVoldemortTx(owner : DataStore) extends Transaction
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

class KeyValueVoldemort(parameter: PropertyMap) extends DataStore
{
	var hostname = parameter.getOrElse("hostname", "tcp://localhost:6666")
	var keyspace = parameter.getOrElse("schema", "default")
	var table = parameter.getOrElse("table", "default")

	var config = new ClientConfig().setBootstrapUrls(hostname)
	var factory = new SocketStoreClientFactory(config)
	var store : StoreClient[String, String] = factory.getStoreClient("test") //keyspace + "_" + table)

	// Little helper, I won't have time to reconfigure voldemort right now
	val seperator = "\r\n"
	val seperatorBytes = seperator.getBytes("UTF-8")
	val base64 = new Base64(1024, seperatorBytes, true)
	val base64key = new Base64(1024, Array.empty[Byte], true)

	def add(source: IStorage) =
	{
	    val key = base64key.encodeToString(source.Key.toArray[Byte])
	    val value = base64.encodeToString(source.Value.toArray[Byte])
        store.put(key, value);
	}

	def put(source: IStorage) =
	{
	    val key = base64key.encodeToString(source.Key.toArray[Byte])
	    val value = base64.encodeToString(source.Value.toArray[Byte])
        store.put(key, value);
	}


	def putBatch(sourceArray: Array[IStorage]) =
	{
	  sourceArray.foreach( source => {
	    val key = base64key.encodeToString(source.Key.toArray[Byte])
	    val value = base64.encodeToString(source.Value.toArray[Byte])
            store.put(key, value);
	  })
	}

	def delBatch(keyArray: Array[Key]) = {
	  keyArray.foreach( k => {
	    val key1 = base64key.encodeToString(k.toArray[Byte])
	    store.delete(key1)
	  })
	}

	def get(key: Key, handler : (Value) => Unit) =
	{
	    val key1 = base64key.encodeToString(key.toArray[Byte])
	    val value1 = store.get(key1).getValue;
	    val value2 : Array[Byte] = base64.decode(value1)
	    val value3 = ByteBuffer.wrap(value2)
	    //
	    val value = new Value
	    value += value3.get()

		handler(value)
	}
	
	def get(key: Key, target: IStorage)  =
	{
	    val key1 = base64key.encodeToString(key.toArray[Byte])
	    val value1 = store.get(key1).getValue;
	    val value2 : Array[Byte] = base64.decode(value1)
	    val value3 = ByteBuffer.wrap(value2)
	    //
	    val value = new Value
	    value += value3.get()

		target.Construct(key, value)
	}

	def del(key: Key) =
	{
		val key1 = base64key.encodeToString(key.toArray[Byte])
		store.delete(key1)
	}

	def del(source: IStorage) = { del(source.Key) }

	def beginTx() : Transaction = { new KeyValueVoldemortTx(this) }

	def endTx(tx : Transaction) = {}

	def commitTx(tx : Transaction) = {}

	override def Shutdown() =
	{
		factory.close();
	}

	def TruncateStore() =
	{
		throw new Exception("Don't know how to truncate store")
	}

	def getAllKeys( handler: (Key) => Unit)
	{
		throw new Exception("Don't know how to get all keys")
	}

}
