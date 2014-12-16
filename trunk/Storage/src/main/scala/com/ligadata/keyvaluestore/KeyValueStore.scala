/*
 * Takes any object and stores it with the key
 *
 */
package com.ligadata.keyvaluestore

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import com.ligadata.keyvaluestore.cassandra.KeyValueCassandra
import java.lang.Comparable

case class KeyNotFoundException(e: String) extends Exception(e)
case class ConnectionFailedException(e: String) extends Exception(e)

class ByteArray extends ArrayBuffer[Byte]

class Key extends ByteArray
/*
with Comparable[Key]
{
	def compareTo(other : Key) : Int =
	{
		return (this.hashCode - other.hashCode)
	}
}
*/
class Value extends ByteArray

class PropertyMap extends HashMap[String, String]

trait IStorage
{
	def Key : Key
	def Value : Value
	def Construct(Key: Key, Value: Value)
}

trait DataStoreOperations
{
	def add(source: IStorage)
	def put(source: IStorage)
	def get(key: Key, target: IStorage)
	def get(key: Key, handler : (Value) => Unit)
	def del(key: Key)
	def del(source: IStorage)
	def getAllKeys( handler : (Key) => Unit)
	def putBatch(sourceArray: Array[IStorage])
	def delBatch(keyArray: Array[Key])
}

trait DataStore extends DataStoreOperations
{
	def beginTx() : Transaction
	def endTx(tx : Transaction)
	def commitTx(tx : Transaction)

	def Shutdown() = {}
	def TruncateStore()
}

trait Transaction extends DataStoreOperations
{
}
