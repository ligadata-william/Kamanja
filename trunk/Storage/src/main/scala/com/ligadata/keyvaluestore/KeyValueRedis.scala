package com.ligadata.keyvaluestore.redis

import com.ligadata.keyvaluestore._
import java.io.File
import java.nio.ByteBuffer
import com.redis.cluster._
import com.redis._
import collection.mutable.WrappedArray

class KeyValueRedisTx(owner: DataStore) extends Transaction {
  var parent: DataStore = owner

  def add(source: IStorage) = { owner.add(source) }
  def put(source: IStorage) = { owner.put(source) }
  def get(key: Key, target: IStorage) = { owner.get(key, target) }
  def get(key: Key, handler: (Value) => Unit) = { owner.get(key, handler) }
  def del(key: Key) = { owner.del(key) }
  def del(source: IStorage) = { owner.del(source) }
  def getAllKeys(handler: (Key) => Unit) = { owner.getAllKeys(handler) }
  def putBatch(sourceArray: Array[IStorage]) = { owner.putBatch(sourceArray) }
}

class KeyValueRedis(parameter: PropertyMap) extends DataStore {
  var path = parameter.getOrElse("path", ".")
  var keyspace = parameter.getOrElse("schema", "default")
  var table = parameter.getOrElse("table", "default")

  var InMemory = parameter.getOrElse("inmemory", "false")
  val withTransactions = parameter.getOrElse("withtransaction", "false").toBoolean
  val host = parameter.getOrElse("host", "localhost")
  val port = parameter.getOrElse("port", "6379").toInt

  // val nodes = Array(ClusterNode("node1", host, port), ClusterNode("node2", host, port + 1), ClusterNode("node3", host, port + 2))
  val nodes = Array(ClusterNode("node1", host, port))

  val cluster = new RedisCluster(new WrappedArray.ofRef(nodes): _*) {
    val keyTag = Some(RegexKeyTag)
  }

  def add(source: IStorage) = {
    cluster.setnx(source.Key.toArray[Byte], source.Value.toArray[Byte])
  }

  def put(source: IStorage) = {
    cluster.set(source.Key.toArray[Byte], source.Value.toArray[Byte])
  }

  def putBatch(sourceArray: Array[IStorage]) = {
    sourceArray.foreach(source => {
      cluster.set(source.Key.toArray[Byte], source.Value.toArray[Byte])
    })
  }

  def get(key: Key, handler: (Value) => Unit) = {
    val buffer = cluster.get(key.toArray[Byte]).getOrElse(null)
    // Construct the output value
    // BUGBUG-jh-20140703: There should be a more concise way to get the data
    //
    println("buffer " + buffer)
    val value = new Value
    if (buffer != null) {
      for (b <- buffer.getBytes())
        value += b
    } else {
      throw new KeyNotFoundException("Key Not found")
    }

    handler(value)
  }

  def get(key: Key, target: IStorage) = {
    val buffer = cluster.get(key.toArray[Byte]).getOrElse(null)

    // Construct the output value
    // BUGBUG-jh-20140703: There should be a more concise way to get the data
    //
    println("buffer.getBytes() " + buffer.getBytes())
    val value = new Value
    if (buffer != null) {
      value ++= buffer.getBytes()
      println("value " + value)
    } else {
      throw new KeyNotFoundException("Key Not found")
    }

    target.Construct(key, value)

  }

  def del(key: Key) = {
    cluster.del(key.toArray[Byte])
  }

  def del(source: IStorage) = { del(source.Key) }

  def beginTx(): Transaction = { new KeyValueRedisTx(this) }

  def endTx(tx: Transaction) = {}

  def commitTx(tx: Transaction) = {}

  override def Shutdown() = {
    cluster.close
  }

  def TruncateStore() {
    cluster.flushdb
  }

  def getAllKeys(handler: (Key) => Unit) = {

    val bufferlist = cluster.keys().get
    if (bufferlist != null)
      bufferlist.foreach { buffer =>
        val key = new Key
        for (b <- buffer.get.getBytes())
          key += b
        handler(key)
        println(key)

      }
  }

}





