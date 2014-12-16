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
  def delBatch(keyArray: Array[Key]) = { owner.delBatch(keyArray) }
}

class KeyValueRedis(parameter: PropertyMap) extends DataStore {
  var path = parameter.getOrElse("path", ".")
  var keyspace = parameter.getOrElse("schema", "default")
  var table = parameter.getOrElse("table", "default")

  var InMemory = parameter.getOrElse("inmemory", "false")
  val withTransactions = parameter.getOrElse("withtransaction", "false").toBoolean
  val hostnames = parameter.getOrElse("hostlist", "localhost:6379");
  val hosts = hostnames.split(",").map(hst => hst.trim).filter(hst => hst.size > 0)
  if (hosts.size == 0) {
    throw new Exception("Not found any valid hosts in hostlist")
  }

  val nodes = new Array[ClusterNode](hosts.size)

  for (i <- 0 until hosts.size) {
    val nodeInfo = hosts(i).split(":")
    if (nodeInfo.size == 2) {
      nodes(i) = ClusterNode("node" + i + "_" + hashCode, nodeInfo(0), nodeInfo(1).toInt)
    } else if (nodeInfo.size == 1) {
      nodes(i) = ClusterNode("node" + i + "_" + hashCode, nodeInfo(0), 6379)
    } else {
      throw new Exception("Expecting hostname/ip and port number as host information in hostslist in the format of hostname:port. But found " + hosts(i) + ". Format hostname:port[,hostname:port...]")
    }
  }

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

  def delBatch(keyArray: Array[Key]) = {
    keyArray.foreach( k => {
      cluster.del(k.toArray[Byte])
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





