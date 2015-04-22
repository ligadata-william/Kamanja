package com.ligadata.MetadataAPI

import org.apache.curator.framework.recipes.locks._
import org.apache.curator.framework._
import org.apache.curator._
import org.apache.curator.retry._
import java.util.concurrent.TimeUnit

object MetadataLockService {
  
  private var mdLocks: scala.collection.mutable.Map[String,InterProcessLock] = _ 
  private var zkc: CuratorFramework = _
  private var zkBasePath: String = _
  private var client: CuratorFramework = _
    
  val LOCK_WAIT = 10  
  val LOCK_GRANTED = 0
  val LOCK_TIMEOUT = 1
  val LOCK_DENIED = 2
  
  /**
   * init - Initialize a new Metadata Lock Services.
   */
  def init: Unit = {
    zkBasePath = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("ZNODE_PATH")
    mdLocks = scala.collection.mutable.Map[String, InterProcessLock]()
    var retryPolicy: RetryPolicy = new ExponentialBackoffRetry(1000, 3);
    client =  CuratorFrameworkFactory.newClient(MetadataAPIImpl.GetMetadataAPIConfig.getProperty("ZOOKEEPER_CONNECT_STRING"), retryPolicy);
    client.start();
  }
  
  def shutdown: Unit = {
    client.stop
    client.shutdown
  }
  
  /**
   * getLock - Request a distributed lock for a given lock
   *   @param  String - Lock name
   *   @return 0 if lack is granted, 1 - if the lock has timed out.
   */
  def getLock(lockName: String): Int = {
    var lock: String =  zkBasePath + "/locks/" + lockName
    
    // See if this is the first time this lock is aquired
    if (!mdLocks.contains(lock)) {
      mdLocks(lock) = new InterProcessMutex(client, lock);
    }
    // Try to acquire the lock
    if (mdLocks(lock).acquire(LOCK_WAIT, TimeUnit.SECONDS)) {
      LOCK_GRANTED
    }  else {
      LOCK_TIMEOUT
    }
  }
  
  /**
   * releaseLock - Request a distributed lock to be released
   *   @param String - Lock name
   */
  def releaseLock(lockName: String): Unit = {
    var lock: String = zkBasePath + "/locks/" + lockName
    mdLocks(lock).release  
  }
  

}