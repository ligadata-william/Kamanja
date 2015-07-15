package com.ligadata.automation.zookeeper

import org.apache.curator.framework.{CuratorFrameworkFactory, CuratorFramework}
import org.apache.curator.retry.ExponentialBackoffRetry

/**
 * Created by will on 6/19/15.
 */

object ZookeeperClient {
  private var zkc: ZookeeperClient = _
  def instance: ZookeeperClient = {
    if (zkc == null){
      zkc = new ZookeeperClient(zkcConnectString = EmbeddedZookeeper.instance.getConnection, 30000, 250)
    }
    return zkc
  }
}

protected class ZookeeperClient(zkcConnectString:String, sessionTimeoutMs:Int, connectionTimeoutMs:Int) {
  private val logger = org.apache.log4j.Logger.getLogger(this.getClass)
  private val retryPolicy = new ExponentialBackoffRetry(1000, 3)
  private val zkc = CuratorFrameworkFactory.newClient(zkcConnectString, sessionTimeoutMs, connectionTimeoutMs, retryPolicy)
  private var isRunning: Boolean = false

  def start(): Unit = {
    if(!isRunning) {
      try {
        logger.info("AUTOMATION-ZOOKEEPER: Starting Curator Framework")
        zkc.start()
        isRunning = true
      }
      catch {
        case e: Exception => {
          logger.error("AUTOMATION-ZOOKEEPER: Failed to start Curator Framework")
          throw new EmbeddedZookeeperException("AUTOMATION-ZOOKEEPER: Failed to start Curator Framework")
        }
      }
    }
  }

  def isZkcRunning: Boolean={
    this.isRunning
  }
  def stop: Unit = {
    if(isRunning) {
      try {
        logger.info("AUTOMATION-ZOOKEEPER: Stopping Curator Framework")
        zkc.close()
        isRunning = false
      }
      catch {
        case e: Exception => {
          logger.error("AUTOMATION-ZOOKEEPER: Failed to stop Curator Framework")
          throw new EmbeddedZookeeperException("AUTOMATION-ZOOKEEPER: Failed to stop Curator Framework")
        }
      }
    }
  }

  def doesNodeExist(znodePath:String): Boolean = {
    try {
      logger.info("AUTOMATION-ZOOKEEPER: Checking if zookeeper node path '" + znodePath + "' exists")
      if (zkc.checkExists().forPath(znodePath) == null) {
        logger.info("AUTOMATION-ZOOKEEPER: Zookeeper node path '" + znodePath + "' doesn't exist")
        return false
      }
      else {
        logger.info("AUTOMATION-ZOOKEEPER: Zookeeper node path found with data: " + zkc.checkExists().forPath(znodePath))
        return true
      }
    }
    catch {
      case e: Exception => {
        logger.error("AUTOMATION-ZOOKEEPER: Failed to verify node exists with exception:\n" + e)
        throw new EmbeddedZookeeperException("AUTOMATION-ZOOKEEPER: Failed to verify node exists with exception:\n" + e)
      }
    }
  }

  def waitForNodeToExist(znodePath:String, timeout:Int):Boolean = {
    var count = 0
    while(count < timeout) {
      count += 1
      try {
        if (doesNodeExist(znodePath))
          return true
      }
      catch {
        case e: EmbeddedZookeeperException => {
          logger.error("AUTOMATION-ZOOKEEPER: Caught exception\n" + e)
          throw new EmbeddedZookeeperException("AUTOMATION-ZOOKEEPER: Caught exception\n" + e)
        }
        case e: Exception => throw new EmbeddedZookeeperException("AUTOMATION-ZOOKEEPER: Caught exception\n" + e)
      }
      Thread sleep 1000
    }
    logger.warn("AUTOMATION-ZOOKEEPER: Node '" + znodePath + "' doesn't exist after " + timeout + " seconds")
    return false
  }
}
