package com.ligadata.ZooKeeper

import com.ligadata.Serialize._
import com.ligadata.MetadataAPI._
import com.ligadata.olep.metadata._

import org.apache.curator.RetryPolicy
import org.apache.curator.framework._
import org.apache.curator.framework.recipes.cache._
import org.apache.curator.framework.api._
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.utils._
import java.util.concurrent.locks._
import org.apache.log4j._
import org.apache.zookeeper.CreateMode

import scala.util.control.Breaks._

object ZooKeeperListener {

  val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)
  var zkc: CuratorFramework = null

  private def ProcessData(newData: ChildData, UpdOnLepMetadataCallback: (ZooKeeperTransaction, MdMgr) => Unit) = {
    try{
      if( newData.getData() != null ){
	val receivedJsonStr = new String(newData.getData())
	logger.debug("New data received => " + receivedJsonStr)
	val zkMessage = JsonSerializer.parseZkTransaction(receivedJsonStr, "JSON")
	MetadataAPIImpl.UpdateMdMgr(zkMessage)
	if (UpdOnLepMetadataCallback != null)
	  UpdOnLepMetadataCallback(zkMessage, com.ligadata.olep.metadata.MdMgr.GetMdMgr)
      }
    } catch {
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }

  def CreateNodeIfNotExists(zkcConnectString:String,znodePath: String) = {
    try {
      zkc = CreateClient.createSimple(zkcConnectString)
      if (zkc.checkExists().forPath(znodePath) == null) {
        zkc.create().withMode(CreateMode.PERSISTENT).forPath(znodePath, null);
      }
    } catch {
      case e: Exception => {
        throw new Exception("Failed to start a zookeeper session with(" + zkcConnectString + "): " + e.getMessage())
      }
    } finally {
      if (zkc != null) {
        zkc.close()
      }
    }
  }

  def CreateListener(zkcConnectString:String,znodePath: String, UpdOnLepMetadataCallback: (ZooKeeperTransaction, MdMgr) => Unit) = {
   try{
      zkc = CreateClient.createSimple(zkcConnectString)
      val nodeCache = new NodeCache(zkc, znodePath)
      nodeCache.getListenable.addListener(new NodeCacheListener {
	@Override
	def nodeChanged = {
          try {
            val dataFromZNode = nodeCache.getCurrentData
            ProcessData(dataFromZNode, UpdOnLepMetadataCallback)
          }catch {
            case ex: Exception => {
              logger.error("Exception while fetching properties from zookeeper ZNode, reason " + ex.getCause())
            }
          }
	}
      })
      nodeCache.start
      logger.setLevel(Level.TRACE);
    }catch {
      case e: Exception => {
        throw new Exception("Failed to start a zookeeper session with(" + zkcConnectString + "): " + e.getMessage())
      }
    }
  }

  def StartLocalListener = {
    try{
      val znodePath = "/ligadata/metadata"
      val zkcConnectString = "localhost:2181"
      JsonSerializer.SetLoggerLevel(Level.TRACE)
      CreateNodeIfNotExists(zkcConnectString,znodePath)
      CreateListener(zkcConnectString,znodePath, null)

      breakable{
	for (ln <- io.Source.stdin.getLines) { // Exit after getting input from console
	  if (zkc != null)
            zkc.close
	  zkc = null
	  println("Exiting")
	  break
	}
      }
    }catch {
      case e: Exception => {
        throw new Exception("Failed to start a zookeeper session: " + e.getMessage())
      }
    } finally {
      if (zkc != null) {
        zkc.close()
      }
    }
  }

  def main(args: Array[String]) = {
    try{
      MetadataAPIImpl.InitMdMgrFromBootStrap
      MetadataAPIImpl.SetLoggerLevel(Level.TRACE)
      MdMgr.GetMdMgr.SetLoggerLevel(Level.TRACE)
      JsonSerializer.SetLoggerLevel(Level.TRACE)
      StartLocalListener
    }catch {
      case e: Exception => {
        throw new Exception("Failed to start a zookeeper session: " + e.getMessage())
      }
    } finally {
      MetadataAPIImpl.CloseDbStore
      if (zkc != null) {
        zkc.close()
      }
    }
  }
}
