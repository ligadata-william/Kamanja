package com.ligadata.metadataapiservice

import akka.actor.{ActorSystem, Props}
import akka.actor.ActorDSL._
import akka.event.Logging
import akka.io.IO
import akka.io.Tcp._
import spray.can.Http

import com.ligadata.olep.metadata.ObjType._
import com.ligadata.olep.metadata._
import com.ligadata.olep.metadataload.MetadataLoad
import com.ligadata.MetadataAPI._
import org.apache.log4j._
import com.ligadata.Utils._

object APIInit {
  val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)
 // logger.setLevel(Level.TRACE);
  //MetadataAPIImpl.SetLoggerLevel(Level.TRACE)
  //MdMgr.GetMdMgr.SetLoggerLevel(Level.INFO)
  var databaseOpen = false
  var configFile:String = _
  private[this] val lock = new Object()

  def Shutdown(exitCode: Int): Unit = lock.synchronized{
    if( databaseOpen ){
      MetadataAPIImpl.CloseDbStore
      databaseOpen = false;
    }
    MetadataAPIServiceLeader.Shutdown
  }

  def SetConfigFile(cfgFile:String): Unit = {
    configFile = cfgFile
  }

  def SetDbOpen : Unit = {
    databaseOpen = true
  }

  def InitLeaderLatch: Unit = {
      // start Leader detection component
      val nodeId     = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("NODE_ID")
      val zkNode     = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("API_LEADER_SELECTION_ZK_NODE")  + "/metadataleader"
      val zkConnStr  = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("ZOOKEEPER_CONNECT_STRING")
      val sesTimeOut = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("ZK_SESSION_TIMEOUT_MS").toInt
      val conTimeOut = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("ZK_CONNECTION_TIMEOUT_MS").toInt

      MetadataAPIServiceLeader.Init(nodeId,zkConnStr,zkNode,sesTimeOut,conTimeOut)
  }

  def Init : Unit = {
    try{
      // Open db connection
      //MetadataAPIImpl.InitMdMgrFromBootStrap(configFile)
      //databaseOpen = true

      // start Leader detection component
      logger.debug("Initialize Leader Latch")
      InitLeaderLatch
    } catch {
      case e: Exception => {
	e.printStackTrace()
      }
    }
  }
}
