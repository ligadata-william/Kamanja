package com.ligadata.MetadataAPI
import com.ligadata.Exceptions.InternalErrorException
import org.apache.zookeeper.KeeperException.NoNodeException
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.apache.log4j._

/**
 * MonitorAPIImpl - Implementation for methods required to access Monitor related methods. 
 * @author danielkozin
 */
object MonitorAPIImpl {
  
  val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)
  
  /**
   * getHeartbeatInfo - get the heartbeat information from the zookeeper.  This informatin is placed there
   *                    by Kamanja Engine instances.
   * @return - String
   */
   def getHeartbeatInfo(nodeIds: List[String] = List[String]()) : scala.collection.mutable.Map[String,Any] = {
     
     // If the Metadata did not initialize zookeeper, do it here.
     var zkMonitorInfoPath = MetadataAPIImpl.metadataAPIConfig.getProperty("ZNODE_PATH").trim + "/monitor/engine"
     if (MetadataAPIImpl.zkc == null) {
       MetadataAPIImpl.InitZooKeeper
     }
     
     // If there were no NodeIds passed in, we want to get information on all 
     // knowd NodeIds...
     var listOfIds: List[String] = List[String]()
     if (nodeIds == null || nodeIds.length == 0) {
       var tempA = MetadataAPIImpl.getNodeList1
       for (i <- 0 until tempA.length) {
         println(tempA(i).nodeId)
         listOfIds = tempA(i).nodeId :: listOfIds 
       }
     } 
     else {
       listOfIds = nodeIds
     }

     var hbResult: scala.collection.mutable.Map[String,Any] = scala.collection.mutable.Map[String,Any]()
     
     // Get all the nodeIds.
     listOfIds.foreach (nodeId => {
       try {
         //hbResult = hbResult + "......" + new String(MetadataAPIImpl.zkc.getData.forPath(zkMonitorInfoPath+"/"+ nodeId))
         hbResult(nodeId) = new String(MetadataAPIImpl.zkc.getData.forPath(zkMonitorInfoPath+"/"+ nodeId))
       } catch {
         case e: org.apache.zookeeper.KeeperException.NoNodeException => {
           logger.warn("Unable to get information for NodeId: "+ nodeId)
         }
         case e: Exception => {
           e.printStackTrace
           throw new InternalErrorException("Failed to notify a zookeeper message from the objectList " + e.getMessage())
         }      
       }         
     })
     return hbResult
   }
}