package com.ligadata.MetadataAPI
import com.ligadata.Exceptions.InternalErrorException
import org.apache.zookeeper.KeeperException.NoNodeException
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

/**
 * MonitorAPIImpl - Implementation for methods required to access Monitor related methods. 
 * @author danielkozin
 */
object MonitorAPIImpl {
  
  /**
   * getHeartbeatInfo - get the heartbeat information from the zookeeper.  This informatin is placed there
   *                    by Kamanja Engine instances.
   * @return - String
   */
   def getHeartbeatInfo(nodeIds: List[String] = List[String]()) : scala.collection.mutable.Map[String,Any] = {
     
     
     var zkMonitorInfoPath = MetadataAPIImpl.metadataAPIConfig.getProperty("ZNODE_PATH").trim + "/monitor/engine"
     if (MetadataAPIImpl.zkc == null) {
       MetadataAPIImpl.InitZooKeeper
     }
     
     
     
     //*******************************
     try {
        MetadataAPIImpl.zkc.create.forPath(zkMonitorInfoPath+"/1")
         
     } catch {
       case e: Exception => {e.printStackTrace()}
     }
     
     try {
        MetadataAPIImpl.zkc.create.forPath(zkMonitorInfoPath+"/2")
         
     } catch {
       case e: Exception => {e.printStackTrace()}
     }

     
     
     val test = 
          ("Name" -> "TestInfo") ~
            ("UniqueId" -> "55") ~
            ("LastSeen" -> 34534) ~
            ("StartTime" -> 1232)
            
     val test2 = 
          ("Name" -> "TestInfo2") ~
            ("UniqueId" -> "56") ~
            ("LastSeen" -> 333) ~
            ("StartTime" -> 12)
            
     val data = compact(render(test)).getBytes
     val data2 = compact(render(test2)).getBytes
     MetadataAPIImpl.zkc.setData().forPath(zkMonitorInfoPath+"/1", data) 
     MetadataAPIImpl.zkc.setData().forPath(zkMonitorInfoPath+"/2", data2) 
     
      
     //*******************************    
     
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
  
     if (MetadataAPIImpl.zkc == null) {
       MetadataAPIImpl.InitZooKeeper
     }

     var hbResult: scala.collection.mutable.Map[String,Any] = scala.collection.mutable.Map[String,Any]()
     
     listOfIds.foreach (nodeId => {
       try {
         //hbResult = hbResult + "......" + new String(MetadataAPIImpl.zkc.getData.forPath(zkMonitorInfoPath+"/"+ nodeId))
         hbResult(nodeId) = new String(MetadataAPIImpl.zkc.getData.forPath(zkMonitorInfoPath+"/"+ nodeId))
       } catch {
         case e: org.apache.zookeeper.KeeperException.NoNodeException => {
           println("Unable to get information for NodeId: "+ nodeId)
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