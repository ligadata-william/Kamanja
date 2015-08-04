package com.ligadata.MetadataAPI
import com.ligadata.Exceptions.InternalErrorException
import org.apache.zookeeper.KeeperException.NoNodeException
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.apache.log4j._
import scala.collection.JavaConverters._
import org.apache.curator.framework.recipes.cache._
import scala.actors.threadpool.{ Executors, ExecutorService }


/**
 * MonitorAPIImpl - Implementation for methods required to access Monitor related methods. 
 * @author danielkozin
 */
object MonitorAPIImpl {
  
  val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)
  val CHILD_ADDED_ACTION = "CHILD_ADDED"
  val CHILD_REMOVED_ACTION = "CHILD_REMOVED"
  val CHILD_UPDATED_ACTION = "CHILD_UPDATED"
  val ENGINE = "engine"
  val METADATA = "metadata"
  
  private var healthInfo: scala.collection.mutable.Map[String,Any] = scala.collection.mutable.Map[String,Any]()
  
  /**
   * updateHeartbeatInfo - this is a callback function, zookeeper listener will call here to update the local cache. Users should not
   *                       calling in here by themselves
   * 
   */
  def updateHeartbeatInfo(eventType: String, eventPath: String, eventPathData: Array[Byte], children: Array[(String, Array[Byte])]): Unit = {
    
    try {
      if (eventPathData == null) return
      logger.debug("eventType ->" + eventType)
      logger.debug("eventPath ->" + eventPath)
      if (eventPathData == null) {  logger.debug("eventPathData is null"); return; }
      logger.debug("eventPathData ->" + new String(eventPathData))
    
      // Ok, we got an event, parse to see what it is.
      var pathTokens = eventPath.split('/')
     
      // We are guaranteed the pathTokens.length - 2 is either a Metadata or Engine
      var componentName = pathTokens(pathTokens.length - 2)
      var nodeName = pathTokens(pathTokens.length - 1)
      var key = componentName+"."+nodeName
    
      // Add or Remove the data to/from the map.
      if (eventType.equals(CHILD_ADDED_ACTION) || eventType.equals(CHILD_UPDATED_ACTION)) 
        healthInfo(key) = new String(eventPathData)
      else 
        if (healthInfo.contains(key)) healthInfo.remove(key)
    } catch {
      case e: Exception => {
        e.printStackTrace
      }
    }  
  }
  
  /**
   * getHeartbeatInfo - get the heartbeat information from the zookeeper.  This informatin is placed there
   *                    by Kamanja Engine instances.
   * @return - String
   */
   def getHeartbeatInfo(ids: List[String]) :String = {
     var ar = healthInfo.values.toArray
     var isFirst = true
     var resultJson = "["
  
     // If List is empty, then return everything. otherwise, return only those items that are present int he
     // list    
     if (ids.length > 0) {
       ids.foreach (id => {
         if (healthInfo.contains(id)) {
           if (!isFirst) resultJson = resultJson + ","
           resultJson = resultJson +  healthInfo(id).asInstanceOf[String]
           isFirst = false
         }
       })
     } else {
       if (healthInfo != null && ar.length > 0) {
         for(i <- 0 until ar.length ) {
           if (!isFirst) resultJson = resultJson + ","
           resultJson = resultJson + ar(i).asInstanceOf[String]
           isFirst = false
         }
       }      
     }
     return resultJson + "]"
     
   }
   
   /**
    * startMetadataHeartbeat - will be called internally by the MetadataAPI task to update the healthcheck every 5 seconds.
    */
   def startMetadataHeartbeat: Unit = {
     var _exec = Executors.newFixedThreadPool(1)

     _exec.execute(new Runnable() {
       override def run() = {
         var startTime = System.currentTimeMillis
         while (_exec.isShutdown == false) {
           Thread.sleep(5000)
           MetadataAPIImpl.clockNewActivity
         }
       }
      })
   }
   
}