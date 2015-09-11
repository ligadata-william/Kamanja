package com.ligadata.KamanjaShell

import scala.actors.threadpool.{ Executors, ExecutorService }
import sys.process._

/**
 * @author danielkozin
 */
object ZookeeperCommands {
  
  val tmpZKPath = "/tmp/zookeeper-3.4.6" 
  var tmpZKPath2 = ""
  /**
   * Start an internal Zookeeper PROCESS.  Need to figure out the port available, then 
   * set up the right configuration for this local zookeeper instance.  
   * 
   */  
  def startLocalZookeeper(opts: InstanceContext, exec: ExecutorService): Unit = {
      tmpZKPath2 = opts.getIPath + "/zookeeper"
      opts.zkPort = KShellUtils.getAvailablePort(opts.zkPort).toString
      // Setup the zookeeper config.
      var zkProps = new java.util.Properties
      zkProps.setProperty("tickTime", "2000")
      zkProps.setProperty("dataDir", opts.getIPath + "/zookeeper/data")
      zkProps.setProperty("clientPort", opts.zkPort)
      KShellUtils.savePropFile(zkProps, "zoo.cfg" ,tmpZKPath2 + "/conf")
      
     // KShellUtils.savePropFile(zkProps, "zoo.cfg" ,opts.getIPath + "/zookeeper/conf")
        
      // Start Zookeeper
      println("Auto-Starting Zookeeper at " + opts.zkLocation + ":" + opts.zkPort)
      exec.execute(new Runnable() {
        override def run() = {
          var cmd = s"$tmpZKPath/bin/zkServer.sh start "+tmpZKPath2+"/conf/zoo.cfg"
          println(" Zookeeper start command =>"+ cmd)
          val result =  cmd.!
        }
      })  
  }
}