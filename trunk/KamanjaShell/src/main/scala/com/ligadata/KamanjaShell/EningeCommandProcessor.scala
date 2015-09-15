package com.ligadata.KamanjaShell

import scala.actors.threadpool.{ Executors, ExecutorService }
import com.ligadata.KamanjaManager._

/**
 * @author danielkozin
 */
object EningeCommandProcessor {
  
  var mgr: KamanjaManager = null
  
  def startEngineProcess (opts: InstanceContext, exec: ExecutorService) {
     
    KamanjaConfiguration.shutdown = true   
    mgr = new KamanjaManager 

    scala.sys.addShutdownHook({
      if (KamanjaConfiguration.shutdown == false) {
        println("Got Shutdown request")
        KamanjaConfiguration.shutdown = true // Setting the global shutdown
      }
     })

    exec.execute(new Runnable() {
      override def run() = {
        
        var args: Array[String] = new Array[String](2)
        args(0) = "--config"
        args(1) = opts.getIPath + "/config/engineConfig_"+ opts.getIName +".properties"

        println("**** STARTING KAMANJA using "+ args(1))
        mgr.run(args)        
      }
    })       
  }
  
  def shutdown: Unit = {
    if (mgr != null) {
      KamanjaConfiguration.shutdown = true
      mgr = null
    }
  }

  
}