package com.ligadata.KamanjaShell

import scala.actors.threadpool.{ Executors, ExecutorService }
import com.ligadata.KamanjaManager._

/**
 * @author danielkozin
 */
object EningeCommandProcessor {
  
  def startEngineProcess (opts: InstanceContext, exec: ExecutorService) {

    exec.execute(new Runnable() {
      override def run() = {
        
        var args: Array[String] = new Array[String](2)
        args(0) = "--config"
        args(1) = opts.getIPath + "/config/engineConfig_"+ opts.getIName +".properties"

        println("**** STARTING KAMANJA using "+ args(1))
        
        val mgr = new KamanjaManager
        sys.exit(mgr.run(args))
      }
    })
  }
  
}