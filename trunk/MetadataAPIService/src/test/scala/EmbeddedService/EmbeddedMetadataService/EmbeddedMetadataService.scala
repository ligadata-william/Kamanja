package scala.EmbeddedServices.EmbeddedMetadataService

import com.ligadata.metadataapiservice.APIService

/**
 * Created by dhaval on 7/13/15.
 */
 class EmbeddedMetadataService(){
  var apiServiceThread:Thread=null
  def run(config: String) {
    // start the APIService runnable thread
    val args: Array[String]=Array("--config",config)
    apiServiceThread=new Thread(new APIService(args))
    println("api service: "+apiServiceThread.getName)
    apiServiceThread.start()
    println("Thread for api service is: "+apiServiceThread+"is in state: "+apiServiceThread.getState.toString)
  }

  def isRunning: Boolean ={
   // println("Checking the heartbeat of web service: "+apiServiceThread.getState)
    apiServiceThread.getState==("RUNNABLE")
  }
}

object EmbeddedMetadataService {
  private var ems: EmbeddedMetadataService = null
  def instance: EmbeddedMetadataService = {
    if(ems == null) {
      ems = new EmbeddedMetadataService()
    }
    return ems
  }
}

