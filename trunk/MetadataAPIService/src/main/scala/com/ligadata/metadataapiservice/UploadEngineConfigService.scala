package com.ligadata.metadataapiservice

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.io.IO

import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport
import spray.client.pipelining._

import scala.util.{ Success, Failure }

import com.ligadata.MetadataAPI._

object UploadEngineConfigService {
  case class Process(cfgJson:String)
}

class UploadEngineConfigService(requestContext: RequestContext, userid:Option[String], password:Option[String], cert:Option[String]) extends Actor {

  import UploadEngineConfigService._
  
  implicit val system = context.system
  import system.dispatcher
  val log = Logging(system, getClass)
  val APIName = "UploadEngineConfigService"
  
  def receive = {
    case Process(cfgJson) =>
      process(cfgJson)
      context.stop(self)
  }
  
  def process(cfgJson:String) = {
    
    log.info("Requesting UploadEngineConfig {}",cfgJson)
    val objectName = cfgJson.substring(0,100)    
    if (!MetadataAPIImpl.checkAuth(userid,password,cert, MetadataAPIImpl.getPrivilegeName("update","configuration"))) {
      MetadataAPIImpl.logAuditRec(userid,Some("update"),"UploadConfig",objectName,"Failed","unknown","UPDATE not allowed for this user") 
      requestContext.complete(new ApiResult(-1, APIName, null, "Error:UPDATE not allowed for this user").toString )
    }
    
    val apiResult = MetadataAPIImpl.UploadConfig(cfgJson)
    MetadataAPIImpl.logAuditRec(userid,Some("update"),"UploadConfig",objectName,"Finished","unknown",apiResult)            
    requestContext.complete(apiResult)
  }
}
