package com.ligadata.metadataapiservice

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.io.IO

import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport
import spray.client.pipelining._
import com.ligadata.olep.metadata._
import scala.util.{ Success, Failure }

import com.ligadata.MetadataAPI._

object RemoveEngineConfigService {
  case class Process(cfgJson:String)
}

class RemoveEngineConfigService(requestContext: RequestContext, userid:Option[String], password:Option[String], cert:Option[String]) extends Actor {

  import RemoveEngineConfigService._
  
  implicit val system = context.system
  import system.dispatcher
  val log = Logging(system, getClass)
  val APIName = "RemoveEngineConfigService"
  
  def receive = {
    case Process(cfgJson) =>
      process(cfgJson)
      context.stop(self)
  }
  
  def process(cfgJson:String) = {
    
    log.info("Requesting RemoveEngineConfig {}",cfgJson)
    val objectName = cfgJson.substring(0,100)        
    if (!MetadataAPIImpl.checkAuth(userid,password,cert,"write")) {
      MetadataAPIImpl.logAuditRec(userid,Some(AuditConstants.WRITE),AuditConstants.REMOVECONFIG,AuditConstants.CONFIG,AuditConstants.FAIL,"",objectName.substring(0,20)) 
      requestContext.complete(new ApiResult(-1, APIName, null, "Error:UPDATE not allowed for this user").toString )
    }
    
    val apiResult = MetadataAPIImpl.RemoveConfig(cfgJson)
    MetadataAPIImpl.logAuditRec(userid,Some(AuditConstants.WRITE),AuditConstants.REMOVECONFIG,AuditConstants.CONFIG,AuditConstants.SUCCESS,"",objectName.substring(0,20))    
    requestContext.complete(apiResult)
  }
}
