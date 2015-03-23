package com.ligadata.metadataapiservice

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.io.IO

import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport
import spray.client.pipelining._

import scala.util.{ Success, Failure }

import com.ligadata.MetadataAPI._

object UpdateTypeService {
  case class Process(typeJson:String,formatType:String)
}

class UpdateTypeService(requestContext: RequestContext, userid:Option[String], password:Option[String], cert:Option[String]) extends Actor {

  import UpdateTypeService._
  
  implicit val system = context.system
  import system.dispatcher
  val log = Logging(system, getClass)
  
  def receive = {
    case Process(typeJson,formatType) =>
      process(typeJson,formatType)
      context.stop(self)
  }
  
  def process(typeJson:String, formatType:String) = {
    log.info("Requesting Update {},{}",typeJson,formatType)

    val objectName = typeJson.substring(0,100)    
    if (!MetadataAPIImpl.checkAuth(userid,password,cert, MetadataAPIImpl.getPrivilegeName("update","type"))) {
      MetadataAPIImpl.logAuditRec(userid,Some("update"),"UpdateType",objectName,"Failed","unknown","UPDATE not allowed for this user") 
      requestContext.complete(new ApiResult(-1,"Security","UPDATE not allowed for this user").toString )
    }
    
    val apiResult = MetadataAPIImpl.UpdateType(typeJson,formatType)
    MetadataAPIImpl.logAuditRec(userid,Some("update"),"UpdateType",objectName,"Finished","unknown",apiResult)        
    requestContext.complete(apiResult)
  }
}
