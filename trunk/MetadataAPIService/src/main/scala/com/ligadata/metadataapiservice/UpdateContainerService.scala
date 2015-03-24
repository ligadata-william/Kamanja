package com.ligadata.metadataapiservice

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.io.IO

import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport
import spray.client.pipelining._

import scala.util.{ Success, Failure }

import com.ligadata.MetadataAPI._

object UpdateContainerService {
  case class Process(containerJson:String)
}

class UpdateContainerService(requestContext: RequestContext, userid:Option[String], password:Option[String], cert:Option[String]) extends Actor {

  import UpdateContainerService._
  
  implicit val system = context.system
  import system.dispatcher
  val log = Logging(system, getClass)
  val APIName = "UpdateContainerService"
  
  def receive = {
    case Process(containerJson) =>
      process(containerJson)
      context.stop(self)
  }
  
  def process(containerJson:String) = {
    log.info("Requesting UpdateContainer {}",containerJson)
    
    val objectName = containerJson.substring(0,100)

    if (!MetadataAPIImpl.checkAuth(userid,password,cert, MetadataAPIImpl.getPrivilegeName("update","container"))) {
      MetadataAPIImpl.logAuditRec(userid,Some("update"),"UpdateContainer",objectName,"Failed","unknown","UPDATE not allowed for this user") 
      requestContext.complete(new ApiResult(-1, APIName, null, "Error:UPDATE not allowed for this user").toString )
    }
    
    val apiResult = MetadataAPIImpl.UpdateContainer(containerJson,"JSON")
    MetadataAPIImpl.logAuditRec(userid,Some("update"),"UpdateContainer",objectName,"Finished","unknown",apiResult)
    requestContext.complete(apiResult)
  }
}
