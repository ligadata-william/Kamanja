package com.ligadata.metadataapiservice

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.io.IO
import com.ligadata.fatafat.metadata._
import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport
import spray.client.pipelining._
import com.ligadata.fatafat.metadata._
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
    
    var nameVal = APIService.extractNameFromJson(containerJson, AuditConstants.CONTAINER) 

    if (!MetadataAPIImpl.checkAuth(userid,password,cert, MetadataAPIImpl.getPrivilegeName("update","container"))) {
       MetadataAPIImpl.logAuditRec(userid,Some(AuditConstants.WRITE),AuditConstants.UPDATEOBJECT,containerJson,AuditConstants.FAIL,"",nameVal) 
       requestContext.complete(new ApiResult(ErrorCodeConstants.Failure, APIName, null, "Error:UPDATE not allowed for this user").toString )
    } else {
      val apiResult = MetadataAPIImpl.UpdateContainer(containerJson,"JSON",userid)
      requestContext.complete(apiResult)     
    }
  }
}
