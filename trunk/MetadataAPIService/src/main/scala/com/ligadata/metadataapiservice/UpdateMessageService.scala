package com.ligadata.metadataapiservice

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.io.IO
import com.ligadata.fatafat.metadata._
import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport
import spray.client.pipelining._

import scala.util.{ Success, Failure }

import com.ligadata.MetadataAPI._

object UpdateMessageService {
  case class Process(messageJson:String, formatType:String)
}

class UpdateMessageService(requestContext: RequestContext, userid:Option[String], password:Option[String], cert:Option[String]) extends Actor {

  import UpdateMessageService._
  
  implicit val system = context.system
  import system.dispatcher
  val log = Logging(system, getClass)
  val APIName = "UpdateMessageService"
  
  def receive = {
    case Process(messageJson, formatType) =>
      process(messageJson, formatType)
      context.stop(self)
  }
  
  def process(messageJson:String, formatType:String): Unit = {
    
    log.info("Requesting Update {},{}",messageJson,formatType)

    var nameVal: String = null
    if (formatType.equalsIgnoreCase("json")) {
      nameVal = APIService.extractNameFromJson(messageJson, AuditConstants.MESSAGE)    
    } else {
      requestContext.complete(new ApiResult(ErrorCodeConstants.Failure, APIName, null, "Error:Unsupported format: "+formatType).toString ) 
      return
    }

    if (!MetadataAPIImpl.checkAuth(userid,password,cert, MetadataAPIImpl.getPrivilegeName("update","message"))) {
       MetadataAPIImpl.logAuditRec(userid,Some(AuditConstants.WRITE),AuditConstants.UPDATEOBJECT,AuditConstants.MESSAGE,AuditConstants.FAIL,"",nameVal) 
      requestContext.complete(new ApiResult(ErrorCodeConstants.Failure, APIName, null, "Error:UPDATE not allowed for this user").toString )
    } else {
      val apiResult = MetadataAPIImpl.UpdateMessage(messageJson,formatType)
      MetadataAPIImpl.logAuditRec(userid,Some(AuditConstants.WRITE),AuditConstants.UPDATEOBJECT,AuditConstants.MESSAGE,AuditConstants.SUCCESS,"",nameVal)        
      requestContext.complete(apiResult)     
    }
  }
}
