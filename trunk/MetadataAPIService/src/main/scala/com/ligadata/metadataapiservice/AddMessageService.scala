package com.ligadata.metadataapiservice

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.io.IO

import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport
import spray.client.pipelining._
import com.ligadata.fatafat.metadata._

import scala.util.{ Success, Failure }

import com.ligadata.MetadataAPI._

object AddMessageService {
  case class Process(messageJson:String)
}

class AddMessageService(requestContext: RequestContext, userid:Option[String], password:Option[String], cert:Option[String]) extends Actor {

  import AddMessageService._
  
  implicit val system = context.system
  import system.dispatcher
  val log = Logging(system, getClass)
  val APIName = "AddMessageService"
  
  def receive = {
    case Process(messageJson) =>
      process(messageJson)
      context.stop(self)
  }
  
  def process(messageJson:String) = {
    
    log.debug("Requesting AddMessage {}",messageJson)

    var nameVal = APIService.extractNameFromJson(messageJson,AuditConstants.MESSAGE)
    
    if (!MetadataAPIImpl.checkAuth(userid,password,cert, MetadataAPIImpl.getPrivilegeName("insert","message"))) {
      MetadataAPIImpl.logAuditRec(userid,Some(AuditConstants.WRITE),AuditConstants.INSERTOBJECT,messageJson,AuditConstants.FAIL,"",nameVal)   
      requestContext.complete(new ApiResult(ErrorCodeConstants.Failure, APIName, null,  "Error:UPDATE not allowed for this user").toString )      
    } else {
      val apiResult = MetadataAPIImpl.AddMessage(messageJson,userid)   
      requestContext.complete(apiResult)
    }
  }
}
