package com.ligadata.metadataapiservice

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.io.IO
import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport
import spray.client.pipelining._
import scala.util.{ Success, Failure }
import com.ligadata.MetadataAPI._
import com.ligadata.fatafat.metadata._

object UpdateModelService {
  case class Process(pmmlStr:String)
}

class UpdateModelService(requestContext: RequestContext, userid:Option[String], password:Option[String], cert:Option[String]) extends Actor {

  import UpdateModelService._
  
  implicit val system = context.system
  import system.dispatcher
  val log = Logging(system, getClass)
  val APIName = "UpdateModelService"
  
  def receive = {
    case Process(pmmlStr) =>
      process(pmmlStr)
      context.stop(self)
  }
  
  def process(pmmlStr:String) = {
    
    log.debug("Requesting UpdateModel {}",pmmlStr)

    var nameVal = APIService.extractNameFromPMML(pmmlStr) 

    if (!MetadataAPIImpl.checkAuth(userid,password,cert, MetadataAPIImpl.getPrivilegeName("update","model"))) {
       MetadataAPIImpl.logAuditRec(userid,Some(AuditConstants.WRITE),AuditConstants.UPDATEOBJECT,pmmlStr,AuditConstants.FAIL,"",nameVal)
      requestContext.complete(new ApiResult(ErrorCodeConstants.Failure, APIName, null, "Error:UPDATE not allowed for this user").toString )
    } else {
      val apiResult = MetadataAPIImpl.UpdateModel(pmmlStr)
      MetadataAPIImpl.logAuditRec(userid,Some(AuditConstants.WRITE),AuditConstants.UPDATEOBJECT,pmmlStr,AuditConstants.SUCCESS,"",nameVal)    
      requestContext.complete(apiResult)      
    }
  }
}
