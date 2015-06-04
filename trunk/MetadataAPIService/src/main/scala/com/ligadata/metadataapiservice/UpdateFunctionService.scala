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

object UpdateFunctionService {
  case class Process(functionJson:String)
}

class UpdateFunctionService(requestContext: RequestContext, userid:Option[String], password:Option[String], cert:Option[String]) extends Actor {

  import UpdateFunctionService._
  
  implicit val system = context.system
  import system.dispatcher
  val log = Logging(system, getClass)
  val APIName = "UpdateFunctionService"
  
  def receive = {
    case Process(functionJson) =>
      process(functionJson)
      context.stop(self)
  }
  
  def process(functionJson:String) = {
    
    log.debug("Requesting UpdateFunction {}",functionJson)

    var nameVal = APIService.extractNameFromJson(functionJson, AuditConstants.FUNCTION) 

    if (!MetadataAPIImpl.checkAuth(userid,password,cert, MetadataAPIImpl.getPrivilegeName("update","function"))) {
      MetadataAPIImpl.logAuditRec(userid,Some(AuditConstants.WRITE),AuditConstants.UPDATEOBJECT,functionJson,AuditConstants.FAIL,"",nameVal) 
      requestContext.complete(new ApiResult(ErrorCodeConstants.Failure, APIName, null, "Error:UPDATE not allowed for this user").toString )
    } else {
      val apiResult = MetadataAPIImpl.UpdateFunctions(functionJson,"JSON",userid)
      MetadataAPIImpl.logAuditRec(userid,Some(AuditConstants.WRITE),AuditConstants.UPDATEOBJECT,functionJson,AuditConstants.SUCCESS,"",nameVal)            
      requestContext.complete(apiResult)    
    }
  }
}
