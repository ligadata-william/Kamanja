package com.ligadata.metadataapiservice

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.io.IO
import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport
import spray.client.pipelining._
import scala.util.{ Success, Failure }
import com.ligadata.MetadataAPI._
import com.ligadata.olep.metadata._

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
    
    log.info("Requesting UpdateFunction {}",functionJson)

    var nameVal = APIService.extractNameFromJson(functionJson) 

    if (!MetadataAPIImpl.checkAuth(userid,password,cert, MetadataAPIImpl.getPrivilegeName("update","function"))) {
      MetadataAPIImpl.logAuditRec(userid,Some(AuditConstants.WRITE),AuditConstants.UPDATEOBJECT,AuditConstants.FUNCTION,AuditConstants.FAIL,"",nameVal) 
      requestContext.complete(new ApiResult(-1, APIName, null, "Error:UPDATE not allowed for this user").toString )
    } else {
      val apiResult = MetadataAPIImpl.UpdateFunctions(functionJson,"JSON")
      MetadataAPIImpl.logAuditRec(userid,Some(AuditConstants.WRITE),AuditConstants.UPDATEOBJECT,AuditConstants.FUNCTION,AuditConstants.SUCCESS,"",nameVal)            
      requestContext.complete(apiResult)    
    }
  }
}
