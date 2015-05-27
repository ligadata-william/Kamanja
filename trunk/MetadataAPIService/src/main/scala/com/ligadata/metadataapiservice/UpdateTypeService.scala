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

object UpdateTypeService {
  case class Process(typeJson:String,formatType:String)
}

class UpdateTypeService(requestContext: RequestContext, userid:Option[String], password:Option[String], cert:Option[String]) extends Actor {

  import UpdateTypeService._
  
  implicit val system = context.system
  import system.dispatcher
  val log = Logging(system, getClass)
  val APIName = "UpdateTypeService"
  
  def receive = {
    case Process(typeJson,formatType) =>
      process(typeJson,formatType)
      context.stop(self)
  }
  
  def process(typeJson:String, formatType:String): Unit = {
    log.info("Requesting Update {},{}",typeJson,formatType)
    var nameVal: String = null
    if (formatType.equalsIgnoreCase("json")) {
      nameVal = APIService.extractNameFromJson(typeJson, AuditConstants.TYPE) 
    } else {
      requestContext.complete(new ApiResult(ErrorCodeConstants.Failure, APIName, null, "Error:Unsupported format: "+formatType).toString ) 
      return
    }

    val objectName = typeJson.substring(0,100)    
    if (!SecAuditImpl.checkAuth(userid,password,cert, SecAuditImpl.getPrivilegeName("update","type"))) {
       SecAuditImpl.logAuditRec(userid,Some(AuditConstants.WRITE),AuditConstants.UPDATEOBJECT,typeJson,AuditConstants.FAIL,"",nameVal) 
      requestContext.complete(new ApiResult(ErrorCodeConstants.Failure, APIName, null, "Error:UPDATE not allowed for this user").toString )
    } else {
      val apiResult = MetadataAPIImpl.UpdateType(typeJson,formatType)
      SecAuditImpl.logAuditRec(userid,Some(AuditConstants.WRITE),AuditConstants.UPDATEOBJECT,typeJson,AuditConstants.SUCCESS,"",nameVal)
      requestContext.complete(apiResult)     
    }
  }
}
