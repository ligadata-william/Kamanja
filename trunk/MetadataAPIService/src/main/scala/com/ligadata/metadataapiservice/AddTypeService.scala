package com.ligadata.metadataapiservice

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.io.IO
import com.ligadata.olep.metadata._
import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport
import spray.client.pipelining._

import scala.util.{ Success, Failure }

import com.ligadata.MetadataAPI._

object AddTypeService {
  case class Process(typeJson:String, formatType:String)
}

class AddTypeService(requestContext: RequestContext, userid:Option[String], password:Option[String], cert:Option[String]) extends Actor {

  import AddTypeService._
  
  implicit val system = context.system
  import system.dispatcher
  val log = Logging(system, getClass)
  val APIName = "AddTypeService"
  
  def receive = {
    case Process(typeJson, formatType) =>
      process(typeJson, formatType)
      context.stop(self)
  }
  
  def process(typeJson:String, formatType:String) = {
    
    log.info("Requesting AddType {},{}",typeJson.substring(1,200) + " .....",formatType)
    val objectName = typeJson.substring(0,100)
    
    if (!MetadataAPIImpl.checkAuth(userid,password,cert, MetadataAPIImpl.getPrivilegeName("insert","type"))) {
      MetadataAPIImpl.logAuditRec(userid,Some(AuditConstants.WRITE),AuditConstants.INSERTOBJECT,AuditConstants.TYPE,AuditConstants.FAIL,"",objectName.substring(0,20))
      requestContext.complete(new ApiResult(-1, APIName, null, "Error:UPDATE not allowed for this user").toString )
    }
    
    val apiResult = MetadataAPIImpl.AddTypes(typeJson,formatType)
    MetadataAPIImpl.logAuditRec(userid,Some(AuditConstants.WRITE),AuditConstants.INSERTOBJECT,AuditConstants.TYPE,AuditConstants.SUCCESS,"",objectName.substring(0,20))       
    requestContext.complete(apiResult)
  }
}
