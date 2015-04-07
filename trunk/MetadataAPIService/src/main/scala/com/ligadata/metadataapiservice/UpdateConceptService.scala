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

object UpdateConceptService {
  case class Process(conceptJson:String)
}

class UpdateConceptService(requestContext: RequestContext, userid:Option[String], password:Option[String], cert:Option[String]) extends Actor {

  import UpdateConceptService._
  
  implicit val system = context.system
  import system.dispatcher
  val log = Logging(system, getClass)
  val APIName = "UpdateConceptService"
  
  def receive = {
    case Process(conceptJson) =>
      process(conceptJson)
      context.stop(self)
  }
  
  def process(conceptJson:String) = {
    
    log.info("Requesting UpdateConcept {}",conceptJson)
    val objectName = conceptJson.substring(0,100)        
    if (!MetadataAPIImpl.checkAuth(userid,password,cert, MetadataAPIImpl.getPrivilegeName("update","concept"))) {
      MetadataAPIImpl.logAuditRec(userid,Some(AuditConstants.WRITE),AuditConstants.UPDATEOBJECT,AuditConstants.CONCEPT,AuditConstants.FAIL,"",objectName.substring(0,20)) 
      requestContext.complete(new ApiResult(-1,APIName, null, "Error:UPDATE not allowed for this user").toString )
    } else {
      val apiResult = MetadataAPIImpl.UpdateConcepts(conceptJson,"JSON")
      MetadataAPIImpl.logAuditRec(userid,Some(AuditConstants.WRITE),AuditConstants.UPDATEOBJECT,AuditConstants.CONCEPT,AuditConstants.SUCCESS,"",objectName.substring(0,20))            
      requestContext.complete(apiResult)     
    }
  }
}
