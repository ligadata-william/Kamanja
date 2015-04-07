package com.ligadata.metadataapiservice

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.io.IO

import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport
import spray.client.pipelining._
import com.ligadata.olep.metadata._

import scala.util.{ Success, Failure }

import com.ligadata.MetadataAPI._

object AddConceptService {
	case class Process(conceptJson:String, formatType:String)
}

class AddConceptService(requestContext: RequestContext, userid:Option[String], password:Option[String], cert:Option[String]) extends Actor {

	import AddConceptService._
	
	implicit val system = context.system
	import system.dispatcher
	val log = Logging(system, getClass)
  val APIName = "AddConceptService"
	
	def receive = {
	  case Process(conceptJson, formatType) =>
	    process(conceptJson, formatType)
	    context.stop(self)
	}
	
	def process(conceptJson:String, formatType:String) = {
	  log.info("Requesting AddConcept {},{}",conceptJson,formatType)
    
	  val objectName = conceptJson.substring(0,100)

	  if (!MetadataAPIImpl.checkAuth(userid, password, cert, MetadataAPIImpl.getPrivilegeName("insert","concept"))) {
	    MetadataAPIImpl.logAuditRec(userid,Some(AuditConstants.WRITE),AuditConstants.INSERTOBJECT,AuditConstants.CONCEPT,AuditConstants.FAIL,"",objectName.substring(0,20))
	    requestContext.complete(new ApiResult(-1, APIName, null, "Error:UPDATE not allowed for this user").toString )
	  } else {
	    val apiResult = MetadataAPIImpl.AddConcepts(conceptJson,formatType)
	    MetadataAPIImpl.logAuditRec(userid,Some(AuditConstants.WRITE),AuditConstants.INSERTOBJECT,AuditConstants.CONCEPT,AuditConstants.SUCCESS,"",objectName.substring(0,20))
	    requestContext.complete(apiResult)
    }
	}
}
