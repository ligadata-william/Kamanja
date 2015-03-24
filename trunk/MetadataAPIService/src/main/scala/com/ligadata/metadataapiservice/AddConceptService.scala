package com.ligadata.metadataapiservice

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.io.IO

import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport
import spray.client.pipelining._

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
	    MetadataAPIImpl.logAuditRec(userid,Some("insert"),"AddConcept",objectName,"Failed","unknown","UPDATE not allowed for this user")
	    requestContext.complete(new ApiResult(-1, APIName, null, "Error:UPDATE not allowed for this user").toString )
	  }
		
	  val apiResult = MetadataAPIImpl.AddConcepts(conceptJson,formatType)
	  MetadataAPIImpl.logAuditRec(userid,Some("insert"),"AddConcept",objectName,"Finished","unknown",apiResult)
	  requestContext.complete(apiResult)
	}
}
