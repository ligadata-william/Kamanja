package com.ligadata.metadataapiservice

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.io.IO

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
	
	def receive = {
		case Process(messageJson, formatType) =>
			process(messageJson, formatType)
			context.stop(self)
	}
	
	def process(messageJson:String, formatType:String) = {
	  
		log.info("Requesting Update {},{}",messageJson,formatType)

    if (!MetadataAPIImpl.checkAuth(userid,password,cert, MetadataAPIImpl.getPrivilegeName("update","message"))) {
      requestContext.complete(new ApiResult(-1,"Security","UPDATE not allowed for this user").toString )
    }
		
		val apiResult = MetadataAPIImpl.UpdateMessage(messageJson,formatType)
		
		requestContext.complete(apiResult)
	}
}
