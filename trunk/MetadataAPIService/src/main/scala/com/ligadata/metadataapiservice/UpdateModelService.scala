package com.ligadata.metadataapiservice

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.io.IO

import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport
import spray.client.pipelining._

import scala.util.{ Success, Failure }

import com.ligadata.MetadataAPI._

object UpdateModelService {
	case class Process(objectName:String)
}

class UpdateModelService(requestContext: RequestContext) extends Actor {

	import UpdateModelService._
	
	implicit val system = context.system
	import system.dispatcher
	val log = Logging(system, getClass)
	
	def receive = {
		case Process(pmmlStr) =>
			process(pmmlStr)
			context.stop(self)
	}
	
	def process(pmmlStr:String) = {
	  
		log.info("Requesting UpdateModel {}",pmmlStr)
		
		val apiResult = MetadataAPIImpl.UpdateModel(pmmlStr)
		
		requestContext.complete(apiResult)
	}
}
