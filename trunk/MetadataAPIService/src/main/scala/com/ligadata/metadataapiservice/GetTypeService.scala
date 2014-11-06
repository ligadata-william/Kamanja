package com.ligadata.metadataapiservice

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.io.IO

import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport
import spray.client.pipelining._

import scala.util.{ Success, Failure }

import com.ligadata.MetadataAPI._

object GetTypeService {
	case class Process(objectName:String)
}

class GetTypeService(requestContext: RequestContext) extends Actor {

	import GetTypeService._
	
	implicit val system = context.system
	import system.dispatcher
	val log = Logging(system, getClass)
	
	def receive = {
		case Process(objectName) =>
			process(objectName)
			context.stop(self)
	}
	
	def process(objectName:String) = {
	
		log.info("Requesting GetType {}",objectName)
		
		val apiResult = MetadataAPIImpl.GetType(objectName,"JSON")
		
		requestContext.complete(apiResult)
	}
}
