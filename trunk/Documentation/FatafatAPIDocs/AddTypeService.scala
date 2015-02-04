package com.ligadata.metadataapiservice

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.io.IO

import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport
import spray.client.pipelining._

import scala.util.{ Success, Failure }

import com.ligadata.MetadataAPI._

object AddTypeService {
	case class Process(typeJson:String, formatType:String)
}

class AddTypeService(requestContext: RequestContext) extends Actor {

	import AddTypeService._
	
	implicit val system = context.system
	import system.dispatcher
	val log = Logging(system, getClass)
	
	def receive = {
		case Process(typeJson, formatType) =>
			process(typeJson, formatType)
			context.stop(self)
	}
	
	def process(typeJson:String, formatType:String) = {
	  
		log.info("Requesting AddType {},{}",typeJson.substring(1,200) + " .....",formatType)
		
		val apiResult = MetadataAPIImpl.AddTypes(typeJson,formatType)
		
		requestContext.complete(apiResult)
	}
}
