package com.ligadata.metadataapiservice

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.io.IO

import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport
import spray.client.pipelining._

import scala.util.{ Success, Failure }

import com.ligadata.MetadataAPI._

object AddModelService {
	case class Process(pmmlStr:String)
}

class AddModelService(requestContext: RequestContext) extends Actor {

	import AddModelService._
	
	implicit val system = context.system
	import system.dispatcher
	val log = Logging(system, getClass)
	
	def receive = {
		case Process(pmmlStr) =>
			process(pmmlStr)
			context.stop(self)
	}
	
	def process(pmmlStr:String) = {
	  
		log.info("Requesting AddModel {}",pmmlStr)
		
		val apiResult = MetadataAPIImpl.AddModel(pmmlStr)
		
		requestContext.complete(apiResult)
	}
}
