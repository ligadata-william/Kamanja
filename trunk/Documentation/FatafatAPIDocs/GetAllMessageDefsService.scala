package com.ligadata.metadataapiservice

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.io.IO
import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport
import spray.client.pipelining._
import scala.util.{ Success, Failure }
import com.ligadata.MetadataAPI._

object GetAllMessageDefsService {
	case class Process(formatType:String)
}

class GetAllMessageDefsService(requestContext: RequestContext) extends Actor {

	import GetAllMessageDefsService._
	
	implicit val system = context.system
	import system.dispatcher
	val log = Logging(system, getClass)
	
	def receive = {
		case Process(formatType) =>
			process(formatType)
			context.stop(self)
	}
	
	def process(formatType:String) = {
		log.info("Requesting GetAllModelDefs {}",formatType)
		
		val apiResult = MetadataAPIImpl.GetAllMessageDefs(formatType)
		
		requestContext.complete(apiResult)
	}
}


