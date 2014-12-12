package com.ligadata.metadataapiservice

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.io.IO
import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport
import spray.client.pipelining._
import scala.util.{ Success, Failure }
import com.ligadata.MetadataAPI._

object GetModelDefService {
	case class ProcessAll(objectName:String, formatType:String)
	case class Process(nameSpace:String, objectName:String, formatType:String)
}

class GetModelDefService(requestContext: RequestContext) extends Actor {

	import GetModelDefService._
	
	implicit val system = context.system
	import system.dispatcher
	val log = Logging(system, getClass)
	
	def receive = {
		case ProcessAll(objectName, formatType) =>
			process(objectName, formatType)
			context.stop(self)
			
		case Process(nameSpace, objectName, formatType) =>
			process(nameSpace, objectName, formatType)
			context.stop(self)
	}
	
	def process(objectName:String, formatType:String) = {
	  
		log.info("Requesting GetModelDef {} {}",objectName, formatType)
		
		val apiResult = MetadataAPIImpl.GetModelDef(objectName, formatType)
		
		requestContext.complete(apiResult)
	}
	
	def process(nameSpace:String, objectName:String, formatType:String) = {
	  
		log.info("Requesting GetModelDef {} {} {}",nameSpace, objectName, formatType)
		
		val apiResult = MetadataAPIImpl.GetModelDef(nameSpace, objectName, formatType)
		
		requestContext.complete(apiResult)
	}
}
