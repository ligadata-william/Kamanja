package com.ligadata.metadataapiservice

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.io.IO

import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport
import spray.client.pipelining._

import scala.util.{ Success, Failure }

import com.ligadata.MetadataAPI._

object AddFunctionService {
	case class Process(functionJson:String, formatType:String)
}

class AddFunctionService(requestContext: RequestContext, userid:Option[String], password:Option[String], cert:Option[String]) extends Actor {

	import AddFunctionService._
	
	implicit val system = context.system
	import system.dispatcher
	val log = Logging(system, getClass)
	
	def receive = {
		case Process(functionJson, formatType) =>
			process(functionJson, formatType)
			context.stop(self)
	}
	
	def process(functionJson:String, formatType:String) = {
	  
		log.info("Requesting AddFunction {},{}",functionJson,formatType)
		
		val apiResult = MetadataAPIImpl.AddFunctions(functionJson,formatType)
		
		requestContext.complete(apiResult)
	}
}
