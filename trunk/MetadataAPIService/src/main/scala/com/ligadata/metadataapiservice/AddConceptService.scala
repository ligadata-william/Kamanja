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

class AddConceptService(requestContext: RequestContext) extends Actor {

	import AddConceptService._
	
	implicit val system = context.system
	import system.dispatcher
	val log = Logging(system, getClass)
	
	def receive = {
		case Process(conceptJson, formatType) =>
			process(conceptJson, formatType)
			context.stop(self)
	}
	
	def process(conceptJson:String, formatType:String) = {
	  
		log.info("Requesting AddConcept {},{}",conceptJson,formatType)
		
		val apiResult = MetadataAPIImpl.AddConcepts(conceptJson,formatType)
		
		requestContext.complete(apiResult)
	}
}
