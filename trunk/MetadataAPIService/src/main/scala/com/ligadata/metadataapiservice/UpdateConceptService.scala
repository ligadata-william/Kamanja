package com.ligadata.metadataapiservice

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.io.IO
import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport
import spray.client.pipelining._
import scala.util.{ Success, Failure }
import com.ligadata.MetadataAPI._

object UpdateConceptService {
  case class Process(conceptJson:String)
}

class UpdateConceptService(requestContext: RequestContext, userid:Option[String], password:Option[String], cert:Option[String]) extends Actor {

  import UpdateConceptService._
  
  implicit val system = context.system
  import system.dispatcher
  val log = Logging(system, getClass)
  
  def receive = {
    case Process(conceptJson) =>
      process(conceptJson)
      context.stop(self)
  }
  
  def process(conceptJson:String) = {
    
    log.info("Requesting UpdateConcept {}",conceptJson)
    
    if (!MetadataAPIImpl.checkAuth(userid,password,cert,"write")) {
      requestContext.complete(new ApiResult(-1,"Security","UPDATE not allowed for this user").toString )
    }
    
    val apiResult = MetadataAPIImpl.UpdateConcepts(conceptJson,"JSON")
    
    requestContext.complete(apiResult)
  }
}
