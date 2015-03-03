package com.ligadata.metadataapiservice

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.io.IO

import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport
import spray.client.pipelining._

import scala.util.{ Success, Failure }

import com.ligadata.MetadataAPI._

object AddMessageService {
  case class Process(messageJson:String)
}

class AddMessageService(requestContext: RequestContext, userid:Option[String], password:Option[String], cert:Option[String]) extends Actor {

  import AddMessageService._
  
  implicit val system = context.system
  import system.dispatcher
  val log = Logging(system, getClass)
  
  def receive = {
    case Process(messageJson) =>
      process(messageJson)
      context.stop(self)
  }
  
  def process(messageJson:String) = {
    
    log.info("Requesting AddMessage {}",messageJson)
    
    val apiResult = MetadataAPIImpl.AddMessage(messageJson)
    
    requestContext.complete(apiResult)
  }
}
