package com.ligadata.metadataapiservice

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.io.IO

import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport
import spray.client.pipelining._

import scala.util.{ Success, Failure }

import com.ligadata.MetadataAPI._

object RemoveEngineConfigService {
  case class Process(cfgJson:String)
}

class RemoveEngineConfigService(requestContext: RequestContext) extends Actor {

  import RemoveEngineConfigService._
  
  implicit val system = context.system
  import system.dispatcher
  val log = Logging(system, getClass)
  
  def receive = {
    case Process(cfgJson) =>
      process(cfgJson)
      context.stop(self)
  }
  
  def process(cfgJson:String) = {
    
    log.info("Requesting RemoveEngineConfig {}",cfgJson)
    
    val apiResult = MetadataAPIImpl.RemoveConfig(cfgJson)
    
    requestContext.complete(apiResult)
  }
}
