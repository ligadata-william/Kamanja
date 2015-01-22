package com.ligadata.metadataapiservice

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.io.IO

import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport
import spray.client.pipelining._

import scala.util.{ Success, Failure }

import com.ligadata.MetadataAPI._

object UploadEngineConfigService {
  case class Process(cfgJson:String)
}

class UploadEngineConfigService(requestContext: RequestContext) extends Actor {

  import UploadEngineConfigService._
  
  implicit val system = context.system
  import system.dispatcher
  val log = Logging(system, getClass)
  
  def receive = {
    case Process(cfgJson) =>
      process(cfgJson)
      context.stop(self)
  }
  
  def process(cfgJson:String) = {
    
    log.info("Requesting UploadEngineConfig {}",cfgJson)
    
    val apiResult = MetadataAPIImpl.UploadConfig(cfgJson)
    
    requestContext.complete(apiResult)
  }
}
