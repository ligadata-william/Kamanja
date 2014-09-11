package com.ligadata.metadataapiservice

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.io.IO

import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport
import spray.client.pipelining._

import scala.util.{ Success, Failure }

import com.ligadata.MetadataAPI._

object GetMessageService {
  case class Process(nameSpace:String, name:String, version:String)
}

class GetMessageService(requestContext: RequestContext) extends Actor {

  import GetMessageService._

  implicit val system = context.system
  import system.dispatcher
  val log = Logging(system, getClass)

  def receive = {
    case Process(nameSpace,name,version) =>
      process(nameSpace,name,version)
      context.stop(self)
  }

  def process(nameSpace:String, name:String, version:String) = { 

    log.info("Requesting GetMessage {},{},{}",nameSpace,name,version)

    val apiResult = MetadataAPIImpl.GetMessageDefFromCache(nameSpace,name,"JSON",version)
    
    requestContext.complete(apiResult)
  }
}
