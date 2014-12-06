package com.ligadata.metadataapiservice

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.io.IO

import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport
import spray.client.pipelining._

import scala.util.{ Success, Failure }

import com.ligadata.MetadataAPI._

object GetModelService {
  case class Process(nameSpace:String, name:String, formatType:String, version:String)
}

class GetModelService(requestContext: RequestContext) extends Actor {

  import GetModelService._

  implicit val system = context.system
  import system.dispatcher
  val log = Logging(system, getClass)

  def receive = {
    case Process(nameSpace,name,formatType,version) =>
      process(nameSpace,name,formatType,version)
      context.stop(self)
  }

  def process(nameSpace:String, name:String, formatType:String, version:String) = { 
    log.info("Requesting GetModel {},{},{}",nameSpace,name,formatType,version)
    if ( MetadataAPIServiceLeader.IsLeader == true ){
      val apiResult = MetadataAPIImpl.GetModelDefFromCache(nameSpace,name,formatType,version)
      requestContext.complete(apiResult)
    }
    else{
      val apiResult = new ApiResult(-1,"Failed to execute the request, I am not the leader node",
				    "Please execute the request on the leader node " + MetadataAPIServiceLeader.LeaderNode)
      requestContext.complete(apiResult.toString())
    }
  }
}
