package com.ligadata.metadataapiservice

import akka.actor.{ Actor, ActorRef }
import akka.event.Logging
import akka.io.IO

import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport
import spray.client.pipelining._

import scala.util.{ Success, Failure }

import com.ligadata.MetadataAPI._

object GetMessageService {
  case class Process(objectName: String, version :String , formatType: String)
  case class ProcessAll(objectName: String, formatType: String)
}

class GetMessageService(requestContext: RequestContext) extends Actor {

  import GetMessageService._

  implicit val system = context.system
  import system.dispatcher
  val log = Logging(system, getClass)
  val APIName = "GetMessageService"

  def receive = {
  case Process(objectName, version, formatType) =>
      process(objectName, version, formatType)
      context.stop(self)
  
  case ProcessAll(objectName, formatType) =>
      process(objectName, formatType)
      context.stop(self)
  }

  def process(objectName: String, formatType: String) = {
    System.out.println(objectName,formatType)
    log.info("Requesting GetMessage {},{}", objectName, formatType)
    if (MetadataAPIServiceLeader.IsLeader == true) {
      val apiResult = MetadataAPIImpl.GetMessageDef(objectName, formatType)
      requestContext.complete(apiResult)
    } else {
      val apiResult = new ApiResult(-1,  APIName, null, "Failed to execute the request, I am not the leader node." + 
        "Please execute the request on the leader node " + MetadataAPIServiceLeader.LeaderNode)
      requestContext.complete(apiResult.toString())
    }
  }
  
  def process(objectName: String, version : String, formatType: String) = {
    log.info("Requesting GetMessage {},{},{}", objectName, version, formatType)
    if (MetadataAPIServiceLeader.IsLeader == true) {
      val apiResult = MetadataAPIImpl.GetMessageDef(objectName, version, formatType)
      requestContext.complete(apiResult)
    } else {
      val apiResult = new ApiResult(-1,  APIName, null, "Failed to execute the request, I am not the leader node." + 
        "Please execute the request on the leader node " + MetadataAPIServiceLeader.LeaderNode)
      requestContext.complete(apiResult.toString())
    }
  }
}
