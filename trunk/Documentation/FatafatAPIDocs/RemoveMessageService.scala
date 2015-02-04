package com.ligadata.metadataapiservice

import akka.actor.{ Actor, ActorRef }
import akka.event.Logging
import akka.io.IO

import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport
import spray.client.pipelining._

import scala.util.{ Success, Failure }

import com.ligadata.MetadataAPI._

object RemoveMessageService {
  case class Process(messageName :String, version :String)
}

class RemoveMessageService(requestContext: RequestContext) extends Actor {

  import RemoveMessageService._

  implicit val system = context.system
  import system.dispatcher
  val log = Logging(system, getClass)

  def receive = {
  case Process(messageName,version) =>
      process(messageName, version)
      context.stop(self)
  }

  def process(messageName: String, version: String) = {
    log.info("Requesting GetMessage {},{}", messageName, version)
    if (MetadataAPIServiceLeader.IsLeader == true) {
      val apiResult = MetadataAPIImpl.RemoveMessage(messageName, Integer.parseInt(version))
      requestContext.complete(apiResult)
    } else {
      val apiResult = new ApiResult(-1, "Failed to execute the request, I am not the leader node",
        "Please execute the request on the leader node " + MetadataAPIServiceLeader.LeaderNode)
      requestContext.complete(apiResult.toString())
    }
  }
}
