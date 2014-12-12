package com.ligadata.metadataapiservice

import akka.actor.{ Actor, ActorRef }
import akka.event.Logging
import akka.io.IO
import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport
import spray.client.pipelining._
import scala.util.{ Success, Failure }
import com.ligadata.MetadataAPI._

object RemoveModelService {
  case class Process(modelName:String, version :String)
}

class RemoveModelService(requestContext: RequestContext) extends Actor {

  import RemoveModelService._

  implicit val system = context.system
  import system.dispatcher
  val log = Logging(system, getClass)

  def receive = {
  case Process(modelName,version) =>
      process(modelName, version)
      context.stop(self)
  }

  def process(modelName:String, version: String) = {
    log.info("Requesting RemoveModel {},{}", modelName, version)
    if (MetadataAPIServiceLeader.IsLeader == true) {
      val apiResult = MetadataAPIImpl.RemoveModel(modelName, Integer.parseInt(version))
      requestContext.complete(apiResult)
    } else {
      val apiResult = new ApiResult(-1, "Failed to execute the request, I am not the leader node",
        "Please execute the request on the leader node " + MetadataAPIServiceLeader.LeaderNode)
      requestContext.complete(apiResult.toString())
    }
  }
}