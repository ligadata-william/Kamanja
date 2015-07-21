package com.ligadata.metadataapiservice

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.io.IO
import com.ligadata.fatafat.metadata._
import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport
import spray.client.pipelining._
import org.json4s.jackson.JsonMethods._
import scala.util.{ Success, Failure }

import com.ligadata.MetadataAPI._

object UploadModelConfigService {
  case class Process(cfgJson:String)
}

class UploadModelConfigService(requestContext: RequestContext, userid:Option[String], password:Option[String], cert:Option[String]) extends Actor {

  import UploadModelConfigService._

  implicit val system = context.system
  import system.dispatcher
  val log = Logging(system, getClass)
  val APIName = "UploadModelConfigService"

  def receive = {
    case Process(cfgJson) =>
      log.debug("Received a upload config request by the actor")
      process(cfgJson)
      context.stop(self)
  }

  def process(cfgJson:String) = {
    log.debug("Requesting UploadModelConfig {}", cfgJson)
    val apiResult = MetadataAPIImpl.UploadModelsConfig(cfgJson, userid, null)
    requestContext.complete(apiResult)
  }

}
