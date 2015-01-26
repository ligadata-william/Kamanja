package com.ligadata.metadataapiservice

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.io.IO
import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport
import spray.client.pipelining._
import scala.util.{ Success, Failure }
import com.ligadata.MetadataAPI._

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import scala.util.control._
import org.apache.log4j._

object GetConfigObjectsService {
  case class Process(formatType:String)
}

class GetConfigObjectsService(requestContext: RequestContext) extends Actor {

  import GetConfigObjectsService._
  
  implicit val system = context.system
  import system.dispatcher
  val log = Logging(system, getClass)

  val loggerName = this.getClass.getName
  val logger = Logger.getLogger(loggerName)
  logger.setLevel(Level.TRACE);

  val APIName = "GetConfigObjects"

  def GetConfigObjects(objectType:String): String = {
    var apiResult:String = ""

    objectType match {
      case "Node" => {
	apiResult = MetadataAPIImpl.GetAllNodes("JSON")
      }
      case "Cluster" => {
	apiResult = MetadataAPIImpl.GetAllClusters("JSON")
      }
      case "Adapter" => {
	apiResult = MetadataAPIImpl.GetAllAdapters("JSON")
      }
      case "ClusterCfg" => {
	apiResult = MetadataAPIImpl.GetAllClusterCfgs("JSON")
      }
      case "ALL" => {
	apiResult = MetadataAPIImpl.GetAllCfgObjects("JSON")
      }
      case _ => {
	apiResult = "The " + objectType + " is not supported yet "
      }
    }
    val (statusCode,resultData) = MetadataAPIImpl.getApiResult(apiResult)
    resultData
  }

  
  def receive = {
    case Process(objectType) =>
      process(objectType)
      context.stop(self)
  }
  
  def process(objectType:String) = {
    log.info("Requesting GetConfigObjects {}",objectType)
    val apiResult = GetConfigObjects(objectType)
    requestContext.complete(apiResult)
  }
}


