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

object GetAllObjectsService {
  case class Process(formatType:String)
}

class GetAllObjectsService(requestContext: RequestContext) extends Actor {

  import GetAllObjectsService._
  
  implicit val system = context.system
  import system.dispatcher
  val log = Logging(system, getClass)

  val loggerName = this.getClass.getName
  val logger = Logger.getLogger(loggerName)
  logger.setLevel(Level.TRACE);

  val APIName = "GetAllObjects"

  def GetAllObjects(objectType:String): String = {
    var apiResult:String = ""

    objectType match {
      case "model" => {
	apiResult = MetadataAPIImpl.GetAllModelDefs("JSON")
      }
      case "message" => {
	apiResult = MetadataAPIImpl.GetAllMessageDefs("JSON")
      }
      case "container" => {
	apiResult = MetadataAPIImpl.GetAllContainerDefs("JSON")
      }
      case "function" => {
	val(fsize,result) = MetadataAPIImpl.GetAllFunctionDefs("JSON")
	apiResult = result
      }
      case "concept" => {
	apiResult = MetadataAPIImpl.GetAllConcepts("JSON")
      }
      case "type" => {
	apiResult = MetadataAPIImpl.GetAllTypes("JSON")
      }
      case "all" => {
	apiResult = MetadataAPIImpl.GetAllModelDefs("JSON") +
		    MetadataAPIImpl.GetAllMessageDefs("JSON") +
		    MetadataAPIImpl.GetAllContainerDefs("JSON") +
		    MetadataAPIImpl.GetAllFunctionDefs("JSON") +
		    MetadataAPIImpl.GetAllConcepts("JSON") +
		    MetadataAPIImpl.GetAllTypes("JSON")
      }
      case "node" => {
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
      case "AllConfigs" => {
	apiResult = MetadataAPIImpl.GetAllCfgObjects("JSON")
      }
      case _ => {
	apiResult = "The " + objectType + " is not supported yet "
      }
    }
    apiResult
  }

  
  def receive = {
    case Process(objectType) =>
      process(objectType)
      context.stop(self)
  }
  
  def process(objectType:String) = {
    log.info("Requesting GetAllObjects {}",objectType)
    val apiResult = GetAllObjects(objectType)
    requestContext.complete(apiResult)
  }
}


