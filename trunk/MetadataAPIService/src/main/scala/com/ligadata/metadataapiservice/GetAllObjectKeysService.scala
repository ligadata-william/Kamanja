package com.ligadata.metadataapiservice

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.io.IO
import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport
import spray.client.pipelining._
import scala.util.{ Success, Failure }
import com.ligadata.MetadataAPI._
import com.ligadata.Serialize._

import scala.util.control._
import org.apache.log4j._

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._


object GetAllObjectKeysService {
  case class Process(formatType:String)
}

class GetAllObjectKeysService(requestContext: RequestContext) extends Actor {

  import GetAllObjectKeysService._
  
  implicit val system = context.system
  import system.dispatcher
  val log = Logging(system, getClass)
  
  val loggerName = this.getClass.getName
  val logger = Logger.getLogger(loggerName)
  logger.setLevel(Level.TRACE);

  val APIName = "GetAllObjectKeys"

  def receive = {
    case Process(objectType) =>
      process(objectType)
      context.stop(self)
  }

  def GetAllObjectKeys(objectType:String): String = {
    var apiResult:Array[String] = new Array[String](0)

    objectType match {
      case "Model" => {
	apiResult = MetadataAPIImpl.GetAllModelsFromCache(false)
      }
      case "Message" => {
	apiResult = MetadataAPIImpl.GetAllMessagesFromCache(true)
      }
      case "Container" => {
	apiResult = MetadataAPIImpl.GetAllContainersFromCache(true)
      }
      case "Function" => {
	apiResult = MetadataAPIImpl.GetAllFunctionsFromCache(true)
      }
      case "Concept" => {
	apiResult = MetadataAPIImpl.GetAllConceptsFromCache(true)
      }
      case "Type" => {
	apiResult = MetadataAPIImpl.GetAllTypesFromCache(true)
      }
      case "ALL" => {
	apiResult = MetadataAPIImpl.GetAllModelsFromCache(true) ++
		    MetadataAPIImpl.GetAllMessagesFromCache(true) ++
		    MetadataAPIImpl.GetAllContainersFromCache(true) ++
		    MetadataAPIImpl.GetAllFunctionsFromCache(true) ++
		    MetadataAPIImpl.GetAllConceptsFromCache(true) ++
		    MetadataAPIImpl.GetAllTypesFromCache(true)
      }
      case _ => {
	apiResult(0) = "The " + objectType + " is not supported yet "
      }
    }
    new ApiResult(0, "Object Keys", apiResult.mkString(",")).toString
  }

  def process(objectType: String) = {
    logger.trace(APIName + ":" + objectType)
    val objectList = GetAllObjectKeys(objectType)
    logger.trace(APIName + "(results):" + objectList)
    requestContext.complete(objectList)
  }
}


