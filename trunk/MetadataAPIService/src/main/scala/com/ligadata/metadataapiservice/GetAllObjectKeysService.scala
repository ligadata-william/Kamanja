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

class GetAllObjectKeysService(requestContext: RequestContext, userid:Option[String], password:Option[String], cert:Option[String]) extends Actor {

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
    
    if (!MetadataAPIImpl.checkAuth(userid,password,cert, MetadataAPIImpl.getPrivilegeName("get","keys"))) {
      return new ApiResult(-1,"Security","READ not allowed for this user").toString  
    }

    objectType match {
      case "model" => {
	      apiResult = MetadataAPIImpl.GetAllModelsFromCache(false)
      }
      case "message" => {
	      apiResult = MetadataAPIImpl.GetAllMessagesFromCache(true)
      }
      case "container" => {
	      apiResult = MetadataAPIImpl.GetAllContainersFromCache(true)
      }
      case "function" => {
	      apiResult = MetadataAPIImpl.GetAllFunctionsFromCache(true)
      }
      case "concept" => {
	      apiResult = MetadataAPIImpl.GetAllConceptsFromCache(true)
      }
      case "type" => {
	      apiResult = MetadataAPIImpl.GetAllTypesFromCache(true)
      }
      case "all" => {
	      apiResult = MetadataAPIImpl.GetAllModelsFromCache(true) ++
		      MetadataAPIImpl.GetAllMessagesFromCache(true) ++
		      MetadataAPIImpl.GetAllContainersFromCache(true) ++
		      MetadataAPIImpl.GetAllFunctionsFromCache(true) ++
		      MetadataAPIImpl.GetAllConceptsFromCache(true) ++
		      MetadataAPIImpl.GetAllTypesFromCache(true)
      }
      case _ => {
         apiResult = Array[String]("The " + objectType + " is not supported yet ")
         return new ApiResult(-1,"Invalid URL",apiResult.mkString).toString
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


