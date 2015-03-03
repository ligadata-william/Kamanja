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

object RemoveObjectsService {
	case class Process(apiArgListJson: String)
}

class RemoveObjectsService(requestContext: RequestContext, userid:Option[String], password:Option[String], cert:Option[String]) extends Actor {

  import RemoveObjectsService._
  
  implicit val system = context.system
  import system.dispatcher
  val log = Logging(system, getClass)
  
  
  val loggerName = this.getClass.getName
  val logger = Logger.getLogger(loggerName)
  logger.setLevel(Level.TRACE);

  val APIName = "RemoveObjects"

  def receive = {
    case Process(apiArgListJson: String) =>
      process(apiArgListJson)
      context.stop(self)
  }

  def RemoveObjectDef(arg: MetadataApiArg): String = {
    var resultStr:String = ""
    var nameSpace = "str"
    var version = "-1"
    var formatType = "JSON"
    var apiResult:String = ""

    if( arg.NameSpace != null ){
      nameSpace = arg.NameSpace
    }
    if( arg.Version != null ){
      version = arg.Version
    }
    if( arg.FormatType != null ){
      formatType = arg.FormatType
    }

    arg.ObjectType match {
      case "model" => {
	      return MetadataAPIImpl.RemoveModel(nameSpace,arg.Name,version.toInt)
      }
      case "message" => {
	      return MetadataAPIImpl.RemoveMessage(nameSpace,arg.Name,version.toInt)
      }
      case "container" => {
	      return MetadataAPIImpl.RemoveContainer(nameSpace,arg.Name,version.toInt)
      }
      case "function" => {
	      return MetadataAPIImpl.RemoveFunction(nameSpace,arg.Name,version.toInt)
      }
      case "concept" => {
	      return MetadataAPIImpl.RemoveConcept(nameSpace,arg.Name,version.toInt)
      }
      case "type" => {
	      return MetadataAPIImpl.RemoveType(nameSpace,arg.Name,version.toInt)
      }
    }
  }

  def process(apiArgListJson: String) = {
    
    logger.trace(APIName + ":" + apiArgListJson)
    
    if (!MetadataAPIImpl.checkAuth(userid,password,cert,"write")) {
      requestContext.complete(new ApiResult(-1,"Security","UPDATE not allowed for this user").toString )
    }

    val apiArgList = JsonSerializer.parseApiArgList(apiArgListJson)
    val arguments = apiArgList.ArgList
    var resultStr:String = ""
    var finalRC: Int = 0
    var deletedObjects: Array[String] = new Array[String](0)
    var finalAPIResult = ""

    if ( arguments.length > 0 ){
      var loop = new Breaks
      loop.breakable{
        arguments.foreach(arg => {
          if(arg.ObjectType == null ) {
            deletedObjects +:= APIName + ":Error: The value of object type can't be null"
            finalRC = -1 
            loop.break
          } else if(arg.Name == null ) {
            deletedObjects +:= APIName + ":Error: The value of object name can't be null"
            finalRC = -1
            loop.break
          } else {
            val iResult = RemoveObjectDef(arg)
            val (iStatusCode,iResultData) = MetadataAPIImpl.getApiResult(iResult)
            if (iStatusCode == 0)  deletedObjects +:= iResultData else finalRC = -1
          }
        })
      }
      finalAPIResult = (new ApiResult(finalRC, "Deleted Objects", deletedObjects.mkString(","))).toString
    }
    else{
      finalAPIResult = (new ApiResult(-1, "Deleted Objects", APIName + ":No arguments passed to the API, nothing much to do")).toString
    }
    requestContext.complete(finalAPIResult)
  }
}
