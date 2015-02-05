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

object GetObjectsService {
	case class Process(apiArgListJson: String)
}

class GetObjectsService(requestContext: RequestContext) extends Actor {

  import GetObjectsService._
  
  implicit val system = context.system
  import system.dispatcher
  val log = Logging(system, getClass)
  
  
  val loggerName = this.getClass.getName
  val logger = Logger.getLogger(loggerName)
  logger.setLevel(Level.TRACE);

  val APIName = "GetObjects"

  def receive = {
    case Process(apiArgListJson: String) =>
      process(apiArgListJson)
      context.stop(self)
  }

  def GetObjectDef(arg: MetadataApiArg): String = {
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
      case "Model" => {
	      apiResult = MetadataAPIImpl.GetModelDef(nameSpace,arg.Name,formatType,version)
      }
      case "Message" => {
	      apiResult = MetadataAPIImpl.GetMessageDef(nameSpace,arg.Name,formatType,version)
      }
      case "Container" => {
	      apiResult = MetadataAPIImpl.GetContainerDef(nameSpace,arg.Name,formatType,version)
      }
      case "Function" => {
	      apiResult = MetadataAPIImpl.GetFunctionDef(nameSpace,arg.Name,formatType,version)
      }
      case "Concept" => {
	      apiResult = MetadataAPIImpl.GetConceptDef(nameSpace,arg.Name,formatType,version)
      }
      case "Type" => {
	      apiResult = MetadataAPIImpl.GetTypeDef(nameSpace,arg.Name,formatType,version)
      }
    }
    apiResult
  }

  def process(apiArgListJson: String) = {
    
    logger.trace(APIName + ":" + apiArgListJson)

    val apiArgList = JsonSerializer.parseApiArgList(apiArgListJson)
    val arguments = apiArgList.ArgList
    var resultStr:String = ""

    if ( arguments.length > 0 ){
      var loop = new Breaks
      loop.breakable{
	arguments.foreach(arg => {
	  if(arg.ObjectType == null ){
	    resultStr = APIName + ":Error: The value of object type can't be null"
	    loop.break
	  }
	  if(arg.Name == null ){
	    resultStr = APIName + ":Error: The value of object name can't be null"
	    loop.break
	  }
	  else {
	    resultStr = resultStr + GetObjectDef(arg)
	  }
	})
      }
    }
    else{
      resultStr = APIName + ":No arguments passed to the API, nothing much to do"
    }
    logger.trace("resultStr => " + resultStr)
    requestContext.complete(resultStr)
  }
}