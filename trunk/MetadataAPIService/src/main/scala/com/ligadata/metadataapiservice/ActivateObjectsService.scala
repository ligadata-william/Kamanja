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

object ActivateObjectsService {
	case class Process(apiArgListJson: String)
}

class ActivateObjectsService(requestContext: RequestContext, userid:Option[String], password:Option[String], cert:Option[String]) extends Actor {

  import ActivateObjectsService._
  
  implicit val system = context.system
  import system.dispatcher
  val log = Logging(system, getClass)
  
  
  val loggerName = this.getClass.getName
  val logger = Logger.getLogger(loggerName)
  logger.setLevel(Level.TRACE);

  val APIName = "ActivateObjects"

  def receive = {
    case Process(apiArgListJson: String) =>
      process(apiArgListJson)
      context.stop(self)
  }

  def ActivateObjectDef(arg: MetadataApiArg): String = {
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
	      MetadataAPIImpl.ActivateModel(nameSpace,arg.Name,version.toInt).toString
      }
      case _ => {
	      new ApiResult(-1, APIName, "Deactivate/Activate on " + arg.ObjectType + " is not supported yet").toString 
      }
    }
  }

  def process(apiArgListJson: String) = {
    
    logger.trace(APIName + ":" + apiArgListJson)

    if (!MetadataAPIImpl.checkAuth(userid, password, cert, MetadataAPIImpl.getPrivilegeName("activate","model"))) {
      requestContext.complete(new ApiResult(-1,"Security","UPDATE not allowed for this user").toString )
    }

    val apiArgList = JsonSerializer.parseApiArgList(apiArgListJson)
    val arguments = apiArgList.ArgList
    var resultStr:String = ""

    if ( arguments.length > 0 ){
      var loop = new Breaks
      loop.breakable{
	      arguments.foreach(arg => {
	        if(arg.ObjectType == null ){
	          resultStr =  new ApiResult(-1, APIName, "Error: The value of object type can't be null").toString
	          loop.break
	        }
	        if(arg.Name == null ){
	          resultStr = new ApiResult(-1, APIName, "Error: The value of object name can't be null").toString 
	          loop.break
	        }
	        else {
	          resultStr = resultStr + ActivateObjectDef(arg)
	        }
	      })
      }
    }
    else{
      resultStr = new ApiResult(-1, APIName, "No arguments passed to the API, nothing much to do").toString 
    }
    requestContext.complete(resultStr)
  }
}
