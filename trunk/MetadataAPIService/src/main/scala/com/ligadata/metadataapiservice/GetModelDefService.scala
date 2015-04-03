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

object GetModelDefService {
	case class Process(apiArgListJson: String)
}

class GetModelDefService(requestContext: RequestContext) extends Actor {

  import GetModelDefService._
  
  implicit val system = context.system
  import system.dispatcher
  val log = Logging(system, getClass)
  
  
  val loggerName = this.getClass.getName
  val logger = Logger.getLogger(loggerName)
 // logger.setLevel(Level.TRACE);

  val APIName = "GetModelDef"

  def receive = {
    case Process(apiArgListJson: String) =>
      process(apiArgListJson)
      context.stop(self)
  }
  
  def process(apiArgListJson: String) = {
    
    logger.debug(APIName + ":" + apiArgListJson)

    val apiArgList = JsonSerializer.parseApiArgList(apiArgListJson)
    val arguments = apiArgList.ArgList
    var resultStr:String = ""

    if ( arguments.length > 0 ){
      var loop = new Breaks
      loop.breakable{
	arguments.foreach(arg => {
	  if(arg.Name == null ){
	    resultStr = APIName + ":Error: The value of object name can't be null"
	    loop.break
	  }
	  else {
	    var nameSpace = "system"
	    if( arg.NameSpace != null ){
	      nameSpace = arg.NameSpace
	    }
	    if( arg.Version != null ){
	      var apiResult = MetadataAPIImpl.GetModelDefFromCache(arg.NameSpace,arg.Name,arg.FormatType,arg.Version)
	      val apiResultStr = MetadataAPIImpl.getApiResult(apiResult)
	      logger.debug("API Result => " + apiResultStr)
	      resultStr = apiResultStr
	    }
	    else{
	      var apiResult = MetadataAPIImpl.GetModelDef(arg.NameSpace,arg.Name,arg.FormatType)
	      val apiResultStr = MetadataAPIImpl.getApiResult(apiResult)
	      logger.debug("API Result => " + apiResultStr)
	      resultStr = apiResultStr
	    }
	  }
	})
      }
    }
    else{
      resultStr = APIName + ":No arguments passed to the API, nothing much to do"
    }
    requestContext.complete(resultStr)
  }
}
