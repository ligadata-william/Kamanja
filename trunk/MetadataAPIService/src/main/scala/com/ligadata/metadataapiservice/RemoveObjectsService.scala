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
    
    val objectName = (nameSpace + arg.Name + version).toLowerCase
    if (!MetadataAPIImpl.checkAuth(userid,password,cert, MetadataAPIImpl.getPrivilegeName("delete", arg.ObjectType))) {
	      MetadataAPIImpl.logAuditRec(userid,Some("delete"),"RemoveObjects",objectName,"Failed","unknown","DELETE not allowed for this user") 
              requestContext.complete(new ApiResult(-1,"Security","UPDATE not allowed for this user").toString )
    }


    arg.ObjectType match {
      case "model" => {
	      apiResult = MetadataAPIImpl.RemoveModel(nameSpace,arg.Name,version.toInt)
	      MetadataAPIImpl.logAuditRec(userid,Some("delete"),"RemoveModel",objectName,"Finished","unknown",apiResult)
     }
      case "message" => {
	      apiResult = MetadataAPIImpl.RemoveMessage(nameSpace,arg.Name,version.toInt)
	      MetadataAPIImpl.logAuditRec(userid,Some("delete"),"RemoveMessage",objectName,"Finished","unknown",apiResult)
      }
      case "container" => {
	      apiResult = MetadataAPIImpl.RemoveContainer(nameSpace,arg.Name,version.toInt)
	      MetadataAPIImpl.logAuditRec(userid,Some("delete"),"RemoveContainer",objectName,"Finished","unknown",apiResult)
      }
      case "function" => {
	      apiResult = MetadataAPIImpl.RemoveFunction(nameSpace,arg.Name,version.toInt)
	      MetadataAPIImpl.logAuditRec(userid,Some("delete"),"RemoveFunction",objectName,"Finished","unknown",apiResult)
      }
      case "concept" => {
	      apiResult = MetadataAPIImpl.RemoveConcept(nameSpace,arg.Name,version.toInt)
	      MetadataAPIImpl.logAuditRec(userid,Some("delete"),"RemoveConcept",objectName,"Finished","unknown",apiResult)
      }
      case "type" => {
	      apiResult = MetadataAPIImpl.RemoveType(nameSpace,arg.Name,version.toInt)
	      MetadataAPIImpl.logAuditRec(userid,Some("delete"),"RemoveType",objectName,"Finished","unknown",apiResult)
      }
    }
    apiResult
  }

  def process(apiArgListJson: String) = {
    
    logger.trace(APIName + ":" + apiArgListJson)

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
