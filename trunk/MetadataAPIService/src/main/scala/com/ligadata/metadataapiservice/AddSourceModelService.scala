package com.ligadata.metadataapiservice

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.io.IO

import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport
import spray.client.pipelining._
import com.ligadata.fatafat.metadata._
import scala.util.{ Success, Failure }

import com.ligadata.MetadataAPI._

import scala.util.control._
import org.apache.log4j._

object AddSourceModelService {
  case class ProcessJava(sourceCode:String)
  case class ProcessScala(sourceCode:String)
}

class AddSourceModelService(requestContext: RequestContext, userid:Option[String], password:Option[String], cert:Option[String]) extends Actor {

  import AddSourceModelService._

  implicit val system = context.system
  import system.dispatcher
  val log = Logging(system, getClass)
  val APIName = "AddSourceModelService"

  val loggerName = this.getClass.getName
  val logger = Logger.getLogger(loggerName)
  //logger.setLevel(Level.TRACE);

  def receive = {
    case ProcessJava(sourceCode) =>{
      processJava(sourceCode)
      context.stop(self)
    }
    case ProcessScala(sourceCode) =>{
      processScala(sourceCode)
      context.stop(self)
    }

  }

  def processJava(sourceCode:String) = {

    logger.debug("Requesting AddSourceModel JAVA.")

    //var nameVal = APIService.extractNameFromPMML(pmmlStr)

    if (!MetadataAPIImpl.checkAuth(userid,password,cert, MetadataAPIImpl.getPrivilegeName("insert","model"))) {
	   // MetadataAPIImpl.logAuditRec(userid,Some(AuditConstants.WRITE),AuditConstants.INSERTOBJECT,sourceCode,AuditConstants.FAIL,"",nameVal)
	    requestContext.complete(new ApiResult(ErrorCodeConstants.Failure, APIName, null,  "Error:UPDATE not allowed for this user").toString )
    } else {
      //def AddModelFromSource(sourceCode: String, sourceLang: String, modelName: String, userid: Option[String]): String = {
      val apiResult = MetadataAPIImpl.AddModelFromSource(sourceCode,"java","LowBalanceAlertModel",userid)
      requestContext.complete(apiResult)
    }
  }

  def processScala(sourceCode:String) = {

    logger.debug("Requesting AddSourceModel SCALA.")

    //var nameVal = APIService.extractNameFromPMML(pmmlStr)

    if (!MetadataAPIImpl.checkAuth(userid,password,cert, MetadataAPIImpl.getPrivilegeName("insert","model"))) {
     // MetadataAPIImpl.logAuditRec(userid,Some(AuditConstants.WRITE),AuditConstants.INSERTOBJECT,sourceCode,AuditConstants.FAIL,"",nameVal)
      requestContext.complete(new ApiResult(ErrorCodeConstants.Failure, APIName, null,  "Error:UPDATE not allowed for this user").toString )
    } else {
      //def AddModelFromSource(sourceCode: String, sourceLang: String, modelName: String, userid: Option[String]): String = {
      val apiResult = MetadataAPIImpl.AddModelFromSource(sourceCode,"scala",".HelloWorld",userid)
      requestContext.complete(apiResult)
    }
  }
}
