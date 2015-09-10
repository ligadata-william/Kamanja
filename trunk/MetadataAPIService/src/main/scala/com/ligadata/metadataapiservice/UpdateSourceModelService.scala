package com.ligadata.metadataapiservice

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.io.IO
import com.ligadata.metadataapiservice.AddSourceModelService.{ProcessScala, ProcessJava}
import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport
import spray.client.pipelining._
import scala.util.{ Success, Failure }
import com.ligadata.MetadataAPI._
import com.ligadata.kamanja.metadata._
import com.ligadata.AuditAdapterInfo.AuditConstants
import scala.util.control._
import org.apache.log4j._

object UpdateSourceModelService {
  case class ProcessJava(sourceCode:String)
  case class ProcessScala(sourceCode:String)
}


class UpdateSourceModelService(requestContext: RequestContext, userid:Option[String], password:Option[String], cert:Option[String], modelname: Option[String]) extends Actor {

  import UpdateModelService._

  implicit val system = context.system
  import system.dispatcher
  val log = Logging(system, getClass)
  val APIName = "UpdateSourceModelService"

  val loggerName = this.getClass.getName
  val logger = Logger.getLogger(loggerName)

  def receive = {
    case ProcessJava(sourceCode) => {
      process(sourceCode)
      context.stop(self)
    }
    case ProcessScala(sourceCode) => {
      process(sourceCode)
      context.stop(self)
    }
  }

  def process(pmmlStr:String) = {

    log.debug("Requesting UpdateSourceModel {}",pmmlStr)

    logger.debug("Requesting AddSourceModel.")
    val usersModelName=userid.getOrElse("")+"."+modelname.getOrElse("")
    logger.debug("user model name is: "+usersModelName)

    if (!MetadataAPIImpl.checkAuth(userid,password,cert, MetadataAPIImpl.getPrivilegeName("update","model"))) {
    //  MetadataAPIImpl.logAuditRec(userid,Some(AuditConstants.WRITE),AuditConstants.UPDATEOBJECT,pmmlStr,AuditConstants.FAIL,"",nameVal)
      requestContext.complete(new ApiResult(ErrorCodeConstants.Failure, APIName, null, "Error:UPDATE not allowed for this user").toString )
    }else if((modelname.getOrElse(""))=="") {
      requestContext.complete(new ApiResult(ErrorCodeConstants.Failure, APIName, null,  "Failed to add model. No model configuration name supplied. Please specify in the header the model configuration name where the key is 'modelname' and the value is the name of the configuration.").toString )
    }
    else {
      val apiResult = MetadataAPIImpl.UpdateModel(pmmlStr,userid)
      requestContext.complete(apiResult)
    }
  }

}
