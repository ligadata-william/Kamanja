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

object AddModelService {
  case class Process(pmmlStr:String)
}

class AddModelService(requestContext: RequestContext, userid:Option[String], password:Option[String], cert:Option[String]) extends Actor {

  import AddModelService._
  
  implicit val system = context.system
  import system.dispatcher
  val log = Logging(system, getClass)
  val APIName = "AddModelService"
  
  val loggerName = this.getClass.getName
  val logger = Logger.getLogger(loggerName)
  //logger.setLevel(Level.TRACE);

  def receive = {
    case Process(pmmlStr) =>
      process(pmmlStr)
      context.stop(self)
  }
  
  def process(pmmlStr:String) = {
    
    logger.debug("Requesting AddModel: " + pmmlStr.substring(0,500))

    var nameVal = APIService.extractNameFromPMML(pmmlStr) 
    
    if (!MetadataAPIImpl.checkAuth(userid,password,cert, MetadataAPIImpl.getPrivilegeName("insert","model"))) {
	    MetadataAPIImpl.logAuditRec(userid,Some(AuditConstants.WRITE),AuditConstants.INSERTOBJECT,pmmlStr,AuditConstants.FAIL,"",nameVal)    
	    requestContext.complete(new ApiResult(ErrorCodeConstants.Failure, APIName, null,  "Error:UPDATE not allowed for this user").toString )
    } else {
      val apiResult = MetadataAPIImpl.AddModel(pmmlStr,userid) 
      requestContext.complete(apiResult)      
    }
  }
}
