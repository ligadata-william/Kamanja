package com.ligadata.metadataapiservice

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.io.IO
import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport
import spray.client.pipelining._
import scala.util.{ Success, Failure }
import com.ligadata.MetadataAPI._

object UpdateModelService {
  case class Process(pmmlStr:String)
}

class UpdateModelService(requestContext: RequestContext, userid:Option[String], password:Option[String], cert:Option[String]) extends Actor {

  import UpdateModelService._
  
  implicit val system = context.system
  import system.dispatcher
  val log = Logging(system, getClass)
  
  def receive = {
    case Process(pmmlStr) =>
      process(pmmlStr)
      context.stop(self)
  }
  
  def process(pmmlStr:String) = {
    
    log.info("Requesting UpdateModel {}",pmmlStr)

    val objectName = pmmlStr.substring(0,100)

    if (!MetadataAPIImpl.checkAuth(userid,password,cert, MetadataAPIImpl.getPrivilegeName("update","model"))) {
      MetadataAPIImpl.logAuditRec(userid,Some("update"),"UpdateModel",objectName,"Failed","unknown","UPDATE not allowed for this user") 
      requestContext.complete(new ApiResult(-1,"Security","UPDATE not allowed for this user").toString )
    }
    
    val apiResult = MetadataAPIImpl.UpdateModel(pmmlStr)
    MetadataAPIImpl.logAuditRec(userid,Some("update"),"UpdateModel",objectName,"Finished","unknown",apiResult)    
    requestContext.complete(apiResult)
  }
}
