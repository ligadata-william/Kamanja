package com.ligadata.metadataapiservice

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.io.IO

import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport
import spray.client.pipelining._
import com.ligadata.fatafat.metadata._
import scala.util.{ Success, Failure }
import org.json4s.jackson.JsonMethods._
import com.ligadata.MetadataAPI._

object RemoveEngineConfigService {
  case class Process(cfgJson:String)
}

class RemoveEngineConfigService(requestContext: RequestContext, userid:Option[String], password:Option[String], cert:Option[String]) extends Actor {

  import RemoveEngineConfigService._
  
  implicit val system = context.system
  import system.dispatcher
  val log = Logging(system, getClass)
  val APIName = "RemoveEngineConfigService"
  
  def receive = {
    case Process(cfgJson) =>
      process(cfgJson)
      context.stop(self)
  }
  
  def process(cfgJson:String) = { 
    log.info("Requesting RemoveEngineConfig {}",cfgJson)

    var objectList: List[String] = List[String]()

    var inParm: Map[String,Any] = parse(cfgJson).values.asInstanceOf[Map[String,Any]]   
    var args: List[Map[String,String]] = inParm.getOrElse("ArgList",null).asInstanceOf[List[Map[String,String]]]   //.asInstanceOf[List[Map[String,String]]
    args.foreach(elem => {
      objectList :::= List(elem.getOrElse("NameSpace","system")+"."+elem.getOrElse("Name","")+"."+elem.getOrElse("Version","-1"))
    })
        
    if (!SecAuditImpl.checkAuth(userid,password,cert,"write")) {
      SecAuditImpl.logAuditRec(userid,Some(AuditConstants.WRITE),AuditConstants.REMOVECONFIG,cfgJson,AuditConstants.FAIL,"",objectList.mkString(",")) 
      requestContext.complete(new ApiResult(ErrorCodeConstants.Failure, APIName, null, "Error:UPDATE not allowed for this user").toString )
    } else {
      val apiResult = MetadataAPIImpl.RemoveConfig(cfgJson)
      SecAuditImpl.logAuditRec(userid,Some(AuditConstants.WRITE),AuditConstants.REMOVECONFIG,cfgJson,AuditConstants.SUCCESS,"",objectList.mkString(","))    
      requestContext.complete(apiResult)     
    }
  }
}
