package com.ligadata.metadataapiservice

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.io.IO
import com.ligadata.fatafat.metadata._
import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport
import spray.client.pipelining._
import org.json4s.jackson.JsonMethods._
import scala.util.{ Success, Failure }

import com.ligadata.MetadataAPI._

object UploadEngineConfigService {
  case class Process(cfgJson:String)
}

class UploadEngineConfigService(requestContext: RequestContext, userid:Option[String], password:Option[String], cert:Option[String]) extends Actor {

  import UploadEngineConfigService._
  
  implicit val system = context.system
  import system.dispatcher
  val log = Logging(system, getClass)
  val APIName = "UploadEngineConfigService"
  
  def receive = {
    case Process(cfgJson) =>
      process(cfgJson)
      context.stop(self)
  }
  
  def process(cfgJson:String) = {
    
    log.info("Requesting UploadEngineConfig {}",cfgJson)
    
    var objectList: List[String] = List[String]()

    var inParm: Map[String,Any] = parse(cfgJson).values.asInstanceOf[Map[String,Any]]   
    var args: List[Map[String,String]] = inParm.getOrElse("Clusters",null).asInstanceOf[List[Map[String,String]]]   //.asInstanceOf[List[Map[String,String]]
    args.foreach(elem => {
      objectList :::= List(elem.getOrElse("ClusterId",""))
    })
   
    if (!SecAuditImpl.checkAuth(userid,password,cert, SecAuditImpl.getPrivilegeName("update","configuration"))) {
      SecAuditImpl.logAuditRec(userid,Some(AuditConstants.WRITE),AuditConstants.INSERTCONFIG,cfgJson,AuditConstants.FAIL,"",objectList.mkString(","))
      requestContext.complete(new ApiResult(ErrorCodeConstants.Failure, APIName, null, "Error:UPDATE not allowed for this user").toString )
    } else {
      val apiResult = MetadataAPIImpl.UploadConfig(cfgJson)
      SecAuditImpl.logAuditRec(userid,Some(AuditConstants.WRITE),AuditConstants.INSERTCONFIG,cfgJson,AuditConstants.SUCCESS,"",objectList.mkString(","))            
      requestContext.complete(apiResult)    
    }
  }
}
