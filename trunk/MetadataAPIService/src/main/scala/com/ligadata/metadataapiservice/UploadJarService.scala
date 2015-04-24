package com.ligadata.metadataapiservice

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.io.IO
import com.ligadata.fatafat.metadata._
import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport
import spray.client.pipelining._

import scala.util.{ Success, Failure }

import com.ligadata.MetadataAPI._

object UploadJarService {
  case class Process(jarName:String,byteArray:Array[Byte])
}

class UploadJarService(requestContext: RequestContext, userid:Option[String], password:Option[String], cert:Option[String]) extends Actor {

  import UploadJarService._
  
  implicit val system = context.system
  import system.dispatcher
  val log = Logging(system, getClass)
  val APIName = "UploadJarService"
  
  def receive = {
    case Process(jarName,byteArray) =>
      process(jarName,byteArray)
      context.stop(self)
  }
  
  def process(jarName:String,byteArray:Array[Byte]) = {
    
    log.info("Requesting UploadJar {}",jarName)

    if (!MetadataAPIImpl.checkAuth(userid,password,cert, MetadataAPIImpl.getPrivilegeName("update","jars"))) {
      MetadataAPIImpl.logAuditRec(userid,Some(AuditConstants.WRITE),AuditConstants.INSERTJAR,AuditConstants.JAR,AuditConstants.FAIL,"",jarName)
      requestContext.complete(new ApiResult(-1,"Security", null,"UPDATE not allowed for this user").toString )
    } else {
      val apiResult = MetadataAPIImpl.UploadJarToDB(jarName,byteArray)
      MetadataAPIImpl.logAuditRec(userid,Some(AuditConstants.WRITE),AuditConstants.INSERTJAR,AuditConstants.JAR,AuditConstants.FAIL,"",jarName)            
      requestContext.complete(apiResult.toString)     
    }
  }
}
