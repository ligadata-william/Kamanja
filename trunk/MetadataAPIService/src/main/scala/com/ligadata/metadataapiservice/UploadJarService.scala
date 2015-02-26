package com.ligadata.metadataapiservice

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.io.IO

import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport
import spray.client.pipelining._

import scala.util.{ Success, Failure }

import com.ligadata.MetadataAPI._

object UploadJarService {
  case class Process(jarName:String,byteArray:Array[Byte])
}

class UploadJarService(requestContext: RequestContext) extends Actor {

  import UploadJarService._
  
  implicit val system = context.system
  import system.dispatcher
  val log = Logging(system, getClass)
  
  def receive = {
    case Process(jarName,byteArray) =>
      process(jarName,byteArray)
      context.stop(self)
  }
  
  def process(jarName:String,byteArray:Array[Byte]) = {
    
    log.info("Requesting UploadJar {}",jarName)
    
    val apiResult = MetadataAPIImpl.UploadJarToDB(jarName,byteArray)
    
    requestContext.complete(apiResult)
  }
}
