package com.ligadata.metadataapiservice

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.io.IO
import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport
import spray.client.pipelining._
import scala.util.{ Success, Failure }
import com.ligadata.MetadataAPI._

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import scala.util.control._
import org.apache.log4j._

object GetAllTypesByObjTypeService {
  case class Process(formatType:String)
}

class GetAllTypesByObjTypeService(requestContext: RequestContext) extends Actor {

  import GetAllTypesByObjTypeService._
  
  implicit val system = context.system
  import system.dispatcher
  val log = Logging(system, getClass)

  val loggerName = this.getClass.getName
  val logger = Logger.getLogger(loggerName)
  logger.setLevel(Level.TRACE);

  val APIName = "GetAllTypesByObjType"
  
  def receive = {
    case Process(objectType) =>
      process(objectType)
      context.stop(self)
  }
  
  def process(objectType:String) = {
    log.info("Requesting GetAllTypesByObjType {}",objectType)
    val apiResult = MetadataAPIImpl.GetAllTypesByObjType("JSON",objectType)
    requestContext.complete(apiResult)
  }
}


