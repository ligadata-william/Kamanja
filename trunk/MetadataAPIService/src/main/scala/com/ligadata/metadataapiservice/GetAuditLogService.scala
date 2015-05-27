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

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._


object GetAuditLogService {
  case class Process(filterParameters:Array[String])
}

class GetAuditLogService(requestContext: RequestContext, userid:Option[String], password:Option[String], cert:Option[String]) extends Actor {

  import GetAuditLogService._
  
  implicit val system = context.system
  import system.dispatcher
  val log = Logging(system, getClass)
  
  val loggerName = this.getClass.getName
  val logger = Logger.getLogger(loggerName)
//  logger.setLevel(Level.TRACE);

  val APIName = "GetAuditLog"

  def receive = {
    case Process(filterParameters) =>
      process(filterParameters)
      context.stop(self)
  }

  def process(filterParameters: Array[String]) = {
    val auditLog = SecAuditImpl.getAuditRec(filterParameters)
    requestContext.complete(auditLog)
  }
}


