package com.ligadata.metadataapiservice

import akka.actor.{ Actor, ActorRef }
import akka.event.Logging
import akka.io.IO
import com.ligadata.fatafat.metadata._
import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport
import spray.client.pipelining._

import scala.util.{ Success, Failure }

import com.ligadata.MetadataAPI._

object UpdateOutputMsgService {
  case class Process(outputMsgJson: String, formatType: String)
}

class UpdateOutputMsgService(requestContext: RequestContext, userid: Option[String], password: Option[String], cert: Option[String]) extends Actor {

  import UpdateOutputMsgService._

  implicit val system = context.system
  import system.dispatcher
  val log = Logging(system, getClass)
  val APIName = "UpdateOutputMsgService"

  def receive = {
    case Process(outputMsgJson, formatType) =>
      process(outputMsgJson, formatType)
      context.stop(self)
  }

  def process(outputMsgJson: String, formatType: String): Unit = {

    log.info("Requesting Update {},{}", outputMsgJson, formatType)

    var nameVal: String = null
    if (formatType.equalsIgnoreCase("json")) {
      nameVal = APIService.extractNameFromJson(outputMsgJson, AuditConstants.OUTPUTMSG)
    } else {
      requestContext.complete(new ApiResult(ErrorCodeConstants.Failure, APIName, null, "Error:Unsupported format: " + formatType).toString)
      return
    }

    if (!MetadataAPIImpl.checkAuth(userid, password, cert, MetadataAPIImpl.getPrivilegeName("update", "outputmsg"))) {
      MetadataAPIImpl.logAuditRec(userid, Some(AuditConstants.WRITE), AuditConstants.UPDATEOBJECT, outputMsgJson, AuditConstants.FAIL, "", nameVal)
      requestContext.complete(new ApiResult(ErrorCodeConstants.Failure, APIName, null, "Error:UPDATE not allowed for this user").toString)
    } else {
      val apiResult = MetadataAPIOutputMsg.UpdateOutputMsg(outputMsgJson, userid)
      requestContext.complete(apiResult)
    }
  }
}
