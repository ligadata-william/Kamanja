package com.ligadata.metadataapiservice

import akka.actor.{Actor, Props}
import akka.event.Logging
import spray.routing._
import spray.http._
import MediaTypes._

class MetadataAPIServiceActor extends Actor with MetadataAPIService {
  
  def actorRefFactory = context

  def receive = runRoute(metadataAPIRoute)
}

trait MetadataAPIService extends HttpService {
  val metadataAPIRoute =
    pathPrefix("api") {
      path("GetModel" / Segment / Segment / Segment) { (nameSpace,name,version) =>
        requestContext =>
          val getModelService = actorRefFactory.actorOf(Props(new GetModelService(requestContext)))
          getModelService ! GetModelService.Process(nameSpace,name,version)
      } ~
      path("GetMessage" / Segment / Segment / Segment) { (nameSpace,name,version) =>
        requestContext =>
          val getMessageService = actorRefFactory.actorOf(Props(new GetMessageService(requestContext)))
          getMessageService ! GetMessageService.Process(nameSpace,name,version)
      }
	  path("GetType" / Segment / Segment) { (objectName, formatType) =>
		requestContext =>
			val getTypeService = actorRefFactory.actorOf(Props(new GetTypeService(requestContext)))
			getTypeService ! GetTypeService.Process(objectName, formatType)
	  }
    }
}
