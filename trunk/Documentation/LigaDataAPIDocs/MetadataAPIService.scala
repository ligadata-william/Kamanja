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

  APIInit.Init

  val metadataAPIRoute =
    pathPrefix("api") {
      (get & path("GetModel" / Segment / Segment / Segment)) { (nameSpace,name,version) =>
        requestContext =>
          val getModelService = actorRefFactory.actorOf(Props(new GetModelService(requestContext)))
          getModelService ! GetModelService.Process(nameSpace,name,version)
      } ~
      (get & path("GetMessage" / Segment / Segment / Segment)) { (nameSpace,name,version) =>
        requestContext =>
          val getMessageService = actorRefFactory.actorOf(Props(new GetMessageService(requestContext)))
          getMessageService ! GetMessageService.Process(nameSpace,name,version)
      } ~
      (get & path("GetType" / Segment )) { (objectName) =>
        requestContext =>
          val getTypeService = actorRefFactory.actorOf(Props(new GetTypeService(requestContext)))
          getTypeService ! GetTypeService.Process(objectName)
      } ~
      (put & path("AddModel")) { 
        entity(as[String]) { pmmlStr => 
          requestContext =>
            val addModelService = actorRefFactory.actorOf(Props(new AddModelService(requestContext)))
            addModelService ! AddModelService.Process(pmmlStr)
        }
      }
    }
}
