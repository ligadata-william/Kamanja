package com.ligadata.metadataapiservice

import akka.actor.{ Actor, Props }
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
      (get & path("GetModel" / Segment / Segment / Segment / Segment)) { (nameSpace, name, formatType, version) =>
        requestContext =>
          val getModelService = actorRefFactory.actorOf(Props(new GetModelService(requestContext)))
          getModelService ! GetModelService.Process(nameSpace, name, formatType, version)
      } ~
        (put & path("AddModel")) {
          entity(as[String]) { pmmlStr =>
            requestContext =>
              val addModelService = actorRefFactory.actorOf(Props(new AddModelService(requestContext)))
              addModelService ! AddModelService.Process(pmmlStr)
          }
        } ~
        (put & path("RemoveModel" / Segment / Segment / Segment)) { (nameSpace, name, version) =>
          requestContext =>
            val removeModelService = actorRefFactory.actorOf(Props(new RemoveModelService(requestContext)))
            removeModelService ! RemoveModelService.Process(nameSpace, name, version)
        } ~
        (put & path("UpdateModel")) {
          entity(as[String]) { pmmlStr =>
            requestContext =>
              val updateModelService = actorRefFactory.actorOf(Props(new UpdateModelService(requestContext)))
              updateModelService ! UpdateModelService.Process(pmmlStr)
          }
        } ~
        (get & path("GetModelDef" / Segment / Segment / Segment / Segment)) { (nameSpace, objectName, formatType, version) =>
          requestContext =>
            val modelDefService = actorRefFactory.actorOf(Props(new GetModelService(requestContext)))
            modelDefService ! GetModelService.Process(nameSpace, objectName, formatType, version)
        } ~
        (get & path("GetAllModelDefs" / Segment)) { (formatType) =>
          requestContext =>
            val allModelDefsService = actorRefFactory.actorOf(Props(new GetAllModelDefsService(requestContext)))
            allModelDefsService ! GetAllModelDefsService.Process(formatType)
        } ~
        (get & path("GetAllMessageDefs" / Segment)) { (formatType) =>
          requestContext =>
            val allMessageDefsService = actorRefFactory.actorOf(Props(new GetAllMessageDefsService(requestContext)))
            allMessageDefsService ! GetAllMessageDefsService.Process(formatType)
        } ~
        (get & path("GetMessageDef" / Segment / Segment)) { (objectName, formatType) =>
          requestContext =>
            val getMessageService = actorRefFactory.actorOf(Props(new GetMessageService(requestContext)))
            getMessageService ! GetMessageService.ProcessAll(objectName, formatType)
        } ~
        (get & path("GetMessageDef" / Segment / Segment / Segment)) { (objectName, version, formatType) =>
          requestContext =>
            val getMessageService = actorRefFactory.actorOf(Props(new GetMessageService(requestContext)))
            getMessageService ! GetMessageService.Process(objectName, version, formatType)
        } ~
        (put & path("AddMessageDef" / Segment )) { (formatType) =>
          entity(as[String]) { (messageJson) =>
            requestContext =>
              val allMessageDefsService = actorRefFactory.actorOf(Props(new AddMessageService(requestContext)))
              allMessageDefsService ! AddMessageService.Process(messageJson, formatType)
          }
        } ~
        (put & path("UpdateMessage" / Segment )) { (formatType) =>
          entity(as[String]) { (messageJson) =>
            requestContext =>
              val allMessageDefsService = actorRefFactory.actorOf(Props(new AddMessageService(requestContext)))
              allMessageDefsService ! AddMessageService.Process(messageJson, formatType)
          }
        } ~
        (delete & path("RemoveMessage" / Segment / Segment)) { (messageName,version) =>
            requestContext =>
              val removeMessageService = actorRefFactory.actorOf(Props(new RemoveMessageService(requestContext)))
              removeMessageService ! RemoveMessageService.Process(messageName, version)
        }
    }
}
