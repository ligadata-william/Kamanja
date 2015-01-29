package com.ligadata.metadataapiservice

import akka.actor.{ Actor, Props }
import akka.event.Logging
import spray.routing._
import spray.http._
import MediaTypes._
import org.apache.log4j._

class MetadataAPIServiceActor extends Actor with MetadataAPIService {

  def actorRefFactory = context

  def receive = runRoute(metadataAPIRoute)
}

trait MetadataAPIService extends HttpService {

  APIInit.Init
  val loggerName = this.getClass.getName
  val logger = Logger.getLogger(loggerName)
  logger.setLevel(Level.TRACE);

  val metadataAPIRoute =
    pathPrefix("api") {
      (put & path("AddModel")) {
        entity(as[String]) { pmmlStr =>
          requestContext =>
          val addModelService = actorRefFactory.actorOf(Props(new AddModelService(requestContext)))
          addModelService ! AddModelService.Process(pmmlStr)
        }
      } ~
      (put & path("GetAllObjectKeys")) {
          entity(as[String]) { objectType =>
            requestContext =>
	    logger.trace("invoke GetAllObjectKeysService")
            val allObjectKeysService = actorRefFactory.actorOf(Props(new GetAllObjectKeysService(requestContext)))
            allObjectKeysService ! GetAllObjectKeysService.Process(objectType)
	  }
      } ~
      (put & path("GetAllObjects")) {
          entity(as[String]) { objectType =>
            requestContext =>
            val allObjectsService = actorRefFactory.actorOf(Props(new GetAllObjectsService(requestContext)))
            allObjectsService ! GetAllObjectsService.Process(objectType)
	  }
      } ~
      (put & path("GetObjects")) {
          entity(as[String]) { argListJson =>
            requestContext =>
            val getObjectsService = actorRefFactory.actorOf(Props(new GetObjectsService(requestContext)))
            getObjectsService ! GetObjectsService.Process(argListJson)
	  }
      } ~
      (put & path("GetAllModelDefs")) {
          entity(as[String]) { formatType =>
            requestContext =>
            val allObjectsService = actorRefFactory.actorOf(Props(new GetAllObjectsService(requestContext)))
            allObjectsService ! GetAllObjectsService.Process("Model")
	  }
      } ~
      (put & path("GetModelDef")) {
          entity(as[String]) { argListJson =>
            requestContext =>
            val getObjectsService = actorRefFactory.actorOf(Props(new GetObjectsService(requestContext)))
            getObjectsService ! GetObjectsService.Process(argListJson)
	  }
      } ~
      (put & path("RemoveObjects")) { 
          entity(as[String]) { argListJson =>
            requestContext =>
            val removeObjectsService = actorRefFactory.actorOf(Props(new RemoveObjectsService(requestContext)))
            removeObjectsService ! RemoveObjectsService.Process(argListJson)
	  }
      } ~
      (put & path("RemoveModel")) { 
          entity(as[String]) { argListJson =>
            requestContext =>
            val removeObjectsService = actorRefFactory.actorOf(Props(new RemoveObjectsService(requestContext)))
            removeObjectsService ! RemoveObjectsService.Process(argListJson)
	  }
      } ~
      (put & path("ActivateObjects")) { 
          entity(as[String]) { argListJson =>
            requestContext =>
            val activateObjectsService = actorRefFactory.actorOf(Props(new ActivateObjectsService(requestContext)))
            activateObjectsService ! ActivateObjectsService.Process(argListJson)
	  }
      } ~
      (put & path("DeactivateObjects")) { 
          entity(as[String]) { argListJson =>
            requestContext =>
            val deactivateObjectsService = actorRefFactory.actorOf(Props(new DeactivateObjectsService(requestContext)))
            deactivateObjectsService ! DeactivateObjectsService.Process(argListJson)
	  }
      } ~
      (put & path("UpdateModel")) {
        entity(as[String]) { pmmlStr =>
          requestContext =>
          val updateModelService = actorRefFactory.actorOf(Props(new UpdateModelService(requestContext)))
          updateModelService ! UpdateModelService.Process(pmmlStr)
        }
      } ~
      (put & path("GetAllMessageDefs")) {
          entity(as[String]) { formatType =>
            requestContext =>
            val allObjectsService = actorRefFactory.actorOf(Props(new GetAllObjectsService(requestContext)))
            allObjectsService ! GetAllObjectsService.Process("Message")
	  }
      } ~
      (put & path("GetMessageDef")) {
          entity(as[String]) { argListJson =>
            requestContext =>
            val getObjectsService = actorRefFactory.actorOf(Props(new GetObjectsService(requestContext)))
            getObjectsService ! GetObjectsService.Process(argListJson)
	  }
      } ~
      (put & path("AddMessageDef")) { 
        entity(as[String]) { messageJson =>
          requestContext =>
          val addMessageDefsService = actorRefFactory.actorOf(Props(new AddMessageService(requestContext)))
          addMessageDefsService ! AddMessageService.Process(messageJson)
        }
      } ~
      (put & path("UpdateMessage" )) { 
        entity(as[String]) { messageJson =>
          requestContext =>
          val addMessageDefsService = actorRefFactory.actorOf(Props(new AddMessageService(requestContext)))
          addMessageDefsService ! AddMessageService.Process(messageJson)
        }
      } ~
      (put & path("RemoveMessage")) { 
        entity(as[String]) { argListJson =>
          requestContext =>
          val removeObjectsService = actorRefFactory.actorOf(Props(new RemoveObjectsService(requestContext)))
          removeObjectsService ! RemoveObjectsService.Process(argListJson)
	}
      } ~
      (put & path("GetAllContainerDefs")) {
          entity(as[String]) { formatType =>
            requestContext =>
            val allObjectsService = actorRefFactory.actorOf(Props(new GetAllObjectsService(requestContext)))
            allObjectsService ! GetAllObjectsService.Process("Container")
	  }
      } ~
      (put & path("GetContainerDef")) {
          entity(as[String]) { argListJson =>
            requestContext =>
            val getObjectsService = actorRefFactory.actorOf(Props(new GetObjectsService(requestContext)))
            getObjectsService ! GetObjectsService.Process(argListJson)
	  }
      } ~
      (put & path("AddContainerDef")) { 
        entity(as[String]) { containerJson =>
          requestContext =>
          val addContainerDefsService = actorRefFactory.actorOf(Props(new AddContainerService(requestContext)))
          addContainerDefsService ! AddContainerService.Process(containerJson)
        }
      } ~
      (put & path("UpdateContainer" )) { 
        entity(as[String]) { containerJson =>
          requestContext =>
          val addContainerDefsService = actorRefFactory.actorOf(Props(new AddContainerService(requestContext)))
          addContainerDefsService ! AddContainerService.Process(containerJson)
        }
      } ~
      (put & path("RemoveContainer")) { 
        entity(as[String]) { argListJson =>
          requestContext =>
          val removeObjectsService = actorRefFactory.actorOf(Props(new RemoveObjectsService(requestContext)))
          removeObjectsService ! RemoveObjectsService.Process(argListJson)
	}
      } ~
      (put & path("GetAllFunctions")) {
          entity(as[String]) { formatType =>
            requestContext =>
            val allObjectsService = actorRefFactory.actorOf(Props(new GetAllObjectsService(requestContext)))
            allObjectsService ! GetAllObjectsService.Process("Function")
	  }
      } ~
      (put & path("GetAllConcepts")) {
          entity(as[String]) { formatType =>
            requestContext =>
            val allObjectsService = actorRefFactory.actorOf(Props(new GetAllObjectsService(requestContext)))
            allObjectsService ! GetAllObjectsService.Process("Concept")
	  }
      } ~
      (put & path("AddType")) { 
        entity(as[String]) { typeJson =>
          requestContext =>
          val addTypeDefsService = actorRefFactory.actorOf(Props(new AddTypeService(requestContext)))
          addTypeDefsService ! AddTypeService.Process(typeJson,"JSON")
        }
      } ~
      (put & path("UpdateType" )) { 
        entity(as[String]) { typeJson =>
          requestContext =>
          val addTypeDefsService = actorRefFactory.actorOf(Props(new AddTypeService(requestContext)))
          addTypeDefsService ! AddTypeService.Process(typeJson,"JSON")
        }
      } ~
      (put & path("AddConcept")) { 
        entity(as[String]) { conceptJson =>
          requestContext =>
          val addConceptDefsService = actorRefFactory.actorOf(Props(new AddConceptService(requestContext)))
          addConceptDefsService ! AddConceptService.Process(conceptJson,"JSON")
        }
      } ~
      (put & path("UpdateConcept" )) { 
        entity(as[String]) { conceptJson =>
          requestContext =>
          val addConceptDefsService = actorRefFactory.actorOf(Props(new AddConceptService(requestContext)))
          addConceptDefsService ! AddConceptService.Process(conceptJson,"JSON")
        }
      } ~
      (put & path("AddFunction")) { 
        entity(as[String]) { functionJson =>
          requestContext =>
          val addFunctionDefsService = actorRefFactory.actorOf(Props(new AddFunctionService(requestContext)))
          addFunctionDefsService ! AddFunctionService.Process(functionJson,"JSON")
        }
      } ~
      (put & path("UpdateFunction" )) { 
        entity(as[String]) { functionJson =>
          requestContext =>
          val addFunctionDefsService = actorRefFactory.actorOf(Props(new AddFunctionService(requestContext)))
          addFunctionDefsService ! AddFunctionService.Process(functionJson,"JSON")
        }
      } ~
      (put & path("GetAllTypes")) {
          entity(as[String]) { formatType =>
            requestContext =>
            val allObjectsService = actorRefFactory.actorOf(Props(new GetAllObjectsService(requestContext)))
            allObjectsService ! GetAllObjectsService.Process("Type")
	  }
      } ~
      (put & path("GetAllTypesByObjType")) {
          entity(as[String]) { objectType =>
            requestContext =>
            val allTypesService = actorRefFactory.actorOf(Props(new GetAllTypesByObjTypeService(requestContext)))
            allTypesService ! GetAllTypesByObjTypeService.Process(objectType)
	  }
      } ~
      (put & path("GetConfigObjects")) {
          entity(as[String]) { objectType =>
            requestContext =>
            val allObjectsService = actorRefFactory.actorOf(Props(new GetConfigObjectsService(requestContext)))
            allObjectsService ! GetConfigObjectsService.Process(objectType)
	  }
      } ~
      (put & path("UploadConfig")) {
          entity(as[String]) { configJson =>
            requestContext =>
            val uploadConfigService = actorRefFactory.actorOf(Props(new UploadEngineConfigService(requestContext)))
            uploadConfigService ! UploadEngineConfigService.Process(configJson)
	  }
      } ~
      (put & path("RemoveConfig")) {
          entity(as[String]) { configJson =>
            requestContext =>
            val removeConfigService = actorRefFactory.actorOf(Props(new RemoveEngineConfigService(requestContext)))
            removeConfigService ! RemoveEngineConfigService.Process(configJson)
	  }
      } ~
      // Must have the name of the Jar file in the Name parameter
      (put & path("UploadJar")) {
        parameters('name) {jarName => 
          entity(as[Array[Byte]]) { byteArray =>
            requestContext =>
            val uploadJarService = actorRefFactory.actorOf(Props(new UploadJarService(requestContext)))
            uploadJarService ! UploadJarService.Process(jarName,byteArray)
          }          
        }
      }
    }
}
