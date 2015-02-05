package com.ligadata.metadataapiservice

import akka.actor._
import akka.event.Logging
import spray.routing._
import spray.http._
import MediaTypes._
import org.apache.log4j._
import com.ligadata.Serialize._


class MetadataAPIServiceActor extends Actor with MetadataAPIService {

  def actorRefFactory = context

  def receive = runRoute(metadataAPIRoute)
}

trait MetadataAPIService extends HttpService {

  APIInit.Init
  val KEY_TOKN = "keys"

  val loggerName = this.getClass.getName
  val logger = Logger.getLogger(loggerName)
  logger.setLevel(Level.TRACE);

  val metadataAPIRoute = {
    get {     
      path("api" / Rest) {str => 
        {      
          val toknRoute = str.split("/") 
          logger.info(str + " => "+ toknRoute(0))
          if (toknRoute.size == 1) {
            requestContext =>  processGetObjectRequest(toknRoute(0),"",requestContext)
          } else if (toknRoute(0).equalsIgnoreCase(KEY_TOKN)) {
            requestContext =>  processGetKeysRequest(toknRoute(1),requestContext)
          } else { 
            requestContext => processGetObjectRequest(toknRoute(0), toknRoute(1), requestContext)
          }       
        } 
      } 
    } ~
    put {
       path("api" / Rest) {str =>
         {
            logger.info("PUT: " + str)
            val toknRoute = str.split("/")
            if(toknRoute(0).equalsIgnoreCase("UploadJars")) {
               entity(as[Array[Byte]]) { reqBody => {
                 parameters('name) {jarName => 
                   {
                     logger.info("Uploading jar "+ jarName)
                     requestContext => 
                       val uploadJarService = actorRefFactory.actorOf(Props(new UploadJarService(requestContext)))
                       uploadJarService ! UploadJarService.Process(jarName,reqBody)
                   }   
                 }
                 }
               }
            } else {
              entity(as[String]) { reqBody =>  
                {
                  if (toknRoute.size == 1) {      
                    requestContext => processPutRequest (toknRoute(0),reqBody,requestContext)
                  } else {
                    requestContext => processPutRequest (toknRoute(1),reqBody,requestContext)
                  }             
                }
              }
            }
         }
       }
    } ~
    post {
      entity(as[String]) { reqBody =>
        path("api" / Rest) {str => 
          {
            val toknRoute = str.split("/") 
            logger.info ("POST REQUEST: "+str)
            requestContext => processPutRequest(toknRoute(0), reqBody, requestContext)
          }
        }
      }
    } ~
    delete {
      entity(as[String]) { reqBody =>
        path("api" / Rest) { str =>
          {
            val toknRoute = str.split("/")
            logger.info("DELETE "+str)
            requestContext => processDeleteRequest(toknRoute(0), toknRoute(1), requestContext)   
          }
        }
      }
    }
  }
  
  /**
   * 
   */
  private def processPutRequest(objtype:String, body: String, rContext: RequestContext):Unit = {
    if (objtype.equalsIgnoreCase("Container")) {
        val addContainerDefsService = actorRefFactory.actorOf(Props(new AddContainerService(rContext)))
        addContainerDefsService ! AddContainerService.Process(body)
    } else if (objtype.equalsIgnoreCase("Model")) {
        val addModelService:ActorRef = actorRefFactory.actorOf(Props(new AddModelService(rContext)))
        addModelService ! AddModelService.Process(body)
    } else if (objtype.equalsIgnoreCase("Message")) {
        val addMessageDefsService = actorRefFactory.actorOf(Props(new AddMessageService(rContext)))
        addMessageDefsService ! AddMessageService.Process(body)
    } else if (objtype.equalsIgnoreCase("AddType")) {
        val addTypeDefsService = actorRefFactory.actorOf(Props(new AddTypeService(rContext)))
        addTypeDefsService ! AddTypeService.Process(body,"JSON")
    } else if (objtype.equalsIgnoreCase("Concept")) {
        val addConceptDefsService = actorRefFactory.actorOf(Props(new AddConceptService(rContext)))
        addConceptDefsService ! AddConceptService.Process(body,"JSON")
    } else if (objtype.equalsIgnoreCase("Function")) {
        val addFunctionDefsService = actorRefFactory.actorOf(Props(new AddFunctionService(rContext)))
        addFunctionDefsService ! AddFunctionService.Process(body,"JSON")
    } else if (objtype.equalsIgnoreCase("RemoveConfig")) {
        val removeConfigService = actorRefFactory.actorOf(Props(new RemoveEngineConfigService(rContext)))
        removeConfigService ! RemoveEngineConfigService.Process(body)
    } else if (objtype.equalsIgnoreCase("Activate")) {
        val activateObjectsService = actorRefFactory.actorOf(Props(new ActivateObjectsService(rContext)))
        activateObjectsService ! ActivateObjectsService.Process(body)
    } else if (objtype.equalsIgnoreCase("Deactivate")) {
       val deactivateObjectsService = actorRefFactory.actorOf(Props(new DeactivateObjectsService(rContext)))
       deactivateObjectsService ! DeactivateObjectsService.Process(body)
    }
  }
  
  /**
   * 
   */
  private def processGetKeysRequest(objtype:String,rContext: RequestContext): Unit = {
    val allObjectKeysService = actorRefFactory.actorOf(Props(new GetAllObjectKeysService(rContext)))
    allObjectKeysService ! GetAllObjectKeysService.Process(objtype)        
  }
  
  /**
   *  
   */
  private def processGetObjectRequest(objtype: String, objKey: String, rContext: RequestContext): Unit = {
    if (objtype.equalsIgnoreCase("GetConfigObjects")) {
        val allObjectsService = actorRefFactory.actorOf(Props(new GetConfigObjectsService(rContext)))
        allObjectsService ! GetConfigObjectsService.Process(objKey) 
    } else {
        val getObjectsService = actorRefFactory.actorOf(Props(new GetObjectsService(rContext)))
        getObjectsService ! GetObjectsService.Process(createGetArg(objKey,objtype))  
    }
  }
  
  /**
   * 
   */
  private def processDeleteRequest(objtype: String, objKey: String, rContext: RequestContext):Unit = {
    if (objtype.equalsIgnoreCase("Container") ) {
      println(createGetArg(objKey,objtype))
      val removeObjectsService = actorRefFactory.actorOf(Props(new RemoveObjectsService(rContext)))
      removeObjectsService ! RemoveObjectsService.Process(createGetArg(objKey,objtype))
    } else {
      val removeConfigService = actorRefFactory.actorOf(Props(new RemoveEngineConfigService(rContext)))
      removeConfigService ! RemoveEngineConfigService.Process(createGetArg(objKey,objtype))   
    }
  }
  
  /**
   * MakeJsonStrForArgList
   */
  private def createGetArg(objKey:String,objectType:String): String = {
    try{
      val keyTokens = objKey.split("\\.")
      val nameSpace = keyTokens(0)
      val name = keyTokens(1)
      val version = keyTokens(2)
      val mdArg = new MetadataApiArg(objectType,nameSpace,name,version,"JSON")
      val argList = new Array[MetadataApiArg](1)
      argList(0) = mdArg
      val mdArgList = new MetadataApiArgList(argList.toList)
      val apiArgJson = JsonSerializer.SerializeApiArgListToJson(mdArgList)
      apiArgJson
    }catch {
      case e: Exception => {
        e.printStackTrace()
        throw new Exception("Failed to convert given object key into json string" + e.getMessage())
      }
    }
  }
}