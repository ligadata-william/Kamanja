package com.ligadata.metadataapiservice

import akka.actor._
import akka.event.Logging
import spray.routing._
import spray.http._
import MediaTypes._
import org.apache.log4j._
import com.ligadata.MetadataAPI._
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
            requestContext =>  processGetKeysRequest(toknRoute(1).toLowerCase,requestContext)
          } else { 
            requestContext => processGetObjectRequest(toknRoute(0).toLowerCase, toknRoute(1).toLowerCase, requestContext)
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
               entity(as[Array[Byte]]) { 
                 reqBody => {
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
            }  else if( toknRoute(0).equalsIgnoreCase("Activate") || 
                        toknRoute(0).equalsIgnoreCase("Deactivate")) {
              entity(as[String]) { reqBody =>
                requestContext => processPutRequest(toknRoute(0),toknRoute(1).toLowerCase,toknRoute(2), requestContext) 
              }
            } else {
              entity(as[String]) { reqBody =>  
                {
                  if (toknRoute.size == 1) {      
                    requestContext => processPutRequest (toknRoute(0),reqBody,requestContext)
                  } else {
                    requestContext => processPutRequest (toknRoute(1).toLowerCase,reqBody,requestContext)
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
            if (toknRoute.size == 1) {
              entity(as[String]) { 
                reqBody => {
                  requestContext => processPostRequest (toknRoute(0),reqBody,requestContext) 
                }
              }
            } else {
              requestContext => println ("UNKNOWN REQUEST - " + str)
            }
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
            requestContext => processDeleteRequest(toknRoute(0), toknRoute(1).toLowerCase, requestContext)   
          }
        }
      }
    }
  }
  
  /**
   * Modify Existing objects in the Metadata 
   */
  private def processPutRequest(objtype:String, body: String, rContext: RequestContext):Unit = {
    if (objtype.equalsIgnoreCase("Container")) {
        val updateContainerDefsService = actorRefFactory.actorOf(Props(new UpdateContainerService(rContext)))
        updateContainerDefsService ! UpdateContainerService.Process(body)
    } else if (objtype.equalsIgnoreCase("Model")) {
        val updateModelService:ActorRef = actorRefFactory.actorOf(Props(new UpdateModelService(rContext)))
        updateModelService ! UpdateModelService.Process(body)
    } else if (objtype.equalsIgnoreCase("Message")) {
        val updateMessageDefsService = actorRefFactory.actorOf(Props(new UpdateMessageService(rContext)))
        updateMessageDefsService ! UpdateMessageService.Process(body,"JSON")
    } else if (objtype.equalsIgnoreCase("Type")) {
        val updateTypeDefsService = actorRefFactory.actorOf(Props(new UpdateTypeService(rContext)))
        updateTypeDefsService ! AddTypeService.Process(body,"JSON")
    } else if (objtype.equalsIgnoreCase("Concept")) {
        val updateConceptDefsService = actorRefFactory.actorOf(Props(new UpdateConceptService(rContext)))
        updateConceptDefsService ! UpdateConceptService.Process(body)
    } else if (objtype.equalsIgnoreCase("Function")) {
        val updateFunctionDefsService = actorRefFactory.actorOf(Props(new UpdateFunctionService(rContext)))
        updateFunctionDefsService ! UpdateFunctionService.Process(body)
    } else if (objtype.equalsIgnoreCase("RemoveConfig")) {
        val removeConfigService = actorRefFactory.actorOf(Props(new RemoveEngineConfigService(rContext)))
        removeConfigService ! RemoveEngineConfigService.Process(body)
    } else if (objtype.equalsIgnoreCase("UploadConfig")) {
        val uploadConfigService = actorRefFactory.actorOf(Props(new UploadEngineConfigService(rContext)))
        uploadConfigService ! UploadEngineConfigService.Process(body)
    } else {
      rContext.complete((new ApiResult(-1, "Unknown URL", "PUT for api/"+ objtype)).toString)
    }
  }
  
    /**
   * Modify Existing objects in the Metadata 
   */
  private def processPutRequest(action: String, objtype:String, objKey: String, rContext: RequestContext):Unit = {
    if (action.equalsIgnoreCase("Activate")) {
        val activateObjectsService = actorRefFactory.actorOf(Props(new ActivateObjectsService(rContext)))
        activateObjectsService ! ActivateObjectsService.Process(createGetArg(objKey,objtype))
    } else if (action.equalsIgnoreCase("Deactivate")) {
       val deactivateObjectsService = actorRefFactory.actorOf(Props(new DeactivateObjectsService(rContext)))
       deactivateObjectsService ! DeactivateObjectsService.Process(createGetArg(objKey,objtype))
    } else {
      rContext.complete((new ApiResult(-1, "Unknown URL", "PUT for api/"+ action + "/" + objtype + "/" + objKey)).toString)     
    } 
  }
  
  /**
   * Create new Objects in the Metadata
   */
  private def processPostRequest(objtype:String, body: String, rContext: RequestContext):Unit = {
   if (objtype.equalsIgnoreCase("Container")) {
        val addContainerDefsService = actorRefFactory.actorOf(Props(new AddContainerService(rContext)))
        addContainerDefsService ! AddContainerService.Process(body)
    } else if (objtype.equalsIgnoreCase("Model")) {
        val addModelService:ActorRef = actorRefFactory.actorOf(Props(new AddModelService(rContext)))
        addModelService ! AddModelService.Process(body)
    } else if (objtype.equalsIgnoreCase("Message")) {
        val addMessageDefsService = actorRefFactory.actorOf(Props(new AddMessageService(rContext)))
        addMessageDefsService ! AddMessageService.Process(body)
    } else if (objtype.equalsIgnoreCase("Type")) {
        val addTypeDefsService = actorRefFactory.actorOf(Props(new AddTypeService(rContext)))
        addTypeDefsService ! AddTypeService.Process(body,"JSON")
    } else if (objtype.equalsIgnoreCase("Concept")) {
        val addConceptDefsService = actorRefFactory.actorOf(Props(new AddConceptService(rContext)))
        addConceptDefsService ! AddConceptService.Process(body,"JSON")
    } else if (objtype.equalsIgnoreCase("Function")) {
        val addFunctionDefsService = actorRefFactory.actorOf(Props(new AddFunctionService(rContext)))
        addFunctionDefsService ! AddFunctionService.Process(body,"JSON")
    } else {
      rContext.complete((new ApiResult(-1, "Unknown URL", "POST for api/"+ objtype)).toString)     
    } 
  }
  
  /**
   * 
   */
  private def processGetKeysRequest(objtype:String,rContext: RequestContext): Unit = {
    if (objtype.equalsIgnoreCase("Container") || objtype.equalsIgnoreCase("Model") || objtype.equalsIgnoreCase("Message") || objtype.equalsIgnoreCase("Function") ||
        objtype.equalsIgnoreCase("Concept") || objtype.equalsIgnoreCase("Type")) {
      val allObjectKeysService = actorRefFactory.actorOf(Props(new GetAllObjectKeysService(rContext)))
      allObjectKeysService ! GetAllObjectKeysService.Process(objtype)  
    } else {
      rContext.complete((new ApiResult(-1, "Unknown URL", "GET for api/keys"+ objtype)).toString) 
    }
  }
  
  /**
   *  
   */
  private def processGetObjectRequest(objtype: String, objKey: String, rContext: RequestContext): Unit = {
    if (objtype.equalsIgnoreCase("Config")) {
        val allObjectsService = actorRefFactory.actorOf(Props(new GetConfigObjectsService(rContext)))
        allObjectsService ! GetConfigObjectsService.Process(objKey) 
    } else if (objtype.equalsIgnoreCase("Container") || objtype.equalsIgnoreCase("Model") || objtype.equalsIgnoreCase("Message") || 
               objtype.equalsIgnoreCase("Function") || objtype.equalsIgnoreCase("Concept") || objtype.equalsIgnoreCase("Type"))  {
        val getObjectsService = actorRefFactory.actorOf(Props(new GetObjectsService(rContext)))
        getObjectsService ! GetObjectsService.Process(createGetArg(objKey,objtype))  
    } else {
        rContext.complete((new ApiResult(-1, "Unknown URL", "GET for api/"+ objtype)).toString) 
    }
  }
  
  /**
   * 
   */
  private def processDeleteRequest(objtype: String, objKey: String, rContext: RequestContext):Unit = {
    if (objtype.equalsIgnoreCase("Container") || objtype.equalsIgnoreCase("Model") || objtype.equalsIgnoreCase("Message") ||
        objtype.equalsIgnoreCase("Function") || objtype.equalsIgnoreCase("Concept") || objtype.equalsIgnoreCase("Type")) {
      val removeObjectsService = actorRefFactory.actorOf(Props(new RemoveObjectsService(rContext)))
      removeObjectsService ! RemoveObjectsService.Process(createGetArg(objKey,objtype))
    } else if (objtype.equalsIgnoreCase("Config")) {
      val removeConfigService = actorRefFactory.actorOf(Props(new RemoveEngineConfigService(rContext)))
      removeConfigService ! RemoveEngineConfigService.Process(createGetArg(objKey,objtype))   
    } else {
      rContext.complete((new ApiResult(-1, "Unknown URL", "DELETE for api/"+ objtype + "/" + objKey)).toString)     
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
