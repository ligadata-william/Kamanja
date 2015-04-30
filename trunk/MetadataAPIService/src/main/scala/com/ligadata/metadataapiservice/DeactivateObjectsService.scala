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
import com.ligadata.fatafat.metadata._
import scala.util.control._

import org.apache.log4j._

object DeactivateObjectsService {
	case class Process(apiArgListJson: String)
}

class DeactivateObjectsService(requestContext: RequestContext, userid:Option[String], password:Option[String], cert:Option[String]) extends Actor {

  import DeactivateObjectsService._
  
  implicit val system = context.system
  import system.dispatcher
  val log = Logging(system, getClass)
  
  
  val loggerName = this.getClass.getName
  val logger = Logger.getLogger(loggerName)
  //logger.setLevel(Level.TRACE);

  val APIName = "DeactivateObjects"

  def receive = {
    case Process(apiArgListJson: String) =>
      process(apiArgListJson)
      context.stop(self)
  }

  def DeactivateObjectDef(nameSpace: String, name: String, version: String, objectType: String ): String = {

    objectType match {
      case "model" => {
 	      MetadataAPIImpl.DeactivateModel(nameSpace,name,version.toLong).toString
      }
      case _ => {
	      new ApiResult(ErrorCodeConstants.Failure, APIName, null, "Deactivate/Activate on " + objectType + " is not supported yet").toString 
      }
    }
  }

  /**
   * process - perform the deactivation process on the list of objects in the parameter var.
   */
  def process(apiArgListJson: String): Unit = {
    
    logger.debug(APIName + ":" + apiArgListJson)
    
    var activatedType = "unkown"
    var nameSpace = "str"
    var version = "-1"
    var name = ""
    var authDone = false
    val apiArgList = JsonSerializer.parseApiArgList(apiArgListJson)
    val arguments = apiArgList.ArgList
    var resultStr:String = ""
    var objectList: List[String]  = List[String]()

    if ( arguments.length > 0 ){
      var loop = new Breaks
      loop.breakable{
	      arguments.foreach(arg => {
          
          // Extract the object name from the ARGS
          if( arg.NameSpace != null ){
            nameSpace = arg.NameSpace
          }
          if( arg.Version != null ){
            version = arg.Version
          }
          if( arg.Name != null ){
            name = arg.Name
          }
          
           // Do it here so that we know which OBJECT is being activated for the Audit purposes.
          if ((!MetadataAPIImpl.checkAuth(userid,password,cert, MetadataAPIImpl.getPrivilegeName("deactivate","model"))) && !authDone) {
            MetadataAPIImpl.logAuditRec(userid,Some(AuditConstants.WRITE),AuditConstants.DEACTIVATEOBJECT,arg.ObjectType,AuditConstants.FAIL,"unknown",nameSpace+"."+name+"."+version)
            requestContext.complete(new ApiResult(-1, APIName, null, "Error:UPDATE not allowed for this user").toString )
            return
          }
          
          // Make sure that we do not perform another AUTH check, and add this object name to the list of objects processed in this call.
          authDone = true
          objectList :::= List(nameSpace+"."+name+"."+version)
          
	        if(arg.ObjectType == null ){
	          resultStr = new ApiResult(ErrorCodeConstants.Failure, APIName, null, "Error: The value of object type can't be null").toString
	          loop.break
	        }
	        if(arg.Name == null ){
	          resultStr = new ApiResult(ErrorCodeConstants.Failure, APIName, null, "Error: The value of object name can't be null").toString
	          loop.break
	        }
	        else {
	         resultStr = resultStr + DeactivateObjectDef(nameSpace,name,version, arg.ObjectType)
           activatedType = arg.ObjectType
	        }
	      })
      }
    }
    else{
      resultStr = new ApiResult(ErrorCodeConstants.Failure, APIName, null, "No arguments passed to the API, nothing much to do").toString 
    }
    MetadataAPIImpl.logAuditRec(userid,Some(AuditConstants.WRITE),AuditConstants.DEACTIVATEOBJECT, activatedType, AuditConstants.SUCCESS,"",objectList.mkString(","))
    requestContext.complete(resultStr)
  }
}
