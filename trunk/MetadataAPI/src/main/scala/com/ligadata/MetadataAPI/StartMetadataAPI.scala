package scala.com.ligadata.MetadataAPI

import java.util.logging.Logger

import com.ligadata.MetadataAPI.{TestMetadataAPI, MetadataAPIImpl}
import main.scala.com.ligadata.MetadataAPI.Utility.{ContainerService, Action, ModelService, MessageService}


/**
 * Created by dhaval Kolapkar on 7/24/15.
 */

object StartMetadataAPI {
  var response = ""
  val defaultConfig = sys.env("HOME") + "/MetadataAPIConfig.properties"
  val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)
  var action = ""
  var location = ""
  var config = ""

  def main(args: Array[String]) {
    val arglist = args.toList
    if (args.length == 0) {
      config = defaultConfig
      MetadataAPIImpl.InitMdMgrFromBootStrap(config)
      TestMetadataAPI.StartTest
    }
    else if (args(0) == " config") {
      config = defaultConfig
    }
    else {
      for (arg <- arglist) {
        if (arg.endsWith(".json") || arg.endsWith(".xml")) {
          location = arg
        } else if (arg.endsWith(".properties")) {
          config = arg
        } else {
          action = arg
        }
      }

      //add configuration
      if (config == "")
        config = defaultConfig
      MetadataAPIImpl.InitMdMgrFromBootStrap(config)
      response = route(Action.withName(action), location)
    }
    println("Result: " + response)
    response
  }

  def route(action: Action.Value, input: String): String = {
    var response = ""
    try {
      action match {
        case Action.ADDMESSAGE => response = MessageService.addMessage(input)
        case Action.UPDATEMESSAGE => response = MessageService.updateMessage(input)
        case Action.REMOVEMESSAGE => response = MessageService.removeMessage
        case Action.GETALLMESSAGES => response = MessageService.getAllMessages
        case Action.ADDOUTPUTMESSAGE => response = MessageService.addOutputMessage(input)
        case Action.UPDATEOUTPUTMESSAGE => response =MessageService.updateOutputMessage(input)
        case Action.REMOVEOUTPUTMESSAGE => response =MessageService.removeOutputMessage
        case Action.GETALLOUTPUTMESSAGES => response =MessageService.getAllOutputMessages
        case Action.ADDMODELPMMML => response = ModelService.addModelPmml(input)
        case Action.ADDMODELSCALA => response = ModelService.addModelScala(input)
        case Action.ADDMODELJAVA => response = ModelService.addModelJava(input)
        case Action.REMOVEMODEL => response = ModelService.removeModel
        case Action.ACTIVATEMODEL => response = ModelService.activateModel
        case Action.DEACTIVATEMODEL => response = ModelService.deactivateModel
        case Action.UPDATEMODEL => response = ModelService.updateModel(input)
        case Action.GETALLMODELS => response = ModelService.getAllModels
        case Action.GETMODEL => response = ModelService.getModel
        case Action.ADDCONTAINER => response = ContainerService.addContainer(input)
        case Action.UPDATECONTAINER => response = ContainerService.updateContainer(input)
        case Action.GETCONTAINER => response = ContainerService.getContainer
        case Action.GETALLCONTAINERS => response = ContainerService.getAllContainers
        case Action.REMOVECONTAINER => response = ContainerService.removeContainer

        case _ => response = "Unexpected action!"
      }
    }
    catch {
      case e: Exception => response = e.getMessage
    }
    response
  }
}
