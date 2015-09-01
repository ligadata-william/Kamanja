package scala.com.ligadata.MetadataAPI

import java.io.File
import java.util.logging.Logger

import com.ligadata.MetadataAPI.{TestMetadataAPI, MetadataAPIImpl}
import com.ligadata.MetadataAPI.Utility._
import scala.io.Source



/**
 * Created by dhaval Kolapkar on 7/24/15.
 */

object StartMetadataAPI {

  var response = ""
  //get default config
  val defaultConfig = sys.env("KAMANJA_HOME") + "/config/MetadataAPIConfig.properties"
  val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)
  var action = ""
  var location = ""
  var config = ""
  val WITHDEP = "dependsOn"
  val REMOVE = "remove"
  var expectDep = false
  var expectRemoveParm = false
  var depName: String = ""
  var parmName: String = ""

  def main(args: Array[String]) {
    try {
      var argsUntilParm = 2
      args.foreach( arg => {
         if (arg.endsWith(".json") || arg.endsWith(".xml") || arg.endsWith(".scala") || arg.endsWith(".java") || arg.endsWith(".jar")) {
          location = arg
        } else if (arg.endsWith(".properties")) {
          config = arg
        } else {
          if (arg.equalsIgnoreCase(WITHDEP)) {
            expectDep = true
          }
          else if (expectDep) {
            depName = arg
            expectDep = false
          } else {
            if (arg.equalsIgnoreCase(REMOVE)) {
              expectRemoveParm = true
            }

            if (expectRemoveParm) {
              argsUntilParm = argsUntilParm - 1
            }

            if (argsUntilParm < 0)
              depName = arg
            else
              action += arg
          }
        }
      })

      //add configuration
      if (config == "") {
        println("Using default configuration " + defaultConfig)
        config = defaultConfig
      }

      MetadataAPIImpl.InitMdMgrFromBootStrap(config, false)
      if (action == "")
        TestMetadataAPI.StartTest
      else {
        response = route(Action.withName(action.trim), location, depName)
        println("Result: " + response)
      }
    }
    catch {
      case nosuchelement: NoSuchElementException => {
        println(action+ " is an unrecognized command. \n USAGE: kamanja <action> <optional input> \n e.g. kamanja add message $HOME/msg.json")
        //response = action+ " is an unrecognized command. \n USAGE: kamanja <action> <optional input> \n e.g. kamanja add message $HOME/msg.json"
      }
      case e: Throwable => e.getStackTrace.toString
    } finally {
      MetadataAPIImpl.shutdown
    }
  }


  def route(action: Action.Value, input: String, param: String = ""): String = {
    var response = ""
    try {
      action match {
        //message management
        case Action.ADDMESSAGE => response = MessageService.addMessage(input)
        case Action.UPDATEMESSAGE => response = MessageService.updateMessage(input)

        case Action.REMOVEMESSAGE => {
          if (param.length == 0)
            response = MessageService.removeMessage()
          else
            response = MessageService.removeMessage(param)
        }


        case Action.GETALLMESSAGES => response = MessageService.getAllMessages
        case Action.GETMESSAGE => response =MessageService.getMessage
        //output message management
        case Action.ADDOUTPUTMESSAGE => response = MessageService.addOutputMessage(input)
        case Action.UPDATEOUTPUTMESSAGE => response =MessageService.updateOutputMessage(input)
        case Action.REMOVEOUTPUTMESSAGE => response =MessageService.removeOutputMessage
        case Action.GETALLOUTPUTMESSAGES => response =MessageService.getAllOutputMessages
        //model management
        case Action.ADDMODELPMMML => response = ModelService.addModelPmml(input)

        case Action.ADDMODELSCALA => {
          if (param.length == 0)
            response = ModelService.addModelScala(input)
          else
            response = ModelService.addModelScala(input, param)
        }

        case Action.ADDMODELJAVA => {
          if (param.length == 0)
            response = ModelService.addModelJava(input)
          else
            response = ModelService.addModelJava(input, param)
        }

        case Action.REMOVEMODEL => {
          if (param.length == 0)
            response = ModelService.removeModel()
          else
            response = ModelService.removeModel(param)
        }

        case Action.ACTIVATEMODEL => response = ModelService.activateModel
        case Action.DEACTIVATEMODEL => response = ModelService.deactivateModel
        case Action.UPDATEMODEL => response = ModelService.updateModel(input)
        case Action.GETALLMODELS => response = ModelService.getAllModels
        case Action.GETMODEL => response = ModelService.getModel
        //container management
        case Action.ADDCONTAINER => response = ContainerService.addContainer(input)
        case Action.UPDATECONTAINER => response = ContainerService.updateContainer(input)
        case Action.GETCONTAINER => response = ContainerService.getContainer
        case Action.GETALLCONTAINERS => response = ContainerService.getAllContainers

        case Action.REMOVECONTAINER => {
          if (param.length == 0)
            response = ContainerService.removeContainer()
          else
            response = ContainerService.removeContainer(param)
        }
        //Type management
        case Action.ADDTYPE => response = TypeService.addType(input)
        case Action.GETTYPE => response = TypeService.getType
        case Action.GETALLTYPES => response = TypeService.getAllTypes
        case Action.REMOVETYPE => response = TypeService.removeType
        case Action.LOADTYPESFROMAFILE=> response = TypeService.loadTypesFromAFile
        case Action.DUMPALLTYPESBYOBJTYPEASJSON => response = TypeService.dumpAllTypesByObjTypeAsJson
        //function management
        case Action.ADDFUNCTION => response = FunctionService.addFunction(input)
        case Action.GETFUNCTION => response = FunctionService.getFunction
        case Action.REMOVEFUNCTION => response = FunctionService.removeFunction
        case Action.UPDATEFUNCTION => response = FunctionService.updateFunction(input)
        case Action.LOADFUNCTIONSFROMAFILE => response = FunctionService.loadFunctionsFromAFile
        case Action.DUMPALLFUNCTIONSASJSON => response = FunctionService.dumpAllFunctionsAsJson
        //config
        case Action.UPLOADENGINECONFIG => response = ConfigService.uploadEngineConfig(input)
        case Action.UPLOADCOMPILECONFIG => response = ConfigService.uploadCompileConfig(input)
        case Action.DUMPALLCFGOBJECTS => response = ConfigService.dumpAllCfgObjects
        case Action.REMOVEENGINECONFIG => response = ConfigService.removeEngineConfig
        //service
        case Action.ADDCONCEPT => response = ConceptService.addConcept(input)
        case Action.REMOVECONCEPT => response =ConceptService.removeConcept
        case Action.LOADCONCEPTSFROMAFILE => response =ConceptService.loadConceptsFromAFile
        case Action.DUMPALLCONCEPTSASJSON => response =ConceptService.dumpAllConceptsAsJson
        //jar
        case Action.UPLOADJAR=>response = JarService.uploadJar(input)
        //dumps
        case Action.DUMPMETADATA=>response =DumpService.dumpMetadata
        case Action.DUMPALLNODES=>response =DumpService.dumpAllNodes
        case Action.DUMPALLCLUSTERS=>response =DumpService.dumpAllClusters
        case Action.DUMPALLCLUSTERCFGS=>response =DumpService.dumpAllClusterCfgs
        case Action.DUMPALLADAPTERS=>response =DumpService.dumpAllAdapters
        case _ => {
          println("Unexpected action!")
          sys.exit(1)
        }
      }
    }
    catch {
      case e: Exception => response = e.getStackTraceString
    }
    response
  }
}
