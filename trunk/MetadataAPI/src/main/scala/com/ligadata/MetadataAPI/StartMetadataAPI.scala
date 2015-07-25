package scala.com.ligadata.MetadataAPI

import java.io.File
import java.nio.file.{Paths, Files}
import java.util.logging.Logger

import com.ligadata.MetadataAPI.MetadataAPIImpl

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
 * Created by dhaval Kolapkar on 7/24/15.
 */

object StartMetadataAPI {
  private val userid: Option[String] = Some("metadataapi")
  val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)

  def main (args: Array[String]) {
    if(args.length==0){
      println("Mininum Usage: --config /install-path/MetadataAPIConfig.properties")
      sys.exit(1)
    }
    val arglist = args.toList
    type OptionMap = Map[Symbol, Any]
    def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
      list match {
        case Nil => map
        case "--action" :: value :: tail =>
          nextOption(map ++ Map('action -> value.toString), tail)
        case "--input" :: value :: tail =>
          nextOption(map ++ Map('input -> value.toString), tail)
        case "--config" :: value :: tail =>
          nextOption(map ++ Map('config -> value.toString), tail)
        case option :: tail => println("Unknown option "+option)
          sys.exit(1)
      }
    }
    val options: OptionMap = nextOption(Map(),arglist)
    uploadConfiguration(options.getOrElse('config,None).toString)
   // val input=scala.io.Source.fromFile(options.getOrElse('input, None).toString).mkString
   val input=options.getOrElse('input, None)
    val action=options.getOrElse('action, None)
    route(action,input)
  }

  def route(action: Any, input: Any): Unit ={
    if(action=="addMessage"){
      println("Adding message")
      addMessage(input)
    }
    else if(action==None){
      //give all options to perform action

    }
  }

  def addMessage(input: Any): Unit ={
    var msgFileDir:String=""
    val gitMsgFileDir="/Fatafat/trunk/MetadataAPI/src/test/SampleTestFiles/Messages"
    println("Input is: "+input)
    if(input==None){
      //get the messages location from the config file. If error get the location from github
      msgFileDir=MetadataAPIImpl.GetMetadataAPIConfig.getProperty("MESSAGE_FILES_DIR")
      msgFileDir match {
        case null =>{
          msgFileDir=MetadataAPIImpl.GetMetadataAPIConfig.getProperty("GIT_ROOT") + gitMsgFileDir
        }
        case option => //do nothing
      }
      //verify the directory where messages can be present
      IsValidDir(msgFileDir) match {
        case true => {
          //get all files with json extension
          val messages = new java.io.File(msgFileDir).listFiles.filter(_.getName.endsWith(".json"))
         messages.length match {
           case 0 =>{
             println("Messages not found at " + msgFileDir)
             return
           }
           case option => {
             println("\nPick a Message Definition file(s) from below choices\n")
             for(message <- messages){
               println(message)
             }
           }
         }
        }
        case false => {
        println("Message directory is invalid.")
          return
        }
      }
    }else{
      println("Path provided. Added msg")
    }
  }

  //print all files in that location with json extension
  //give option to select one
  //if valid option process it
  def IsValidDir(dirName: String): Boolean = {
    val iFile = new File(dirName)
    if (!iFile.exists) {
      println("The File Path (" + dirName + ") is not found: ")
      false
    } else if (!iFile.isDirectory) {
      println("The File Path (" + dirName + ") is not a directory: ")
      false
    } else
      true
  }

  //Verify and upload the configuration
  def uploadConfiguration(config: String): Unit ={
    if(config==None){
      //throw exception
      println("Mininum Usage: --config /install-path/MetadataAPIConfig.properties")
      sys.exit(1)
    }else{
      //upload cluster metadata config
      MetadataAPIImpl.InitMdMgrFromBootStrap(config)
    }
  }

}