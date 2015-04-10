package com.ligadata.metadataapiservice

import akka.actor.{ActorSystem, Props}
import akka.actor.ActorDSL._
import akka.event.Logging
import akka.io.IO
import akka.io.Tcp._
import spray.can.Http
import org.json4s.jackson.JsonMethods._
import com.ligadata.olep.metadata.ObjType._
import com.ligadata.olep.metadata._
import com.ligadata.olep.metadataload.MetadataLoad
import com.ligadata.MetadataAPI._
import org.apache.log4j._
import com.ligadata.Utils._
import scala.util.control.Breaks._

class APIService extends LigadataSSLConfiguration with Runnable{

  private type OptionMap = Map[Symbol, Any]
  var inArgs: Array[String] = null

  // we need an ActorSystem to host our application in
  implicit val system = ActorSystem("metadata-api-service")
  val log = Logging(system, getClass)

  val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)
  //logger.setLevel(Level.TRACE);
 // MetadataAPIImpl.SetLoggerLevel(Level.TRACE)
  //MdMgr.GetMdMgr.SetLoggerLevel(Level.INFO)
  var databaseOpen = false
  
  /**
   * 
   */
  def this(args: Array[String]) = {
    this
    inArgs = args  
  }

  /**
   * 
   */
  def run() {
    StartService(inArgs) 
  }
  
  
  private def PrintUsage(): Unit = {
    logger.warn("    --config <configfilename>")
  }

  private def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    def isSwitch(s: String) = (s(0) == '-')
    list match {
      case Nil => map
      case "--config" :: value :: tail =>
        nextOption(map ++ Map('config -> value), tail)
      case option :: tail => {
        logger.error("Unknown option " + option)
        sys.exit(1)
      }
    }
  }

  private def Shutdown(exitCode: Int): Unit = {
    APIInit.Shutdown(0)
    //System.exit(0)
  }

  private def StartService(args: Array[String]) : Unit = {
    try{
      var configFile = System.getenv("HOME") + "/MetadataAPIConfig.properties"
      if (args.length == 0) {
        logger.error("Config File defaults to " + configFile)
        logger.error("One Could optionally pass a config file as a command line argument:  --config myConfig.properties")
        logger.error("The config file supplied is a complete path name of a  json file similar to one in github/RTD/trunk/MetadataAPI/src/main/resources/MetadataAPIConfig.properties")
      } else {
        val options = nextOption(Map(), args.toList)
        val cfgfile = options.getOrElse('config, null)
        if (cfgfile == null) {
          logger.error("Need configuration file as parameter")
          throw new MissingArgumentException("Usage: configFile  supplied as --config myConfig.properties")
        }
        configFile = cfgfile.asInstanceOf[String]
      }

      val (loadConfigs, failStr) = Utils.loadConfiguration(configFile.toString, true)
      if (failStr != null && failStr.size > 0) {
        logger.error(failStr)
        Shutdown(1)
        return
      }
      if (loadConfigs == null) {
        Shutdown(1)
        return
      }

      APIInit.SetConfigFile(configFile.toString)

      // Read properties file and Open db connection
      MetadataAPIImpl.InitMdMgrFromBootStrap(configFile)
      // APIInit deals with shutdown activity and it needs to know
      // that database connections were successfully made
      APIInit.SetDbOpen

      logger.debug("API Properties => " + MetadataAPIImpl.GetMetadataAPIConfig)

      // Identify the host and port the service is listening on
      val serviceHost = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("SERVICE_HOST")
      val servicePort = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("SERVICE_PORT").toInt

      // create and start our service actor
      val callbackActor = actor(new Act {
        become {
          case b @ Bound(connection) => logger.debug(b.toString)
          case cf @ CommandFailed(command) => logger.error(cf.toString)
          case all => logger.debug("ApiService Received a message from Akka.IO: " + all.toString)
        }
      })
      val service = system.actorOf(Props[MetadataAPIServiceActor], "metadata-api-service")

      // start a new HTTP server on a specified port with our service actor as the handler
      IO(Http).tell(Http.Bind(service, serviceHost, servicePort), callbackActor)

      logger.info("MetadataAPIService started, listening on (%s,%s)".format(serviceHost,servicePort))

      sys.addShutdownHook({
        logger.debug("ShutdownHook called")
        Shutdown(0)
      })

      Thread.sleep(365*24*60*60*1000L)
    } catch {
      case e: InterruptedException => {
        logger.debug("Unexpected Interrupt")
      }
      case e: Exception => {
        e.printStackTrace()
      }
    } finally {
      Shutdown(0)
    }
  }
}
 
object APIService {

  def main(args: Array[String]): Unit = {
    val mgr = new APIService
    mgr.StartService(args) 
  }
  
  
  /**
   * extractNameFromJson - applies to a simple Fatafat object
   */
  def extractNameFromJson (jsonObj: String, objType: String): String = {
    println (objType +" ->"+ jsonObj)
    var inParm: Map[String,Any] = parse(jsonObj).values.asInstanceOf[Map[String,Any]]   
    var vals: Map[String,String] = inParm.getOrElse(objType,null).asInstanceOf[Map[String,String]]
    if (vals == null) {
      return "unknown "+ objType
    }
    return vals.getOrElse("NameSpace","system")+"."+vals.getOrElse("Name","")+"."+vals.getOrElse("Version","-1")
  }

  
  /**
   * extractNameFromPMML - pull the Application name="xxx" version="xxx.xx.xx" from the PMML doc and construct
   *                       a name  string from it
   */
  def extractNameFromPMML (pmmlObj: String): String = {
    var firstOccurence: String = "unknownModel"
    val pattern = """Application[ ]*name="([^ ]*)"[ ]*version="([^ ]*)"""".r
    val allMatches = pattern.findAllMatchIn(pmmlObj)
    allMatches.foreach( m => {
      if (firstOccurence.equalsIgnoreCase("unknownModel")) {
      firstOccurence = (m.group(1)+"."+m.group(2))
      }
    })
    return firstOccurence
  }
}
