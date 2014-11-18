package com.ligadata.metadataapiservice

import akka.actor.{ActorSystem, Props}
import akka.actor.ActorDSL._
import akka.event.Logging
import akka.io.IO
import akka.io.Tcp._
import spray.can.Http

import com.ligadata.olep.metadata.ObjType._
import com.ligadata.olep.metadata._
import com.ligadata.olep.metadataload.MetadataLoad
import com.ligadata.MetadataAPI._
import org.apache.log4j._

object Boot extends App {

  private type OptionMap = Map[Symbol, Any]

  // we need an ActorSystem to host our application in
  implicit val system = ActorSystem("metadata-api-service")
  val log = Logging(system, getClass)

  val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)
  logger.setLevel(Level.TRACE);
  MetadataAPIImpl.SetLoggerLevel(Level.TRACE)
  MdMgr.GetMdMgr.SetLoggerLevel(Level.INFO)

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

  var databaseOpen = false
  try{
    var jsonConfigFile = System.getenv("HOME") + "/MetadataAPIConfig.json"
    if (args.length == 0) {
      logger.error("Config File defaults to " + jsonConfigFile)
      logger.error("One Could optionally pass a config file as a command line argument:  --config myConfig.json")
      logger.error("The config file supplied is a complete path name of a  json file similar to one in github/RTD/trunk/MetadataAPI/src/main/resources/MetadataAPIConfig.json")
    }
    else{
      val options = nextOption(Map(), args.toList)
      val cfgfile = options.getOrElse('config, null)
      if (cfgfile == null) {
	logger.error("Need configuration file as parameter")
	throw new MissingArgumentException("Usage: configFile  supplied as --config myConfig.json")
      }
      jsonConfigFile = cfgfile.asInstanceOf[String]
    }
    MetadataAPIImpl.InitMdMgrFromBootStrap(jsonConfigFile)
    databaseOpen = true

    val callbackActor = actor(new Act {
      become {
	case b @ Bound(connection) => log.info(b.toString)
	case cf @ CommandFailed(command) => log.error(cf.toString)
	case all => logger.debug("ApiService Received a message from Akka.IO: " + all.toString)
      }
    })

    // create and start our service actor
    val service = system.actorOf(Props[MetadataAPIServiceActor], "metadata-api-service")

    // start a new HTTP server on port 8080 with our service actor as the handler
    IO(Http).tell(Http.Bind(service, "localhost", 8080), callbackActor)

  } catch {
     case e: Exception => {
      e.printStackTrace()
    }
  } finally {
    if( databaseOpen ){
      MetadataAPIImpl.CloseDbStore
    }
  }
}
