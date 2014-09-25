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

  // we need an ActorSystem to host our application in
  implicit val system = ActorSystem("metadata-api-service")
  val log = Logging(system, getClass)

  val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)
  logger.setLevel(Level.TRACE);
  MetadataAPIImpl.SetLoggerLevel(Level.TRACE)
  MdMgr.GetMdMgr.SetLoggerLevel(Level.INFO)

  try{
    MdMgr.GetMdMgr.truncate
    val mdLoader = new com.ligadata.olep.metadataload.MetadataLoad (MdMgr.mdMgr, logger,"","","","")
    mdLoader.initialize
    MetadataAPIImpl.readMetadataAPIConfig
    MetadataAPIImpl.OpenDbStore(MetadataAPIImpl.GetMetadataAPIConfig.getProperty("DATABASE"))
    MetadataAPIImpl.LoadObjectsIntoCache

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
      MetadataAPIImpl.CloseDbStore
      e.printStackTrace()
    }
  }
}
